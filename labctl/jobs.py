# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""One probe Job of `lab check`: start it, wait for it, read its logs, never leave it stuck.

The add-on tiers prove themselves by running one Kubernetes Job each (the spark
smoke run, the streaming probe). This module holds what those Jobs share:

- a Job that cannot start (an image missing from the node, a bad config) fails
  at once and is deleted, so it neither holds the node's memory nor blocks the
  next check;
- a pull error fails only after PULL_GRACE_S of kubelet retries (a mirror hiccup
  clears);
- an interrupted or timed-out run deletes its Job too;
- a finished Job stays until its ttlSecondsAfterFinished, for its logs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from labctl import kube

# Every probe Job of `lab check` carries this label (its value names the tier); its own record judges it.
CHECK_LABEL = "fluid-fabric-lab/check"
# Waiting reasons that do not clear until the operator acts: report them at once.
STUCK = {"ErrImageNeverPull", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError"}
# Pull failures kubelet retries with back-off (10 s, doubling to 5 min). A mirror
# hiccup clears, so these fail the run only when they last PULL_GRACE_S.
PULL_RETRY = {"ErrImagePull", "ImagePullBackOff"}
PULL_GRACE_S = 300


class JobError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


@dataclass(frozen=True)
class Probe:
    """Where a tier's probe Jobs live and how they are told apart."""

    namespace: str
    selector: str                 # label selector of this tier's probe Jobs, e.g. `fluid-fabric-lab/check=spark`
    what: str                     # how a person calls it, e.g. "spark smoke Job"
    containers: Sequence[str]     # whose logs to read, in order
    deadline_s: int               # the Job's activeDeadlineSeconds


def _condition(status: dict, kind: str) -> bool:
    return any(c.get("type") == kind and c.get("status") == "True" for c in status.get("conditions") or [])


def finished(status: dict) -> str:
    """The Job's outcome once its controller has decided ("succeeded" or "failed"); "" while unfinished."""
    if status.get("succeeded") or _condition(status, "Complete"):
        return "succeeded"
    if status.get("failed") or _condition(status, "Failed"):
        return "failed"
    return ""


def blocked(container: dict) -> Optional[tuple]:
    """(reason, description) when a container status waits in STUCK or PULL_RETRY."""
    waiting = (container.get("state") or {}).get("waiting") or {}
    reason = waiting.get("reason")
    if reason not in STUCK and reason not in PULL_RETRY:
        return None
    return reason, f"container {container.get('name')}: {reason}: {str(waiting.get('message') or '')[:300]}"


def waiting(k: kube.Kube, namespace: str, name: str) -> Optional[tuple]:
    """The first blocked container of the Job's pod, a STUCK one before a PULL_RETRY one."""
    pods = (k.get("pods", namespace=namespace, selector=f"job-name={name}") or {}).get("items") or []
    statuses = [c for pod in pods for key in ("initContainerStatuses", "containerStatuses")
                for c in (pod.get("status") or {}).get(key) or []]
    found = [b for b in map(blocked, statuses) if b]
    return next((b for b in found if b[0] in STUCK), found[0] if found else None)


def clear_leftovers(k: kube.Kube, probe: Probe) -> None:
    """Refuse while another probe Job of the tier is unfinished; delete one that can never start.

    A just-created Job has no status yet, so only a Job the controller has finished
    counts as finished. Finished Jobs stay until ttlSecondsAfterFinished: deleting one
    here could take it from a concurrent check before that check has read its logs.
    """
    jobs = (k.get("jobs", namespace=probe.namespace, selector=probe.selector) or {}).get("items") or []
    for j in jobs:
        if finished(j.get("status") or {}):
            continue
        name = j["metadata"]["name"]
        stuck = waiting(k, probe.namespace, name)
        if not (stuck and stuck[0] in STUCK):
            raise JobError(f"another {probe.what} is unfinished ({name})",
                           f"Wait for it, or delete it with `kubectl -n {probe.namespace} delete job {name}`.")
        k.run("delete", "job", name, "-n", probe.namespace, "--ignore-not-found", "--wait=true", timeout=180)


def stop_reason(status: dict, stuck: Optional[tuple], pull_s: float) -> str:
    """Why the wait ends now ("" to keep waiting); pull_s is how long a pull error has lasted."""
    if finished(status):
        return finished(status)
    if stuck and stuck[0] in STUCK:
        return f"cannot start: {stuck[1]}"
    if stuck and pull_s >= PULL_GRACE_S:
        return f"cannot pull its image (still failing after {PULL_GRACE_S} s): {stuck[1]}"
    return ""


def wait(k: kube.Kube, probe: Probe, name: str, clock, sleep) -> tuple:
    """Poll the Job until it finishes, cannot start, or runs out of time: (its last status, why the wait ended)."""
    deadline, status, pulling_since = clock() + probe.deadline_s + 120, {}, None
    while clock() < deadline:
        found = k.get("job", name, probe.namespace)
        if found is None:
            return status, "disappeared (deleted by someone else?)"
        status = found.get("status") or {}
        stuck = None if finished(status) else waiting(k, probe.namespace, name)
        now = clock()
        pulling_since = (now if pulling_since is None else pulling_since) if stuck else None
        why = stop_reason(status, stuck, now - pulling_since if stuck else 0)
        if why:
            return status, why
        sleep(10)
    return status, "did not finish in time"


def logs(k: kube.Kube, probe: Probe, name: str, redact: Callable[[str], str]) -> str:
    text = ""
    for container in probe.containers:
        # The whole log: a JVM's INFO lines can push the step markers far back.
        res = k.run("logs", f"job/{name}", "-n", probe.namespace, "-c", container, "--tail=-1", check=False,
                    timeout=120)
        text += f"\n=== {container}\n" + ((res.stdout or "") or (res.stderr or ""))
    return redact(text)


def run(k: kube.Kube, probe: Probe, name: str, manifest: dict, *, redact: Callable[[str], str] = lambda t: t,
        clock=time.monotonic, sleep=time.sleep) -> tuple:
    """Start the Job, wait for it and read its logs: (outcome "succeeded"/"failed"/"", why the wait ended, logs).

    A Job that does not finish (it cannot start, runs out of time, or this run is
    interrupted) is deleted. Every log line passes through `redact` before anyone sees it.
    """
    clear_leftovers(k, probe)
    status: dict = {}
    text = ""
    try:
        # Inside the try: an interrupt while the Job is being created must delete it too.
        k.apply([manifest])
        status, why = wait(k, probe, name, clock, sleep)
        text = logs(k, probe, name, redact)
    finally:
        if not finished(status):
            k.run("delete", "job", name, "-n", probe.namespace, "--ignore-not-found", "--wait=false", check=False,
                  timeout=60)
    return finished(status), why, text


def failure(probe: Probe, name: str, done: str, why: str, text: str, main_container: str) -> JobError:
    """The error for a Job that did not succeed: the last log lines, and what to do next."""
    tail = " | ".join(line for line in text.splitlines()[-12:] if line.strip())
    advice = (f"`kubectl -n {probe.namespace} logs job/{name} -c {main_container}` shows the whole run; "
              "the Job stays an hour." if done else
              "The Job did not finish, so it was deleted. Fix the cause above, then re-run the check.")
    return JobError(f"{probe.what} {probe.namespace}/{name} {why}: {tail[-900:]}", advice)
