# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab down`: remove one lab's cluster and sibling containers, nothing else.

Only objects named after THIS lab are touched: the kind cluster `<name>`, the
container `<name>-openbao`, and with --purge the volume `<name>-openbao-data`
and the lab's state directory. The shared mirror registry is left running
(`lab mirror --stop` stops it). Nothing uses `docker compose`, and nothing
prunes: other containers, networks and volumes on the machine are never
listed for removal.

Without --purge the OpenBao volume and the state directory survive, so the
next `lab up` unseals the same OpenBao and keeps every credential (this is
what `lab check` (e) relies on).
"""

from __future__ import annotations

import shutil
from pathlib import Path

from labctl import containers, proc, state
from labctl.output import FAIL, PASS, WARN, check

STATE_MARKER = ".fluid-fabric-lab-state"


class NotALab(Exception):
    """A directory or object that this lab did not create."""


def owns_state(lab: state.Lab) -> bool:
    try:
        return (lab.state / STATE_MARKER).read_text(encoding="utf-8").strip() == lab.name
    except OSError:
        return False


def mark_state_dir(lab: state.Lab) -> None:
    """Called by `lab up`: proves later that the directory is this lab's state directory.

    Refuses an existing, non-empty directory that carries no marker for this
    lab: `lab down --purge` deletes what is marked, so a stray LAB_STATE_DIR
    such as a home directory must never be marked.
    """
    if lab.state.exists() and any(lab.state.iterdir()) and not owns_state(lab):
        raise NotALab(f"{lab.state} already holds files and is not lab {lab.name!r}'s state directory; "
                      "pick an empty or new directory for LAB_STATE_DIR")
    state.ensure_dir(lab.state)
    (lab.state / STATE_MARKER).write_text(f"{lab.name}\n", encoding="utf-8")


def _purge_state(lab: state.Lab) -> dict:
    marker = lab.state / STATE_MARKER
    if not lab.state.exists():
        return check("down:state", PASS, f"state directory {lab.state} already gone")
    try:
        owner = marker.read_text(encoding="utf-8").strip()
    except OSError:
        owner = None
    if owner != lab.name:
        return check("down:state", FAIL,
                     f"{lab.state} has no {STATE_MARKER} naming lab {lab.name!r}; refusing to delete it",
                     "Delete it yourself if it really is this lab's state directory.")
    shutil.rmtree(lab.state)
    return check("down:state", PASS, f"removed state directory {lab.state} (lab CA, OpenBao shares, kubeconfig)")


def _remove(runner: proc.Runner, cid: str, what: str, exists_argv: list, remove_argv: list) -> dict:
    # `what` is "<kind> <name>"; Docker's not-found answer must name the object.
    """Remove one named object if it exists.

    Only Docker's own "no such ..." answer counts as already gone; a probe
    that fails for any other reason (daemon down, timeout) is a failure.
    """
    probe = runner(exists_argv, timeout=60)
    if containers.absent(probe, what.split()[-1]):
        return check(cid, PASS, f"no {what}")
    if not probe.ok:
        return check(cid, FAIL, f"could not check {what}: {probe.output_tail(300).strip() or 'no answer'}",
                     "Is the container runtime running? Nothing was removed.")
    res = runner(remove_argv, timeout=600)
    if res.ok:
        return check(cid, PASS, f"removed {what}")
    return check(cid, FAIL, f"could not remove {what}: {res.output_tail(400).strip()}")


def _remove_network(runner: proc.Runner, lab: state.Lab) -> dict:
    """The lab's Docker network, once only the shared mirror is left on it."""
    import json

    from labctl.mirror import REGISTRY

    res = runner(["docker", "network", "inspect", lab.network, "--format", "{{json .}}"], timeout=60)
    if containers.absent(res, lab.network):
        return check("down:network", PASS, f"no network {lab.network}")
    if not res.ok:
        return check("down:network", FAIL, f"could not check network {lab.network}: "
                     f"{res.output_tail(300).strip() or 'no answer'}")
    try:
        info = json.loads(res.stdout)
    except ValueError:
        info = {}
    if (info.get("Labels") or {}).get(state.LABEL) != lab.name:
        return check("down:network", FAIL, f"network {lab.network} is not this lab's (no label "
                     f"{state.LABEL}={lab.name}); it is left alone")
    names = [c.get("Name") for c in (info.get("Containers") or {}).values()]
    if REGISTRY in names:
        runner(["docker", "network", "disconnect", lab.network, REGISTRY], timeout=60)
        names.remove(REGISTRY)
    if names:
        return check("down:network", FAIL, f"network {lab.network} still has containers: {', '.join(names)}; "
                     "it is left in place")
    return _remove(runner, "down:network", f"network {lab.network}",
                   ["docker", "network", "inspect", lab.network], ["docker", "network", "rm", lab.network])


def _cluster_exists(runner: proc.Runner, lab: state.Lab) -> bool:
    """Whether kind lists the cluster; raises DockerError when kind cannot answer."""
    res = runner(["kind", "get", "clusters"], timeout=60)
    if not res.ok:
        raise containers.DockerError(f"kind get clusters: {res.output_tail(300).strip() or 'no answer'}")
    return lab.cluster in (res.stdout or "").split()


def run_down(root: Path, *, name: str = state.DEFAULT_NAME, purge: bool = False,
             runner: proc.Runner = proc.run, env=None) -> dict:
    lab = state.lab_for(root, name, env)
    if not owns_state(lab):
        # Without this lab's state there is no proof that a kind cluster or a
        # container of this name came from `lab up` here; never delete it.
        return {"command": "down", "name": lab.name, "purge": purge, "ok": False, "checks": [check(
            "down:state", FAIL, f"no state for lab {lab.name!r} in {lab.state}; nothing is removed",
            "`lab down` removes only labs that `lab up` created on this machine. Delete anything else "
            "with kind or docker yourself.")]}
    checks: list = []
    try:
        return _down(runner, lab, purge, checks)
    except containers.DockerError as exc:
        checks.append(check("down:runtime", FAIL, f"{exc}; nothing more is removed and the state directory is kept",
                            "Start the container runtime and re-run `lab down`."))
        return {"command": "down", "name": lab.name, "purge": purge, "ok": False, "checks": checks}


def _down(runner: proc.Runner, lab: state.Lab, purge: bool, checks: list) -> dict:
    if _cluster_exists(runner, lab):
        res = runner(["kind", "delete", "cluster", "--name", lab.cluster,
                      "--kubeconfig", str(lab.kubeconfig)], timeout=600)
        checks.append(check("down:cluster", PASS if res.ok else FAIL,
                            f"deleted kind cluster {lab.cluster}" if res.ok
                            else f"kind delete failed: {res.output_tail(400).strip()}"))
    else:
        checks.append(check("down:cluster", PASS, f"no kind cluster {lab.cluster}"))
    if checks[-1]["status"] == PASS:
        # The images `lab build` loaded went with the node.
        try:
            lab.images_record.unlink(missing_ok=True)
        except OSError as exc:
            checks.append(check("down:images", WARN, f"could not drop {lab.images_record}: {exc}",
                                "Remove it by hand; `lab check` compares it with the node and would reject it."))
    found = containers.inspect(runner, lab.openbao)
    if found is None:
        checks.append(check("down:openbao", PASS, f"no container {lab.openbao}"))
    elif ((found.get("Config") or {}).get("Labels") or {}).get(state.LABEL) != lab.name:
        checks.append(check("down:openbao", FAIL, f"container {lab.openbao} does not carry the label "
                            f"{state.LABEL}={lab.name}; it is not this lab's and is left alone"))
    else:
        checks.append(_remove(runner, "down:openbao", f"container {lab.openbao}",
                              ["docker", "inspect", "--type", "container", lab.openbao],
                              ["docker", "rm", "-f", lab.openbao]))
    checks.append(_remove_network(runner, lab))
    if purge:
        if any(c["status"] == FAIL for c in checks):
            # The volume and the state (the only unseal shares) go only once
            # nothing else of the lab may still exist.
            checks.append(check("down:purge", FAIL, f"OpenBao volume {lab.openbao_volume} and state directory "
                                f"{lab.state} kept: an earlier removal failed",
                                "Fix that, then re-run `lab down --purge`."))
        else:
            checks.append(_remove(runner, "down:volume", f"volume {lab.openbao_volume}",
                                  ["docker", "volume", "inspect", lab.openbao_volume],
                                  ["docker", "volume", "rm", lab.openbao_volume]))
            if checks[-1]["status"] == FAIL:
                checks.append(check("down:state", FAIL, f"state directory {lab.state} kept: the OpenBao volume "
                                    "could not be removed", "Fix that, then re-run `lab down --purge`."))
            else:
                checks.append(_purge_state(lab))
    ok = all(c["status"] != FAIL for c in checks)
    return {"command": "down", "name": lab.name, "purge": purge, "ok": ok, "checks": checks}
