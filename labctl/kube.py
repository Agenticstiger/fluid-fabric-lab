# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""kubectl, always against the lab's own kubeconfig and context.

`lab up` writes the cluster's kubeconfig into the lab's state directory and
every call here passes it explicitly, so the user's ~/.kube/config is never
read or changed. Objects (Secrets included) go to `kubectl apply
--server-side -f -` on stdin, never as files or arguments.
"""

from __future__ import annotations

import json
import queue
import re
import subprocess
import threading
import time
from typing import Iterable, Optional

from labctl import proc
from labctl.state import Lab


class KubeError(Exception):
    pass


class Kube:
    def __init__(self, lab: Lab, runner: proc.Runner = proc.run):
        self.lab = lab
        self.runner = runner

    def argv(self, *args: str) -> list:
        return ["kubectl", "--kubeconfig", str(self.lab.kubeconfig), "--context", self.lab.context, *args]

    def run(self, *args: str, input: Optional[str] = None, timeout: float = 120,  # noqa: A002
            check: bool = True) -> proc.Result:
        res = self.runner(self.argv(*args), input=input, timeout=timeout)
        if check and not res.ok:
            what = " ".join(a for a in args if not a.startswith("-"))[:80]
            # stderr only: the stdout of `get ... -o json` can hold Secret-sourced env values.
            raise KubeError(f"kubectl {what}: {(res.stderr or '').strip()[-600:] or 'failed with no message'}")
        return res

    def apply(self, objects: Iterable[dict]) -> None:
        """Server-side apply, owned by field manager `fluid-fabric-lab`.

        Never client-side apply: it copies the whole object, Secret values
        included, into the `kubectl.kubernetes.io/last-applied-configuration`
        annotation.
        """
        items = list(objects)
        if not items:
            return
        doc = json.dumps({"apiVersion": "v1", "kind": "List", "items": items})
        self.run("apply", "--server-side", "--force-conflicts", "--field-manager=fluid-fabric-lab", "-f", "-",
                 input=doc)

    def get(self, kind: str, name: Optional[str] = None, namespace: Optional[str] = None,
            selector: Optional[str] = None) -> Optional[dict]:
        args = ["get", kind] + ([name] if name else []) + (["-n", namespace] if namespace else [])
        if selector:
            args += ["-l", selector]
        res = self.run(*args, "-o", "json", check=False)
        if not res.ok:
            if "NotFound" in res.output_tail(400):
                return None
            raise KubeError(f"kubectl get {kind} {name or ''}: {(res.stderr or '').strip()[-400:] or 'failed'}")
        return json.loads(res.stdout)

    def wait(self, condition: str, target: str, namespace: Optional[str] = None,
             timeout_s: int = 300) -> None:
        args = ["wait", f"--for={condition}", target, f"--timeout={timeout_s}s"]
        if namespace:
            args += ["-n", namespace]
        self.run(*args, timeout=timeout_s + 30)

    def exec(self, namespace: str, pod: str, command: list, *, container: Optional[str] = None,
             input: Optional[str] = None, timeout: float = 120) -> proc.Result:  # noqa: A002
        args = ["exec", "-n", namespace] + (["-i"] if input is not None else []) + [pod]
        if container:
            args += ["-c", container]
        return self.run(*args, "--", *command, input=input, timeout=timeout, check=False)

    def first_pod(self, namespace: str, selector: str, timeout_s: int = 300) -> str:
        """Name of a Running pod matching `selector` (waits up to timeout_s)."""
        deadline = time.monotonic() + timeout_s
        while True:
            pods = (self.get("pods", namespace=namespace, selector=selector) or {}).get("items") or []
            for pod in pods:
                if (pod.get("status") or {}).get("phase") == "Running":
                    return pod["metadata"]["name"]
            if time.monotonic() > deadline:
                raise KubeError(f"no Running pod for {selector} in {namespace} after {timeout_s}s")
            time.sleep(3)


def secret(name: str, namespace: str, data: dict, *, kind: str = "Opaque", labels: Optional[dict] = None) -> dict:
    """A Secret manifest from plain strings (stringData; never logged)."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace,
                     "labels": {"app.kubernetes.io/part-of": "fluid-fabric-lab", **(labels or {})}},
        "type": kind,
        "stringData": {k: str(v) for k, v in data.items()},
    }


_FORWARDING = re.compile(r"Forwarding from 127\.0\.0\.1:(\d+) ->")


class PortForward:
    """`kubectl port-forward` to a Service, on an ephemeral 127.0.0.1 port.

    The launcher reaches the lab Gateway this way, through the API server,
    instead of through the host port kind maps: on Docker Desktop the
    forward for a privileged host port (443) can go stale after a cluster
    with the same mapping is recreated and then resets every connection,
    while the API server's own forward keeps working (measured 2026-11-18).
    TLS is still end to end: the launcher speaks HTTPS to Envoy with the
    real SNI, and Envoy to Keycloak with the lab CA.

    A reader thread drains kubectl's output for the whole life of the
    forward (kubectl writes one "Handling connection" line per connection
    and would block on a full pipe), and the start-up wait has a real
    deadline even when kubectl prints nothing.
    """

    def __init__(self, kube: "Kube", namespace: str, target: str, remote_port: int, timeout_s: float = 60):
        self.argv = kube.argv("port-forward", "-n", namespace, target, f":{remote_port}", "--address", "127.0.0.1")
        self.timeout_s = timeout_s
        self.proc: Optional[subprocess.Popen] = None
        self.port = 0
        self._lines: "queue.Queue[Optional[str]]" = queue.Queue()

    def _drain(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            self._lines.put(line)
        self._lines.put(None)  # kubectl exited

    def __enter__(self) -> "PortForward":
        self.proc = subprocess.Popen(self.argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,  # noqa: S603
                                     stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._drain, name="lab-port-forward", daemon=True).start()
        deadline = time.monotonic() + self.timeout_s
        seen: list = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                line = self._lines.get(timeout=remaining)
            except queue.Empty:
                break
            if line is None:
                break
            seen.append(line.strip())
            found = _FORWARDING.search(line)
            if found:
                self.port = int(found.group(1))
                return self
        self.close()
        raise KubeError("kubectl port-forward did not start within "
                        f"{self.timeout_s:.0f}s: " + (" | ".join(seen[-3:]) or "no output"))

    def close(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        if self.proc.stdout is not None:
            self.proc.stdout.close()

    def __exit__(self, *exc) -> None:
        self.close()
