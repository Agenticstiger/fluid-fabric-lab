# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Run one PG spike in a lab, as a throwaway pod in the fluid-runner image.

    python3 spikes/run.py --name <lab> pg1|pg2|pg5|pg6

Needs a lab with the spark tier (`lab up --tier spark`: namespace `spark`, the
`spark` client and its grant) and the runner image (`lab build fluid-runner`).
The pod runs in namespace `spark` under Pod Security `restricted`. run.py
prints the spike's one `PGn {...}` line, and exits 0 only when the spike ran
to its end (a finding is in the line, not in the exit code). On exit, Ctrl-C
and SIGTERM included, it then deletes the pod, the ConfigMap that holds the
scripts and, for PG-6, its copy of the `tofu` role's password. What a spike writes to the lab stays: see "Leftovers"
in spikes/README.md. Throwaway by design: the results live in
docs/proven.md.
"""

from __future__ import annotations

import argparse
import base64
import json
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from labctl import build, config, kube, proc, spark, state  # noqa: E402

SPIKES = {"pg1": "pg1.py", "pg2": "pg2.py", "pg5": "pg5.sh", "pg6": "pg6.sh"}
CATALOG = {"pg1", "pg2", "pg5"}   # the spikes that sign in to Lakekeeper as the `spark` client
NAMESPACE = "spark"
WAIT_S = 600
# lib/pq takes the password from PGPASSWORD, so no connection string holds it; CNPG's own CA verifies the server.
PG_CONN_STR = "postgres://tofu@lab-pg-rw.lab-db.svc:5432/tofu_state?sslmode=verify-full&sslrootcert=/s/pg-ca.crt"


def runner_tag(lab: state.Lab) -> str:
    """The fluid-runner image `lab build` loaded into this lab, from the lab's own record (as `lab check` reads it)."""
    again = f"Run `./lab build fluid-runner --name {lab.name}`."
    try:
        record = state.read_json(lab.images_record)
        record = record.get("fluid-runner") if isinstance(record, dict) else None
    except (OSError, ValueError, state.StateError):
        record = None
    tag = record.get("tag") if isinstance(record, dict) else None
    if not tag:
        raise SystemExit(f"run.py: no fluid-runner image recorded for lab {lab.name} in {lab.images_record}. {again}")
    layers = record.get("layers")
    if not isinstance(layers, list) or not layers or build.node_layers(proc.run, lab, tag) != layers:
        raise SystemExit(f"run.py: node {lab.node} does not hold {tag} with the layers `lab build` recorded "
                         f"(a new cluster, or another build since). {again}")
    return tag


def pod(spike: str, runner: str) -> dict:
    script = SPIKES[spike]
    env = [{"name": "HOME", "value": "/tmp"}, {"name": "SSL_CERT_FILE", "value": "/etc/lab-ca/ca.crt"}]
    if spike in CATALOG:
        env += [{"name": k, "valueFrom": {"secretKeyRef": {"name": "spark-lakekeeper-client", "key": k}}}
                for k in ("LAKEKEEPER_CLIENT_ID", "LAKEKEEPER_CLIENT_SECRET")]
    if spike == "pg6":
        env += [{"name": "PGPASSWORD", "valueFrom": {"secretKeyRef": {"name": "pg6-tofu", "key": "password"}}},
                {"name": "PG_CONN_STR", "value": PG_CONN_STR},
                {"name": "TF_VAR_passphrase", "value": "lab-spike-pg6-passphrase-not-a-secret"}]
    command = ["python3", "-I", f"/s/{script}"] if script.endswith(".py") else ["sh", f"/s/{script}"]
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": spike, "namespace": NAMESPACE, "labels": {"fluid-fabric-lab/spike": spike}},
        "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False,
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "volumes": [{"name": "s", "configMap": {"name": f"spike-{spike}"}},
                        {"name": "ca", "configMap": {"name": "lab-ca-bundle"}}],
            "containers": [{
                "name": spike, "image": runner, "imagePullPolicy": "Never", "command": command, "env": env,
                "securityContext": {"runAsUser": 10001, "runAsGroup": 10001, "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]}},
                "volumeMounts": [{"name": "s", "mountPath": "/s"}, {"name": "ca", "mountPath": "/etc/lab-ca"}],
                "resources": {"limits": {"memory": "1Gi"}},
            }],
        },
    }


def with_state_at_rest(k: kube.Kube, logs: str, line: str) -> str:
    """Add to PG-6's result what Postgres stores: the state row must be encrypted and hold no canary."""
    canary = next((ln.split(" ", 1)[1] for ln in logs.splitlines() if ln.startswith("PG6-CANARY ")), "")
    if not canary.startswith("CANARY-") or not canary[7:].isalnum():
        return line
    sql = (f"SELECT position('{canary}' in data), position('encrypted_data' in data) "
           'FROM "Lab Spike-PG6".states')
    res = k.exec("lab-db", "lab-pg-1", ["psql", "-d", "tofu_state", "-At", "-F", ",", "-c", sql],
                 container="postgres")
    rows = (res.stdout or "").split() if res.ok else []
    found = json.loads(line.split(" ", 1)[1])
    # Exactly one state row, as two positions. Anything else is unknown, never clean.
    parts = rows[0].split(",") if len(rows) == 1 else []
    if len(parts) == 2 and all(p.isascii() and p.isdigit() for p in parts):
        found["state_at_rest_cleartext"] = parts[0] != "0"
        found["state_at_rest_encrypted"] = parts[1] != "0"
    else:
        found["state_at_rest"] = f"unknown: {len(rows)} row(s)"
    return "PG6 " + json.dumps(found)


def _wait(k: kube.Kube, name: str) -> tuple:
    """(phase, why) once the pod has ended, cannot start, or WAIT_S has passed."""
    deadline, phase = time.monotonic() + WAIT_S, ""
    while time.monotonic() < deadline:
        status = (k.get("pod", name, NAMESPACE) or {}).get("status") or {}
        phase = status.get("phase", "")
        if phase in ("Succeeded", "Failed"):
            return phase, f"phase {phase}"
        reasons = [((c.get("state") or {}).get("waiting") or {}).get("reason")
                   for c in status.get("containerStatuses") or []]
        stuck = [r for r in reasons if r in spark.STUCK]
        if stuck:
            return phase, f"cannot start: {stuck[0]}"
        time.sleep(3)
    return phase, f"still {phase or 'unknown'} after {WAIT_S} s"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("spike", choices=sorted(SPIKES))
    ap.add_argument("--name", default=state.DEFAULT_NAME, help="lab name")
    args = ap.parse_args()
    try:
        lab = state.lab_for(ROOT, args.name)
        runner = runner_tag(lab)
        k = kube.Kube(lab)
    except (state.StateError, config.ConfigError) as exc:
        print(f"run.py: {exc}", file=sys.stderr)
        return 2
    here = Path(__file__).resolve().parent
    files = {name: (here / name).read_text(encoding="utf-8") for name in SPIKES.values()}
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))   # so a plain kill runs the finally too
    try:
        if args.spike == "pg6":
            # The tofu role's password, from its Secret in lab-db to this namespace; never printed.
            data = (k.get("secret", "lab-pg-tofu", "lab-db") or {}).get("data") or {}
            k.apply([{"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
                      "metadata": {"name": "pg6-tofu", "namespace": NAMESPACE},
                      "data": {"password": data["password"]}}])
            # CNPG's server CA: the certificate only, never its key.
            ca = k.run("get", "secret", "lab-pg-ca", "-n", "lab-db", "-o", r"jsonpath={.data.ca\.crt}").stdout
            files["pg-ca.crt"] = base64.b64decode(ca or "").decode()
        k.apply([{"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": f"spike-{args.spike}", "namespace": NAMESPACE}, "data": files}])
        k.run("delete", "pod", args.spike, "-n", NAMESPACE, "--ignore-not-found", "--wait=true", timeout=120)
        k.apply([pod(args.spike, runner)])
        phase, why = _wait(k, args.spike)
        logs = k.run("logs", args.spike, "-n", NAMESPACE, check=False).stdout or ""
        lines = [ln for ln in logs.splitlines() if ln.startswith(args.spike.upper() + " ")]
        if args.spike == "pg6" and lines:
            lines[-1] = with_state_at_rest(k, logs, lines[-1])
        print(lines[-1] if lines else f"{args.spike}: no result line; {why}")
        return 0 if phase == "Succeeded" and lines else 1
    finally:
        # The password copy first, so an interrupt during the slower pod delete cannot keep it.
        k.run("delete", "secret", "pg6-tofu", "-n", NAMESPACE, "--ignore-not-found", check=False, timeout=60)
        k.run("delete", "pod", args.spike, "-n", NAMESPACE, "--ignore-not-found", check=False, timeout=120)
        k.run("delete", "configmap", f"spike-{args.spike}", "-n", NAMESPACE, "--ignore-not-found", check=False,
              timeout=60)


if __name__ == "__main__":
    sys.exit(main())
