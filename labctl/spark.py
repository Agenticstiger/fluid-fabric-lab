# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The `spark` add-on tier's proof: forge's Spark runtime on the lab's open Spark image, on a contract.

`lab check --tier spark` runs three steps in namespace `spark`:

  seed     a Job on the spark-iceberg image (the lab-built Apache Spark with Iceberg): raw.events gets
           three rows and curated.events_daily is emptied (platform/spark/seed.py);
  apply    in the fluid-host pod, `fluid apply contract.fluid.yaml --env local --mode amend-and-build
           --build-id daily-curated --yes` on platform/spark/probe.fluid.yaml, the command a scheduler
           runs: forge starts one Spark pod on the same image through lab-spark-submit;
  read     a second Job on the spark image reads curated.events_daily back (platform/spark/verify.py).

Spark reaches the lab only through its HTTPS names: Lakekeeper's Iceberg REST
catalog with the `spark` OAuth2 client, and SeaweedFS through storage credentials
Lakekeeper vends per table. No S3 key is configured for the tables.
"""

from __future__ import annotations

import base64
import json
import re
import secrets as _secrets
import time
from pathlib import Path
from typing import Callable, Optional

from labctl import jobs, kube, spark_image, state

NAMESPACE = "spark"
JOB_LABEL = "fluid-fabric-lab/check"
SUBMIT_LABEL = "fluid-fabric-lab/spark-submit"   # on every pod lab-spark-submit creates
APACHE_IMAGE_ID = spark_image.IMAGE      # the lab-built image of tier `spark`
APACHE_UID, APACHE_GID = spark_image.UID, spark_image.UID   # `spark:spark` in the apache/spark image
# Read by platform/components/lab-spark/values.yaml.gotmpl: the fluid-runner image of fluid-host, and the
# image of the Spark pods (`lab up` sets both).
RUNNER_ENV = "LAB_FLUID_RUNNER_IMAGE"
IMAGE_ENV = "LAB_SPARK_IMAGE"
JOB_DEADLINE_S = 900
PRODUCT_ID, BUILD_ID, ENV = "lab.spark-probe", "daily-curated", "local"
CONTRACT_FILE = "contract.fluid.yaml"
WORKDIR = f"/state/spark/{PRODUCT_ID}"          # in fluid-host, on its own volume
APPLY_TIMEOUT_S = 1800
# The rows the build must write from the three rows seed.py puts in raw.events.
EXPECTED = {"2026-11-17": 2, "2026-11-18": 1}
# What the lab, as the operator, allows forge to run for the probe contract (forge refuses each one
# otherwise): the batch's SQL, pinned to what was reviewed (sha256 of the SQL, stripped), and the
# namespaces its local run may write. A test keeps these and the contract in step.
OPERATOR_ALLOWLISTS = {
    "FLUID_SPARK_CONTRACT_SQL_ALLOWLIST":
        "lab.spark-probe/daily-curated@sha256:ae9257b9cb832a81eb1dc1f99af1253ca02f21cddc0892f1b7a7c602a0a16563",
    "FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST": "sales-wh/raw,sales-wh/curated",
}
# In fluid-host: the contract and its overlay from the lab-spark ConfigMap (read-only there) into WORKDIR,
# where forge keeps the run's state beside the contract.
PREPARE = ('mkdir -p "$1/overlays" && cp /opt/lab-spark/probe.fluid.yaml "$1/contract.fluid.yaml" && '
           'cp /opt/lab-spark/probe.overlay.local.yaml "$1/overlays/local.yaml"')
_MARK = re.compile(r"^(LAB-SPARK-[A-Z]+) (.*)$", re.M)
# The Job loop lives in labctl.jobs; these names stay here for the code that reads them (spikes/run.py).
STUCK, PULL_RETRY, PULL_GRACE_S = jobs.STUCK, jobs.PULL_RETRY, jobs.PULL_GRACE_S
PROBE = jobs.Probe(namespace=NAMESPACE, selector=f"{JOB_LABEL}=spark", what="spark smoke Job",
                   containers=("spark",), deadline_s=JOB_DEADLINE_S)


class SparkError(jobs.JobError):
    pass


def recorded_image(record: Path, image: str) -> str:
    """The tag `lab build` recorded for a lab-built image, or "" when there is none."""
    try:
        found = state.read_json(record)
    except (OSError, ValueError, state.StateError):
        return ""
    entry = found.get(image) if isinstance(found, dict) else None
    tag = entry.get("tag") if isinstance(entry, dict) else None
    return tag if isinstance(tag, str) else ""


def recorded_runner(record: Path) -> str:
    """The fluid-runner tag `lab build` recorded, or "" when there is none (fluid-host is then not rendered)."""
    return recorded_image(record, "fluid-runner")


def job(name: str, image: str, script: str) -> dict:
    """A smoke Job: one pod on the spark-iceberg image running /spark/<script> through the entry script.

    Pod Security `restricted`; the client secret comes from a Secret by env only. `image` is the lab-built tag
    (`Never`: it is loaded into the node, never pulled)."""
    secret = [{"name": k, "valueFrom": {"secretKeyRef": {"name": "spark-lakekeeper-client", "key": k}}}
              for k in ("LAKEKEEPER_CLIENT_ID", "LAKEKEEPER_CLIENT_SECRET")]
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE,
                     "labels": {"app.kubernetes.io/part-of": "fluid-fabric-lab", JOB_LABEL: "spark"}},
        "spec": {
            "backoffLimit": 0, "activeDeadlineSeconds": JOB_DEADLINE_S, "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": {JOB_LABEL: "spark"}},
                "spec": {
                    "restartPolicy": "Never", "serviceAccountName": "spark", "automountServiceAccountToken": False,
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "volumes": [{"name": "spark", "configMap": {"name": "lab-spark"}},
                                {"name": "work", "emptyDir": {"sizeLimit": "512Mi"}},
                                {"name": "lab-ca", "configMap": {"name": "lab-ca-bundle"}}],
                    "containers": [{
                        "name": "spark", "image": image, "imagePullPolicy": "Never",
                        "command": ["bash", "/spark/lab-spark-entry.sh", f"/spark/{script}"],
                        "env": secret + [
                            {"name": "AWS_REGION", "value": "us-east-1"},
                            # Trust only the lab CA in every JVM in the pod.
                            {"name": "JAVA_TOOL_OPTIONS", "value": "-Djavax.net.ssl.trustStore=/etc/lab-ca/truststore.p12"
                                                                  " -Djavax.net.ssl.trustStoreType=PKCS12"},
                        ],
                        "securityContext": {"runAsUser": APACHE_UID, "runAsGroup": APACHE_GID,
                                            "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                        "volumeMounts": [{"name": "spark", "mountPath": "/spark", "readOnly": True},
                                         {"name": "work", "mountPath": "/work"},
                                         {"name": "lab-ca", "mountPath": "/etc/lab-ca", "readOnly": True}],
                        "resources": {"requests": {"cpu": "500m", "memory": "2Gi"}, "limits": {"memory": "2560Mi"}},
                    }],
                },
            },
        },
    }


def marks(logs: str) -> dict:
    """The LAB-SPARK-* lines the smoke run prints, as {mark: payload}."""
    return {m.group(1): m.group(2).strip() for m in _MARK.finditer(logs or "")}


def judge(found: dict) -> dict:
    """The run's result; SparkError unless the build wrote exactly what the seed implies."""
    try:
        result = json.loads(found["LAB-SPARK-RESULT"])
        seeded = json.loads(found["LAB-SPARK-SEEDED"])
    except (KeyError, ValueError) as exc:
        raise SparkError(f"the smoke run did not finish its steps (missing or bad {exc})") from exc
    if seeded.get("sink_rows") != 0:
        raise SparkError(f"the seed left {seeded.get('sink_rows')} row(s) in curated.events_daily")
    if result.get("rows") != EXPECTED:
        raise SparkError(f"curated.events_daily holds {result.get('rows')}, expected {EXPECTED}")
    if not result.get("snapshots", 0) > seeded.get("sink_snapshots", 0):
        raise SparkError("curated.events_daily has no snapshot newer than the seed: the build wrote nothing")
    return {**result, "seed_snapshots": seeded.get("sink_snapshots")}


def _redact(text: str, secret: Optional[str], client: str = "spark") -> str:
    """The secret removed in every form a log could show it: raw, and base64 of it and of `client:secret`."""
    if not secret:
        return text
    # Longest first: base64 of `client:secret` contains base64 of the secret when the
    # prefix is a multiple of three bytes ("spark:" is six).
    forms = (base64.b64encode(f"{client}:{secret}".encode()).decode(), base64.b64encode(secret.encode()).decode(),
             secret)
    for form in sorted(forms, key=len, reverse=True):
        text = text.replace(form, "<redacted>")
    return text


def forge_argv(run_id: str) -> list:
    """`fluid apply` of the probe's batch, behind accept_runner.env_prefix (strict, the operator's allowlists)."""
    from labctl import accept_runner

    return accept_runner.env_prefix(ENV, run_id, OPERATOR_ALLOWLISTS) + [
        "fluid", "apply", CONTRACT_FILE, "--env", ENV, "--mode", "amend-and-build", "--build-id", BUILD_ID, "--yes"]


def _run_job(k: kube.Kube, step: str, image: str, redact: Callable[[str], str], clock, sleep) -> tuple:
    """One smoke Job to its end: (its name, its marks). labctl.jobs runs it: a Job that does not finish (it cannot
    start, runs out of time, or this run is interrupted) is deleted, so it neither holds the node's memory nor
    blocks the next check. A finished one stays an hour (ttlSecondsAfterFinished) for its logs."""
    name = f"lab-check-spark-{step}-{int(time.time())}-{_secrets.token_hex(3)}"
    try:
        done, why, logs = jobs.run(k, PROBE, name, job(name, image, f"{step}.py"), redact=redact, clock=clock,
                                   sleep=sleep)
        if done != "succeeded":
            raise jobs.failure(PROBE, name, done, why, logs, "spark")
    except jobs.JobError as exc:
        raise SparkError(str(exc), exc.advice) from exc
    return name, marks(logs)


def run_smoke(k: kube.Kube, image: str, *, secret: Optional[str] = None, log: Callable[[str], None] = lambda _m: None,
              clock=time.monotonic, sleep=time.sleep) -> dict:
    """Seed, apply and read back, then judge. `image` is the recorded spark-iceberg tag. `secret` (the spark
    client's) is redacted from any log text. Raises SparkError."""
    # Imported here: labctl.e2e imports this module for the image's constants.
    from labctl import accept, accept_runner, e2e

    def redact(text: str) -> str:
        return _redact(text, secret)

    log("lab check: spark: seeding raw.events ...")
    seed_name, seeded = _run_job(k, "seed", image, redact, clock, sleep)
    host = e2e.Host(k, workdir=WORKDIR)
    res = host.exec(["sh", "-c", PREPARE, "sh", WORKDIR], timeout=120)
    if not res.ok:
        raise SparkError(f"copying the probe contract into fluid-host:{WORKDIR} failed: "
                         f"{accept.oneline(redact(accept_runner.safe_tail(res, 400)))}")
    run_id = f"spark-{int(time.time())}-{_secrets.token_hex(3)}"
    log(f"lab check: spark: fluid apply --mode amend-and-build --build-id {BUILD_ID} ...")
    res = host.forge(forge_argv(run_id), timeout=APPLY_TIMEOUT_S)
    if not res.ok:
        tail = host.python(e2e.LOG, WORKDIR, PRODUCT_ID, BUILD_ID)
        raise SparkError(f"`fluid apply --mode amend-and-build --build-id {BUILD_ID}` exited {res.returncode} in "
                         f"fluid-host: {accept.oneline(redact(accept_runner.safe_tail(res, 400)))}; build run "
                         f"{accept.oneline(redact(tail.stdout or ''), 600)}")
    log("lab check: spark: reading curated.events_daily back ...")
    read_name, read = _run_job(k, "verify", image, redact, clock, sleep)
    return {**judge({**seeded, **read}), "jobs": [seed_name, read_name], "image": image}
