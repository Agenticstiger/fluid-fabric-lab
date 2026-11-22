# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The lab's end-to-end run: products/stream-batch through forge, on the lab (docs/e2e.md).

`lab check --tier e2e` runs the `e2e` record below; `lab e2e` brings the tier up
first (mirror, up, the two lab-built images) and then runs `lab check --tier e2e`
for the records that judge the run. The product is a Kafka topic landed in
Iceberg by a forge Spark stream, and one forge Spark SQL batch over it:

  copy      the product's files into the fluid-host pod, under WORKDIR. forge keeps
            its local runs' state beside the contract (`.fluid/`), so the directory
            is the same on every run, on the pod's own volume
  validate  fluid validate contract.fluid.yaml --env local --strict
  plan      fluid plan contract.fluid.yaml --env local --out runtime/plan.json
  apply     fluid apply runtime/plan.json --env local --yes: the datasets, and the
            always-on stream started (forge's supervisor keeps it in fluid-host)
  produce   EVENTS JSON events carrying this run's id to topic TOPIC, from a Job on
            the Strimzi Kafka image, over the plain listener the stream reads
  land      until the raw table's current snapshot holds EVENTS more records than
            before the produce (Lakekeeper's table metadata, as the `spark` client)
  stop      the stream stopped through forge's own cancel (LocalRuns.cancel), so
            the batch and the read-back each run with no other Spark pod beside them
  batch     fluid apply contract.fluid.yaml --env local --mode amend-and-build
            --build-id daily-counts --yes, the command a scheduler runs
  verify    a Job on the spark tier's image reads both tables back for this run id

The record passes only when the raw table holds exactly this run's EVENTS events
(that many distinct ids), the daily table holds exactly EXPECTED for this run, its
snapshot changed during the batch, and no stream run was left running.

Every forge command runs in fluid-host through labctl.accept_runner's FORGE_WRAPPER:
the pod's environment is cleaned to the lab's own variables, and the operator's
allowlists (products/stream-batch/operator.env) are the only ones forge sees.
"""

from __future__ import annotations

import base64
import json
import re
import secrets as _secrets
import time
from pathlib import Path
from typing import Callable, Optional

from labctl import accept, accept_runner, jobs, kube, proc, spark, state, streaming

PRODUCT_DIR = "products/stream-batch"
PRODUCT_ID = "lab.stream-batch"
CONTRACT = "contract.fluid.yaml"
ENV = "local"
STREAM_BUILD, BATCH_BUILD = "events-stream", "daily-counts"
TOPIC = "lab-e2e.events"
WAREHOUSE = "sales-wh"
RAW, DAILY = ("e2e_raw", "events"), ("e2e_curated", "events_daily")
NAMESPACE = "spark"
HOST_SELECTOR = "app.kubernetes.io/name=fluid-host"
WORKDIR = f"/state/e2e/{PRODUCT_ID}"
PLAN = "runtime/plan.json"
SPARK_IMAGE_ID = spark.APACHE_IMAGE_ID    # the spark tier's lab-built image (labctl.build), recorded in images.json
SPARK_UID, SPARK_GID = spark.APACHE_UID, spark.APACHE_GID   # `spark:spark` in the apache/spark image
# Per run: 7 events on the first date and 5 on the second, all with the run's id.
DATES = {"2026-11-20": 7, "2026-11-21": 5}
EXPECTED = dict(DATES)
EVENTS = sum(DATES.values())
TIMEOUT_S = {"validate": 300, "plan": 300, "apply": 900, "batch": 1800}
LAND_TIMEOUT_S = 900
POLL_S = 10
JOB_LABEL = jobs.CHECK_LABEL
PRODUCE = jobs.Probe(namespace=streaming.NAMESPACE, selector=f"{JOB_LABEL}=e2e", what="e2e producer Job",
                     containers=("produce",), deadline_s=300)
VERIFY = jobs.Probe(namespace=NAMESPACE, selector=f"{JOB_LABEL}=e2e", what="e2e read-back Job",
                    containers=("spark",), deadline_s=900)
_RUN_ID = re.compile(r"^e2e-[0-9]{10}-[0-9a-f]{6}$")
_MARK = re.compile(r"^(LAB-E2E-[A-Z]+) (.*)$", re.M)

PRODUCE_SCRIPT = r"""set -u
B=/opt/kafka/bin
printf '%s\n' "$RECORDS" > /work/records.jsonl
n=$(grep -c . /work/records.jsonl)
timeout 120 $B/kafka-console-producer.sh --bootstrap-server "$PLAIN" --topic "$TOPIC" \
  --producer-property enable.idempotence=false --producer-property acks=all \
  < /work/records.jsonl > /work/p.out 2>&1
rc=$?
printf 'LAB-E2E-PRODUCED {"rc":%s,"n":%s}\n' "$rc" "$n"
tail -n 5 /work/p.out | sed 's/^/  producer: /'
exit 0
"""

# Runs in fluid-host (python3 -I -c STOP <workdir> <product> <build>): forge's own cancel of every active local
# run of the build (the `cancel` mark, then SIGTERM to the run's process group, SIGKILL after the grace period),
# the protocol `fluid apply` uses for a build the contract no longer runs here.
STOP = r"""
import json, sys
from pathlib import Path
from fluid_build.build_runners.spark._local import LocalHost, LocalRuns
work, product, build = sys.argv[1:4]
runs = LocalRuns(LocalHost(Path(work)), product, build)
active = [r.run_id for r in runs.runs() if r.active]
for run_id in active:
    runs.cancel(run_id)
after = runs.runs()
print("LAB-E2E-STOP " + json.dumps({"stopped": active, "active": [r.run_id for r in after if r.active],
                                    "states": {r.run_id: str(r.raw_state) for r in after[:3]}}, sort_keys=True))
"""

# The end of the newest local run's driver log of a build, for a failure message (python3 -I -c LOG ...).
LOG = r"""
import sys
from pathlib import Path
from fluid_build.build_runners.spark._local import LocalHost, LocalRuns
work, product, build = sys.argv[1:4]
runs = LocalRuns(LocalHost(Path(work)), product, build)
found = runs.runs()
if not found:
    print("(no local run of " + build + ")")
else:
    log = runs.dir / "runs" / found[0].run_id / "driver.log"
    lines = log.read_text(errors="replace").splitlines() if log.is_file() else ["(no driver.log)"]
    print(found[0].run_id + " " + str(found[0].raw_state) + ": " + " | ".join(lines[-15:]))
"""


class E2EError(jobs.JobError):
    pass


def new_run_id(now: Optional[float] = None) -> str:
    return f"e2e-{int(time.time() if now is None else now)}-{_secrets.token_hex(3)}"


def records(run_id: str) -> list:
    """The run's events, one JSON object per line: DATES[d] events on each date d, ids `<run>-<nn>`."""
    out, i = [], 0
    for day, count in DATES.items():
        for k in range(count):
            out.append(json.dumps({"event_id": f"{run_id}-{i:02d}", "run_id": run_id, "user_id": f"u{i % 4}",
                                   "event_time": f"{day}T09:{k:02d}:00.000Z"}, sort_keys=True))
            i += 1
    return out


def marks(logs: str) -> dict:
    return {m.group(1): m.group(2).strip() for m in _MARK.finditer(logs or "")}


def _json(found: dict, key: str) -> dict:
    try:
        value = json.loads(found[key])
    except (KeyError, ValueError) as exc:
        raise E2EError(f"no usable {key} line (missing or bad {exc})") from exc
    if not isinstance(value, dict):
        raise E2EError(f"{key} is not a JSON object")
    return value


def _is_int(*values) -> bool:
    return all(type(v) is int for v in values)


# --------------------------------------------------------------------------- the pods

def produce_job(name: str, image: str, run_id: str) -> dict:
    """The producer Job: Pod Security `restricted`, the Strimzi image's own console producer, no credentials."""
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": streaming.NAMESPACE,
                     "labels": {"app.kubernetes.io/part-of": "fluid-fabric-lab", JOB_LABEL: "e2e"}},
        "spec": {
            "backoffLimit": 0, "activeDeadlineSeconds": PRODUCE.deadline_s, "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": {JOB_LABEL: "e2e"}},
                "spec": {
                    "restartPolicy": "Never", "automountServiceAccountToken": False,
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "volumes": [{"name": "work", "emptyDir": {"sizeLimit": "16Mi"}}],
                    "containers": [{
                        "name": "produce", "image": image, "imagePullPolicy": "IfNotPresent",
                        "command": ["bash", "-c", PRODUCE_SCRIPT],
                        "env": [{"name": "PLAIN", "value": streaming.BOOTSTRAP_PLAIN}, {"name": "TOPIC", "value": TOPIC},
                                {"name": "RECORDS", "value": "\n".join(records(run_id))}],
                        "securityContext": {"runAsUser": streaming.KAFKA_UID, "runAsGroup": 0,
                                            "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                        "volumeMounts": [{"name": "work", "mountPath": "/work"}],
                        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "512Mi"}},
                    }],
                },
            },
        },
    }


def verify_job(name: str, image: str, run_id: str) -> dict:
    """The read-back Job on the spark tier's image, as the `spark` client, through the spark tier's own entry
    script (ConfigMap lab-spark: its properties file, the 0600 credential file, `--master local[2]`)."""
    secret = [{"name": k, "valueFrom": {"secretKeyRef": {"name": "spark-lakekeeper-client", "key": k}}}
              for k in ("LAKEKEEPER_CLIENT_ID", "LAKEKEEPER_CLIENT_SECRET")]
    restricted = {"runAsUser": SPARK_UID, "runAsGroup": SPARK_GID, "allowPrivilegeEscalation": False,
                  "capabilities": {"drop": ["ALL"]}}
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE,
                     "labels": {"app.kubernetes.io/part-of": "fluid-fabric-lab", JOB_LABEL: "e2e"}},
        "spec": {
            "backoffLimit": 0, "activeDeadlineSeconds": VERIFY.deadline_s, "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": {JOB_LABEL: "e2e"}},
                "spec": {
                    "restartPolicy": "Never", "serviceAccountName": "spark", "automountServiceAccountToken": False,
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "volumes": [{"name": "spark", "configMap": {"name": "lab-spark"}},
                                {"name": "e2e", "configMap": {"name": "lab-e2e"}},
                                {"name": "work", "emptyDir": {"sizeLimit": "512Mi"}},
                                {"name": "lab-ca", "configMap": {"name": "lab-ca-bundle"}}],
                    "containers": [{
                        "name": "spark", "image": image, "imagePullPolicy": "Never",
                        "command": ["bash", "/spark/lab-spark-entry.sh", "/e2e/verify.py", run_id],
                        "env": secret + [
                            {"name": "AWS_REGION", "value": "us-east-1"},
                            {"name": "JAVA_TOOL_OPTIONS", "value": "-Djavax.net.ssl.trustStore=/etc/lab-ca/truststore.p12"
                                                                  " -Djavax.net.ssl.trustStoreType=PKCS12"}],
                        "securityContext": restricted,
                        "volumeMounts": [{"name": "spark", "mountPath": "/spark", "readOnly": True},
                                         {"name": "e2e", "mountPath": "/e2e", "readOnly": True},
                                         {"name": "work", "mountPath": "/work"},
                                         {"name": "lab-ca", "mountPath": "/etc/lab-ca", "readOnly": True}],
                        "resources": {"requests": {"cpu": "500m", "memory": "2Gi"}, "limits": {"memory": "2560Mi"}},
                    }],
                },
            },
        },
    }


# --------------------------------------------------------------------------- fluid-host

class Host:
    """The fluid-host pod: copy the product in, run forge there, cleaned by accept_runner's wrapper."""

    def __init__(self, k: kube.Kube, pod_timeout_s: int = 180, workdir: str = WORKDIR):
        self.kube, self.pod_timeout_s, self.workdir = k, pod_timeout_s, workdir
        self._pod: Optional[str] = None

    def pod(self) -> str:
        if self._pod is None:
            self._pod = self.kube.first_pod(NAMESPACE, HOST_SELECTOR, self.pod_timeout_s)
        return self._pod

    def exec(self, argv: list, *, cwd: Optional[str] = None, input: Optional[str] = None,  # noqa: A002
             timeout: float = 120) -> proc.Result:
        command = ["sh", "-c", accept_runner.CD_WRAPPER, "sh", cwd, *argv] if cwd else list(argv)
        return self.kube.exec(NAMESPACE, self.pod(), command, input=input, timeout=timeout)

    def forge(self, argv: list, *, timeout: float) -> proc.Result:
        accept.strict_guard(argv)
        limit = max(int(timeout) - accept_runner.TIMEOUT_MARGIN_S, 1)
        command = ["sh", "-c", accept_runner.FORGE_WRAPPER, "sh", self.workdir, str(limit), *argv]
        return self.kube.exec(NAMESPACE, self.pod(), command, timeout=timeout)

    def copy(self, directory: Path) -> None:
        payload = base64.b64encode(accept_runner.product_tar(directory)).decode("ascii")
        res = self.exec(["sh", "-c", 'mkdir -p "$1" && base64 -d | tar -xf - -C "$1"', "sh", self.workdir],
                        input=payload, timeout=300)
        if not res.ok:
            raise E2EError(f"copying {PRODUCT_DIR} into fluid-host:{WORKDIR} failed: "
                           f"{accept.oneline(accept_runner.safe_tail(res, 400))}")

    def python(self, script: str, *args: str, timeout: float = 180) -> proc.Result:
        return self.exec(["python3", "-I", "-c", script, *args], timeout=timeout)


def forge_argv(step: str, run_id: str, allowlists: dict) -> list:
    """The forge command of one step, behind accept_runner.env_prefix (FLUID_ENV, FLUID_RUN_ID, strict, allowlists)."""
    commands = {
        "validate": ["fluid", "validate", CONTRACT, "--env", ENV, "--strict"],
        "plan": ["fluid", "plan", CONTRACT, "--env", ENV, "--out", PLAN],
        "apply": ["fluid", "apply", PLAN, "--env", ENV, "--yes"],
        "batch": ["fluid", "apply", CONTRACT, "--env", ENV, "--mode", "amend-and-build", "--build-id", BATCH_BUILD,
                  "--yes"],
    }
    return accept_runner.env_prefix(ENV, run_id, allowlists) + commands[step]


# --------------------------------------------------------------------------- Lakekeeper

# Who asks Lakekeeper about the product's tables: the `spark` client, whose builds create (and so own, on
# OpenFGA) the namespaces they write.
CATALOG_CLIENT = "spark"


def table_state(client, namespace: str, table: str) -> Optional[dict]:
    """The table's current snapshot as Lakekeeper reports it to the `spark` client: {snapshot, total, snapshots};
    None while the table does not exist."""
    auth = {"Authorization": f"Bearer {client.token(CATALOG_CLIENT)}"}
    cfg = client.https("GET", "lakekeeper.lab.test", f"{accept_runner.CATALOG_PATH}/config?warehouse={WAREHOUSE}",
                       headers=auth)
    body = cfg.json() if cfg.status == 200 else {}
    prefix = (body.get("overrides") or {}).get("prefix") or (body.get("defaults") or {}).get("prefix")
    if not prefix:
        raise E2EError(f"Lakekeeper has no warehouse {WAREHOUSE!r} (HTTP {cfg.status})")
    found = client.https("GET", "lakekeeper.lab.test",
                         f"{accept_runner.CATALOG_PATH}/{prefix}/namespaces/{namespace}/tables/{table}",
                         headers=auth, timeout=60)
    if found.status == 404:
        return None
    if found.status != 200:
        raise E2EError(f"Lakekeeper loadTable {namespace}.{table}: HTTP {found.status} {found.text(200)}")
    meta = (found.json() or {}).get("metadata") or {}
    current = meta.get("current-snapshot-id")
    snaps = [s for s in meta.get("snapshots") or [] if isinstance(s, dict)]
    snap = next((s for s in snaps if s.get("snapshot-id") == current), None) or {}
    try:
        total = int((snap.get("summary") or {}).get("total-records", 0))
    except (TypeError, ValueError):
        total = 0
    return {"snapshot": current, "total": total, "snapshots": len(snaps)}


# --------------------------------------------------------------------------- the judge

def judge(produced: dict, result: dict, run_id: str, *, daily_before: Optional[dict], daily_after: Optional[dict],
          stop: dict) -> dict:
    """The run's result; E2EError unless every condition of the module docstring held."""
    if not (_is_int(produced.get("rc"), produced.get("n")) and produced["rc"] == 0 and produced["n"] == EVENTS):
        raise E2EError(f"the producer did not send this run's {EVENTS} events: {produced}")
    raw, daily = result.get("raw") or {}, result.get("daily") or {}
    if result.get("run") != run_id:
        raise E2EError(f"the read-back answered for run {result.get('run')!r}, not {run_id!r}")
    if not (_is_int(raw.get("rows"), raw.get("event_ids")) and raw["rows"] == EVENTS and raw["event_ids"] == EVENTS):
        raise E2EError(f"{'.'.join(RAW)} holds {raw.get('rows')} row(s) with {raw.get('event_ids')} distinct id(s) "
                       f"for this run, expected exactly {EVENTS} of each: the stream lost or duplicated events")
    rows = daily.get("rows")
    if rows != EXPECTED or not all(_is_int(v) for v in (rows or {}).values()):
        raise E2EError(f"{'.'.join(DAILY)} holds {rows} for this run, expected {EXPECTED}")
    if daily_after is None or (daily_before or {}).get("snapshot") == daily_after.get("snapshot"):
        raise E2EError(f"{'.'.join(DAILY)} has no new snapshot after the batch: the batch wrote nothing")
    if not stop.get("stopped") or stop.get("active"):
        raise E2EError(f"the stream was not stopped cleanly through forge: {stop}")
    return {"raw_rows": raw["rows"], "daily": rows, "raw_snapshots": raw.get("snapshots"),
            "daily_snapshots": daily.get("snapshots"), "spark": result.get("spark"), "stopped": stop["stopped"]}


# --------------------------------------------------------------------------- the record

def spark_image(record: Path) -> str:
    """The spark tier's image tag `lab build spark-iceberg` recorded, or E2EError."""
    try:
        found = state.read_json(record)
    except (OSError, ValueError, state.StateError):
        found = {}
    entry = found.get(SPARK_IMAGE_ID) if isinstance(found, dict) else None
    tag = entry.get("tag") if isinstance(entry, dict) else None
    if not isinstance(tag, str) or not tag:
        raise E2EError("the spark tier's image is not in this lab", "Run `./lab build spark-iceberg --name <lab>`, "
                       "then `./lab up --tier e2e --name <lab>`.")
    return tag


def _redactor(secret: Optional[str]) -> Callable[[str], str]:
    def redact(text: str) -> str:
        return text.replace(secret, "<redacted>") if secret else text
    return redact


def run(k: kube.Kube, root: Path, lock: dict, client, record: Path, *, secret: Optional[str] = None,
        log: Callable[[str], None] = lambda _m: None, clock=time.monotonic, sleep=time.sleep) -> dict:
    """The `e2e` record: every step of the module docstring, in order. Raises E2EError."""
    run_id = new_run_id()
    image = spark_image(record)
    allowlists = accept.parse_operator_env(root / PRODUCT_DIR / "operator.env")
    if not allowlists:
        raise E2EError(f"{PRODUCT_DIR}/operator.env is absent or empty: forge would refuse the product")
    host = Host(k)
    redact = _redactor(secret)
    host.copy(root / PRODUCT_DIR)
    def forge_step(step: str) -> None:
        log(f"lab check: e2e: fluid {step} ...")
        res = host.forge(forge_argv(step, run_id, allowlists), timeout=TIMEOUT_S[step])
        if not res.ok:
            raise E2EError(f"`fluid {step}` exited {res.returncode} in fluid-host: "
                           f"{accept.oneline(accept_runner.safe_tail(res, 600))}")

    forge_step("validate")
    forge_step("plan")
    stop: dict = {}
    try:
        # Inside the try: an apply that started the stream and then failed must not leave it running.
        before = table_state(client, *RAW)
        forge_step("apply")
        log(f"lab check: e2e: producing {EVENTS} events for run {run_id} ...")
        name = f"lab-check-e2e-produce-{int(time.time())}-{_secrets.token_hex(3)}"
        done, why, text = jobs.run(k, PRODUCE, name, produce_job(name, streaming.kafka_ref(lock), run_id),
                                   clock=clock, sleep=sleep)
        if done != "succeeded":
            raise jobs.failure(PRODUCE, name, done, why, text, "produce")
        produced = _json(marks(text), "LAB-E2E-PRODUCED")
        if not (_is_int(produced.get("rc"), produced.get("n")) and produced["rc"] == 0 and produced["n"] == EVENTS):
            raise E2EError(f"the producer did not send this run's {EVENTS} events: {produced}: {text[-600:]}")
        log(f"lab check: e2e: waiting for the stream to land them in {'.'.join(RAW)} ...")
        want = (before or {}).get("total", 0) + EVENTS
        deadline, now = clock() + LAND_TIMEOUT_S, None
        while True:
            now = table_state(client, *RAW)
            if now is not None and now["total"] >= want:
                break
            if clock() >= deadline:
                tail = host.python(LOG, WORKDIR, PRODUCT_ID, STREAM_BUILD)
                raise E2EError(f"the stream did not land {EVENTS} events in {'.'.join(RAW)} within "
                               f"{LAND_TIMEOUT_S} s (table {now}): stream run "
                               f"{accept.oneline(redact(tail.stdout or tail.output_tail(400)), 900)}")
            sleep(POLL_S)
    finally:
        log("lab check: e2e: stopping the stream through forge ...")
        res = host.python(STOP, WORKDIR, PRODUCT_ID, STREAM_BUILD, timeout=240)
        found = marks(res.stdout or "")
        stop = json.loads(found["LAB-E2E-STOP"]) if "LAB-E2E-STOP" in found else {
            "error": accept.oneline(res.output_tail(300))}
    daily_before = table_state(client, *DAILY)
    log("lab check: e2e: fluid apply --mode amend-and-build --build-id daily-counts ...")
    res = host.forge(forge_argv("batch", run_id, allowlists), timeout=TIMEOUT_S["batch"])
    if not res.ok:
        tail = host.python(LOG, WORKDIR, PRODUCT_ID, BATCH_BUILD)
        raise E2EError(f"the batch `fluid apply --mode amend-and-build --build-id {BATCH_BUILD}` exited "
                       f"{res.returncode}: {accept.oneline(accept_runner.safe_tail(res, 400))}; batch run "
                       f"{accept.oneline(redact(tail.stdout or ''), 600)}")
    daily_after = table_state(client, *DAILY)
    log("lab check: e2e: reading both tables back ...")
    name = f"lab-check-e2e-verify-{int(time.time())}-{_secrets.token_hex(3)}"
    done, why, text = jobs.run(k, VERIFY, name, verify_job(name, image, run_id), redact=redact,
                               clock=clock, sleep=sleep)
    if done != "succeeded":
        raise jobs.failure(VERIFY, name, done, why, text, "spark")
    result = _json(marks(text), "LAB-E2E-RESULT")
    return {**judge(produced, result, run_id, daily_before=daily_before, daily_after=daily_after, stop=stop),
            "run": run_id, "image": image, "landed": now, "jobs": [name]}
