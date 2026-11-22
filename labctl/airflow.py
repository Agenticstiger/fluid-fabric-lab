# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The `orchestration` add-on tier: what `lab up` prepares for Airflow, and `lab check`'s proof.

Airflow here is the lab's scheduler (docs/orchestration-tier.md). Its DAGs are read from `s3://lab-airflow/dags/` on
SeaweedFS by an S3DagBundle; the product checkout `fluid apply` needs is read
from `s3://lab-airflow/projects/` by a sidecar. This module has:

  prepare       between the helmfile waves of `lab up`: the bucket, a marker object
                under each prefix (an S3DagBundle refuses a prefix that does not exist,
                so without one the dag-processor would crash before the first DAG),
                a check that the read-only identity `airflow` can list them, and the
                spark tier's spark-submit files copied into namespace airflow.
  run_probe     `lab check --tier orchestration`: upload a one-task DAG, wait until
                Airflow's REST API lists it, trigger it, require success and the marker
                its log (or its XCom, the same last line) carries, then clean it up.
  loaded_image  the airflow-fluid image `lab build` loaded into the node.

The REST API is reached through a port-forward to the api-server Service, with a
token from the lab's one Airflow login (SimpleAuthManager; its password is in OpenBao
and in Secret airflow-simple-auth).
"""

from __future__ import annotations

import contextlib
import json
import re
import secrets as _secrets
import time
from pathlib import Path
from typing import Callable, Optional

from labctl import airflow_image, build, credentials, http, kube, s3, state

NAMESPACE = "airflow"
IMAGE_ID = airflow_image.IMAGE
BUCKET = credentials.AIRFLOW_BUCKET
DAGS, PROJECTS, KEEP = "dags/", "projects/", ".lab-keep"
API_SERVICE, API_PORT = "svc/airflow-api-server", 8080
S3_DEPLOYMENT = ("seaweedfs", "deploy/seaweedfs-s3")
DAG_ID, TASK_ID = "lab_check_orchestration", "fluid_version"
PROBE_KEY = f"{DAGS}lab-check/{DAG_ID}.py"
PROBE_SOURCE = "platform/airflow/probe_dag.py"
SPARK_DIR = "platform/spark"
# What FLUID_SPARK_SUBMIT_* name, under /spark: published name -> file in platform/spark.
SPARK_FILES = {"lab-spark-submit": "lab-spark-submit", "spark-defaults.conf": "spark-defaults.apache.conf"}
VISIBLE_WAIT_S = 420       # the bundle refreshes every 30 s, then the dag-processor parses the file
RUN_WAIT_S = 900
STALE_WAIT_S = 150
SERVED_LOGS_ERROR = "Could not read served logs"
_MARK = re.compile(r"^LAB-ORCH (\{.*\})\s*$", re.M)


class AirflowError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


def _dict(value) -> dict:
    return value if isinstance(value, dict) else {}


# --------------------------------------------------------------------------- the image


def expected_forge(lock: dict) -> str:
    """The forge distribution version the image must carry (release version + `+g<commit 8>`)."""
    return build.forge_pin(build.lock_entry(lock, IMAGE_ID)["contents"])["local_version"]


def loaded_image(runner, lab: state.Lab, lock: dict) -> dict:
    """The record `lab build airflow-fluid` wrote, once the node is shown to hold those layers."""
    again = f"Run `./lab build {IMAGE_ID} --name {lab.name}`, then `./lab up` again if it was interrupted."
    want = build.tag_for(IMAGE_ID, build.lock_entry(lock, IMAGE_ID)["contents"])
    try:
        record = _dict(_dict(state.read_json(lab.images_record)).get(IMAGE_ID))
    except (OSError, ValueError, state.StateError):
        record = {}
    if record.get("tag") != want:
        raise AirflowError(f"{lab.images_record} has no {want} (the Airflow pods run it with imagePullPolicy Never)",
                           again)
    layers = record.get("layers")
    if not isinstance(layers, list) or not layers or build.node_layers(runner, lab, want) != layers:
        raise AirflowError(f"node {lab.node} does not hold {want} with the layers `lab build` recorded", again)
    return record


# --------------------------------------------------------------------------- lab up


def spark_configmap(root: Path) -> Optional[dict]:
    """ConfigMap `lab-spark` for namespace airflow: the spark tier's spark-submit and spark-defaults.

    A ConfigMap cannot be mounted across namespaces, so the scheduler mounts a copy. None when the
    checkout has no `lab-spark-submit` (it ships with the spark tier): Spark builds then
    cannot run in Airflow, and everything else can.
    """
    files = {}
    for name, source in SPARK_FILES.items():
        path = root / SPARK_DIR / source
        if path.is_file():
            files[name] = path.read_text(encoding="utf-8")
    if "lab-spark-submit" not in files:
        return None
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "lab-spark", "namespace": NAMESPACE,
                         "labels": {"app.kubernetes.io/part-of": "fluid-fabric-lab"}},
            "data": files}


def prepare(k: kube.Kube, client, creds: dict, root: Path, sleep: Callable = time.sleep) -> str:
    """Everything Airflow needs before its pods start (idempotent). `client` is a bootstrap.LabClient."""
    from labctl import bootstrap

    done = [bootstrap.bucket(k, BUCKET)]
    admin = s3.Bucket(client.https, BUCKET, creds["s3.admin.access"], creds["s3.admin.secret"])
    try:
        for prefix in (DAGS, PROJECTS):
            if admin.get(prefix + KEEP) is None:
                admin.put(prefix + KEEP, b"")
    except s3.S3Error as exc:
        raise AirflowError(f"could not write the prefix markers: {exc}",
                           "The S3 route and SeaweedFS must answer: `lab up` runs the core tier's waves first.") from exc
    done.append(f"{DAGS} and {PROJECTS} exist")
    reader = s3.Bucket(client.https, BUCKET, creds["s3.airflow.access"], creds["s3.airflow.secret"])
    for attempt in (1, 2):
        try:
            if DAGS + KEEP in reader.keys(DAGS):
                break
            raise AirflowError(f"identity `airflow` lists nothing under s3://{BUCKET}/{DAGS}")
        except s3.S3Error as exc:
            # SeaweedFS reads its identities at start: a lab that was up before this tier existed has not
            # loaded the new ones. Restart the S3 gateway once and ask again.
            if attempt == 2 or exc.status not in (403,):
                raise AirflowError(f"the read-only identity `airflow` cannot list s3://{BUCKET}/{DAGS}: {exc}",
                                   "Re-run `lab up`; its secrets step rewrites seaweedfs-s3-config.") from exc
            ns, target = S3_DEPLOYMENT
            k.run("-n", ns, "rollout", "restart", target)
            k.run("-n", ns, "rollout", "status", target, "--timeout=180s", timeout=210)
            sleep(5)
    done.append("identity `airflow` reads them")
    cm = spark_configmap(root)
    if cm is None:
        done.append("no platform/spark/lab-spark-submit in this checkout: Spark builds cannot run in Airflow yet")
    else:
        k.apply([cm])
        done.append(f"spark-submit files copied to ConfigMap {NAMESPACE}/lab-spark")
    return "Airflow: " + "; ".join(done)


# --------------------------------------------------------------------------- lab check


class Api:
    """Airflow's REST API (v2) through a local port, with the token of one login."""

    def __init__(self, port: int, send: Callable = http.request):
        self.base, self.send, self.token = f"http://127.0.0.1:{port}", send, ""

    def call(self, method: str, path: str, body=None, accept: str = "application/json",
             timeout: float = 60) -> http.Response:
        headers = {"Accept": accept}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return self.send(method, self.base + path, headers=headers, body=body, timeout=timeout)

    def login(self, user: str, password: str) -> None:
        resp = self.call("POST", "/auth/token", {"username": user, "password": password})
        token = _dict(_json(resp)).get("access_token") if resp.status in (200, 201) else None
        if not token:
            raise AirflowError(f"Airflow refused the login {user}: HTTP {resp.status} {resp.text(200)}",
                               "`kubectl -n airflow logs deploy/airflow-api-server` shows why; the password is "
                               "kv/lab/credentials airflow.admin_password.")
        self.token = token


def _json(resp: http.Response):
    try:
        return resp.json()
    except ValueError:
        return None


@contextlib.contextmanager
def api_session(k: kube.Kube, user: str, password: str):
    """A logged-in Api through a port-forward to the api-server Service."""
    with kube.PortForward(k, NAMESPACE, API_SERVICE, API_PORT) as fwd:
        api = Api(fwd.port)
        api.login(user, password)
        yield api


def probe_source(root: Path, nonce: str) -> bytes:
    text = (root / PROBE_SOURCE).read_text(encoding="utf-8")
    line = 'NONCE = "LAB_NONCE"'
    if text.count(line) != 1:
        raise AirflowError(f"{PROBE_SOURCE} must hold exactly one line {line}")
    return text.replace(line, f'NONCE = "{nonce}"').encode("utf-8")


def _log_text(api: Api, run: str, try_number: int) -> Optional[str]:
    path = f"/api/v2/dags/{DAG_ID}/dagRuns/{run}/taskInstances/{TASK_ID}/logs/{try_number}"
    resp = api.call("GET", path, accept="application/json")
    if resp.status != 200:
        return None
    body = _json(resp)
    content = body.get("content") if isinstance(body, dict) else None
    if not isinstance(content, list):
        text = resp.text(20000)
    else:
        text = "\n".join(str(_dict(item).get("event", "")) if isinstance(item, dict) else str(item)
                         for item in content)
    # The api-server answers 200 with this text when it cannot reach the pod that ran the task.
    return None if SERVED_LOGS_ERROR in text else text


def _xcom_text(api: Api, run: str) -> Optional[str]:
    resp = api.call("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run}/taskInstances/{TASK_ID}/xcomEntries/return_value")
    value = _dict(_json(resp)).get("value") if resp.status == 200 else None
    return value if isinstance(value, str) else None


def judge(text: str, nonce: str, forge: str) -> dict:
    """The marker the task printed; AirflowError unless it shows this run's nonce and the pinned forge."""
    found = _MARK.findall(text or "")
    if not found:
        raise AirflowError("the task's output has no LAB-ORCH marker line")
    try:
        mark = json.loads(found[-1])
    except ValueError as exc:
        raise AirflowError(f"the LAB-ORCH marker is not JSON: {found[-1][:200]}") from exc
    if mark.get("nonce") != nonce:
        raise AirflowError("the marker carries another run's nonce: the output is not from this check")
    if mark.get("forge") != forge:
        raise AirflowError(f"the image's forge is {mark.get('forge')}, the lock pins {forge}")
    fluid = str(mark.get("fluid") or "").strip()
    if not fluid:
        raise AirflowError("`fluid --version` printed nothing in the task")
    return {"forge": mark["forge"], "fluid_version": fluid[:120], "project_dir": mark.get("project_dir"),
            "dbt_profiles": mark.get("dbt_profiles"), "s3_endpoint": mark.get("s3_endpoint")}


def run_probe(api: Api, publish: s3.Bucket, source: bytes, nonce: str, forge: str, *,
              scheduler_log: Optional[Callable] = None,
              clock: Callable = time.monotonic, sleep: Callable = time.sleep) -> dict:
    """Upload the probe DAG, wait for the API to list it, run it, judge it, clean it up (always)."""
    held = {"uploaded": False}
    try:
        result = _probe(api, publish, source, nonce, forge, clock, sleep, held, scheduler_log)
    except BaseException as exc:
        note = _cleanup(api, publish, held["uploaded"], clock, sleep)
        if isinstance(exc, AirflowError):
            raise AirflowError(f"{exc} (clean-up: {note})", exc.advice) from exc
        raise
    return {**result, "cleanup": _cleanup(api, publish, held["uploaded"], clock, sleep)}


def _probe(api: Api, publish: s3.Bucket, source: bytes, nonce: str, forge: str, clock: Callable, sleep: Callable,
           held: dict, scheduler_log: Optional[Callable] = None) -> dict:
    # Set before the PUT: a request that times out may still have written the object, and clean-up must delete it.
    held["uploaded"] = True
    publish.put(PROBE_KEY, source)
    dag = _wait_listed(api, nonce, clock, sleep)
    paused = api.call("PATCH", f"/api/v2/dags/{DAG_ID}?update_mask=is_paused", {"is_paused": False})
    if paused.status != 200:
        raise AirflowError(f"could not unpause {DAG_ID}: HTTP {paused.status} {paused.text(200)}")
    made = api.call("POST", f"/api/v2/dags/{DAG_ID}/dagRuns", {"logical_date": None, "note": "lab check"})
    run = _dict(_json(made)).get("dag_run_id") if made.status in (200, 201) else None
    if not run:
        raise AirflowError(f"could not trigger {DAG_ID}: HTTP {made.status} {made.text(300)}")
    outcome, ti = _wait_run(api, run, clock, sleep)
    text, found_in = _evidence(api, run, ti)
    if outcome != "success":
        if not text and scheduler_log is not None:
            text, found_in = scheduler_log() or "", "scheduler pod log"
        tail = " | ".join(line for line in (text or "").splitlines()[-8:] if line.strip())
        raise AirflowError(f"run {run} of {DAG_ID} ended {outcome}" + (f" ({found_in}): {tail[-600:]}" if tail else ""),
                           f"`kubectl -n {NAMESPACE} logs deploy/airflow-scheduler -c scheduler` shows the task.")
    if text is None:
        raise AirflowError(f"run {run} succeeded, but neither its log nor its XCom could be read",
                           "The log is served by the scheduler pod to the api-server; check "
                           "`kubectl -n airflow logs deploy/airflow-api-server`.")
    return {**judge(text, nonce, forge), "run": run, "bundle": dag.get("bundle_name"), "evidence": found_in}


def _get(api: Api, path: str) -> tuple:
    """(response, "") or (None, why): a dropped connection is a retry, not the end of the check."""
    try:
        return api.call("GET", path), ""
    except http.HTTPError as exc:
        return None, str(exc)[:200]


def nonce_tag(nonce: str) -> str:
    return f"nonce-{nonce}"


def _wait_listed(api: Api, nonce: str, clock: Callable, sleep: Callable) -> dict:
    """Wait for Airflow to list THIS upload: the DAG carrying the run's nonce tag, not an earlier one."""
    deadline, last = clock() + VISIBLE_WAIT_S, "no answer"
    while True:
        resp, err = _get(api, f"/api/v2/dags/{DAG_ID}")
        if resp is None:
            last = err
        elif resp.status == 200:
            body = _dict(_json(resp))
            tags = {_dict(t).get("name") for t in body.get("tags") or []}
            if not body.get("is_stale") and nonce_tag(nonce) in tags:
                return body
            last = "listed, but not this upload (an earlier copy or no nonce tag yet)"
        else:
            last = f"HTTP {resp.status} {resp.text(120)}"
        if clock() > deadline:
            raise AirflowError(f"Airflow did not list {DAG_ID} of this check within {VISIBLE_WAIT_S} s of its upload "
                               f"(last: {last})",
                               f"`kubectl -n {NAMESPACE} logs deploy/airflow-dag-processor` shows the bundle's "
                               "refresh and any import error.")
        sleep(10)


def _wait_run(api: Api, run: str, clock: Callable, sleep: Callable) -> tuple:
    deadline, ti, last = clock() + RUN_WAIT_S, {}, "no answer"
    while True:
        resp, err = _get(api, f"/api/v2/dags/{DAG_ID}/dagRuns/{run}")
        outcome = _dict(_json(resp)).get("state") if resp is not None and resp.status == 200 else None
        last = err or (f"state {outcome}" if outcome else f"HTTP {resp.status}")
        if outcome in ("success", "failed"):
            got, _err = _get(api, f"/api/v2/dags/{DAG_ID}/dagRuns/{run}/taskInstances/{TASK_ID}")
            ti = _dict(_json(got)) if got is not None and got.status == 200 else {}
            return outcome, ti
        if clock() > deadline:
            raise AirflowError(f"run {run} of {DAG_ID} was not finished after {RUN_WAIT_S} s (last: {last})",
                               f"`kubectl -n {NAMESPACE} logs deploy/airflow-scheduler -c scheduler` shows why.")
        sleep(5)


def _evidence(api: Api, run: str, ti: dict) -> tuple:
    """The task's printed text and where it came from: its log, else its XCom (the same last line)."""
    text = _log_text(api, run, int(ti.get("try_number") or 1))
    if text and _MARK.search(text):
        return text, "task log"
    xcom = _xcom_text(api, run)
    if xcom and _MARK.search(xcom):
        return (text or "") + "\n" + xcom, "task XCom (its log was not served)" if not text else "task log and XCom"
    return text, "task log"


def _cleanup(api: Api, publish: s3.Bucket, uploaded: bool, clock: Callable, sleep: Callable) -> str:
    """Remove the DAG file from S3 (always), then, best effort, its records from Airflow."""
    notes = []
    try:
        if uploaded:
            publish.delete(PROBE_KEY)
            notes.append(f"s3://{BUCKET}/{PROBE_KEY} deleted")
    except (s3.S3Error, http.HTTPError) as exc:
        return f"the probe DAG file could NOT be deleted ({exc}); remove s3://{BUCKET}/{PROBE_KEY}"
    deadline = clock() + STALE_WAIT_S
    try:
        while uploaded:
            resp = api.call("GET", f"/api/v2/dags/{DAG_ID}")
            if resp.status == 404:
                notes.append("Airflow holds no records of it")
                break
            if _dict(_json(resp)).get("is_stale"):
                gone = api.call("DELETE", f"/api/v2/dags/{DAG_ID}")
                notes.append("its Airflow records deleted" if gone.status in (200, 204, 404)
                             else f"its Airflow records stay (HTTP {gone.status})")
                break
            if clock() > deadline:
                notes.append("Airflow still lists the DAG until its bundle refreshes")
                break
            sleep(10)
    except (http.HTTPError, AirflowError) as exc:
        notes.append(f"Airflow records not cleaned: {exc}")
    return "; ".join(notes)


def new_nonce() -> str:
    return _secrets.token_hex(6)


def assumerole_refused(send: Callable, creds: dict) -> str:
    """The Airflow identities cannot assume the warehouse's vended role; Lakekeeper's own key can (positive control).

    SeaweedFS lets an identity assume a role only when the role's trust policy names it
    (credentials.LAKEKEEPER_PRINCIPAL). `airflow` is in every Airflow pod and `airflow-publish` uploads DAGs:
    neither may reach the warehouse bucket through STS.
    """
    control = s3.assume_role(send, creds["s3.lakekeeper.access"], creds["s3.lakekeeper.secret"])
    if control.status != 200:
        raise AirflowError(f"the positive control failed: Lakekeeper's own key could not assume {credentials.STS_ROLE_ARN}"
                           f" (HTTP {control.status} {control.text(200)}), so a refusal below would prove nothing")
    for name in ("airflow", "airflow_publish"):
        got = s3.assume_role(send, creds[f"s3.{name}.access"], creds[f"s3.{name}.secret"])
        if got.status == 200 or b"AccessKeyId" in got.body:
            raise AirflowError(f"identity {name.replace('_', '-')} assumed {credentials.STS_ROLE_ARN}: it can read and "
                               "write the whole warehouse bucket, past Lakekeeper and OpenFGA",
                               "Narrow the role's trust policy (credentials.LAKEKEEPER_PRINCIPAL) and re-run `lab up`.")
        if got.status not in (400, 403):
            raise AirflowError(f"AssumeRole as {name.replace('_', '-')} got HTTP {got.status}, not a refusal: "
                               f"{got.text(200)}")
    return (f"AssumeRole of {credentials.STS_ROLE_ARN} was refused for identities airflow and airflow-publish "
            "(HTTP 403), and accepted for Lakekeeper's own key (positive control; its credentials not shown)")
