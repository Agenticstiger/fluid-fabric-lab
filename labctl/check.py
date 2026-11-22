# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab check`: does the tier work, on real evidence?

One record per criterion, (a) to (j). Three of them are split into
halves that are proven separately: (a) the catalog and Trino, (b) JWT and
OAuth2 browser login, (h) the mirror and egress. Three more records make the evidence mean something: every
workload is running, the machine has the memory and the disk, and the runner
image that `lab build` loaded is in the node.

A criterion this lab cannot prove yet FAILs and says what is missing, so
`lab check` stays red until every criterion passes on evidence. A record that
cannot run may be skipped: it says so and proves nothing, and the run does not
pass (exit 1), since a skip is never a pass.

What `lab check` changes, and nothing else:
- namespace `lab_check` and table `lab_check.probe` in warehouse sales-wh,
  created once, by lab-admin; for (g), lab-admin also creates and deletes
  namespace `lab_check_admin` as the positive control;
- for (b-jwt), Trino statements as `fluid-pipeline` (SELECT 1, SHOW SCHEMAS,
  and a SHOW TABLES that OPA must refuse) and requests that Trino must refuse
  with 401; no table, schema or other state is changed;
- the lab's OpenBao container, restarted for (e) and unsealed again from the
  stored shares. A sealed OpenBao (after a host restart, say) is unsealed
  the same way before anything is read; `lab check` never initialises or
  reconfigures it.

Secrets never reach a record: vended credentials are reported by key name
only, and only key names of Secrets are read.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence

from labctl import (airflow, bootstrap, build, cluster, config, containers, credentials, doctor, down, e2e, http,
                    jobs, kube, mirror, openbao, pki, proc, s3, spark, state, streaming)
from labctl.output import FAIL, PASS, SKIP, check, passed

CREDENTIALS_PATH = "lab/credentials"   # kv/data/lab/credentials, as `lab up` writes it
NAMESPACE, TABLE = "lab_check", "probe"
DENIED_NAMESPACE, ADMIN_NAMESPACE = "lab_check_denied", "lab_check_admin"
TABLE_SCHEMA = {"type": "struct", "schema-id": 0,
                "fields": [{"id": 1, "name": "x", "required": False, "type": "long"}]}
VENDED = {"X-Iceberg-Access-Delegation": "vended-credentials"}
VENDED_KEYS = ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
# Lakekeeper hides a warehouse from a caller who may not see it (v0.14.0).
HIDDEN = "NoSuchWarehouseException"
# kind's own system pods run images preloaded in kindest/node, never pulled.
KIND_NAMESPACES = ("kube-system", "local-path-storage")
KIND_IMAGES = ("registry.k8s.io/etcd", "registry.k8s.io/kube-apiserver", "registry.k8s.io/kube-controller-manager",
               "registry.k8s.io/kube-scheduler", "registry.k8s.io/kube-proxy", "registry.k8s.io/coredns/coredns",
               "docker.io/kindest/kindnetd", "docker.io/kindest/local-path-provisioner",
               "docker.io/kindest/local-path-helper")   # the provisioner's short-lived helper pods
# What each endpoint answers when it is its backend answering (not the Gateway's own 404).
ENDPOINTS = {
    "keycloak.lab.test": (f"/realms/{bootstrap.REALM}/.well-known/openid-configuration", {200}),
    "lakekeeper.lab.test": ("/health", {200}),
    "s3.lab.test": ("/", {200, 403}),          # SeaweedFS refuses an anonymous bucket list
    "trino.lab.test": ("/v1/info", {200}),
}
# Endpoints an add-on tier adds to the Gateway, checked by (i) on a tier that includes the release (none today).
ADDON_ENDPOINTS: dict = {}
STATIC_ENV = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}
_STATIC_KEY = re.compile(r"^\s*[\w.-]*s3\.aws-(access|secret)-key\s*=", re.M)   # hive.s3.aws-... too
# Key names of a Secret, never its values (the values are not even fetched).
SECRET_KEYS = "go-template={{range $k, $v := .data}}{{$k}}{{\"\\n\"}}{{end}}"
KV2_RECORD = ("docs/proven.md", "Lakekeeper's KV2 secret backend against OpenBao", "FAILED")
CRITERIA = {
    "ready": "every workload of the tier is running",
    "capacity": "the machine has the tier's memory and the free disk",
    "runner": "the fluid-runner image `lab build` checked is in the node",
    "a-catalog": "(a) the catalog probe gets temporary storage credentials; no static keys in Trino",
    "a-trino": "(a) Trino reads Lakekeeper tables with vended credentials",
    "b-jwt": "(b) Trino accepts Keycloak JWTs for the CLI and dbt, refuses the rest, and OPA decides per principal",
    "b-oauth2": "(b) Trino accepts Keycloak OAuth2 browser logins",
    "c": "(c) a forge-shaped Rego package filters rows and masks columns; a failed group lookup denies",
    "d": "(d) a KafkaConnector writes Iceberg through OAuth2 and vended credentials; a no-ACL user is denied",
    "e": "(e) OpenBao survives a restart with its keys; Lakekeeper's KV2 backend works or is recorded as failed",
    "f": "(f) OPA polls a signed bundle from SeaweedFS with a static read-only key",
    "g": "(g) Lakekeeper on OpenFGA denies an unauthorized request",
    "h-mirror": "(h) installation used only the local mirror",
    "h-egress": "(h) default-deny egress holds and the canary passes",
    "i": "(i) every endpoint is HTTPS, a wrong CA is rejected, Pod Security is enforced as recorded",
    "j": "(j) an apply run by the generated Airflow DAG succeeds through the env allowlist",
    "spark": "spark tier: forge's `fluid apply` runs a Spark batch on the lab's open Spark image (spark-iceberg) "
             "and writes Iceberg through Lakekeeper with vended credentials",
    "orchestration": "orchestration tier: Airflow lists a DAG uploaded to SeaweedFS and runs it, with forge, in the airflow-fluid image",
    "streaming": "streaming tier: records cross Kafka's plain listener intact; a SCRAM user with an ACL writes over TLS and one without is refused; anonymous clients on plain cannot reach the cluster or an ACL topic",
    "e2e": "e2e tier: products/stream-batch runs through forge (validate, plan, apply --env local): a Spark stream lands a Kafka topic in Iceberg and a Spark SQL batch counts it, exactly this run's rows",
}
# Records of an add-on tier: checked only on a tier that includes the add-on.
ADDON_RECORDS = {"spark": "lab-spark", "orchestration": "airflow",
                 "streaming": "strimzi-kafka-operator", "e2e": "lab-e2e"}
# Every probe Job of `lab check` carries this label; its own record judges it, not `ready`.
PROBE_JOB_LABEL = jobs.CHECK_LABEL
# Pods owned by one of these are one-off runs (a probe Job's pod): their own record
# judges them, so a finished or failed one never fails `ready`.
RUN_OWNERS = {"Job"}
# What each unproven criterion needs. It FAILs with this reason until its probe exists and passes.
NOT_YET = {
    "a-trino": "no Trino read of a table through the vended credentials has run: `b-jwt` proves a pipeline "
               "identity may query Trino, but not that it reads Iceberg data files; the lab seeds no events_raw table",
    "b-oauth2": "Trino's browser login (http-server.authentication.type OAUTH2 beside JWT) is not configured, and "
                "it needs a confidential realm client for the web UI and a browser to prove it",
    "c": "no forge-shaped rows/masks package exists: Trino's lab policy (platform/policy/trino-lab) returns no "
         "row filters and no column masks",
    "d": "Kafka is up (`lab up --tier streaming`) but the KafkaConnector needs KafkaConnect on the connect-iceberg "
         "image, which the lab does not build",
    "f": "OPA reads its policy from a ConfigMap; no signed bundle is served from SeaweedFS",
    "j": "needs the orchestration tier (`lab up --tier orchestration`, then `lab check --tier orchestration`), whose "
         "`orchestration` record proves a DAG from S3 runs in the airflow-fluid image; (j) itself, forge's generated "
         "apply DAG running a real contract through the env allowlist, is not run",
}


class CheckError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


class Skipped(CheckError):
    """A record whose prerequisite the operator chose not to provide (see labctl.output)."""


@dataclass
class Ctx:
    root: Path
    lab: state.Lab
    runner: proc.Runner
    kube: kube.Kube
    lock: dict
    tiers_doc: dict
    tier_name: str
    tier: config.Tier
    creds: dict = field(default_factory=dict)
    bao: Optional[openbao.Bao] = None
    client: Optional[bootstrap.LabClient] = None
    missing: dict = field(default_factory=dict)   # what could not be set up, and why
    env: Mapping[str, str] = field(default_factory=dict)   # the launcher's environment (an add-on's token)
    log: Callable[[str], None] = lambda _m: None              # progress lines for a person (stderr)

    @property
    def cafile(self) -> Path:
        return self.lab.state / "ca" / "ca.crt"

    def need(self, *what: str) -> None:
        for item in what:
            if item in self.missing:
                raise CheckError(f"needs {item}, which could not be reached: {self.missing[item]}")

    def json(self, *args: str) -> dict:
        return json.loads(self.kube.run(*args, "-o", "json").stdout)

    def items(self, *args: str) -> list:
        return [i for i in self.json(*args).get("items") or [] if isinstance(i, dict)]


# --------------------------------------------------------------------------- pure helpers


def _dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _credential_configs(body: dict) -> list:
    return [_dict(body.get("config"))] + [_dict(_dict(c).get("config")) for c in body.get("storage-credentials") or []]


def vended_keys(body: dict) -> set:
    """The storage-credential keys a loadTable answer carries (`config` and `storage-credentials`)."""
    return {k for cfg in _credential_configs(body) for k in cfg}


def vended_access_keys(body: dict) -> set:
    return {cfg["s3.access-key-id"] for cfg in _credential_configs(body) if cfg.get("s3.access-key-id")}


def not_ready(objects: list, namespaces: set) -> list:
    """Deployments, StatefulSets and DaemonSets in `namespaces` with fewer ready replicas than they want."""
    late = []
    for obj in objects:
        meta, spec, status = _dict(obj.get("metadata")), _dict(obj.get("spec")), _dict(obj.get("status"))
        if meta.get("namespace") not in namespaces:
            continue
        if obj.get("kind") == "DaemonSet":
            want, have = status.get("desiredNumberScheduled") or 0, status.get("numberReady") or 0
        else:
            want, have = spec.get("replicas", 1) or 0, status.get("readyReplicas") or 0
        if have < want:
            late.append(f"{meta.get('namespace')}/{str(obj.get('kind')).lower()}/{meta.get('name')} {have}/{want}")
    return late


CONTROLLERS = {"ReplicaSet", "StatefulSet", "DaemonSet"}


def _owner_kinds(obj: dict) -> set:
    return {o.get("kind") for o in _dict(obj.get("metadata")).get("ownerReferences") or [] if isinstance(o, dict)}


def pod_problems(pods: list, jobs: list, namespaces: set, on_demand: frozenset = frozenset()) -> list:
    """Every way the tier's pods fall short: an empty namespace (other than an on-demand
    one), a pod not running and ready (operator-made pods such as CNPG's Postgres
    included), a Job that failed. A probe Job of `lab check` (PROBE_JOB_LABEL) is judged
    by its own record, not here: a failed one stays an hour for its logs."""
    problems, seen = [], set()
    for pod in pods:
        meta, status = _dict(pod.get("metadata")), _dict(pod.get("status"))
        ns = meta.get("namespace")
        if ns not in namespaces or _owner_kinds(pod) & RUN_OWNERS:
            continue
        if ns == spark.NAMESPACE and spark.SUBMIT_LABEL in (_dict(meta.get("labels"))):
            continue  # a build pod of `fluid apply --env local`: its wrapper owns and deletes it
        if status.get("phase") in ("Succeeded", "Failed") and _owner_kinds(pod) & CONTROLLERS:
            continue  # an evicted or finished pod its controller has replaced; not_ready() judges the controller
        seen.add(ns)
        ready = all(_dict(c).get("ready") for c in status.get("containerStatuses") or [{}])
        if status.get("phase") != "Running" or not ready:
            problems.append(f"pod {ns}/{meta.get('name')} {status.get('phase')}{'' if ready else ', not ready'}")
    for job in jobs:
        meta, status = _dict(job.get("metadata")), _dict(job.get("status"))
        if meta.get("namespace") in on_demand or PROBE_JOB_LABEL in _dict(meta.get("labels")):
            continue  # the check that runs these Jobs judges them (namespace kafka also has standing pods)
        if meta.get("namespace") in namespaces and not (status.get("succeeded") or status.get("active")):
            problems.append(f"job {meta.get('namespace')}/{meta.get('name')} failed")
    problems += [f"namespace {ns} has no running workload" for ns in sorted(namespaces - seen - on_demand)]
    return problems


def pod_images(pods: list) -> list:
    """(namespace, image) for every container and init container."""
    out = []
    for pod in pods:
        ns = _dict(pod.get("metadata")).get("namespace")
        spec = _dict(pod.get("spec"))
        out += [(ns, _dict(c).get("image", "")) for c in (spec.get("containers") or []) + (spec.get("initContainers") or [])]
    return out


def repository(ref: str) -> str:
    """`registry/repo` of an image reference, without tag or digest."""
    name = ref.split("@", 1)[0]
    head, _, last = name.rpartition("/")
    return f"{head}/{last.split(':', 1)[0]}" if head else last.split(":", 1)[0]


def unpinned(images: list, lock: dict, built: set) -> list:
    """Images not pinned by the lock digest of their own repository, not a lab-built image
    `lab build` loaded, and (in kind's system namespaces) not one of kind's own images."""
    pins = {}
    for i in lock.get("images") or []:
        if isinstance(i, dict) and i.get("digest"):
            pins.setdefault(i["digest"], set()).add(f"{i.get('registry')}/{i.get('repository')}")
    bad = set()
    for ns, ref in images:
        if ref in built:
            continue
        if ns in KIND_NAMESPACES:
            if repository(ref) not in KIND_IMAGES:
                bad.add(f"{ns}: {ref} (not one of kind's own images)")
            continue
        digest = ref.split("@", 1)[1] if "@" in ref else None
        if repository(ref) not in pins.get(digest, set()):
            bad.add(f"{ns}: {ref}")
    return sorted(bad)


def parse_hosts(listing: str) -> dict:
    """`=== <path>` blocks printed from the node, as {registry directory: hosts.toml text}."""
    out, current, lines = {}, None, []
    for line in listing.splitlines(keepends=True):
        m = re.match(r"^=== /etc/containerd/certs\.d/([^/]+)/hosts\.toml\s*$", line)
        if m:
            if current is not None:
                out[current] = "".join(lines)
            current, lines = m.group(1), []
        elif current is not None:
            lines.append(line)
    if current is not None:
        out[current] = "".join(lines)
    return out


def expected_hosts(lock: dict, tiers_doc: dict) -> dict:
    """What `lab up` writes into the node: every upstream to the mirror, `_default` to a closed port."""
    want = {up: cluster.hosts_toml(up) for up in mirror.upstream_hosts(mirror.all_images(lock))}
    want["_default"] = cluster.default_hosts_toml()
    return want


def pss_problems(live: dict, recorded: dict) -> list:
    """Namespaces whose enforced Pod Security level is not the recorded one, or lacks its exemption."""
    problems = []
    for name, spec in recorded.items():
        want = spec.get("pod_security", "restricted")
        have = _dict(_dict(live.get(name)).get("labels")).get("pod-security.kubernetes.io/enforce")
        if have != want:
            problems.append(f"{name} enforces {have or 'nothing'}, recorded {want}")
        elif want != "restricted" and not spec.get("exemption"):
            problems.append(f"{name} enforces {want} with no recorded exemption")
    return problems


def endpoint_problems(answers: dict, endpoints: Optional[dict] = None) -> list:
    """Endpoints whose status is not what their backend answers ({host: status})."""
    return [f"{host} answered HTTP {answers.get(host)}, expected {'/'.join(map(str, sorted(want)))}"
            for host, (_path, want) in (ENDPOINTS if endpoints is None else endpoints).items()
            if answers.get(host) not in want]


def tier_endpoints(tier: config.Tier) -> dict:
    """The Gateway endpoints of a tier: the core ones, plus those of each add-on release it includes."""
    found = dict(ENDPOINTS)
    for release, hosts in ADDON_ENDPOINTS.items():
        if release in tier.releases:
            found.update(hosts)
    return found


def tier_namespaces(ctx: Ctx) -> dict:
    return cluster.tier_namespaces(ctx.root, ctx.runner, ctx.tiers_doc, ctx.tier_name)


def _json(res: http.Response):
    try:
        return res.json()
    except ValueError:
        return None


def _error_type(res: http.Response) -> str:
    return str(_dict(_dict(_json(res)).get("error")).get("type") or "")


# --------------------------------------------------------------------------- criteria


def ready(ctx: Ctx) -> str:
    recorded = tier_namespaces(ctx)
    nss = set(recorded)
    on_demand = frozenset(n for n, s in recorded.items() if s.get("workloads") == "on-demand")
    late = not_ready(ctx.items("get", "deploy,sts,ds", "-A"), nss)
    pods = ctx.items("get", "pods", "-A")
    late += pod_problems(pods, ctx.items("get", "jobs", "-A"), nss, on_demand)
    if late:
        raise CheckError(f"{len(late)} problem(s): {', '.join(late[:8])}",
                         f"`kubectl --kubeconfig {ctx.lab.kubeconfig} get pods -A` shows why; `lab up` resumes.")
    running = sum(1 for p in pods if _dict(p.get("metadata")).get("namespace") in nss and "Job" not in _owner_kinds(p))
    demand = f" ({', '.join(sorted(on_demand))} runs Jobs on demand)" if on_demand else ""
    return (f"each of the tier's {len(nss) - len(on_demand)} standing namespaces runs workloads{demand}, and all "
            f"{running} pods are running and ready (operator-made pods such as CNPG's Postgres included); no Job failed")


def capacity(ctx: Ctx) -> str:
    rt_check, vm_bytes, _ = doctor.probe_runtime("docker", ctx.runner)
    if rt_check["status"] == FAIL:
        raise CheckError(rt_check["message"], rt_check.get("advice", ""))
    all_tiers = {n: config.resolve_tier(ctx.tiers_doc, n) for n in config.tier_names(ctx.tiers_doc)}
    mem = doctor.memory_checks(ctx.tier, all_tiers, vm_bytes, None, "docker", None)[0]
    disk = doctor.disk_check(ctx.root, doctor.DEFAULT_MIN_FREE_DISK_GIB)
    short = [c for c in (mem, disk) if c["status"] == FAIL]
    if short:
        raise CheckError("; ".join(c["message"] for c in short), short[0].get("advice", ""))
    return f"{mem['message']}; {disk['message']} (on the volume that holds the repository)"


def runner_image(ctx: Ctx) -> str:
    again = f"Run `./lab build fluid-runner --name {ctx.lab.name}`."
    try:
        record = _dict(_dict(state.read_json(ctx.lab.images_record)).get("fluid-runner"))
    except (OSError, ValueError, state.StateError):
        record = {}
    layers = record.get("layers")
    if not record.get("tag") or not isinstance(layers, list) or not layers:
        raise CheckError(f"no usable fluid-runner record in {ctx.lab.images_record}", again)
    node = ctx.runner(["docker", "inspect", "--format", "{{.Id}}", ctx.lab.node], timeout=60)
    if not node.ok or node.stdout.strip() != record.get("node_id"):
        raise CheckError(f"the record is for another node than {ctx.lab.node} (was the cluster recreated?)", again)
    if build.node_layers(ctx.runner, ctx.lab, record["tag"]) != layers:
        raise CheckError(f"node {ctx.lab.node} does not hold the recorded layers of {record['tag']}", again)
    versions = _dict(record.get("versions"))
    return (f"{record['tag']} is in node {ctx.lab.node} with the {len(layers)} layers `lab build` checked "
            f"(forge {versions.get('data-product-forge')}, OpenTofu {versions.get('opentofu')})")


def _prefix(client, auth: dict) -> str:
    resp = client.https("GET", "lakekeeper.lab.test", f"/catalog/v1/config?warehouse={bootstrap.WAREHOUSE}",
                        headers=auth)
    # The REST spec lets the server put `prefix` in `overrides` or `defaults`; Lakekeeper
    # v0.14.0 uses `defaults` (seen 2026-11-18). An override wins.
    body = _dict(_json(resp)) if resp.status == 200 else {}
    prefix = _dict(body.get("overrides")).get("prefix") or _dict(body.get("defaults")).get("prefix")
    if not prefix:
        raise CheckError(f"Lakekeeper /catalog/v1/config?warehouse={bootstrap.WAREHOUSE}: HTTP {resp.status} "
                         f"{resp.text(300)}")
    return prefix


def a_catalog(ctx: Ctx) -> str:
    ctx.need("credentials", "gateway")
    client = ctx.client
    auth = {"Authorization": f"Bearer {client.token('lab-admin')}"}
    base = f"/catalog/v1/{_prefix(client, auth)}/namespaces"
    resp = client.https("POST", "lakekeeper.lab.test", base, headers=auth, body={"namespace": [NAMESPACE]})
    if resp.status not in (200, 409):
        raise CheckError(f"namespace {NAMESPACE}: HTTP {resp.status} {resp.text(300)}")
    resp = client.https("POST", "lakekeeper.lab.test", f"{base}/{NAMESPACE}/tables", headers={**auth, **VENDED},
                        body={"name": TABLE, "schema": TABLE_SCHEMA}, timeout=60)
    if resp.status not in (200, 409):
        raise CheckError(f"table {NAMESPACE}.{TABLE}: HTTP {resp.status} {resp.text(300)}")
    resp = client.https("GET", "lakekeeper.lab.test", f"{base}/{NAMESPACE}/tables/{TABLE}",
                        headers={**auth, **VENDED}, timeout=60)
    if resp.status != 200:
        raise CheckError(f"loadTable {NAMESPACE}.{TABLE}: HTTP {resp.status} {resp.text(300)}")
    body = _dict(_json(resp))
    missing = [k for k in VENDED_KEYS if k not in vended_keys(body)]
    if missing:
        raise CheckError(f"loadTable answered without vended credentials ({', '.join(missing)} missing)",
                         "The warehouse needs `sts-enabled` and SeaweedFS's STS (platform/components/seaweedfs).")
    if ctx.creds.get("s3.lakekeeper.access") in vended_access_keys(body):
        raise CheckError("loadTable handed out the warehouse's own static key, not a temporary one")
    _trino_asks_for_vended(ctx)
    static = _trino_static_keys(ctx)
    if static:
        raise CheckError(f"Trino holds static S3 keys: {', '.join(static)}")
    return (f"loadTable {NAMESPACE}.{TABLE} as lab-admin issued {', '.join(VENDED_KEYS)}, a temporary credential "
            "(not the warehouse's static key; values not shown); Trino's lakehouse catalog asks for vended "
            "credentials, and its catalog, pods and the Secrets they load hold no static S3 key")


def _trino_asks_for_vended(ctx: Ctx) -> None:
    for cm in ctx.items("get", "configmaps", "-n", "trino"):
        text = _dict(cm.get("data")).get("lakehouse.properties") or ""
        if re.search(r"^\s*iceberg\.rest-catalog\.vended-credentials-enabled\s*=\s*true\s*$", text, re.M):
            return
    raise CheckError("Trino has no lakehouse catalog that asks for vended credentials "
                     "(lakehouse.properties with iceberg.rest-catalog.vended-credentials-enabled=true)")


def _trino_static_keys(ctx: Ctx) -> list:
    found = [f"ConfigMap {cm['metadata']['name']}/{key}" for cm in ctx.items("get", "configmaps", "-n", "trino")
             for key, text in _dict(cm.get("data")).items() if _STATIC_KEY.search(str(text or ""))]
    secrets = set()
    for pod in ctx.items("get", "pods", "-n", "trino"):
        spec = _dict(pod.get("spec"))
        for c in (spec.get("containers") or []) + (spec.get("initContainers") or []):
            names = {_dict(e).get("name") for e in _dict(c).get("env") or []}
            found += [f"pod {pod['metadata']['name']} env {n}" for n in sorted(names & STATIC_ENV)]
            secrets |= {_dict(_dict(s).get("secretRef")).get("name") for s in _dict(c).get("envFrom") or []}
    for name in sorted(n for n in secrets if n):
        keys = set(ctx.kube.run("get", "secret", name, "-n", "trino", "-o", SECRET_KEYS).stdout.split())
        found += [f"Secret {name} key {k}" for k in sorted(keys & STATIC_ENV)]
    return found


def _kv2_recorded(root: Path) -> bool:
    path, row, verdict = KV2_RECORD
    try:
        text = (root / path).read_text(encoding="utf-8")
    except OSError:
        return False
    return any(row in line and verdict in line for line in text.splitlines())


def e_openbao(ctx: Ctx) -> str:
    ctx.need("credentials")
    found = containers.inspect(ctx.runner, ctx.lab.openbao) or {}
    mounts = {m.get("Name") for m in found.get("Mounts") or [] if m.get("Destination") == "/openbao/file"}
    if ctx.lab.openbao_volume not in mounts:
        raise CheckError(f"{ctx.lab.openbao} does not keep its data on volume {ctx.lab.openbao_volume}")
    if not _kv2_recorded(ctx.root):
        raise CheckError(f"Lakekeeper's KV2 backend neither runs against OpenBao nor is recorded as failed in "
                         f"{KV2_RECORD[0]}")
    before = ctx.bao.kv_get(openbao.KV_MOUNT, CREDENTIALS_PATH)
    res = ctx.runner(["docker", "restart", ctx.lab.openbao], timeout=180)
    if not res.ok:
        raise CheckError(f"docker restart {ctx.lab.openbao}: {res.output_tail(300).strip()}")
    unseal = f"OpenBao may be left sealed; `./lab up --name {ctx.lab.name}` unseals it from the stored shares."
    try:
        bao = openbao.Bao(openbao.host_port(ctx.runner, ctx.lab.openbao), str(ctx.cafile))
        bao.wait_up()
        sealed = bao.health().get("sealed")
        step = openbao.open_existing(bao, ctx.lab)
    except (openbao.BaoError, http.HTTPError) as exc:
        raise CheckError(f"after the restart: {exc}", unseal) from exc
    ctx.bao = bao
    if not sealed:
        raise CheckError(f"{ctx.lab.openbao} came back unsealed after a restart, so its keys are not only in the "
                         "lab's stored shares")
    after = bao.kv_get(openbao.KV_MOUNT, CREDENTIALS_PATH)
    if not before or after != before:
        raise CheckError("kv/lab/credentials did not read back unchanged after the restart")
    return (f"restarted {ctx.lab.openbao}: it came back sealed and was {step}; kv/lab/credentials read back "
            f"unchanged ({len(after)} entries) over TLS checked against the lab CA; its data lives on volume "
            f"{ctx.lab.openbao_volume}. Lakekeeper's KV2 backend is recorded as failed in {KV2_RECORD[0]}")


def g_openfga(ctx: Ctx) -> str:
    ctx.need("credentials", "gateway")
    client = ctx.client
    admin = {"Authorization": f"Bearer {client.token('lab-admin')}"}
    info = client.https("GET", "lakekeeper.lab.test", "/management/v1/info", headers=admin)
    backend = _dict(_json(info)).get("authz-backend")
    if info.status != 200 or backend != "openfga":
        raise CheckError(f"Lakekeeper's authorization backend is {backend or 'unknown'} (HTTP {info.status}), "
                         "not openfga", "Check `authz.backend` in platform/components/lakekeeper.")
    base = f"/catalog/v1/{_prefix(client, admin)}/namespaces"
    # Positive control: lab-admin may create on the very route lab-probe will be refused on.
    made = client.https("POST", "lakekeeper.lab.test", base, headers=admin, body={"namespace": [ADMIN_NAMESPACE]})
    gone = client.https("DELETE", "lakekeeper.lab.test", f"{base}/{ADMIN_NAMESPACE}", headers=admin)
    if made.status not in (200, 409) or gone.status not in (200, 204):
        raise CheckError(f"lab-admin could not create and delete a namespace on the same route "
                         f"(HTTP {made.status}, then {gone.status}): the deny below would prove nothing")
    probe = {"Authorization": f"Bearer {client.token('lab-probe')}"}
    resp = client.https("POST", "lakekeeper.lab.test", base, headers=probe, body={"namespace": [DENIED_NAMESPACE]})
    # A Lakekeeper refusal carries Lakekeeper's error body; a bare 403 or 404 could be the Gateway's own.
    if (resp.status == 403 and _error_type(resp)) or (resp.status == 404 and _error_type(resp) == HIDDEN):
        left = client.https("GET", "lakekeeper.lab.test", f"{base}/{DENIED_NAMESPACE}", headers=admin)
        if left.status != 404:
            raise CheckError(f"Lakekeeper refused lab-probe, yet namespace {DENIED_NAMESPACE} answers HTTP {left.status}")
        return (f"Lakekeeper (authz backend openfga) refused lab-probe, signed in with no grant, creating namespace "
                f"{DENIED_NAMESPACE} in {bootstrap.WAREHOUSE}: HTTP {resp.status} {_error_type(resp)}, "
                "and nothing was created; lab-admin created and deleted a namespace on the same route")
    if resp.status == 200:
        cleanup = client.https("DELETE", "lakekeeper.lab.test", f"{base}/{DENIED_NAMESPACE}", headers=admin)
        raise CheckError(f"Lakekeeper let lab-probe, which has no grant, create a namespace: authorization is not "
                         f"enforced (clean-up DELETE: HTTP {cleanup.status})",
                         "Check `authz.backend` in platform/components/lakekeeper.")
    raise CheckError(f"lab-probe's request got HTTP {resp.status} {_error_type(resp)}, not a refusal: "
                     f"{resp.text(300)}")


def h_mirror(ctx: Ctx) -> str:
    res = ctx.runner(["docker", "exec", ctx.lab.node, "sh", "-c",
                      'for f in /etc/containerd/certs.d/*/hosts.toml; do echo "=== $f"; cat "$f"; done'], timeout=60)
    if not res.ok:
        raise CheckError(f"could not read {ctx.lab.node}'s containerd pull config: {res.output_tail(300).strip()}")
    have, want = parse_hosts(res.stdout), expected_hosts(ctx.lock, ctx.tiers_doc)
    if have != want:
        wrong = sorted(set(have) ^ set(want)) or sorted(k for k in want if have.get(k) != want[k])
        raise CheckError(f"the node's pull config differs from what `lab up` writes: {', '.join(wrong)}",
                         "Re-run `lab up`; its cluster step rewrites the pull config.")
    try:
        built = {r.get("tag") for r in _dict(state.read_json(ctx.lab.images_record)).values() if isinstance(r, dict)}
    except (OSError, ValueError, state.StateError):
        built = set()
    images = pod_images(ctx.items("get", "pods", "-A"))
    bad = unpinned(images, ctx.lock, built)
    if bad:
        raise CheckError(f"{len(bad)} running image(s) not pinned by the lock digest of their repository: "
                         f"{', '.join(bad[:6])}")
    kind_only = sum(1 for ns, _ in images if ns in KIND_NAMESPACES)
    return (f"{ctx.lab.node} is configured to pull {len(want) - 1} upstream registries from the mirror only and "
            f"anything else from a closed port; all {len(images) - kind_only} other container images are pinned by "
            f"the lock digest of their own repository; {kind_only} kind system containers run kind's own "
            "preloaded images")


def h_egress(ctx: Ctx) -> str:
    nss = set(tier_namespaces(ctx))
    policies = [p for p in ctx.items("get", "networkpolicies", "-A") if _dict(p.get("metadata")).get("namespace") in nss]
    raise CheckError(f"{len(policies)} NetworkPolicy object(s) in the tier's {len(nss)} namespaces: no default-deny "
                     "egress yet, and no egress canary")


def i_tls(ctx: Ctx) -> str:
    ctx.need("gateway")
    endpoints = tier_endpoints(ctx.tier)
    answers = {host: ctx.client.https("GET", host, path, timeout=30).status for host, (path, _) in endpoints.items()}
    problems = endpoint_problems(answers, endpoints)
    if problems:
        raise CheckError("; ".join(problems), "An HTTP 404 with no body is the Gateway itself: a route is missing.")
    with tempfile.TemporaryDirectory() as tmp:
        wrong, _ = pki.ensure_ca(ctx.runner, Path(tmp), "not-this-lab")
        for host, (path, _) in endpoints.items():
            try:
                http.request("GET", f"https://{host}{path}", cafile=str(wrong),
                             connect_to=("127.0.0.1", ctx.client.port), timeout=30)
            except http.HTTPError as exc:
                if "CERTIFICATE_VERIFY_FAILED" not in str(exc):
                    raise CheckError(f"the wrong-CA request to {host} failed for another reason: {exc}") from exc
            else:
                raise CheckError(f"a client that trusts another CA got an answer from {host}")
    listeners = [li for gw in ctx.items("get", "gateways.gateway.networking.k8s.io", "-A")
                 for li in _dict(gw.get("spec")).get("listeners") or []]
    plain = [_dict(li).get("name") for li in listeners if _dict(li).get("protocol") != "HTTPS"]
    if not listeners or plain:
        raise CheckError(f"Gateway listeners that are not HTTPS: {plain or 'no listener at all'}")
    recorded = tier_namespaces(ctx)
    live = {ns["metadata"]["name"]: ns["metadata"] for ns in ctx.items("get", "namespaces")}
    problems = pss_problems(live, recorded)
    if problems:
        raise CheckError("Pod Security differs from platform/namespaces.yaml: " + "; ".join(problems))
    strict = sum(1 for s in recorded.values() if s.get("pod_security", "restricted") == "restricted")
    shown = ", ".join(f"{host.split('.')[0]} {status}" for host, status in answers.items())
    return (f"at the Gateway edge, the {len(endpoints)} *.lab.test endpoints answer from their backends over TLS "
            f"checked against the lab CA ({shown}); a "
            "client that trusts another CA is refused by each (CERTIFICATE_VERIFY_FAILED); the Gateway has only "
            f"HTTPS listeners; {strict} namespaces enforce restricted, and {len(recorded) - strict} enforce baseline "
            "with a recorded exemption")


# ---------------------------------------------------------------- (b) Trino JWT

TRINO_HOST = "trino.lab.test"
PIPELINE = "fluid-pipeline"          # the Keycloak client dbt-trino runs as (platform/identity/realm-import.yaml)
TRINO_SCOPE = "trino"                # the client scope whose audience mapper puts aud: trino in the token
TRINO_SHOW_SCHEMAS = "SHOW SCHEMAS FROM lakehouse"                # granted: the catalog is in the principal's grants
TRINO_DENIED = f"SHOW TABLES FROM lakehouse.{NAMESPACE}"          # not granted: lab_check is in no principal's grants
# What dbt-trino 1.10.6 asks first on every run (macros/adapters.sql, trino__list_relations_without_caching):
# a join with system.metadata.materialized_views, which Trino 483 guards with AccessCatalog on `system`.
TRINO_RAW_SCHEMA = "SHOW TABLES FROM lakehouse.events_raw"      # a schema-level grant (read)
TRINO_LIST_RELATIONS = (
    "select t.table_catalog as database, t.table_name as name, t.table_schema as schema, "
    "case when mv.name is not null then 'materialized_view' when t.table_type = 'BASE TABLE' then 'table' "
    "when t.table_type = 'VIEW' then 'view' else t.table_type end as table_type "
    "from lakehouse.information_schema.tables t left join (select * from system.metadata.materialized_views "
    "where catalog_name = 'lakehouse' and schema_name = 'events_marts') mv on mv.catalog_name = t.table_catalog "
    "and mv.schema_name = t.table_schema and mv.name = t.table_name where t.table_schema = 'events_marts'")
TRINO_WAIT_S = 90


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "X-Trino-Source": "lab-check"}


def trino_run(client, token: str, sql: str, *, timeout_s: float = TRINO_WAIT_S, sleep=time.sleep) -> dict:
    """Run one statement over Trino's REST protocol; returns {"state", "rows", "error"} once it ends.

    The token goes only in the Authorization header of requests to trino.lab.test: the host of a nextUri is
    ignored and only its path is followed.
    """
    headers = _bearer(token)
    resp = client.https("POST", TRINO_HOST, "/v1/statement", headers=headers, body=sql)
    deadline = time.monotonic() + timeout_s
    rows: list = []
    while True:
        if resp.status != 200:
            raise CheckError(f"Trino answered `{sql}` with HTTP {resp.status}: {resp.text(200)}")
        doc = _dict(_json(resp))
        rows += doc.get("data") or []
        if doc.get("error"):
            return {"state": "FAILED", "rows": rows, "error": _dict(doc["error"])}
        nxt = doc.get("nextUri")
        if not nxt:
            return {"state": _dict(doc.get("stats")).get("state") or "UNKNOWN", "rows": rows, "error": {}}
        # Only the path is used and every request goes to TRINO_HOST (Envoy may rewrite the host in nextUri).
        parts = urllib.parse.urlsplit(nxt)
        if time.monotonic() > deadline:
            raise CheckError(f"`{sql}` did not finish within {timeout_s:.0f} s")
        sleep(0.3)
        resp = client.https("GET", TRINO_HOST, parts.path + (f"?{parts.query}" if parts.query else ""), headers=headers)


def _refused_401(client, what: str, headers: dict) -> None:
    resp = client.https("POST", TRINO_HOST, "/v1/statement", headers=headers, body="SELECT 1")
    challenge = str((resp.headers or {}).get("www-authenticate") or "")
    # Trino's own refusal carries a Bearer challenge; a bare 401 could be anything in front of it.
    if resp.status != 401 or "bearer" not in challenge.lower():
        raise CheckError(f"{what} got HTTP {resp.status}, not Trino's 401 with a Bearer challenge"
                         + (" (HTTP 403 means Trino saw the request as insecure: check http-server.process-forwarded "
                            "and the Gateway's TLS hop)" if resp.status == 403 else ""))


def b_jwt(ctx: Ctx) -> str:
    ctx.need("credentials", "gateway")
    client = ctx.client
    if not ctx.creds.get(f"client.{PIPELINE}"):
        raise CheckError(f"the lab has no credential for Keycloak client {PIPELINE}",
                         "Run `./lab up` again: it generates missing credentials, then re-create the lab if the "
                         "realm was imported before this client existed.")
    token = client.token(PIPELINE, scope=TRINO_SCOPE)
    one = trino_run(client, token, "SELECT 1")
    if one["state"] != "FINISHED" or one["rows"] != [[1]]:
        raise CheckError(f"{PIPELINE}'s token did not run SELECT 1 to a FINISHED result: state {one['state']}, "
                         f"{_dict(one['error']).get('message', 'no error')}")
    _refused_401(client, "a request without a token", {})
    _refused_401(client, "a token from another client (lab-probe)", _bearer(client.token("lab-probe")))
    _refused_401(client, f"{PIPELINE}'s token without scope {TRINO_SCOPE} (no aud: trino)",
                 _bearer(client.token(PIPELINE, scope=None)))
    shown = trino_run(client, token, TRINO_SHOW_SCHEMAS)
    if shown["state"] != "FINISHED":
        raise CheckError(f"the granted read `{TRINO_SHOW_SCHEMAS}` did not finish: state {shown['state']}, "
                         f"{_dict(shown['error']).get('message', 'no error')}: the deny below would prove nothing",
                         "Check OPA's policy and grants: `kubectl -n trino get configmap trino-grants`.")
    # dbt-trino's own queries: a read grant on a schema, and the list-relations join with the system catalog.
    # The schema may not exist yet (SCHEMA_NOT_FOUND is fine); Access Denied is not.
    for sql in (TRINO_RAW_SCHEMA, TRINO_LIST_RELATIONS):
        ran = trino_run(client, token, sql)
        said = str(_dict(ran["error"]).get("message") or "")
        if "access denied" in said.lower():
            raise CheckError(f"OPA refused a query dbt-trino runs as {PIPELINE}: `{sql[:80]}`: {said[:300]}",
                             "Check platform/policy/trino-lab (lab.rego, data.json) against what dbt-trino asks.")
        if ran["state"] != "FINISHED" and _dict(ran["error"]).get("errorName") != "SCHEMA_NOT_FOUND":
            raise CheckError(f"`{sql[:80]}` as {PIPELINE} ended {ran['state']}: {said[:300]}")
    denied = trino_run(client, token, TRINO_DENIED)
    message = str(_dict(denied["error"]).get("message") or "")
    if denied["state"] != "FAILED":
        raise CheckError(f"`{TRINO_DENIED}` as {PIPELINE} ended {denied['state']}: OPA let a schema it has no grant "
                         "for through")
    if "access denied" not in message.lower():
        raise CheckError(f"`{TRINO_DENIED}` as {PIPELINE} failed, but not with Access Denied: {message[:300]}")
    return (f"as {PIPELINE} (client credentials, scope {TRINO_SCOPE}): SELECT 1 ran to FINISHED over the Gateway's "
            f"TLS hop to {TRINO_HOST}; a request without a token, a token from another client (lab-probe) and a "
            f"{PIPELINE} token without aud: trino each got Trino's 401; OPA allowed the granted `{TRINO_SHOW_SCHEMAS}` "
            f"and refused `{TRINO_DENIED}` ({message[:120]}); dbt-trino's list-relations join with "
            f"system.metadata.materialized_views and a read of events_raw were not refused. Not proven here: the OAuth2 browser login (b-oauth2); "
            "that Trino reads Iceberg data files (a-trino); that dbt-trino itself runs")



def _runner_record(ctx: Ctx, job: str, image: str = "fluid-runner") -> dict:
    """The record `lab build` wrote for a lab-built image (fluid-runner, or spark-iceberg), once the node holds
    exactly its layers."""
    try:
        record = _dict(_dict(state.read_json(ctx.lab.images_record)).get(image))
    except (OSError, ValueError, state.StateError):
        record = {}
    again = f"Run `./lab build {image} --name {ctx.lab.name}`."
    if not record.get("tag"):
        raise CheckError(f"the {job} runs in the {image} image, and {ctx.lab.images_record} has none", again)
    layers = record.get("layers")
    # A record without layers proves nothing: node_layers is None for an absent image too.
    if not isinstance(layers, list) or not layers or build.node_layers(ctx.runner, ctx.lab, record["tag"]) != layers:
        raise CheckError(f"node {ctx.lab.node} does not hold {record['tag']} with the layers `lab build` recorded, "
                         f"and the {job} runs it with imagePullPolicy Never", again)
    return record


def spark_smoke(ctx: Ctx) -> str:
    ctx.need("credentials")
    runner = _runner_record(ctx, "fluid apply in fluid-host")
    # The Spark pods run the lab-built spark-iceberg image, loaded into the node.
    image = _runner_record(ctx, "smoke Job's Spark container", spark.APACHE_IMAGE_ID)["tag"]
    result = spark.run_smoke(ctx.kube, image, secret=ctx.creds.get("client.spark"), log=ctx.log)
    versions = _dict(runner.get("versions"))
    return (f"forge {versions.get('data-product-forge')} in fluid-host ran `fluid apply --env local --mode "
            f"amend-and-build --build-id {spark.BUILD_ID}` on platform/spark/probe.fluid.yaml: its Spark pod on "
            f"{result['image']} (Spark {result.get('spark')}) wrote curated.events_daily {result.get('rows')} from the "
            f"3 raw.events rows a seed Job wrote, through Lakekeeper's REST catalog with vended credentials "
            f"(snapshot {result.get('snapshots')}, after the seed's {result.get('seed_snapshots')}; Jobs "
            f"{', '.join(result.get('jobs') or [])})")


def orchestration(ctx: Ctx) -> str:
    ctx.need("credentials", "gateway")
    record = airflow.loaded_image(ctx.runner, ctx.lab, ctx.lock)
    forge = airflow.expected_forge(ctx.lock)
    nonce = airflow.new_nonce()
    login = ctx.creds.get("airflow.admin_password")
    if not (login and ctx.creds.get("s3.airflow_publish.access") and ctx.creds.get("s3.airflow_publish.secret")):
        raise CheckError("kv/lab/credentials has no Airflow login or publish key: the lab predates the orchestration "
                         "tier",
                         f"Run `./lab up --tier {ctx.tier_name} --name {ctx.lab.name}`; it generates the missing credentials.")
    publish = s3.Bucket(ctx.client.https, airflow.BUCKET, ctx.creds["s3.airflow_publish.access"],
                        ctx.creds["s3.airflow_publish.secret"])
    source = airflow.probe_source(ctx.root, nonce)
    # First, so a lab whose Airflow keys can reach the warehouse fails before anything is uploaded.
    refusal = airflow.assumerole_refused(ctx.client.https, ctx.creds)

    def scheduler_log() -> str:
        return ctx.kube.run("-n", airflow.NAMESPACE, "logs", "deploy/airflow-scheduler", "-c", "scheduler",
                            "--tail=60", check=False).stdout

    with airflow.api_session(ctx.kube, credentials.AIRFLOW_ADMIN, login) as api:
        result = airflow.run_probe(api, publish, source, nonce, forge, scheduler_log=scheduler_log)
    return (f"uploaded a one-task DAG to s3://{airflow.BUCKET}/{airflow.PROBE_KEY} as the bucket-limited identity "
            f"airflow-publish; Airflow's REST API listed {airflow.DAG_ID} (bundle {result.get('bundle')}, read from "
            f"SeaweedFS by the S3DagBundle); run {result['run']} succeeded, and its {result['evidence']} shows this "
            f"run's nonce, `fluid --version` ({result['fluid_version']}) and forge {result['forge']} from "
            f"{record['tag']} (/opt/fluid/venv; FLUID_PROJECT_DIR {result.get('project_dir')}, DBT_PROFILES_DIR "
            f"{result.get('dbt_profiles')}); {refusal}; clean-up: {result['cleanup']}")


def streaming_check(ctx: Ctx) -> str:
    result = streaming.run_check(ctx.kube, ctx.lock)
    return (f"in {streaming.kafka_ref(ctx.lock).split('@')[0]}, with its own console clients: {result['records']} JSON "
            f"records carrying this run's marker went to {streaming.OPEN_TOPIC} over the plain listener and came back, "
            f"exactly those (from offset {result.get('start_offset')}); {streaming.WRITER} wrote to "
            f"{streaming.RESTRICTED_TOPIC} over TLS (lab CA) with SCRAM-SHA-512, and {streaming.NOBODY}, which has no "
            f"ACL there, was refused with a topic authorization error; an anonymous client on plain was refused adding an ACL "
            f"and reading {streaming.RESTRICTED_TOPIC} (Job {result.get('job')})")


def e2e_run(ctx: Ctx) -> str:
    ctx.need("credentials", "gateway")
    record = _runner_record(ctx, "fluid-host")
    result = e2e.run(ctx.kube, ctx.root, ctx.lock, ctx.client, ctx.lab.images_record,
                     secret=ctx.creds.get("client.spark"), log=ctx.log)
    stopped = ", ".join(result["stopped"])
    return (f"forge in {record['tag']} ran {e2e.PRODUCT_DIR} in fluid-host: `fluid validate "
            f"--strict`, `fluid plan` and `fluid apply --env local` started the stream {e2e.STREAM_BUILD}, which landed "
            f"this run's {result['raw_rows']} events from topic {e2e.TOPIC} in {'.'.join(e2e.RAW)} (Iceberg on "
            f"Lakekeeper, vended credentials; {result['raw_snapshots']} snapshot(s)); forge's cancel stopped it ({stopped}); "
            f"`fluid apply --mode amend-and-build --build-id {e2e.BATCH_BUILD}` wrote {'.'.join(e2e.DAILY)} "
            f"{result['daily']} for run {result['run']}, read back by a Job on {result['image']} (Spark {result['spark']})")


CHECKS: dict = {"ready": ready, "capacity": capacity, "runner": runner_image, "a-catalog": a_catalog,
                "b-jwt": b_jwt, "e": e_openbao, "g": g_openfga, "h-mirror": h_mirror, "h-egress": h_egress,
                "i": i_tls, "spark": spark_smoke, "orchestration": orchestration,
                "streaming": streaming_check, "e2e": e2e_run}
EXPECTED_ERRORS = (CheckError, bootstrap.BootstrapError, http.HTTPError, kube.KubeError, openbao.BaoError,
                   containers.DockerError, cluster.ClusterError, pki.PKIError, config.ConfigError, state.StateError,
                   spark.SparkError, airflow.AirflowError, s3.S3Error, mirror.MirrorError, jobs.JobError)
UNEXPECTED_ERRORS = (OSError, ValueError, KeyError, TypeError, AttributeError)


def _record(cid: str, fn: Optional[Callable], ctx: Ctx) -> dict:
    rid, about = f"check:{cid}", {"criterion": CRITERIA[cid]}
    if fn is None:
        return check(rid, FAIL, f"{CRITERIA[cid]}: not proven yet; {NOT_YET[cid]}", detail=about)
    try:
        return check(rid, PASS, fn(ctx), detail=about)
    except Skipped as exc:
        return check(rid, SKIP, str(exc), exc.advice, detail=about)
    except CheckError as exc:
        return check(rid, FAIL, str(exc), exc.advice, detail=about)
    except EXPECTED_ERRORS as exc:
        return check(rid, FAIL, f"{type(exc).__name__}: {exc}", getattr(exc, "advice", ""), detail=about)
    except UNEXPECTED_ERRORS as exc:
        return check(rid, FAIL, f"unexpected {type(exc).__name__}: {exc}",
                     "If this repeats, it is a launcher defect worth reporting with this output.", detail=about)


def records_for(tier: config.Tier) -> list:
    """The records a tier gets: every criterion, and an add-on's record only on a tier that includes it."""
    return [cid for cid in CRITERIA if cid not in ADDON_RECORDS or ADDON_RECORDS[cid] in tier.releases]


def select_records(tier: config.Tier, only: Optional[Sequence[str]] = None) -> list:
    """The tier's records, in their order; with `only`, just those, each of which must be one of the tier's."""
    wanted = records_for(tier)
    if not only:
        return wanted
    unknown = [r for r in only if r not in wanted]
    if unknown:
        raise ValueError(f"--only {', '.join(unknown)}: not a record of tier {tier.name} (its records: "
                         f"{', '.join(wanted)})")
    return [r for r in wanted if r in only]


def _credentials(ctx: Ctx) -> None:
    """lab-admin's OpenBao session and the lab's credentials; never initialises or reconfigures."""
    try:
        ctx.bao = openbao.Bao(openbao.host_port(ctx.runner, ctx.lab.openbao), str(ctx.cafile))
        openbao.open_existing(ctx.bao, ctx.lab)
        ctx.creds = ctx.bao.kv_get(openbao.KV_MOUNT, CREDENTIALS_PATH) or {}
        if not ctx.creds:
            ctx.missing["credentials"] = "kv/lab/credentials is empty"
    except EXPECTED_ERRORS + UNEXPECTED_ERRORS as exc:
        ctx.missing["credentials"] = f"OpenBao: {exc}"


def run_check(root: Path, tier: str = "core", *, name: str = state.DEFAULT_NAME, runner: proc.Runner = proc.run,
              env: Optional[Mapping[str, str]] = None, log: Callable[[str], None] = lambda _m: None,
              only: Optional[Sequence[str]] = None) -> dict:
    """Every criterion, one record each; with `only`, just those records (each must be one of the tier's).
    Raises state.StateError for a bad state directory."""
    env = os.environ if env is None else env
    lab = state.lab_for(root, name, env)
    checks: list = []
    doc = {"command": "check", "tier": tier, "name": lab.name, "ok": False, "checks": checks, "criteria": {}}
    if not down.owns_state(lab):
        checks.append(check("check:lab", FAIL, f"no lab {lab.name!r} in {lab.state}", f"Run `./lab up --name {lab.name}`."))
        return doc
    try:
        lock = config.load_yaml(root / config.VERSIONS_LOCK, runner)
        tiers_doc = config.load_yaml(root / config.TIERS, runner)
        tier_obj = config.resolve_tier(tiers_doc, tier)
        if not cluster.exists(runner, lab):
            checks.append(check("check:lab", FAIL, f"lab {lab.name} has no kind cluster", f"Run `./lab up --name {lab.name}`."))
            return doc
    except (config.ConfigError, cluster.ClusterError) as exc:
        checks.append(check("check:lab", FAIL, str(exc)))
        return doc
    try:
        wanted = select_records(tier_obj, only)
    except ValueError as exc:
        checks.append(check("check:lab", FAIL, str(exc)))
        return doc
    if only:
        doc["only"] = wanted
    ctx = Ctx(root, lab, runner, kube.Kube(lab, runner), lock, tiers_doc, tier, tier_obj, env=env, log=log)
    log("lab check: reading the lab's credentials from OpenBao ...")
    _credentials(ctx)
    try:
        forward = bootstrap.gateway(ctx.kube)
        forward.__enter__()
    except (kube.KubeError, OSError) as exc:
        forward = None
        ctx.missing["gateway"] = f"the port-forward to the Gateway: {exc}"
    try:
        if forward is not None:
            ctx.client = bootstrap.LabClient(ctx.cafile, forward.port, ctx.creds)
        for cid in wanted:
            log(f"lab check: {cid} ...")
            checks.append(_record(cid, CHECKS.get(cid), ctx))
            doc["criteria"][cid] = checks[-1]["status"]
    finally:
        if forward is not None:
            forward.close()
    doc["ok"] = passed(checks)   # a skipped record never passes the run (SECURITY.md)
    return doc
