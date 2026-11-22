# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `footprint`: each tier's rendered memory fits its tiers.yaml target.

The L0 footprint floor: render each tier with `helmfile
template`, sum resource requests, limits and JVM heaps, and fail if the total
exceeds the tier's `memory_target_gib`. It catches regressions such as the
Trino chart's default of two workers with 8G heaps each.

How a tier is counted (all from the rendered manifests; nothing is skipped)
- Every container must carry a memory LIMIT; one without FAILS (nothing bounds
  it, so it cannot be counted honestly). Counted memory per container is
  max(limit, heap + direct memory + JVM_OVERHEAD), where the heap is the
  largest of -Xmx, -Xms, -XX:MaxHeapSize, -XX:InitialHeapSize,
  -XX:MinHeapSize and the RAM percentages x limit, read from args, command,
  every env value (including valueFrom ConfigMap/Secret keys), envFrom
  ConfigMaps/Secrets and mounted ConfigMaps/Secrets (Trino's jvm.config);
  Strimzi `jvmOptions` too. A heap larger than its limit FAILS (it would be
  OOM-killed); a RAM percentage with no limit FAILS (unbounded).
- Per pod: max(sum of app + sidecar containers, largest plain init container),
  times replicas, or a HorizontalPodAutoscaler's maxReplicas when larger
  (DaemonSet: one node). Every built-in workload kind (Pod, Deployment,
  StatefulSet, DaemonSet, ReplicaSet, ReplicationController, Job, CronJob) is
  counted. Helm hook Jobs are counted too: they run while the tier is up.
- Operator-managed workloads are modelled from their CRs: Keycloak (operator
  default 1700Mi/2Gi when unset), CNPG Cluster/Pooler (shared_buffers must fit
  the limit), Strimzi Kafka + KafkaNodePool + entity operator, KafkaConnect
  (default 3 replicas), KafkaBridge, KafkaMirrorMaker2, and the Envoy proxy
  Deployment a Gateway creates (its EnvoyProxy resources, or Envoy Gateway's
  512Mi default). Every other object must be a known kind that runs nothing
  (BUILTIN_NON_WORKLOAD, CR_NON_WORKLOAD); ANY other kind FAILS as unmodelled.
- A tier whose manifests hold no workload FAILS (an empty render is not a
  footprint).
- Plus the ESTIMATES below for workloads that are not rendered, plus
  `outside_cluster_gib` from tiers.yaml.

CPU is summed and reported; it fails only if a tier sets `cpu_target`.

A run over the whole repository (no --tier, --manifests or --values-overlay) also
checks that docs/proven.md and docs/pins.md state the figures exactly as counted
(`core 7.41 of 8 GiB, streaming ...`), so the published numbers cannot drift.

  footprint.py [--root DIR] [--tier T] [--manifests DIR] [-v]
               [--values-overlay RELEASE=FILE ...]

--values-overlay deep-merges FILE (yq) into platform/components/RELEASE/
values.yaml in a temporary copy of the tree and renders that copy: the
planted-regression path the selftest uses (two Trino workers with 8G heaps).
"""

from __future__ import annotations

import argparse
import base64
import re
import shutil
import tempfile
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

GIB, MIB = c.GIB, c.MIB
JVM_OVERHEAD_FRACTION = 0.25       # metaspace, threads, code cache, direct buffers (est.)
JVM_OVERHEAD_MIN = 192 * MIB
KEYCLOAK_DEFAULT = (1700 * MIB, 2 * GIB)        # operator default request / limit
ENVOY_PROXY_DEFAULT = 512 * MIB                  # Envoy Gateway default envoy container request
ENVOY_SHUTDOWN_MANAGER = 32 * MIB                # Envoy Gateway shutdown-manager sidecar request
# Workloads tiers.yaml lists that the render does not contain. Estimates, to be
# replaced by measurements. Keyed by tier they first appear in.
ESTIMATES = {
    "core": [
        ("kind control plane and node components (apiserver, etcd, controller-manager, "
         "scheduler, kubelet/containerd, kindnet, kube-proxy, CoreDNS x2, local-path)", 1.0 * GIB),
        ("fluid-runner Job (built on the operator's machine; not rendered)", 0.5 * GIB),
    ],
    "spark": [
        ("spark smoke Jobs (`lab check --tier spark`, the seed and the read-back, one at a time): one pod on the spark-iceberg image, local[2], 2.5 GiB limit",
         2.5 * GIB),
    ],
    "e2e": [
        ("e2e producer Job (`lab check --tier e2e`): one pod on the Strimzi Kafka image, 512 MiB limit", 0.5 * GIB),
        ("fluid-host (spark tier; rendered only once `lab build fluid-runner` has run, so never in an offline "
         "render): 1 GiB limit", 1.0 * GIB),
    ],
}
BUILTIN_GROUPS = {"", "apps", "batch", "policy", "networking.k8s.io", "rbac.authorization.k8s.io",
                  "apiextensions.k8s.io", "admissionregistration.k8s.io", "autoscaling", "scheduling.k8s.io",
                  "storage.k8s.io", "coordination.k8s.io", "discovery.k8s.io", "node.k8s.io",
                  "apiregistration.k8s.io", "certificates.k8s.io"}
# Built-in kinds that run nothing (the workload kinds are c.WORKLOAD_KINDS).
BUILTIN_NON_WORKLOAD = {
    "ConfigMap", "Secret", "Service", "ServiceAccount", "Endpoints", "EndpointSlice", "PersistentVolumeClaim",
    "PersistentVolume", "Namespace", "LimitRange", "ResourceQuota", "Role", "RoleBinding", "ClusterRole",
    "ClusterRoleBinding", "CustomResourceDefinition", "MutatingWebhookConfiguration",
    "ValidatingWebhookConfiguration", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding",
    "MutatingAdmissionPolicy", "MutatingAdmissionPolicyBinding", "PodDisruptionBudget", "NetworkPolicy",
    "Ingress", "IngressClass", "PriorityClass", "StorageClass", "APIService", "Lease", "CSIDriver",
    "RuntimeClass", "HorizontalPodAutoscaler", "CertificateSigningRequest",
}
# Custom resources that run nothing, by (API group, kind). Any other custom
# resource FAILS as unmodelled: an operator could turn it into pods the floor
# would never count (e.g. a FlinkDeployment or a KEDA ScaledObject).
CR_NON_WORKLOAD = {
    ("kafka.strimzi.io", k) for k in ("KafkaTopic", "KafkaUser", "KafkaConnector", "KafkaRebalance")
} | {
    ("k8s.keycloak.org", k) for k in ("KeycloakRealmImport", "KeycloakOIDCClient", "KeycloakSAMLClient")
} | {
    ("postgresql.cnpg.io", k) for k in ("Database", "Backup", "ScheduledBackup", "Publication", "Subscription",
                                         "ImageCatalog", "ClusterImageCatalog", "FailoverQuorum")
} | {
    ("gateway.networking.k8s.io", k) for k in ("GatewayClass", "HTTPRoute", "GRPCRoute", "TLSRoute", "TCPRoute",
                                                "UDPRoute", "ReferenceGrant", "BackendTLSPolicy", "ListenerSet")
} | {
    ("gateway.envoyproxy.io", k) for k in ("EnvoyProxy", "SecurityPolicy", "ClientTrafficPolicy",
                                            "BackendTrafficPolicy", "EnvoyExtensionPolicy", "EnvoyPatchPolicy",
                                            "HTTPRouteFilter", "Backend")
} | {
    ("core.strimzi.io", "StrimziPodSet"),
} | {
    ("cert-manager.io", k) for k in ("Certificate", "Issuer", "ClusterIssuer", "CertificateRequest")
} | {
    ("acme.cert-manager.io", k) for k in ("Order", "Challenge")
} | {
    ("trust.cert-manager.io", k) for k in ("Bundle", "ClusterBundle")
}
# Heap flags. Every one is read: the counted heap is the largest of them.
HEAP_FLAGS = re.compile(r"-(?:Xmx|Xms|XX:MaxHeapSize=|XX:InitialHeapSize=|XX:MinHeapSize=)([0-9]+[kKmMgGtT]?)\b")
DIRECT_FLAG = re.compile(r"-XX:MaxDirectMemorySize=([0-9]+[kKmMgGtT]?)\b")
RAMPCT = re.compile(r"-XX:(?:Max|Initial|Min)RAMPercentage=([0-9.]+)")
PG_SIZE = re.compile(r"^\s*([0-9]+)\s*(kB|MB|GB|TB)?\s*$")


def pg_bytes(v) -> float:
    """PostgreSQL memory setting (a bare number is 8 kB pages for shared_buffers)."""
    m = PG_SIZE.match(str(v))
    if not m:
        raise c.GateError(f"cannot read PostgreSQL memory setting {v!r}")
    mult = {"kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40, None: 8 * 2**10}[m.group(2)]
    return float(m.group(1)) * mult


class Tally:
    def __init__(self, tier: str) -> None:
        self.tier = tier
        self.items: list = []      # (label, mem_bytes, cpu_cores)
        self.problems: list = []
        self.notes: list = []
        self.workloads = 0         # objects from the manifests that run pods

    def add(self, label: str, mem: float, cpu: float = 0.0) -> None:
        self.items.append((label, mem, cpu))

    @property
    def mem(self) -> float:
        return sum(i[1] for i in self.items)

    @property
    def cpu(self) -> float:
        return sum(i[2] for i in self.items)


def _res(resources: dict) -> tuple:
    resources = resources or {}
    lim, req = resources.get("limits") or {}, resources.get("requests") or {}
    return (c.mem_bytes(lim.get("memory")), c.mem_bytes(req.get("memory")),
            c.cpu_cores(lim.get("cpu")), c.cpu_cores(req.get("cpu")))


def container_mem(label: str, resources: dict, heap_strings: list, tally: Tally,
                  xmx_explicit: str = None) -> tuple:
    """(counted bytes, cpu, heap). Every container must carry a memory limit:
    without one, nothing bounds it (a JVM sizes itself from the node), so the
    floor cannot count it and FAILS instead of guessing."""
    lim, req, cpu_lim, cpu_req = _res(resources)
    if lim is None:
        tally.problems.append(f"{label}: no memory limit (request {req / MIB:.0f}Mi)" if req else
                              f"{label}: no memory limit and no request")
        lim_or = req or 0.0
    else:
        lim_or = lim
    heap = c.jvm_size(xmx_explicit) if xmx_explicit else None
    direct = 0.0
    for s in heap_strings:
        for m in HEAP_FLAGS.findall(s):
            heap = max(heap or 0, c.jvm_size(m))
        for m in DIRECT_FLAG.findall(s):
            direct = max(direct, c.jvm_size(m))
    pcts = [float(p) for s in heap_strings for p in RAMPCT.findall(s)]
    if pcts and lim is not None:
        heap = max(heap or 0, lim * max(pcts) / 100.0)
    elif pcts:
        tally.problems.append(f"{label}: JVM heap is MaxRAMPercentage={max(pcts):g} with no memory "
                              "limit, so it is unbounded (set a container limit)")
    counted = lim_or
    if heap:
        need = heap + direct + max(heap * JVM_OVERHEAD_FRACTION, JVM_OVERHEAD_MIN)
        if lim is not None and heap + direct > lim:
            tally.problems.append(f"{label}: JVM heap {heap / MIB:.0f}Mi (+{direct / MIB:.0f}Mi direct) exceeds "
                                  f"its memory limit {lim / MIB:.0f}Mi")
        counted = max(counted, need)
    return counted, (cpu_lim or cpu_req or 0.0), heap


def _decode_secret(doc: dict) -> dict:
    out = {}
    for k, v in (doc.get("data") or {}).items():
        try:
            out[k] = base64.b64decode(str(v)).decode("utf-8", "replace")
        except (ValueError, TypeError):
            out[k] = str(v)
    for k, v in (doc.get("stringData") or {}).items():
        out[k] = str(v)
    return out


def config_sources(docs: list) -> dict:
    """{('ConfigMap'|'Secret', namespace, name): {key: text}} for every rendered config object."""
    src = {}
    for _, d in docs:
        kind = d.get("kind")
        if kind not in ("ConfigMap", "Secret"):
            continue
        md = d.get("metadata") or {}
        data = {k: str(v) for k, v in (d.get("data") or {}).items()} if kind == "ConfigMap" else _decode_secret(d)
        for ns in (md.get("namespace"), None):
            src[(kind, ns, md.get("name"))] = data
    return src


def _lookup(src: dict, kind: str, ns, name) -> dict:
    return src.get((kind, ns, name)) or src.get((kind, None, name)) or {}


def pod_mem(label: str, ps: dict, ns, src: dict, tally: Tally) -> tuple:
    vols = {}
    for v in ps.get("volumes") or []:
        refs = []
        if (v.get("configMap") or {}).get("name"):
            refs.append(("ConfigMap", v["configMap"]["name"]))
        if (v.get("secret") or {}).get("secretName"):
            refs.append(("Secret", v["secret"]["secretName"]))
        for s in ((v.get("projected") or {}).get("sources") or []):
            if (s.get("configMap") or {}).get("name"):
                refs.append(("ConfigMap", s["configMap"]["name"]))
            if (s.get("secret") or {}).get("name"):
                refs.append(("Secret", s["secret"]["name"]))
        vols[v.get("name")] = refs
    app_mem = app_cpu = 0.0
    init_peak = 0.0
    for kind in ("initContainers", "containers"):
        for ct in ps.get(kind) or []:
            strings = [s for _, s in c.iter_strings(ct.get("args") or [])]
            strings += [s for _, s in c.iter_strings(ct.get("command") or [])]
            for e in ct.get("env") or []:
                if isinstance(e.get("value"), str):
                    strings.append(e["value"])
                vf = e.get("valueFrom") or {}
                for k, ref in (("ConfigMap", vf.get("configMapKeyRef")), ("Secret", vf.get("secretKeyRef"))):
                    if ref:
                        strings.append(_lookup(src, k, ns, ref.get("name")).get(ref.get("key"), ""))
            for ef in ct.get("envFrom") or []:
                for k, ref in (("ConfigMap", ef.get("configMapRef")), ("Secret", ef.get("secretRef"))):
                    if ref:
                        strings += list(_lookup(src, k, ns, ref.get("name")).values())
            for vm in ct.get("volumeMounts") or []:
                for k, name in vols.get(vm.get("name")) or []:
                    strings += list(_lookup(src, k, ns, name).values())
            mem, cpu, heap = container_mem(f"{label}/{ct.get('name')}", ct.get("resources"), strings, tally)
            if kind == "initContainers" and ct.get("restartPolicy") != "Always":
                init_peak = max(init_peak, mem)
            else:
                app_mem += mem
                app_cpu += cpu
    return max(app_mem, init_peak), app_cpu


def strimzi_container(label: str, spec: dict, tally: Tally) -> tuple:
    spec = spec or {}
    jvm = spec.get("jvmOptions") or {}
    strings = [f"-{k}={v}" if k.startswith("XX") else f"-{k}{v}" for k, v in (jvm.get("-XX") or {}).items()]
    strings += [s for _, s in c.iter_strings(jvm.get("javaSystemProperties") or [])]
    strings += [f"{e.get('value')}" for e in (((spec.get("template") or {}).get("connectContainer") or
                                                (spec.get("template") or {}).get("kafkaContainer") or {})
                                               .get("env") or [])]
    xmx = jvm.get("-Xmx")
    if jvm.get("-Xms"):
        strings.append(f"-Xms{jvm['-Xms']}")
    mem, cpu, _ = container_mem(label, spec.get("resources"), strings, tally, xmx_explicit=xmx)
    return mem, cpu


def hpa_max(docs: list) -> dict:
    """{(kind, namespace, name): maxReplicas} from HorizontalPodAutoscalers."""
    out = {}
    for _, d in docs:
        if d.get("kind") == "HorizontalPodAutoscaler":
            ref = (d.get("spec") or {}).get("scaleTargetRef") or {}
            ns = (d.get("metadata") or {}).get("namespace")
            out[(ref.get("kind"), ns, ref.get("name"))] = int((d.get("spec") or {}).get("maxReplicas") or 1)
    return out


def count(tier: str, docs: list) -> Tally:
    tally = Tally(tier)
    src = config_sources(docs)
    hpas = hpa_max(docs)
    by_kind: dict = {}
    for f, d in docs:
        by_kind.setdefault(d.get("kind"), []).append(d)
    for f, d in docs:
        kind, spec = d.get("kind"), d.get("spec") or {}
        api = str(d.get("apiVersion") or "")
        group = api.split("/")[0] if "/" in api else ""
        md = d.get("metadata") or {}
        ns = md.get("namespace")
        label = c.obj_name(d)
        ps = c.pod_spec(d) if group in BUILTIN_GROUPS else None
        if ps is not None:
            tally.workloads += 1
            if kind == "CronJob":
                tally.notes.append(f"{label}: CronJob counted as one concurrent run")
            reps = 1 if kind in ("DaemonSet", "Job", "CronJob", "Pod") else spec.get("replicas", 1)
            if reps is None:
                reps = 1
            if kind == "Job":
                reps = max(1, spec.get("parallelism", 1) or 1)
            scaled = hpas.get((kind, ns, md.get("name"))) or hpas.get((kind, None, md.get("name")))
            if scaled and scaled > reps:
                tally.notes.append(f"{label}: HorizontalPodAutoscaler maxReplicas {scaled} counted (replicas {reps})")
                reps = scaled
            mem, cpu = pod_mem(label, ps, ns, src, tally)
            tally.add(f"{label} x{reps}", mem * reps, cpu * reps)
        elif kind == "Keycloak" and group == "k8s.keycloak.org":
            tally.workloads += 1
            n = spec.get("instances", 1) or 1
            res = spec.get("resources")
            if not res:
                res = {"requests": {"memory": f"{KEYCLOAK_DEFAULT[0] / MIB:.0f}Mi"},
                       "limits": {"memory": f"{KEYCLOAK_DEFAULT[1] / MIB:.0f}Mi"}}
                tally.notes.append(f"{label}: no resources; operator default 1700Mi/2Gi counted")
            strings = [s for _, s in c.iter_strings(spec.get("env") or [])]
            strings += [s for _, s in c.iter_strings(spec.get("additionalOptions") or [])]
            mem, cpu, _ = container_mem(label, res, strings, tally)
            tally.add(f"{label} x{n}", mem * n, cpu * n)
        elif kind in ("Cluster", "Pooler") and group == "postgresql.cnpg.io":
            tally.workloads += 1
            n = spec.get("instances", 1) or 1
            res = spec.get("resources") if kind == "Cluster" else \
                (((spec.get("template") or {}).get("spec") or {}).get("containers") or [{}])[0].get("resources")
            mem, cpu, _ = container_mem(label, res, [], tally)
            if kind == "Cluster":
                params = ((spec.get("postgresql") or {}).get("parameters") or {})
                lim = _res(res)[0]
                if params.get("shared_buffers") is not None and lim is not None \
                        and pg_bytes(params["shared_buffers"]) >= lim:
                    tally.problems.append(f"{label}: shared_buffers {params['shared_buffers']} does not fit its "
                                          f"memory limit {lim / MIB:.0f}Mi")
            tally.add(f"{label} x{n}", mem * n, cpu * n)
        elif kind == "Kafka" and group == "kafka.strimzi.io":
            tally.workloads += 1
            pools = [p for p in by_kind.get("KafkaNodePool", [])
                     if ((p.get("metadata") or {}).get("labels") or {}).get("strimzi.io/cluster")
                     == md.get("name")]
            if not pools:
                k = spec.get("kafka") or {}
                n = k.get("replicas", 1) or 1
                mem, cpu = strimzi_container(f"{label}/kafka", k, tally)
                tally.add(f"{label}/kafka x{n}", mem * n, cpu * n)
            eo = spec.get("entityOperator")
            if eo:
                op_mem = op_cpu = 0.0
                for op in ("topicOperator", "userOperator"):
                    if op in eo:
                        m, cc = strimzi_container(f"{label}/entity-operator/{op}", eo.get(op) or {}, tally)
                        op_mem, op_cpu = op_mem + m, op_cpu + cc
                tally.add(f"{label}/entity-operator", op_mem, op_cpu)
            for extra in ("cruiseControl", "kafkaExporter"):
                if spec.get(extra) is not None:
                    m, cc = strimzi_container(f"{label}/{extra}", spec.get(extra) or {}, tally)
                    tally.add(f"{label}/{extra}", m, cc)
        elif kind == "KafkaNodePool" and group == "kafka.strimzi.io":
            n = spec.get("replicas", 1)
            mem, cpu = strimzi_container(label, spec, tally)
            tally.add(f"{label} x{n}", mem * n, cpu * n)
        elif kind in ("KafkaConnect", "KafkaMirrorMaker2", "KafkaBridge") and group == "kafka.strimzi.io":
            tally.workloads += 1
            n = spec.get("replicas", 3 if kind != "KafkaBridge" else 1)
            mem, cpu = strimzi_container(label, spec, tally)
            tally.add(f"{label} x{n}", mem * n, cpu * n)
        elif kind == "Gateway" and group == "gateway.networking.k8s.io":
            tally.workloads += 1
            ep = envoy_proxy_for(d, by_kind)
            dep = (((ep or {}).get("spec") or {}).get("provider") or {}).get("kubernetes", {}).get("envoyDeployment") or {}
            n = dep.get("replicas", 1) or 1
            res = (dep.get("container") or {}).get("resources")
            if res:
                mem, cpu, _ = container_mem(f"{label}/envoy", res, [], tally)
            else:
                mem, cpu = ENVOY_PROXY_DEFAULT, 0.1
                tally.notes.append(f"{label}: envoy proxy resources not set; Envoy Gateway default 512Mi counted")
            mem += ENVOY_SHUTDOWN_MANAGER
            tally.add(f"{label} envoy proxy x{n}", mem * n, cpu * n)
        elif group in BUILTIN_GROUPS and (kind in BUILTIN_NON_WORKLOAD):
            continue
        elif group not in BUILTIN_GROUPS and (group, kind) in CR_NON_WORKLOAD:
            continue
        else:
            tally.problems.append(f"{label}: kind {kind} ({api or 'no apiVersion'}) is not modelled by the "
                                  "footprint floor (it may run pods the floor cannot see); model it in "
                                  "tools/l0/footprint.py or list it as running nothing")
    return tally


def envoy_proxy_for(gw: dict, by_kind: dict):
    ref = ((gw.get("spec") or {}).get("infrastructure") or {}).get("parametersRef")
    if not ref:
        cls = (gw.get("spec") or {}).get("gatewayClassName")
        for gc in by_kind.get("GatewayClass", []):
            if (gc.get("metadata") or {}).get("name") == cls:
                ref = (gc.get("spec") or {}).get("parametersRef")
    if not ref:
        return None
    for ep in by_kind.get("EnvoyProxy", []):
        if (ep.get("metadata") or {}).get("name") == ref.get("name"):
            return ep
    return None


def tier_estimates(root: Path, tier: str) -> list:
    tiers = (c.yaml_load(root / "tiers.yaml") or {}).get("tiers") or {}
    chain, seen = [tier], set()
    while chain:
        t = chain.pop()
        if t in seen:
            continue
        seen.add(t)
        chain.extend((tiers.get(t) or {}).get("requires") or [])
    return [e for t in seen for e in ESTIMATES.get(t, [])]


def overlay_copy(root: Path, overlays: list) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="l0-footprint-overlay-"))
    c.copy_tree(root, tmp, ["platform", "tiers.yaml", "versions.lock.yaml"])
    for spec in overlays:
        release, _, file = spec.partition("=")
        src = Path(file).resolve()
        target = tmp / "platform/components" / release / "values.yaml"
        if not release or not src.is_file() or not target.is_file():
            raise c.GateError(f"--values-overlay {spec!r}: need RELEASE=FILE with "
                              f"platform/components/{release}/values.yaml present")
        res = c.run(["yq", "ea", ". as $i ireduce ({}; . * $i)", str(target), str(src)])
        if res.returncode != 0:
            raise c.GateError(f"yq merge failed: {res.stderr.strip()[:300]}")
        target.write_text(res.stdout)
    return tmp


# Docs that state the footprint figures. In a full-repository run each must contain them exactly as this gate
# counts them (whitespace aside), so the published numbers cannot drift from the gate.
DOC_FIGURES = ("docs/proven.md", "docs/pins.md")


def figures(totals: list) -> str:
    """The figures line, for example `core 7.41 of 8 GiB, streaming 9.28 of 13 GiB`."""
    return ", ".join(f"{tier} {total:.2f} of {target:g} GiB" for tier, total, target in totals)


def check_doc_figures(root: Path, totals: list, rep: c.Report) -> None:
    want = figures(totals)
    for rel in DOC_FIGURES:
        path = root / rel
        if not path.is_file():
            rep.fail(f"{rel}: missing; it must state the footprint figures this gate counts: {want}")
            continue
        text = " ".join(path.read_text(encoding="utf-8", errors="replace").split())
        if want in text:
            rep.ok(f"{rel} states the footprint as this gate counts it")
        else:
            rep.fail(f"{rel} does not state the footprint as this gate counts it; it must contain: {want}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--tier")
    ap.add_argument("--manifests", help="count this directory (needs --tier for the target)")
    ap.add_argument("--values-overlay", action="append", default=[], metavar="RELEASE=FILE")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every counted item")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    if args.manifests and not args.tier:
        raise c.GateError("--manifests needs --tier (the target comes from tiers.yaml)")
    tiers_doc = (c.yaml_load(root / "tiers.yaml") or {}).get("tiers") or {}
    rep = c.Report("footprint")
    work = overlay_copy(root, args.values_overlay) if args.values_overlay else None
    render_root = work or root
    base, rendered = c.manifests_by_tier(args, render_root)
    totals: list = []
    try:
        for tier, d in rendered.items():
            spec = tiers_doc.get(tier)
            if not isinstance(spec, dict) or not isinstance(spec.get("memory_target_gib"), (int, float)):
                rep.fail(f"{tier}: tiers.yaml has no numeric memory_target_gib")
                continue
            docs = c.yaml_docs(c.manifest_files(d))
            tally = count(tier, docs)
            if tally.workloads == 0:
                rep.fail(f"{tier}: no workload found in the manifests ({len(docs)} object(s)); an empty "
                         "render is not a footprint")
            for label, mem in tier_estimates(root, tier):
                tally.add(f"estimate: {label}", mem)
            outside = float(spec.get("outside_cluster_gib") or 0)
            if outside:
                tally.add("outside the cluster (tiers.yaml outside_cluster_gib)", outside * GIB)
            for n in tally.notes:
                rep.note(f"{tier}: {n}")
            if args.verbose:
                for lab, m, cpu in sorted(tally.items, key=lambda i: -i[1]):
                    rep.note(f"{tier}:   {m / GIB:6.2f} GiB  {cpu:5.2f} cpu  {lab}")
            top = sorted(tally.items, key=lambda i: -i[1])[:6]
            rep.note(f"{tier}: largest: " + "; ".join(f"{lab} {m / GIB:.2f}GiB" for lab, m, _ in top))
            for p in tally.problems:
                rep.fail(f"{tier}: {p}")
            target = float(spec["memory_target_gib"])
            total = tally.mem / GIB
            line = (f"{tier}: {total:.2f} GiB counted (limits/heaps + estimates + outside) vs target "
                    f"{target:g} GiB; CPU {tally.cpu:.2f} cores")
            if total > target:
                rep.fail(line + " -> OVER TARGET")
            else:
                rep.ok(line)
            totals.append((tier, total, target))
            cpu_target = spec.get("cpu_target")
            if isinstance(cpu_target, (int, float)) and tally.cpu > cpu_target:
                rep.fail(f"{tier}: CPU {tally.cpu:.2f} > cpu_target {cpu_target}")
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)
        if work:
            shutil.rmtree(work, ignore_errors=True)
    # A full-repository run (every tier, the real values) also checks the figures the docs publish.
    if not (args.manifests or args.tier or args.values_overlay) and len(totals) == len(rendered):
        check_doc_figures(root, totals, rep)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
