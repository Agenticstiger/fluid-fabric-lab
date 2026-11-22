# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `exposure`: nothing the lab runs listens beyond this machine. The lab
is for local development only.

On every rendered tier:
1. No Service is NodePort or LoadBalancer, and none has `externalIPs`.
2. The lab Gateway's Envoy Service is declared by an `EnvoyProxy`
   (spec.provider.kubernetes.envoyService). It may be NodePort, and only on
   node port 30443, the one port kind maps. Any other type or node port fails.
3. No pod spec sets `hostNetwork`, `hostPID` or `hostIPC`, and no container
   port sets `hostPort` (workloads, Jobs, CronJobs, Pods and operator custom
   resources: every object is walked).
4. SeaweedFS (namespace `seaweedfs`): every Service is ClusterIP; only the
   master, volume, filer and s3 workloads are rendered (no admin server, no
   worker); every Gateway route into the namespace targets Service
   `seaweedfs-s3` on port 8333. That is the S3 API, where SeaweedFS also
   serves STS `AssumeRole`, so STS is reachable only inside the cluster or
   through the Gateway on 127.0.0.1.
5. Nothing else asks for an exposed Service: no `type: NodePort` or
   `type: LoadBalancer` anywhere in any object (operator custom resources such
   as CloudNativePG's `managed.services`, items of a `List`), no Strimzi Kafka
   listener other than `internal` or `cluster-ip`, no `Ingress`, and no
   Gateway without an `EnvoyProxy` (Envoy Gateway would then make a
   LoadBalancer Service).
Outside the render:
6. kind config: every `extraPortMappings` entry listens on 127.0.0.1 and maps
   containerPort 30443; nothing else is mapped; the API server address, when
   set, is 127.0.0.1.
7. Launcher (labctl/*.py): every Docker port publication binds 127.0.0.1, in
   the separate, `=` and attached forms; no container joins the host network;
   every `--address` (kubectl port-forward) is 127.0.0.1.

  exposure.py [--root DIR] [--tier T] [--manifests DIR] [--kind-config FILE] [--launcher DIR]

With no arguments the real repository is checked: every tier is rendered,
platform/kind/cluster.yaml is read and labctl/*.py is scanned. With
`--manifests DIR` only that directory is scanned; the kind config and the
launcher are checked only when `--kind-config` / `--launcher` are given.
"""

from __future__ import annotations

import argparse
import ast
import re
import shutil
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

KIND_NODE_PORT = 30443
SEAWEED_NS = "seaweedfs"
SEAWEED_S3 = ("seaweedfs-s3", 8333)
SEAWEED_COMPONENTS = ("master", "volume", "filer", "s3")
SEAWEED_DENIED_SUBCOMMANDS = ("admin", "worker")
ROUTE_KINDS = ("HTTPRoute", "GRPCRoute", "TLSRoute", "TCPRoute", "UDPRoute")
HOST_FLAGS = ("hostNetwork", "hostPID", "hostIPC")
EXPOSED_TYPES = ("NodePort", "LoadBalancer", "nodeport", "loadbalancer")
KAFKA_LISTENER_TYPES = ("internal", "cluster-ip")
LOOPBACK = ("127.0.0.1", "localhost")
PORT_SPEC = re.compile(r"^[0-9\[]")   # what follows an attached -p: [ip:]port[:port] or [ipv6]:...


def where(doc: dict, f: str) -> str:
    return f"{c.obj_name(doc)} ({Path(f).name})"


# ----------------------------------------------------------------- manifests


def walk(obj, path: str = ""):
    """Yield (path, key, value, parent dict) for every mapping entry."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield f"{path}.{k}", k, v, obj
            yield from walk(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")


def check_services(docs: list, rep: c.Report) -> int:
    bad = 0
    for f, d in docs:
        if d.get("kind") != "Service":
            continue
        spec = d.get("spec") or {}
        if spec.get("type") in ("NodePort", "LoadBalancer"):
            bad += 1
            rep.fail(f"{where(d, f)}: Service type {spec['type']} listens beyond the cluster; use ClusterIP "
                     "(the Gateway is the one entry point)")
        if spec.get("externalIPs"):
            bad += 1
            rep.fail(f"{where(d, f)}: spec.externalIPs {spec['externalIPs']} listens beyond the cluster")
    return bad


def check_envoy(docs: list, rep: c.Report) -> tuple:
    """(problems, number of EnvoyProxy objects seen)."""
    bad = seen = 0
    for f, d in docs:
        if d.get("kind") != "EnvoyProxy":
            continue
        seen += 1
        svc = ((((d.get("spec") or {}).get("provider") or {}).get("kubernetes") or {}).get("envoyService")) or {}
        typ = svc.get("type")
        if typ != "NodePort":
            bad += 1
            rep.fail(f"{where(d, f)}: envoyService type is {typ or 'unset (Envoy Gateway defaults to LoadBalancer)'}; "
                     f"only NodePort {KIND_NODE_PORT} (the port kind maps) is allowed")
            continue
        ports = [p.get("nodePort") for _, k, p, _ in walk(svc) if k == "ports" and isinstance(p, list)
                 for p in p if isinstance(p, dict) and "nodePort" in p]
        ports += [v for _, k, v, _ in walk(svc) if k == "nodePort" and not isinstance(v, (dict, list))]
        ports = sorted(set(ports), key=str)
        if not ports:
            bad += 1
            rep.fail(f"{where(d, f)}: envoyService is NodePort with no pinned nodePort; pin {KIND_NODE_PORT}")
        elif ports != [KIND_NODE_PORT]:
            bad += 1
            rep.fail(f"{where(d, f)}: envoyService nodePort {', '.join(map(str, ports))}; only {KIND_NODE_PORT} "
                     "is mapped by kind")
    return bad, seen


def check_host_access(docs: list, rep: c.Report) -> int:
    bad = 0
    for f, d in docs:
        for path, key, val, parent in walk(d):
            if key in HOST_FLAGS and val is True:
                bad += 1
                rep.fail(f"{where(d, f)}: {key}: true at {path}")
            elif key == "hostPort" and isinstance(val, int) and not isinstance(val, bool) and val > 0:
                bad += 1
                name = parent.get("name") or parent.get("containerPort")
                rep.fail(f"{where(d, f)}: hostPort {val} (port {name}) at {path}")
    return bad


def subcommand_hits(ctr: dict) -> list:
    """`weed admin` / `weed worker` in a container's command and args."""
    cmd = [str(x) for x in (ctr.get("command") or [])]
    args = [str(x) for x in (ctr.get("args") or [])]
    words = [w for s in cmd + args for w in re.split(r"[\s;&|()]+", s) if w]
    hits = []
    for i, w in enumerate(words):
        if w == "weed" or w.endswith("/weed"):
            j = i + 1
            while j < len(words) and words[j].startswith("-"):
                j += 1
            if j < len(words) and words[j] in SEAWEED_DENIED_SUBCOMMANDS:
                hits.append(words[j])
    # An image whose entrypoint is `weed`: the first argument is the subcommand.
    if not cmd and args and args[0] in SEAWEED_DENIED_SUBCOMMANDS:
        hits.append(args[0])
    return hits


def route_backends(doc: dict) -> list:
    """Every backend reference of a Gateway route (rules, filters, mirrors)."""
    refs = []
    for _, key, val, _ in walk(doc.get("spec") or {}):
        if key == "backendRefs" and isinstance(val, list):
            refs += [r for r in val if isinstance(r, dict)]
        elif key == "backendRef" and isinstance(val, dict):
            refs.append(val)
    return refs


def check_seaweedfs(docs: list, rep: c.Report) -> tuple:
    """(problems, number of SeaweedFS objects seen)."""
    bad = seen = 0
    for f, d in docs:
        md = d.get("metadata") or {}
        kind = d.get("kind")
        if md.get("namespace") == SEAWEED_NS and kind not in ("ServiceAccount", "ConfigMap", "Secret"):
            seen += 1
        if kind == "Service" and md.get("namespace") == SEAWEED_NS:
            typ = (d.get("spec") or {}).get("type") or "ClusterIP"
            if typ != "ClusterIP":
                bad += 1
                rep.fail(f"{where(d, f)}: SeaweedFS Service type {typ}; it must be ClusterIP "
                         "(S3 and STS are reachable only inside the cluster or through the Gateway)")
        ps = c.pod_spec(d)
        if ps is not None and md.get("namespace") == SEAWEED_NS:
            labels = list((md.get("labels") or {}).items())
            tmpl = (d.get("spec") or {}).get("template") or {}
            labels += list(((tmpl.get("metadata") or {}).get("labels") or {}).items())
            comps = {v for k, v in labels if k == "app.kubernetes.io/component"}
            for comp in sorted(comps):
                if comp not in SEAWEED_COMPONENTS:
                    bad += 1
                    rep.fail(f"{where(d, f)}: SeaweedFS component '{comp}' is rendered; only "
                             f"{', '.join(SEAWEED_COMPONENTS)} may run (the admin server is unauthenticated "
                             "by default and the lab does not run it)")
            for ctr in c.containers(ps):
                for sub in subcommand_hits(ctr):
                    bad += 1
                    rep.fail(f"{where(d, f)}: container {ctr.get('name')} runs `weed {sub}`; the lab runs "
                             "neither the admin server nor the worker")
        if kind in ROUTE_KINDS:
            for ref in route_backends(d):
                ns = ref.get("namespace") or md.get("namespace")
                if ns != SEAWEED_NS:
                    continue
                seen += 1
                name, port = ref.get("name"), ref.get("port")
                if (name, port) != SEAWEED_S3:
                    bad += 1
                    rep.fail(f"{where(d, f)}: route targets {name}:{port} in namespace {SEAWEED_NS}; only "
                             f"{SEAWEED_S3[0]}:{SEAWEED_S3[1]} (the S3 API, where STS also lives) may be routed")
    return bad, seen


def check_exposed_types(docs: list, rep: c.Report) -> int:
    """`type: NodePort|LoadBalancer` anywhere else: operator templates, List items, Strimzi listeners, Ingress."""
    bad = 0
    for f, d in docs:
        kind = d.get("kind")
        if kind == "Ingress":
            bad += 1
            rep.fail(f"{where(d, f)}: an Ingress listens beyond the cluster; the lab Gateway is the one entry point")
        if kind == "Kafka":
            for i, lst in enumerate(((d.get("spec") or {}).get("kafka") or {}).get("listeners") or []):
                typ = (lst or {}).get("type")
                if typ not in KAFKA_LISTENER_TYPES:
                    bad += 1
                    rep.fail(f"{where(d, f)}: Kafka listener {lst.get('name') or i} has type {typ}; only "
                             f"{' or '.join(KAFKA_LISTENER_TYPES)} keeps it inside the cluster")
        for path, key, val, _parent in walk(d):
            if key != "type" or val not in EXPOSED_TYPES:
                continue
            if kind == "Service" and path == ".spec.type":
                continue      # check_services reports it
            if kind == "EnvoyProxy" and ".envoyService" in path:
                continue      # check_envoy allows NodePort 30443 only
            if kind == "Kafka" and ".listeners[" in path:
                continue      # reported above
            bad += 1
            rep.fail(f"{where(d, f)}: {path} is {val}: it would make a Service that listens beyond the cluster")
    return bad


def scan_manifests(tier: str, directory: Path, rep: c.Report) -> None:
    files = c.manifest_files(directory)
    docs = c.yaml_docs(files) if files else []
    if not docs:
        raise c.GateError(f"tier {tier}: no manifests found in {directory} (an empty render never passes)")
    bad = check_services(docs, rep) + check_host_access(docs, rep) + check_exposed_types(docs, rep)
    envoy_bad, envoy_seen = check_envoy(docs, rep)
    gateways = [where(d, f) for f, d in docs if d.get("kind") == "Gateway"]
    if gateways and not envoy_seen:
        bad += 1
        rep.fail(f"{tier}: {', '.join(gateways)} rendered with no EnvoyProxy: Envoy Gateway would give it a "
                 "LoadBalancer Service")
    sw_bad, sw_seen = check_seaweedfs(docs, rep)
    if bad + envoy_bad + sw_bad:
        return
    tail = (f"SeaweedFS ClusterIP only, no admin server, routes to {SEAWEED_S3[0]}:{SEAWEED_S3[1]} only"
            if sw_seen else "no SeaweedFS")
    extra = f"; Envoy Service NodePort {KIND_NODE_PORT} only" if envoy_seen else ""
    rep.ok(f"{tier}: no NodePort, LoadBalancer or host port; {tail}{extra} ({len(docs)} object(s))")


# --------------------------------------------------------------- kind config


def check_kind(path: Path, rep: c.Report, root: Path) -> None:
    doc = c.yaml_load(path) or {}
    name = c.rel(path, root)
    maps, bad = 0, 0
    for i, node in enumerate(doc.get("nodes") or []):
        for j, m in enumerate((node or {}).get("extraPortMappings") or []):
            maps += 1
            at = f"{name} nodes[{i}].extraPortMappings[{j}]"
            if m.get("listenAddress") != "127.0.0.1":
                bad += 1
                rep.fail(f"{at}: listenAddress {m.get('listenAddress') or 'unset (kind binds 0.0.0.0)'}; "
                         "it must be \"127.0.0.1\"")
            if m.get("containerPort") != KIND_NODE_PORT:
                bad += 1
                rep.fail(f"{at}: containerPort {m.get('containerPort')}; only {KIND_NODE_PORT} (the Gateway) "
                         "may be mapped")
    api = (doc.get("networking") or {}).get("apiServerAddress")
    if api is not None and api != "127.0.0.1":
        bad += 1
        rep.fail(f"{name} networking.apiServerAddress {api}: the API server must listen on 127.0.0.1")
    if not bad:
        rep.ok(f"kind config {name}: {maps} port mapping(s), all 127.0.0.1 to containerPort {KIND_NODE_PORT}")


# ------------------------------------------------------------------ launcher


def literal_head(node) -> object:
    """Leading literal text of a string or f-string, or None when it is not literal."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        if node.values and isinstance(node.values[0], ast.Constant) and isinstance(node.values[0].value, str):
            return node.values[0].value
        return ""
    return None


def scan_launcher_file(path: Path, root: Path, rep: c.Report) -> tuple:
    """(publications seen, problems) in one Python file.

    Every list or tuple of strings is read as a possible argv. Looked at:
    - Docker port publications: `-p SPEC`, `--publish SPEC`, `--publish=SPEC`, `-p=SPEC` and the attached
      `-pSPEC`, as string literals or f-strings (their leading literal text). SPEC must start with
      `127.0.0.1:`, so an f-string must begin with that literal text; a value that is not a string at all
      cannot be checked and fails. `-P` / `--publish-all` fail.
    - `--network host` / `--net host` (and the `=` forms): the container would share the host's interfaces.
    - `--address VALUE` / `--address=VALUE` (kubectl port-forward): VALUE must be 127.0.0.1 or localhost.
    """
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError as exc:
        raise c.GateError(f"{path}: cannot parse: {exc}") from exc
    seen = bad = 0
    name = c.rel(path, root)

    def fail(node, msg: str) -> None:
        nonlocal bad
        bad += 1
        rep.fail(f"{name}:{node.lineno}: {msg}")

    def publication(node, spec, flag: str) -> None:
        nonlocal seen
        seen += 1
        if spec is None:
            fail(node, f"`{flag}` is followed by a value that is not a string literal; write it as a literal that "
                       "starts with 127.0.0.1:")
        elif not spec.startswith("127.0.0.1:"):
            fail(node, f"`{flag}` publishes a port that does not start with 127.0.0.1: (it would listen on every "
                       "interface)")

    for node in ast.walk(tree):
        if not isinstance(node, (ast.List, ast.Tuple)):
            continue
        elts = node.elts
        for i, el in enumerate(elts):
            text = literal_head(el)
            if not isinstance(text, str) or not text.startswith("-"):
                continue
            whole = isinstance(el, ast.Constant)          # the whole element is literal text
            nxt = elts[i + 1] if i + 1 < len(elts) else None
            nxt_text = literal_head(nxt) if nxt is not None else None
            nxt_whole = isinstance(nxt, ast.Constant)
            if whole and text in ("-P", "--publish-all"):
                seen += 1
                fail(el, f"`{text}` publishes every exposed port on all interfaces")
            elif whole and text in ("-p", "--publish"):
                if isinstance(nxt_text, str) and nxt_text.startswith("-"):
                    continue  # another option follows: a tool's own --publish flag, not a docker port publication
                publication(nxt if nxt is not None else el, nxt_text, text)
            elif text.startswith("--publish=") or text.startswith("-p="):
                flag = text.split("=", 1)[0]
                spec = text.split("=", 1)[1]
                publication(el, spec, flag + "=")
            elif text.startswith("-p") and not text.startswith("--") and (PORT_SPEC.match(text[2:]) or
                                                                          (not whole and text == "-p")):
                publication(el, text[2:], "-p")
            elif whole and text in ("--network", "--net"):
                if nxt_whole and nxt_text == "host":
                    fail(nxt, f"`{text} host`: the container would listen on the host's interfaces")
            elif text in ("--network=host", "--net=host") and whole:
                fail(el, f"`{text}`: the container would listen on the host's interfaces")
            elif whole and text == "--address":
                if not (nxt_whole and nxt_text in LOOPBACK):
                    fail(nxt if nxt is not None else el, f"`--address {nxt_text if nxt_whole else '<not a literal>'}`"
                                                         ": a forwarded port must listen on 127.0.0.1")
            elif text.startswith("--address="):
                value = text.split("=", 1)[1]
                if not (whole and value in LOOPBACK):
                    fail(el, f"`{text if whole else text + '...'}`: a forwarded port must listen on 127.0.0.1")
    return seen, bad


def check_launcher(directory: Path, rep: c.Report, root: Path) -> None:
    files = sorted(directory.glob("*.py"))
    if not files:
        raise c.GateError(f"no Python files in {directory}")
    seen = bad = 0
    for p in files:
        s, b = scan_launcher_file(p, root, rep)
        seen, bad = seen + s, bad + b
    if not bad:
        if seen:
            rep.ok(f"launcher {c.rel(directory, root)}: {seen} Docker port publication(s) in {len(files)} file(s), "
                   "all bound to 127.0.0.1")
        else:
            rep.fail(f"launcher {c.rel(directory, root)}: no Docker port publication found in {len(files)} file(s); "
                     "the scan matched nothing")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--tier")
    ap.add_argument("--manifests", help="scan this directory instead of rendering the tiers")
    ap.add_argument("--kind-config", help="kind cluster config to check (default platform/kind/cluster.yaml "
                                          "when the tiers are rendered)")
    ap.add_argument("--launcher", help="directory of launcher *.py files to check (default labctl/ when the "
                                       "tiers are rendered)")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    real = not args.manifests
    kind = args.kind_config or (str(root / "platform" / "kind" / "cluster.yaml") if real else None)
    launcher = args.launcher or (str(root / "labctl") if real else None)
    rep = c.Report("exposure")
    base, tiers = c.manifests_by_tier(args, root)
    try:
        for tier, d in tiers.items():
            scan_manifests(tier, d, rep)
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)
    if kind:
        check_kind(Path(kind).resolve(), rep, root)
    if launcher:
        check_launcher(Path(launcher).resolve(), rep, root)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
