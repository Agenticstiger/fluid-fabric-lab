# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The kind cluster: create it from the mirror, pin its pulls to the mirror,
and give pods the lab's names.

- The node image comes from the mirror (same digest as the lock), and the
  cluster's kubeconfig is written into the lab's state directory; the user's
  ~/.kube/config is never touched.
- containerd in the node gets one `certs.d/<upstream>/hosts.toml` per
  upstream registry, pointing at the mirror with the upstream as a path
  prefix (`override_path`), and a `_default` that points at a closed port.
  The mirror is also the `server`, so there is no upstream fallback: an image
  missing from the mirror fails to pull instead of leaving the machine
  (`lab check` (h)). Pattern: kind's local-registry guide.
- CoreDNS rewrites `*.lab.test` to the lab Gateway's Service, and
  `openbao.lab.test` to the OpenBao container on the lab's Docker network
  (`<lab>-net`, IPv4 only, `ensure_network`), so pods
  use the same HTTPS names as a browser does.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Iterable, Optional

from labctl import config, containers, mirror, proc, render
from labctl.state import LABEL, Lab, ensure_dir

KIND_CONFIG = "platform/kind/cluster.yaml"
GATEWAY_SERVICE = "lab-gateway.envoy-gateway-system.svc.cluster.local"
_CLOSED = "http://127.0.0.1:9"
_REWRITE_MARK = "# fluid-fabric-lab rewrites"


class ClusterError(Exception):
    pass


def hosts_toml(upstream: str) -> str:
    url = mirror.node_mirror_url(upstream)
    return (
        f"# Written by `lab up`: this node pulls {upstream} images only from the lab\n"
        "# mirror. The mirror is also the server, so nothing falls back upstream.\n"
        f'server = "{url}"\n'
        "override_path = true\n\n"
        f'[host."{url}"]\n'
        '  capabilities = ["pull", "resolve"]\n'
        "  override_path = true\n"
    )


def default_hosts_toml() -> str:
    return (
        "# Written by `lab up`: a registry the lab does not mirror resolves to a\n"
        "# closed port, so an unpinned image fails instead of being fetched.\n"
        f'server = "{_CLOSED}"\n\n'
        f'[host."{_CLOSED}"]\n'
        '  capabilities = ["pull", "resolve"]\n'
    )


def runtime_config(root: Path, lab: Lab, lock: dict, https_port: int, mirror_port: int,
                   runner: proc.Runner = proc.run) -> Path:
    """Write the kind config for this lab into its state directory."""
    doc = config.load_yaml(root / KIND_CONFIG, runner)
    node_entry = mirror.lock_image(lock, "kind-node")
    nodes = doc.get("nodes") or []
    if len(nodes) != 1:
        raise ClusterError(f"{KIND_CONFIG} must define exactly one node")
    pinned = f"{node_entry['registry']}/{node_entry['repository']}:{node_entry['tag']}@{node_entry['digest']}"
    if nodes[0].get("image") != pinned:
        raise ClusterError(f"{KIND_CONFIG} node image {nodes[0].get('image')} differs from the lock's {pinned}")
    doc["name"] = lab.cluster
    nodes[0]["image"] = (f"localhost:{mirror_port}/{node_entry['registry']}/{node_entry['repository']}"
                         f":{node_entry['tag']}@{node_entry['digest']}")
    for mapping in nodes[0].get("extraPortMappings") or []:
        if mapping.get("containerPort") == 30443:
            mapping["hostPort"] = https_port
    path = ensure_dir(lab.state) / "kind-config.json"
    path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return path


def ensure_network(runner: proc.Runner, lab: Lab) -> str:
    """Create the lab's Docker network once: IPv4 only, labelled with the lab.

    kind's shared `kind` network is dual-stack, while the lab cluster is IPv4
    only, so its NodePorts answer on IPv4 alone. Docker Desktop then sometimes
    forwards the host's 127.0.0.1:443 to the node's IPv6 address and the
    connection is reset (measured 2026-11-18). A network of the lab's own,
    without IPv6, removes that and keeps labs apart from other kind clusters.
    """
    res = runner(["docker", "network", "inspect", lab.network, "--format", "{{json .}}"], timeout=60)
    if res.ok:
        try:
            info = json.loads(res.stdout)
        except ValueError:
            info = {}
        if (info.get("Labels") or {}).get(LABEL) != lab.name:
            raise ClusterError(f"Docker network {lab.network} exists but is not this lab's "
                               f"(no label {LABEL}={lab.name}); remove or rename it")
        if info.get("EnableIPv6"):
            raise ClusterError(f"Docker network {lab.network} has IPv6 enabled; the lab needs IPv4 only")
        return f"network {lab.network} (reused)"
    res = runner(["docker", "network", "create", "--driver", "bridge", "--label", f"{LABEL}={lab.name}",
                  "--opt", "com.docker.network.bridge.enable_ip_masquerade=true", lab.network], timeout=60)
    if not res.ok:
        raise ClusterError(f"could not create Docker network {lab.network}: {res.output_tail(300).strip()}")
    return f"network {lab.network} created (IPv4 only)"


def gateway_host_port(runner: proc.Runner, lab: Lab) -> Optional[int]:
    """The host port kind mapped to the Gateway's node port when the cluster was made."""
    return containers.host_port(runner, lab.node, "30443/tcp")


def exists(runner: proc.Runner, lab: Lab) -> bool:
    """Whether kind lists the lab's cluster; ClusterError when kind cannot answer."""
    res = runner(["kind", "get", "clusters"], timeout=60)
    if not res.ok:
        raise ClusterError(f"kind get clusters failed: {res.output_tail(300).strip() or 'no answer'}")
    return lab.cluster in (res.stdout or "").split()


def create(runner: proc.Runner, lab: Lab, kind_config: Path, env: Optional[dict] = None) -> str:
    if exists(runner, lab):
        res = runner(["kind", "export", "kubeconfig", "--name", lab.cluster,
                      "--kubeconfig", str(lab.kubeconfig)], timeout=120)
        if not res.ok:
            raise ClusterError(f"cluster {lab.cluster} exists but its kubeconfig could not be exported: "
                               f"{res.output_tail(400).strip()}")
        return f"kind cluster {lab.cluster} already exists (reused)"
    kind_env = dict(env or os.environ)
    kind_env["KIND_EXPERIMENTAL_DOCKER_NETWORK"] = lab.network
    res = runner(["kind", "create", "cluster", "--config", str(kind_config), "--name", lab.cluster,
                  "--kubeconfig", str(lab.kubeconfig), "--wait", "180s"], env=kind_env, timeout=900)
    if not res.ok:
        raise ClusterError(f"kind create cluster failed: {res.output_tail(800).strip()}")
    return f"created kind cluster {lab.cluster}"


_REGISTRY_HOST = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+(:[0-9]{1,5})?$")


def pin_pulls_to_mirror(runner: proc.Runner, lab: Lab, upstreams: Iterable[str]) -> list:
    """Write hosts.toml for every upstream plus `_default` into the node."""
    written = []
    hosts = sorted(set(upstreams))
    for host in hosts:
        # The name becomes a directory in a shell command inside the node.
        if not _REGISTRY_HOST.match(host):
            raise ClusterError(f"registry {host!r} in versions.lock.yaml is not a plain host name")
    files = {up: hosts_toml(up) for up in hosts}
    files["_default"] = default_hosts_toml()
    for host, text in files.items():
        target = f"/etc/containerd/certs.d/{host}"
        res = runner(["docker", "exec", "-i", lab.node, "sh", "-c",
                      f'mkdir -p "{target}" && cat > "{target}/hosts.toml"'], input=text, timeout=60)
        if not res.ok:
            raise ClusterError(f"could not write {target}/hosts.toml in {lab.node}: {res.output_tail(300).strip()}")
        written.append(host)
    err = mirror.connect_network(runner, lab.network)
    if err:
        raise ClusterError(f"could not attach {mirror.REGISTRY} to network {lab.network}: {err}")
    return written


def corefile_with_rewrites(corefile: str, lab: Lab) -> str:
    """Insert the lab's rewrite rules into CoreDNS's server block (idempotent)."""
    lines = [ln for ln in corefile.splitlines() if _REWRITE_MARK not in ln]
    rules = [
        f"    rewrite stop name exact openbao.lab.test {lab.openbao} answer auto  {_REWRITE_MARK}",
        # Anchored: without ^...$ a search-path name such as
        # keycloak.lab.test.trino.svc.cluster.local would match too.
        rf"    rewrite stop name regex ^(.+)\.lab\.test\.$ {GATEWAY_SERVICE}. answer auto  {_REWRITE_MARK}",
    ]
    for i, line in enumerate(lines):
        if line.strip().startswith("kubernetes "):
            return "\n".join(lines[:i] + rules + lines[i:]) + "\n"
    raise ClusterError("CoreDNS Corefile has no `kubernetes` block to put the lab rewrites before")


NAMESPACES = "platform/namespaces.yaml"


def tier_namespaces(root: Path, runner: proc.Runner, tiers_doc: dict, tier: str) -> dict:
    """The namespaces a tier needs (platform/namespaces.yaml), with their Pod Security record."""
    doc = config.load_yaml(root / NAMESPACES, runner).get("namespaces") or {}
    wanted = render.tier_closure(tiers_doc, tier)
    # `tier` is one tier name or a list of them.
    return {n: spec for n, spec in doc.items() if wanted & set(_as_list(spec.get("tier")))}


def _as_list(value) -> list:
    return list(value) if isinstance(value, (list, tuple)) else [value]


def namespace(name: str, pss: str, reason: str = "") -> dict:
    labels = {
        "app.kubernetes.io/part-of": "fluid-fabric-lab",
        "pod-security.kubernetes.io/enforce": pss,
        "pod-security.kubernetes.io/enforce-version": "latest",
        "pod-security.kubernetes.io/warn": "restricted",
        "pod-security.kubernetes.io/audit": "restricted",
    }
    meta = {"name": name, "labels": labels}
    if reason:
        meta["annotations"] = {"fluid-fabric-lab.agentics/pod-security-exemption": reason}
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": meta}
