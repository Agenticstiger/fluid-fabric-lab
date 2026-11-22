# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab mirror`: copy every pinned image into one local registry, by digest.

The cluster pulls only from this registry (Docker Hub
pull limits, and the air-gap rehearsal). It is a plain distribution registry
(`registry:3`, Apache-2.0, pinned in versions.lock.yaml as `lab-registry`),
bound to 127.0.0.1 on the host and attached to each lab's Docker network so
the node can reach it by name. One registry serves every lab on the machine.

Layout: an upstream image `quay.io/jetstack/cert-manager-controller` lives at
`<mirror>/quay.io/jetstack/cert-manager-controller`. containerd sends mirror
requests WITHOUT the upstream host in the path (it appends `?ns=quay.io`,
which distribution ignores), so `lab up` writes one hosts.toml per upstream
with `override_path = true` and the upstream prefix in the mirror URL
(containerd docs, docs/hosts.md). That keeps two upstreams that share a
repository path apart.

Copies use `crane copy` (go-containerregistry, pinned in the lock), which keeps
the image index digest and every platform. Each copy is then verified with
`crane digest` against the lock; an image already in the mirror is verified
the same way, so a cache hit is never trusted unchecked.

Pattern credit: kind's local-registry guide
(https://kind.sigs.k8s.io/docs/user/local-registry/, Apache-2.0).
"""

from __future__ import annotations

import concurrent.futures
import re
from pathlib import Path
from typing import Iterable, Optional

from labctl import config, containers, proc, render
from labctl.output import FAIL, PASS, check
from labctl.state import LABEL

REGISTRY = "fluid-fabric-lab-registry"
REGISTRY_VOLUME = "fluid-fabric-lab-registry-data"
REGISTRY_IMAGE_ID = "lab-registry"
DEFAULT_PORT = 5001
CONTAINER_PORT = 5000
COPY_TIMEOUT_S = 1800
COPY_WORKERS = 2
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
# Image components that no chart owns, and the tier that needs them.
_COMPONENT_TIERS = {"cluster": "core", "openbao": "core", "mirror": "core", "spark": "spark"}

class MirrorError(Exception):
    pass


def host_ref(entry: dict, port: int = DEFAULT_PORT) -> str:
    """Where the host (crane, docker) addresses an image in the mirror."""
    return f"localhost:{port}/{entry['registry']}/{entry['repository']}:{entry['tag']}"


def host_digest_ref(entry: dict, port: int = DEFAULT_PORT) -> str:
    return f"localhost:{port}/{entry['registry']}/{entry['repository']}@{entry['digest']}"


def upstream_ref(entry: dict) -> str:
    return f"{entry['registry']}/{entry['repository']}@{entry['digest']}"


def node_mirror_url(upstream: str) -> str:
    """The mirror URL a kind node's containerd uses for `upstream` (override_path)."""
    return f"http://{REGISTRY}:{CONTAINER_PORT}/v2/{upstream}"


def lock_image(lock: dict, image_id: str) -> dict:
    for entry in lock.get("images") or []:
        if isinstance(entry, dict) and entry.get("id") == image_id:
            return entry
    raise MirrorError(f"{config.VERSIONS_LOCK} has no image {image_id!r}")


def _component_tiers(lock: dict) -> dict:
    tiers = dict(_COMPONENT_TIERS)
    for chart in lock.get("charts") or []:
        if isinstance(chart, dict) and chart.get("component") and chart.get("tier"):
            tiers[chart["component"]] = chart["tier"]
    return tiers


def _check_entry(entry: dict) -> dict:
    if entry.get("use") == "denied":
        raise MirrorError(f"image {entry.get('id')} is denied in {config.VERSIONS_LOCK} and is never mirrored")
    for key in ("id", "ref", "registry", "repository", "tag", "digest"):
        if not entry.get(key):
            raise MirrorError(f"image {entry.get('id')}: no {key} in {config.VERSIONS_LOCK}")
    if not _DIGEST.match(str(entry["digest"])):
        raise MirrorError(f"image {entry['id']}: digest {entry['digest']!r} is not a sha256 digest")
    return entry


def _wanted(entry: dict, owner: dict, tiers: set, with_optional: bool) -> bool:
    """Whether a lock image belongs to one of `tiers` (optional ones only on request)."""
    use = entry.get("use")
    if entry.get("id") == REGISTRY_IMAGE_ID or use not in ("used", "optional"):
        return False
    if use == "optional":
        return with_optional
    comp_tier = owner.get(entry.get("component"))
    if comp_tier is None:
        raise MirrorError(f"image {entry.get('id')}: component {entry.get('component')!r} has no tier "
                          f"(no chart in {config.VERSIONS_LOCK} owns it)")
    return comp_tier in tiers


def tier_images(lock: dict, tiers_doc: dict, tier: str, with_optional: bool = False) -> list:
    """Every `use: used` image whose component belongs to the tier or a tier it
    requires; with `with_optional`, also every optional add-on image, whatever
    its tier (add-ons are switched on separately). The registry image itself
    is not mirrored (it IS the mirror)."""
    config.resolve_tier(tiers_doc, tier)  # validates the name and the requires graph
    wanted = render.tier_closure(tiers_doc, tier)
    owner = _component_tiers(lock)
    entries = [e for e in lock.get("images") or [] if isinstance(e, dict)]
    return [_check_entry(e) for e in entries if _wanted(e, owner, wanted, with_optional)]


def all_images(lock: dict) -> list:
    """Every image any tier or add-on can run (`used` or `optional`), but the registry itself.
    A node's pull config covers all of them, so an add-on tier never pulls past the mirror."""
    return [_check_entry(e) for e in lock.get("images") or []
            if isinstance(e, dict) and e.get("use") in ("used", "optional") and e.get("id") != REGISTRY_IMAGE_ID]


def upstream_hosts(entries: Iterable[dict]) -> list:
    return sorted({e["registry"] for e in entries})


# --------------------------------------------------------------------------- registry container


def registry_state(runner: proc.Runner) -> Optional[dict]:
    """`docker inspect` of the registry container, or None when it does not exist."""
    return containers.inspect(runner, REGISTRY)


def ensure_registry(runner: proc.Runner, lock: dict, port: int = DEFAULT_PORT, *, create: bool = True) -> dict:
    """Start (or reuse) the mirror registry. Returns a check record.

    `create=False` (lab up) never creates it: creating it pulls the registry
    image itself from Docker Hub, and only `lab mirror` may use the network.
    """
    entry = lock_image(lock, REGISTRY_IMAGE_ID)
    image = f"{entry['registry']}/{entry['repository']}@{entry['digest']}"
    found = registry_state(runner)
    if found is None and not create:
        return check("registry", FAIL, f"the mirror registry {REGISTRY} does not exist",
                     "Run `./lab mirror --tier <tier>` first: it creates the registry and fills it.")
    if found is not None:
        running_image = containers.running_image(found)
        if entry["digest"] not in running_image:
            return check("registry", FAIL,
                         f"container {REGISTRY} exists but runs {running_image or 'an unknown image'}, "
                         f"not the pinned {entry['ref']}",
                         f"Remove it (`docker rm -f {REGISTRY}`; the image cache volume {REGISTRY_VOLUME} "
                         "is kept) and re-run `lab mirror`.")
        if not containers.is_running(found):
            res = runner(["docker", "start", REGISTRY], timeout=60)
            if not res.ok:
                return check("registry", FAIL, f"could not start {REGISTRY}: {res.output_tail(400).strip()}")
        published = containers.host_port(runner, REGISTRY, f"{CONTAINER_PORT}/tcp")
        if published != port:
            return check("registry", FAIL, f"{REGISTRY} publishes 127.0.0.1:{published}, not port {port}",
                         f"Pass --registry-port {published} (or --port for `lab mirror`).")
        return check("registry", PASS, f"{REGISTRY} running ({entry['ref']}) on 127.0.0.1:{port}")
    argv = [
        "docker", "run", "-d", "--name", REGISTRY,
        "--label", f"{LABEL}=mirror",
        "--restart", "unless-stopped",
        "-p", f"127.0.0.1:{port}:{CONTAINER_PORT}",
        "-v", f"{REGISTRY_VOLUME}:/var/lib/registry",
        "--read-only", "--tmpfs", "/tmp",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        # Uploads of large layers fill the page cache; 256 MiB was OOM-killed
        # under concurrent copies (measured 2026-11-18).
        "--memory", "1g",
        # distribution v3 exports OpenTelemetry traces by default; nothing may leave.
        "-e", "OTEL_TRACES_EXPORTER=none",
        "-e", "REGISTRY_STORAGE_DELETE_ENABLED=false",
        image,
    ]
    res = runner(argv, timeout=600)
    if not res.ok:
        return check("registry", FAIL, f"could not start {REGISTRY}: {res.output_tail(600).strip()}",
                     f"Is port 127.0.0.1:{port} free? Pick another with --port.")
    return check("registry", PASS, f"started {REGISTRY} ({entry['ref']}) on 127.0.0.1:{port}")


def connect_network(runner: proc.Runner, network: str = "kind") -> Optional[str]:
    """Attach the registry to `network` (idempotent). Returns an error text or None."""
    found = registry_state(runner) or {}
    nets = (found.get("NetworkSettings") or {}).get("Networks") or {}
    if network in nets:
        return None
    res = runner(["docker", "network", "connect", network, REGISTRY], timeout=60)
    if not res.ok and "already exists" not in res.output_tail(400):
        return res.output_tail(400).strip()
    return None


# --------------------------------------------------------------------------- copy and verify


def mirror_digest(runner: proc.Runner, entry: dict, port: int = DEFAULT_PORT) -> Optional[str]:
    res = runner(["crane", "digest", host_digest_ref(entry, port)], timeout=60)
    out = (res.stdout or "").strip()
    return out if res.ok and _DIGEST.match(out) else None


def copy_one(runner: proc.Runner, entry: dict, port: int = DEFAULT_PORT) -> dict:
    cid = f"image:{entry['id']}"
    if mirror_digest(runner, entry, port) == entry["digest"]:
        return check(cid, PASS, f"{entry['ref']} in the mirror (digest verified)")
    res = runner(["crane", "copy", upstream_ref(entry), host_ref(entry, port)], timeout=COPY_TIMEOUT_S)
    if not res.ok:
        return check(cid, FAIL, f"copy of {upstream_ref(entry)} failed: {res.output_tail(500).strip()}",
                     "Check the network and registry pull limits, then re-run `lab mirror` (copies resume).")
    got = mirror_digest(runner, entry, port)
    if got != entry["digest"]:
        return check(cid, FAIL, f"{entry['ref']}: mirror holds {got or 'nothing'}, lock says {entry['digest']}",
                     "The upstream tag moved or the copy was altered; do not use this mirror until it matches.")
    return check(cid, PASS, f"{entry['ref']} copied (digest verified)")


def missing(runner: proc.Runner, entries: list, port: int = DEFAULT_PORT) -> list:
    """Entries whose pinned digest is not in the mirror."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        digests = list(pool.map(lambda e: mirror_digest(runner, e, port), entries))
    return [e for e, d in zip(entries, digests) if d != e["digest"]]


def run_mirror(root: Path, tier: str, *, port: int = DEFAULT_PORT, with_optional: bool = False,
               extra_ids: Iterable[str] = (), runner: proc.Runner = proc.run) -> dict:
    checks: list = []
    try:
        lock = config.load_yaml(root / config.VERSIONS_LOCK, runner)
        tiers_doc = config.load_yaml(root / config.TIERS, runner)
        entries = tier_images(lock, tiers_doc, tier, with_optional)
        for image_id in extra_ids:
            entry = _check_entry(lock_image(lock, image_id))
            if entry not in entries:
                entries.append(entry)
        lock_image(lock, REGISTRY_IMAGE_ID)
    except (config.ConfigError, MirrorError) as exc:
        return {"command": "mirror", "tier": tier, "ok": False, "checks": [check("config", FAIL, str(exc))]}
    try:
        reg = ensure_registry(runner, lock, port)
    except containers.DockerError as exc:
        reg = check("registry", FAIL, str(exc), "Start the container runtime and re-run `lab mirror`.")
    checks.append(reg)
    if reg["status"] == FAIL:
        return {"command": "mirror", "tier": tier, "ok": False, "checks": checks}
    with concurrent.futures.ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        checks.extend(pool.map(lambda e: copy_one(runner, e, port), entries))
    ok = all(c["status"] != FAIL for c in checks)
    return {"command": "mirror", "tier": tier, "port": port, "registry": REGISTRY,
            "images": len(entries), "ok": ok, "checks": checks}
