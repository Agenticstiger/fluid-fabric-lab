# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab doctor`: is this machine able to run a tier?

Checks, in order: Python, config files, container runtime, every tool in
versions.lock.yaml, container VM memory against the tier's memory target in
tiers.yaml, host memory, and free disk. A tier that does not fit is a FAILURE
(that refusal fails the run; it never skips). A value
that cannot be read (runtime down, file unreadable) is also a failure.

Pattern credit: the "record pass/warn/fail, then print one verdict" shape and
reading VM memory from `docker info --format '{{json .}}'` (`MemTotal`) follow
the `doctor` command in AQUIFER's lakehouse.py (MIT, "Copyright (c) 2026
Christopher Ducci and the AQUIFER contributors"). Only the idea is reused; no
AQUIFER code or text is copied here.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import sys
from pathlib import Path
from typing import Callable, Mapping, Optional

from labctl import config, proc
from labctl.output import FAIL, PASS, WARN, check

GIB = 1024**3
# The default free-disk floor: the pinned images are about 3.6 GB compressed and
# roughly double once unpacked, and the lab-built images, the kind node image and
# the volumes come on top. Override with --min-free-disk-gib.
DEFAULT_MIN_FREE_DISK_GIB = 15.0
RUNTIME_TIMEOUT_S = 30
TOOL_TIMEOUT_S = 30

# How to read each pinned tool's version. A tool in versions.lock.yaml that is
# missing here fails the doctor run ("no probe"), it is never ignored.
TOOL_PROBES: dict = {
    "helm": ["helm", "version", "--short"],
    "helm-diff": ["helm", "plugin", "list"],  # parsed by plugin name below
    "helmfile": ["helmfile", "version", "-o", "short"],
    "kind": ["kind", "version"],
    "kubeconform": ["kubeconform", "-v"],
    "gitleaks": ["gitleaks", "version"],
    "reuse": ["reuse", "--version"],
    "trivy": ["trivy", "--version"],
    "crane": ["crane", "version"],
    "yq": ["yq", "--version"],
    "opa": ["opa", "version"],
}
HELM_PLUGINS = {"helm-diff": "diff"}

_SEMVER = re.compile(r"v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)")


def parse_version(text: str) -> Optional[str]:
    """First semantic version in `text`, without a leading `v`."""
    m = _SEMVER.search(text or "")
    return m.group(1) if m else None


def parse_helm_plugin_version(text: str, plugin: str) -> Optional[str]:
    for line in (text or "").splitlines():
        cols = line.split()
        if len(cols) >= 2 and cols[0] == plugin:
            return parse_version(cols[1])
    return None


def norm(version: str) -> str:
    return str(version).strip().lstrip("v")


# --------------------------------------------------------------------------- runtime


def select_runtime(explicit: Optional[str], env: Mapping[str, str], which: Callable) -> str:
    """docker (Docker Desktop, Engine, Colima, Rancher Desktop moby, OrbStack) or podman.

    kind picks its provider from KIND_EXPERIMENTAL_PROVIDER, so the doctor
    honours the same variable.
    """
    if explicit:
        return explicit
    provider = env.get("KIND_EXPERIMENTAL_PROVIDER", "").strip()
    if provider:
        return provider
    if which("docker"):
        return "docker"
    if which("podman"):
        return "podman"
    return "docker"


def probe_runtime(name: str, runner: proc.Runner) -> tuple:
    """Return (check, vm_memory_bytes or None, detail)."""
    hint = {
        "docker": "Start Docker Desktop, Colima, Rancher Desktop (moby) or Docker Engine and retry.",
        "podman": "Start the machine with `podman machine start` (macOS/Windows) or the podman service, and retry.",
    }
    if name == "docker":
        res = runner(["docker", "info", "--format", "{{json .}}"], timeout=RUNTIME_TIMEOUT_S)
    elif name == "podman":
        res = runner(["podman", "info", "--format", "json"], timeout=RUNTIME_TIMEOUT_S)
    else:
        return (
            check("runtime", FAIL, f"container runtime {name!r} is not supported by the lab",
                  "Use docker or podman (kind's KIND_EXPERIMENTAL_PROVIDER)."),
            None,
            {},
        )
    if res.missing:
        return check("runtime", FAIL, f"{name} CLI not found on PATH", hint[name]), None, {}
    if res.timed_out:
        return check("runtime", FAIL, f"`{name} info` did not answer within {RUNTIME_TIMEOUT_S}s", hint[name]), None, {}
    try:
        info = json.loads(res.stdout or "{}")
    except ValueError:
        info = {}
    if name == "docker":
        server = info.get("ServerVersion") or ""
        errors = info.get("ServerErrors") or []
        if res.returncode != 0 or not server or errors:
            why = "; ".join(map(str, errors)) or res.output_tail(300).strip() or "no server version"
            return check("runtime", FAIL, f"docker daemon is not reachable ({why})", hint[name]), None, {}
        mem = info.get("MemTotal")
        detail = {
            "runtime": "docker",
            "server_version": server,
            "operating_system": info.get("OperatingSystem"),
            "cpus": info.get("NCPU"),
            "vm_memory_bytes": mem,
        }
        msg = f"docker {server} on {info.get('OperatingSystem', '?')}, {info.get('NCPU', '?')} CPUs"
    else:
        host = info.get("host") or {}
        version = (info.get("version") or {}).get("Version") or ""
        if res.returncode != 0 or not host:
            return check("runtime", FAIL, f"podman is not reachable ({res.output_tail(300).strip()})", hint[name]), None, {}
        mem = host.get("memTotal")
        detail = {"runtime": "podman", "server_version": version, "cpus": host.get("cpus"), "vm_memory_bytes": mem}
        msg = f"podman {version}, {host.get('cpus', '?')} CPUs"
    if not isinstance(mem, int) or mem <= 0:
        return check("runtime", PASS, msg, detail=detail), None, detail
    return check("runtime", PASS, msg, detail=detail), mem, detail


# --------------------------------------------------------------------------- memory


def host_memory_bytes(runner: proc.Runner, system: str, meminfo: str = "/proc/meminfo") -> Optional[int]:
    if system == "Darwin":
        res = runner(["sysctl", "-n", "hw.memsize"], timeout=10)
        try:
            return int(res.stdout.strip()) if res.ok else None
        except ValueError:
            return None
    if system == "Linux":
        try:
            for line in Path(meminfo).read_text().splitlines():
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
        except (OSError, ValueError, IndexError):
            return None
    return None


def memory_checks(tier: config.Tier, all_tiers: dict, vm_bytes: Optional[int], host_bytes: Optional[int],
                  runtime: str, guidance: Optional[dict]) -> list:
    target = tier.memory_target_gib
    host_gib = host_bytes / GIB if host_bytes else None
    out = []
    if vm_bytes is None:
        out.append(check(
            "memory", FAIL,
            f"cannot read the {runtime} VM's memory, so tier {tier.name!r} (needs {target:g} GiB) cannot be confirmed to fit",
            "Fix the runtime check above; the memory check never passes on an unknown value."))
    else:
        vm_gib = vm_bytes / GIB
        fits = {n: (vm_gib >= t.memory_target_gib) for n, t in all_tiers.items()}
        detail = {"tier": tier.name, "target_gib": target, "vm_gib": round(vm_gib, 2),
                  "fits": fits, "estimate": "tier targets are estimates"}
        if vm_gib >= target:
            out.append(check("memory", PASS,
                             f"{runtime} VM has {vm_gib:.2f} GiB; tier {tier.name!r} target is {target:g} GiB",
                             detail=detail))
        else:
            if host_gib is not None and host_gib < target:
                advice = (f"This machine has {host_gib:.1f} GiB in total, below the tier target. "
                          "Use a larger machine or a smaller tier.")
            else:
                advice = (f"Give the runtime VM at least {target:g} GiB (Docker Desktop: Settings > Resources > Memory; "
                          f"Colima: `colima start --memory {int(-(-target // 1))}`; "
                          "Podman: `podman machine set --memory <MiB>`), or pick a smaller tier.")
            smaller = [n for n, ok in fits.items() if ok]
            if smaller:
                advice += f" Tiers that fit now: {', '.join(smaller)}."
            out.append(check("memory", FAIL,
                             f"tier {tier.name!r} does not fit: target {target:g} GiB, {runtime} VM has {vm_gib:.2f} GiB",
                             advice, detail))
    if host_gib is None:
        out.append(check("host-memory", WARN, f"could not read host memory on {platform.system()}",
                         "The VM memory check above still decides whether the tier fits."))
    else:
        note = ""
        if guidance:
            if host_gib < 24 and guidance.get("laptop_16gb"):
                note = f"; guidance for ~16 GB hosts: {guidance['laptop_16gb']}"
            elif guidance.get("machine_32gb"):
                note = f"; guidance for 32 GB hosts: {guidance['machine_32gb']}"
        status = FAIL if host_gib < target else PASS
        out.append(check("host-memory", status, f"host has {host_gib:.1f} GiB{note}",
                         "No VM setting can make this tier fit on this host." if status == FAIL else "",
                         {"host_gib": round(host_gib, 2)}))
    return out


# --------------------------------------------------------------------------- tools


def tool_checks(lock: dict, runner: proc.Runner, strict: bool) -> list:
    tools = lock.get("tools")
    if not isinstance(tools, list) or not tools:
        return [check("tools", FAIL, f"{config.VERSIONS_LOCK} has no `tools` list")]
    out = []
    plugin_listing: Optional[proc.Result] = None
    for entry in tools:
        name = entry.get("name") if isinstance(entry, dict) else None
        want = entry.get("version") if isinstance(entry, dict) else None
        cid = f"tool:{name}"
        if not name or not want:
            out.append(check("tools", FAIL, f"malformed tools entry in {config.VERSIONS_LOCK}: {entry!r}"))
            continue
        argv = TOOL_PROBES.get(name)
        if argv is None:
            out.append(check(cid, FAIL, f"no version probe for {name} (pinned {want})",
                             "Teach labctl/doctor.py TOOL_PROBES how to read its version."))
            continue
        if name in HELM_PLUGINS:
            if plugin_listing is None:
                plugin_listing = runner(argv, timeout=TOOL_TIMEOUT_S)
            res = plugin_listing
            have = parse_helm_plugin_version(res.stdout, HELM_PLUGINS[name]) if res.ok else None
            missing = res.missing or have is None
            how = f"helm plugin install https://github.com/databus23/helm-diff --version {want}"
        else:
            res = runner(argv, timeout=TOOL_TIMEOUT_S)
            have = parse_version(res.stdout + "\n" + res.stderr) if not res.missing else None
            missing = res.missing or have is None
            how = f"install {name} {want} (checksum source: {entry.get('checksum_url', 'versions.lock.yaml')})"
        if missing:
            out.append(check(cid, FAIL, f"{name} not found (pinned {want})", how))
        elif norm(have) == norm(want):
            out.append(check(cid, PASS, f"{name} {have}", detail={"pinned": want, "found": have}))
        else:
            status = FAIL if strict else WARN
            note = f" ({entry['note']})" if entry.get("note") else ""
            out.append(check(cid, status, f"{name} {have} differs from the pin {want}{note}",
                             f"CI uses {want}; install it to reproduce CI exactly"
                             + ("" if strict else ", or run with --strict to make this a failure"),
                             {"pinned": want, "found": have}))
    return out


# --------------------------------------------------------------------------- disk


def disk_check(root: Path, min_free_gib: float, disk_usage: Callable = shutil.disk_usage) -> dict:
    try:
        free = disk_usage(str(root)).free / GIB
    except OSError as exc:
        return check("disk", FAIL, f"cannot read free disk at {root}: {exc}")
    detail = {"free_gib": round(free, 1), "min_free_gib": min_free_gib,
              "default_min_free_gib": DEFAULT_MIN_FREE_DISK_GIB}
    if free >= min_free_gib:
        return check("disk", PASS, f"{free:.1f} GiB free (minimum {min_free_gib:g} GiB)", detail=detail)
    return check("disk", FAIL, f"only {free:.1f} GiB free; the lab needs at least {min_free_gib:g} GiB",
                 "Free space (e.g. `docker system prune`, or grow the runtime VM disk) and retry.", detail)


# --------------------------------------------------------------------------- driver


def run_doctor(
    root: Path,
    tier_name: str,
    *,
    runtime: Optional[str] = None,
    strict: bool = False,
    min_free_disk_gib: float = DEFAULT_MIN_FREE_DISK_GIB,
    runner: proc.Runner = proc.run,
    which: Callable = proc.which,
    env: Optional[Mapping[str, str]] = None,
    system: Optional[str] = None,
    host_memory: Optional[Callable[[], Optional[int]]] = None,
    disk_usage: Callable = shutil.disk_usage,
) -> dict:
    env = os.environ if env is None else env
    system = system or platform.system()
    checks: list = []

    py = sys.version_info
    checks.append(check("python", PASS if py >= (3, 10) else FAIL, f"Python {py.major}.{py.minor}.{py.micro}",
                        "" if py >= (3, 10) else "Python 3.10 or newer is required."))

    lock = tiers_doc = None
    try:
        lock = config.load_yaml(root / config.VERSIONS_LOCK, runner)
        tiers_doc = config.load_yaml(root / config.TIERS, runner)
        checks.append(check("config", PASS, f"read {config.VERSIONS_LOCK} and {config.TIERS}"))
    except config.ConfigError as exc:
        checks.append(check("config", FAIL, str(exc)))

    tier = None
    all_tiers: dict = {}
    if tiers_doc is not None:
        try:
            tier = config.resolve_tier(tiers_doc, tier_name)
            for n in config.tier_names(tiers_doc):
                all_tiers[n] = config.resolve_tier(tiers_doc, n)
        except config.ConfigError as exc:
            checks.append(check("tier", FAIL, str(exc)))

    rt_name = select_runtime(runtime, env, which)
    rt_check, vm_bytes, _ = probe_runtime(rt_name, runner)
    if rt_check["status"] == PASS and vm_bytes is None:
        rt_check = check("runtime", FAIL, rt_check["message"] + "; it did not report its memory",
                         "The memory check cannot pass without the VM's memory size.", rt_check.get("detail"))
    checks.append(rt_check)

    if lock is not None:
        checks.extend(tool_checks(lock, runner, strict))
    else:
        checks.append(check("tools", FAIL, f"cannot compare tool versions: {config.VERSIONS_LOCK} unreadable"))

    host_bytes = host_memory() if host_memory else host_memory_bytes(runner, system)
    if tier is not None:
        checks.extend(memory_checks(tier, all_tiers, vm_bytes, host_bytes, rt_name,
                                    (tiers_doc or {}).get("host_guidance")))
    else:
        checks.append(check("memory", FAIL, f"cannot check memory: tier {tier_name!r} could not be resolved"))

    checks.append(disk_check(root, min_free_disk_gib, disk_usage))

    failed = any(c["status"] == FAIL for c in checks) or (strict and any(c["status"] == WARN for c in checks))
    return {
        "command": "doctor",
        "tier": tier_name,
        "cluster": "kind",
        "runtime": rt_name,
        "strict": strict,
        "ok": not failed,
        "checks": checks,
    }
