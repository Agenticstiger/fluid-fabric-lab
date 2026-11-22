# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Shared helpers for the L0 gates (standard library only).

Not a gate: the leading underscore keeps `lab l0` from running it.

Conventions every gate follows
- Exit 0 = pass, 1 = the check found a problem, 2 = the check could not run
  (tool missing, bad input). Never 0 on a skip.
- No network. Every subprocess gets the proxy variables pointed at a closed
  loopback port (the same trick labctl/proc.py uses), so a tool that tries the
  network fails instead of silently succeeding.
- YAML is read through `yq -o=json` (mikefarah/yq, pinned in
  versions.lock.yaml): the standard library has no YAML reader and the lab
  already depends on yq (borrow before build, as labctl/config.py does).
- `--root DIR` points a gate at another tree (a planted fixture); the default
  is this repository. Fixtures never change which rules apply.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Iterator, Optional

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parents[2]
OFFLINE_PROXY = "http://127.0.0.1:9"
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")

EXIT_PASS, EXIT_FAIL, EXIT_ERROR = 0, 1, 2


class GateError(Exception):
    """The gate could not run (missing tool, unreadable input). Exit 2."""


# --------------------------------------------------------------------- output


class Report:
    """Collects findings and prints one line each. `finish()` returns the exit code."""

    def __init__(self, gate: str) -> None:
        self.gate = gate
        self.failures: list = []
        self.passes = 0

    def ok(self, msg: str) -> None:
        self.passes += 1
        print(f"  PASS  {msg}")

    def fail(self, msg: str) -> None:
        self.failures.append(msg)
        print(f"  FAIL  {msg}")

    def note(self, msg: str) -> None:
        print(f"  NOTE  {msg}")

    def finish(self) -> int:
        if self.failures:
            print(f"gate {self.gate}: FAIL ({len(self.failures)} problem(s), {self.passes} passed)")
            return EXIT_FAIL
        if self.passes == 0:
            print(f"gate {self.gate}: FAIL (nothing was checked; an empty check never passes)")
            return EXIT_FAIL
        print(f"gate {self.gate}: PASS ({self.passes} check(s))")
        return EXIT_PASS


def main_wrapper(fn) -> None:
    """Run a gate's main(); turn GateError into exit 2 with a clear message."""
    try:
        code = fn()
    except GateError as exc:
        print(f"  ERROR {exc}")
        print("gate could not run: exit 2 (an unrunnable check is a failure, never a skip)")
        code = EXIT_ERROR
    except KeyboardInterrupt:  # pragma: no cover
        code = 130
    sys.stdout.flush()
    sys.exit(code)


# ---------------------------------------------------------------- processes


def offline_env(extra: Optional[dict] = None) -> dict:
    env = dict(os.environ)
    for var in PROXY_VARS:
        env[var] = OFFLINE_PROXY
    env["NO_PROXY"] = env["no_proxy"] = ""
    env["LAB_OFFLINE"] = "1"
    if extra:
        env.update(extra)
    return env


def need(tool: str) -> str:
    path = shutil.which(tool)
    if not path:
        raise GateError(f"{tool} is not on PATH (pinned in versions.lock.yaml tools; see `./lab doctor`)")
    return path


def run(argv: list, *, cwd: Optional[Path] = None, env: Optional[dict] = None,
        timeout: float = 600, stdin: Optional[str] = None) -> subprocess.CompletedProcess:
    """Run a list argv (never a shell) offline and capture output."""
    if isinstance(argv, (str, bytes)):
        raise TypeError("argv must be a list")
    need(argv[0])
    try:
        return subprocess.run(  # noqa: S603 - list argv, shell=False
            [str(a) for a in argv], cwd=str(cwd) if cwd else None,
            env=env if env is not None else offline_env(),
            input=stdin, capture_output=True, text=True, errors="replace",
            timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise GateError(f"{argv[0]} timed out after {timeout}s") from exc


# ---------------------------------------------------------------------- yaml


def yaml_load(path: Path):
    """Parse one YAML file (first document) into Python via yq."""
    if not Path(path).is_file():
        raise GateError(f"{path} not found")
    res = run(["yq", "-o=json", "-I=0", ".", str(path)], timeout=60)
    if res.returncode != 0:
        raise GateError(f"yq could not parse {path}: {res.stderr.strip()[:300]}")
    lines = [ln for ln in res.stdout.splitlines() if ln.strip()]
    return json.loads(lines[0]) if lines else None


def yaml_docs(paths: Iterable[Path]) -> list:
    """Every YAML document in `paths` as (file, doc); empty documents dropped."""
    out: list = []
    paths = [str(p) for p in paths]
    for i in range(0, len(paths), 200):
        chunk = paths[i:i + 200]
        res = run(["yq", "ea", "-o=json", "-I=0", '[{"f": filename, "d": .}]', *chunk], timeout=300)
        if res.returncode != 0:
            raise GateError(f"yq could not parse manifests: {res.stderr.strip()[:400]}")
        for line in res.stdout.splitlines():
            if not line.strip():
                continue
            for item in json.loads(line):
                if isinstance(item.get("d"), dict) and item["d"]:
                    out.append((item["f"], item["d"]))
    return out


def manifest_files(directory: Path) -> list:
    return sorted(p for p in Path(directory).rglob("*")
                  if p.is_file() and p.suffix in (".yaml", ".yml") and p.stat().st_size > 0)


# -------------------------------------------------------------------- render


def _labctl_render():
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from labctl import render  # noqa: E402  (the launcher's module, reused, not copied)

    return render


def tier_names(root: Path) -> list:
    doc = yaml_load(root / "tiers.yaml")
    tiers = (doc or {}).get("tiers")
    if not isinstance(tiers, dict) or not tiers:
        raise GateError(f"{root / 'tiers.yaml'} has no tiers")
    return list(tiers)


def render_tier(root: Path, tier: str, out_dir: Path) -> None:
    """`helmfile template` one tier from the committed chart cache, offline.

    Reuses labctl's render (chart-cache verification, hermetic HOME, poisoned
    proxies, --skip-deps, per-release output check). The helmfile selects the
    tier through its environment, so -e <tier> is passed.
    """
    render = _labctl_render()
    doc = render.run_render(root, tier, out=str(out_dir), environment=tier)
    if not doc.get("ok"):
        bad = []
        for c in doc.get("checks", []):
            if c.get("status") == "pass":
                continue
            detail = c.get("detail") or {}
            tail = detail.get("output_tail") or "; ".join(detail.get("problems") or [])
            bad.append(f"{c.get('id')}: {c.get('message')}" + (f" | {tail[-600:]}" if tail else ""))
        raise RenderFailed(tier, bad or ["render reported not ok with no failing check"])


class RenderFailed(GateError):
    def __init__(self, tier: str, problems: list) -> None:
        self.tier, self.problems = tier, problems
        super().__init__(f"render of tier {tier} failed: " + "; ".join(problems)[:1500])


def render_all(root: Path, tiers: Optional[list] = None) -> tuple:
    """Render every tier into a fresh temp dir. Returns (base_dir, {tier: dir})."""
    base = Path(tempfile.mkdtemp(prefix="l0-render-"))
    out = {}
    for t in tiers or tier_names(root):
        render_tier(root, t, base / t)
        out[t] = base / t
    return base, out


def manifests_by_tier(args, root: Path) -> tuple:
    """Either --manifests DIR (one pseudo tier) or a fresh offline render of every tier."""
    if getattr(args, "manifests", None):
        d = Path(args.manifests).resolve()
        if not d.is_dir():
            raise GateError(f"--manifests {d} is not a directory")
        return None, {getattr(args, "tier", None) or "manifests": d}
    return render_all(root, [args.tier] if getattr(args, "tier", None) else None)


# -------------------------------------------------------------------- images

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def parse_image(ref: str) -> dict:
    """Split an image reference into registry/repository/tag/digest (Docker rules)."""
    ref = ref.strip()
    digest = None
    if "@" in ref:
        ref, digest = ref.split("@", 1)
    tag = None
    last = ref.rsplit("/", 1)[-1]
    if ":" in last:
        ref, tag = ref.rsplit(":", 1)
    parts = ref.split("/")
    if len(parts) > 1 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        registry, repo = parts[0], "/".join(parts[1:])
    else:
        registry, repo = "docker.io", ref
    if registry in ("index.docker.io", "registry-1.docker.io"):
        registry = "docker.io"
    if registry == "docker.io" and "/" not in repo:
        repo = "library/" + repo
    return {"registry": registry, "repository": repo, "tag": tag, "digest": digest,
            "name": f"{registry}/{repo}"}


WORKLOAD_KINDS = ("Pod", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job",
                  "CronJob")


def pod_spec(doc: dict) -> Optional[dict]:
    """The PodSpec of a built-in workload object (WORKLOAD_KINDS), or None."""
    kind = doc.get("kind")
    spec = doc.get("spec") or {}
    if kind == "Pod":
        return spec
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job"):
        return ((spec.get("template") or {}).get("spec")) or {}
    if kind == "CronJob":
        return (((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
    return None


def containers(ps: dict) -> list:
    return list(ps.get("initContainers") or []) + list(ps.get("containers") or []) + \
        list(ps.get("ephemeralContainers") or [])


def iter_strings(obj, path: str = "") -> Iterator:
    """Yield (json-path, string) for every string value in a nested structure."""
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from iter_strings(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from iter_strings(v, f"{path}[{i}]")


def obj_name(doc: dict) -> str:
    md = doc.get("metadata") or {}
    ns = md.get("namespace")
    return f"{doc.get('kind')}/{(ns + '/') if ns else ''}{md.get('name')}"


# ----------------------------------------------------------------- files


def rel(p: Path, root: Path) -> str:
    try:
        return Path(p).resolve().relative_to(Path(root).resolve()).as_posix()
    except ValueError:
        return str(p)


def walk_files(root: Path, skip_dirs: Iterable[str] = (".git",)) -> Iterator[Path]:
    skip = set(skip_dirs)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in skip and d != "__pycache__")
        for f in sorted(filenames):
            yield Path(dirpath) / f


def is_binary(path: Path) -> bool:
    try:
        with open(path, "rb") as fh:
            return b"\0" in fh.read(8192)
    except OSError:
        return True


# Binary formats whose bytes are not text (paths are still checked by callers).
BINARY_MAGIC = (b"\x1f\x8b", b"PK\x03\x04", b"\x89PNG", b"\xff\xd8\xff", b"%PDF", b"\x7fELF", b"BZh",
                b"\xfd7zXZ", b"(\xb5/\xfd", b"GIF8")


def read_text_any(path: Path) -> tuple:
    """(text or None, how). A file with NUL bytes or in UTF-16 is still text:
    only known binary formats (BINARY_MAGIC, .pyc) return None."""
    raw = Path(path).read_bytes()
    if b"\0" not in raw[:8192]:
        return raw.decode("utf-8", "replace"), "text"
    if Path(path).suffix == ".pyc" or raw.startswith(BINARY_MAGIC):
        return None, "binary"
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", "replace"), "utf-16"
    even, odd = raw[0:4096:2].count(0), raw[1:4096:2].count(0)
    if odd > len(raw[1:4096:2]) * 0.4:
        return raw.decode("utf-16-le", "replace"), "utf-16"
    if even > len(raw[0:4096:2]) * 0.4:
        return raw.decode("utf-16-be", "replace"), "utf-16"
    return raw.replace(b"\0", b"").decode("latin-1"), "nul-stripped"


def glob_to_regex(glob: str) -> re.Pattern:
    """Path glob with `**` (any depth), `*` and `?` (within one segment)."""
    out, i = "", 0
    while i < len(glob):
        c = glob[i]
        if glob.startswith("**/", i):
            out += "(?:.*/)?"
            i += 3
        elif glob.startswith("**", i):
            out += ".*"
            i += 2
        elif c == "*":
            out += "[^/]*"
            i += 1
        elif c == "?":
            out += "[^/]"
            i += 1
        else:
            out += re.escape(c)
            i += 1
    return re.compile("^" + out + "$")


# ----------------------------------------------------------------- quantities

_BIN = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60}
_DEC = {"k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15, "E": 10**18, "m": 10**-3, "": 1}


def mem_bytes(q) -> Optional[float]:
    """Kubernetes memory quantity -> bytes."""
    if q is None:
        return None
    if isinstance(q, (int, float)) and not isinstance(q, bool):
        return float(q)
    m = re.fullmatch(r"\s*([0-9.]+(?:[eE][-+]?[0-9]+)?)\s*([KMGTPE]i|[kMGTPEm]?)\s*", str(q))
    if not m:
        raise ValueError(f"not a memory quantity: {q!r}")
    num, unit = float(m.group(1)), m.group(2)
    return num * (_BIN[unit] if unit in _BIN else _DEC[unit])


def cpu_cores(q) -> Optional[float]:
    if q is None:
        return None
    if isinstance(q, (int, float)) and not isinstance(q, bool):
        return float(q)
    s = str(q).strip()
    if s.endswith("m"):
        return float(s[:-1]) / 1000
    return float(s)


def jvm_size(s: str) -> float:
    """JVM size (-Xmx8G, 1g, 512m, 1024k, bytes) -> bytes."""
    m = re.fullmatch(r"([0-9]+)([kKmMgGtT]?)", s.strip())
    if not m:
        raise ValueError(f"not a JVM size: {s!r}")
    mult = {"": 1, "k": 2**10, "m": 2**20, "g": 2**30, "t": 2**40}[m.group(2).lower()]
    return float(m.group(1)) * mult


GIB = float(2**30)
MIB = float(2**20)


def copy_tree(src: Path, dst: Path, names: Iterable[str]) -> None:
    """Copy selected top-level entries of `src` into `dst` (for planted-overlay runs)."""
    dst.mkdir(parents=True, exist_ok=True)
    for n in names:
        s = src / n
        if s.is_dir():
            shutil.copytree(s, dst / n, symlinks=True)
        elif s.exists():
            shutil.copy2(s, dst / n)
