# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab render --tier T`: `helmfile template` for one tier, offline, from the chart cache.

Steps (every one fails the command, none is skipped):
1. Verify platform/charts/ against platform/charts/SHA256SUMS: every listed file
   exists and matches, no unlisted file is present, and every chart sha256 in
   versions.lock.yaml agrees with SHA256SUMS.
2. Resolve the tier's releases from tiers.yaml (its own plus those of `requires`).
3. Check where every release's chart comes from (`helmfile list`, offline):
   each must be an archive or directory versions.lock.yaml pins inside
   platform/charts/, or the lab's own glue chart (platform/components/
   lab-objects). A chart path anywhere else (an unpacked copy in products/,
   a modified chart) fails, even though it would render.
4. Run `helmfile template` hermetically: list argv, a throwaway HOME and Helm
   cache/config/data dirs (so no local repo cache or plugin can help), every
   proxy variable pointed at a closed loopback port (so a tool that honours
   proxies cannot reach the network), an empty KUBECONFIG, `--skip-deps` (no
   `helm repo update` or `helm dependency build`) and `--environment <tier>`.
5. Check the rendered set against tiers.yaml. Every release the tier enables
   must have written manifests (helmfile silently drops a release whose
   `installed:` is false, so this makes "renders every release" true rather
   than assumed). A release that tiers.yaml assigns only to other tiers or to
   an off-by-default add-on must NOT render (the helmfile ignored the tier).
   Releases tiers.yaml does not name (lab-owned glue such as the CNPG Cluster
   or Keycloak CR wrappers) are reported and allowed.

Helmfile contract this relies on: the tiers are the helmfile's ENVIRONMENTS
(`-e <tier>`, generated from tiers.yaml), and the helmfile turns releases on
from the environment name. Name selectors are deliberately not used, so
lab-owned glue releases the tier needs are rendered too. `--environment`
overrides the environment name (it defaults to the tier).

Proxy poisoning stops only tools that honour proxy variables; a helmfile
`exec` or hook could still open a socket. The L0 render gate therefore also
rejects network-capable template functions and hook commands statically, and
CI runs every gate inside a network namespace with only loopback.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Optional

from labctl import config, proc
from labctl.output import FAIL, PASS, check

SUMS_FILE = "SHA256SUMS"
# Files in the chart cache that are documentation or REUSE metadata, not cache entries.
CACHE_META = {SUMS_FILE, "README.md", "REUSE.toml"}
HELMFILE_TIMEOUT_S = 900
# The lab's own glue chart (CNPG Cluster, Keycloak CR, Gateway objects): first
# party, REUSE- and licence-checked like the rest of platform/.
GLUE_CHARTS = ("platform/components/lab-objects",)
MANIFEST_SUFFIXES = (".yaml", ".yml")  # helm keeps a chart's crds/ file names as they are
_SUM_LINE = re.compile(r"^([0-9a-f]{64}) [ *](.+)$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_KIND = re.compile(r"^kind:\s*['\"]?([A-Za-z0-9]+)", re.M)


class CacheError(Exception):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_sums(path: Path) -> dict:
    """Parse a `sha256sum`-format file into {relative path: hex digest}."""
    if not path.is_file():
        raise CacheError(f"{path} not found")
    sums: dict = {}
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        m = _SUM_LINE.match(line)
        if not m:
            raise CacheError(f"{path.name}:{n}: not a sha256sum line: {line!r}")
        digest, rel = m.group(1), m.group(2).strip()
        parts = Path(rel).parts
        if rel.startswith("/") or ".." in parts or not parts:
            raise CacheError(f"{path.name}:{n}: unsafe path {rel!r}")
        if rel in sums:
            raise CacheError(f"{path.name}:{n}: {rel} listed twice")
        sums[rel] = digest
    if not sums:
        raise CacheError(f"{path} lists no files")
    return sums


def verify_chart_cache(root: Path, lock: Optional[dict] = None) -> tuple:
    """Return (number of files verified, list of problems)."""
    charts = root / config.CHARTS_DIR
    try:
        sums = read_sums(charts / SUMS_FILE)
    except CacheError as exc:
        return 0, [str(exc)]
    problems = []
    for rel, digest in sorted(sums.items()):
        f = charts / rel
        if not f.is_file():
            problems.append(f"{config.CHARTS_DIR}/{rel}: listed in {SUMS_FILE} but missing")
        elif sha256_file(f) != digest:
            problems.append(f"{config.CHARTS_DIR}/{rel}: sha256 does not match {SUMS_FILE}")
    for f in sorted(charts.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(charts).as_posix()
        # Dotfiles count too: a planted .helmignore inside a cached chart
        # directory changes what Helm renders, so it must be listed and match.
        if rel in CACHE_META:
            continue
        if rel not in sums:
            problems.append(f"{config.CHARTS_DIR}/{rel}: present but not listed in {SUMS_FILE}")
    if lock is not None:
        for entry in lock.get("charts") or []:
            if not isinstance(entry, dict):
                continue
            file, digest = entry.get("file"), entry.get("sha256")
            if not file or not isinstance(digest, str) or not _HEX64.match(digest):
                continue  # directory charts (sha256: dir) are covered file by file in SHA256SUMS
            prefix = config.CHARTS_DIR + "/"
            if not file.startswith(prefix):
                problems.append(f"{config.VERSIONS_LOCK}: chart {entry.get('name')} file {file} is outside {config.CHARTS_DIR}")
                continue
            rel = file[len(prefix):]
            if sums.get(rel) != digest:
                problems.append(f"{config.VERSIONS_LOCK}: chart {entry.get('name')} sha256 disagrees with {SUMS_FILE} for {rel}")
    return len(sums), problems


def helmfile_argv(helmfile: Path, tier: config.Tier, out_dir: Path, environment: Optional[str] = None) -> list:
    argv = ["helmfile", "--file", str(helmfile), "--no-color", "--quiet",
            "--environment", environment or tier.name]
    argv += [
        "template",
        "--skip-deps",
        "--include-crds",
        "--skip-tests",
        "--output-dir", str(out_dir),
        "--output-dir-template", "{{ .OutputDir }}/{{ .Release.Name }}",
    ]
    return argv


def hermetic_env(home: Path) -> dict:
    """Minimal environment for an offline render: nothing from the user's Helm state."""
    base = {k: v for k, v in os.environ.items() if k in ("PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT")}
    kubeconfig = home / "kubeconfig"
    kubeconfig.write_text("")
    return proc.offline_env(
        base,
        HOME=str(home),
        HELM_CACHE_HOME=str(home / "helm-cache"),
        HELM_CONFIG_HOME=str(home / "helm-config"),
        HELM_DATA_HOME=str(home / "helm-data"),
        KUBECONFIG=str(kubeconfig),
    )


def known_releases(tiers_doc: dict) -> dict:
    """{release: [tiers or add-ons that enable it]} for every release tiers.yaml names."""
    owners: dict = {}
    for name, spec in (tiers_doc.get("tiers") or {}).items():
        for rel in (spec or {}).get("releases") or []:
            owners.setdefault(rel, []).append(name)
    for name, spec in (tiers_doc.get("optional_addons") or {}).items():
        rel = (spec or {}).get("release")
        if rel:
            owners.setdefault(rel, []).append(f"add-on {name} (off by default)")
    return owners


def collect_outputs(out_dir: Path, releases: list) -> dict:
    found = {}
    for rel in releases:
        d = out_dir / rel
        files = sorted(p for p in d.rglob("*") if p.is_file() and p.suffix in MANIFEST_SUFFIXES) if d.is_dir() else []
        kinds: Counter = Counter()
        for p in files:
            kinds.update(_KIND.findall(p.read_text(errors="replace")))
        found[rel] = {"files": len(files), "objects": sum(kinds.values()), "kinds": dict(sorted(kinds.items()))}
    return found


# Static rules for the Helmfile and every .gotmpl beside it (checked BEFORE the
# render runs anything). Proxy poisoning does not stop a template `exec` or a
# hook from opening a socket, so these are refused outright.
_TEMPLATE_ACTION = re.compile(r"\{\{.*?\}\}", re.S)
_NET_FUNCS = re.compile(r"\b(exec|envExec|fetchSecretValue|expandSecretRefs|httpGet|readURL)\b")
_REMOTE_STATE = re.compile(r"(?:git::|s3::|gcs::|https?://)")
_HOOK_CMD = re.compile(r"^\s*command:\s*['\"]?([^'\"\s#]+)", re.M)
_NET_TOOLS = re.compile(r"(?<![\w./-])(curl|wget|nc|ncat|socat|telnet|ssh|scp|python[0-9.]*|perl|ruby|node|"
                        r"openssl|busybox)(?![\w./-])|/dev/(?:tcp|udp)/")
HOOK_COMMANDS = {"sh"}


def static_helmfile_problems(helmfile: Path) -> list:
    """Network-capable constructs in the Helmfile (and .gotmpl files beside it)."""
    problems = []
    files = [helmfile] + sorted(p for p in helmfile.parent.rglob("*.gotmpl") if p != helmfile
                                and "charts" not in p.relative_to(helmfile.parent).parts)
    for f in files:
        text = f.read_text(errors="replace")
        shown = f.name if f == helmfile else f.relative_to(helmfile.parent).as_posix()
        for m in _TEMPLATE_ACTION.finditer(text):
            action = m.group(0)
            if action.startswith(("{{/*", "{{- /*")):
                continue
            fn = _NET_FUNCS.search(action)
            if fn:
                line = text.count("\n", 0, m.start()) + 1
                problems.append(f"{shown}:{line}: template function `{fn.group(1)}` can reach the network")
        lines = text.splitlines()
        in_composition = False
        for n, line in enumerate(lines, 1):
            s = line.split("#", 1)[0] if not line.lstrip().startswith("- |") else line
            if re.match(r"^(helmfiles|bases):", s):
                in_composition = True
            elif re.match(r"^\S", s):
                in_composition = False
            if in_composition and _REMOTE_STATE.search(s):
                problems.append(f"{shown}:{n}: remote Helmfile composition ({s.strip()[:80]})")
        for m in _HOOK_CMD.finditer(text):
            if m.group(1) not in HOOK_COMMANDS:
                line = text.count("\n", 0, m.start()) + 1
                problems.append(f"{shown}:{line}: hook command {m.group(1)!r} (only {', '.join(sorted(HOOK_COMMANDS))} "
                                "running the chart-cache check is allowed)")
        for n, line in enumerate(lines, 1):
            s = line.strip()
            if s.startswith(("#", "{{/*", "{{- /*")):
                continue
            hit = _NET_TOOLS.search(s)
            if hit:
                problems.append(f"{shown}:{n}: runs or names network tool {hit.group(0)!r}: {s[:80]}")
    return problems


# Release name for a chart whose release is named differently (tiers.yaml header).
RELEASE_FOR_CHART = {"gateway-helm": "envoy-gateway"}


def tier_closure(tiers_doc: dict, name: str) -> set:
    tiers = tiers_doc.get("tiers") or {}
    seen, todo = set(), [name]
    while todo:
        t = todo.pop()
        if t not in seen:
            seen.add(t)
            todo.extend(((tiers.get(t) or {}).get("requires")) or [])
    return seen


def tier_membership_problems(lock: dict, tiers_doc: dict, tier: config.Tier) -> list:
    """versions.lock.yaml records the tier each chart belongs to; tiers.yaml must agree.

    This is an independent record, so a release cannot silently drop out of a
    tier (or out of the footprint count) by an edit to tiers.yaml alone.
    """
    closure = tier_closure(tiers_doc, tier.name)
    if tier.name == "full":
        # Every tier but the opt-in add-ons (`opt_in: true`), which `full` leaves out on purpose.
        closure |= {n for n, spec in (tiers_doc.get("tiers") or {}).items()
                    if not (isinstance(spec, dict) and spec.get("opt_in"))}
    problems = []
    for e in (lock or {}).get("charts") or []:
        if not isinstance(e, dict) or e.get("use") in ("denied", None):
            continue
        want = e.get("tier")
        release = e.get("release") or RELEASE_FOR_CHART.get(e.get("name"), e.get("name"))
        if want is None:
            problems.append(f"chart {e.get('name')}: versions.lock.yaml has no `tier`")
            continue
        if want == "addon":
            if e.get("use") == "used":
                problems.append(f"chart {e.get('name')}: `use: used` but tier addon (off by default)")
            continue
        if want in closure and release not in tier.releases:
            problems.append(f"release {release}: versions.lock.yaml puts chart {e.get('name')} in tier {want}, "
                            f"but tiers.yaml tier {tier.name} does not enable it")
    return problems


def list_argv(helmfile: Path, environment: str) -> list:
    return ["helmfile", "--file", str(helmfile), "--no-color", "--quiet", "--environment", environment,
            "list", "--output", "json"]


def allowed_chart_paths(root: Path, lock: dict) -> dict:
    """{resolved path: what pins it} for every chart a release may use."""
    out = {}
    for entry in (lock or {}).get("charts") or []:
        if isinstance(entry, dict) and entry.get("file") and entry.get("use") != "denied":
            f = str(entry["file"])
            if f.startswith(config.CHARTS_DIR + "/"):
                out[(root / f).resolve()] = f"versions.lock.yaml chart {entry.get('name')}"
    for g in GLUE_CHARTS:
        out[(root / g).resolve()] = "lab glue chart"
    return out


def chart_source_problems(root: Path, helmfile: Path, lock: dict, listing: str) -> tuple:
    """(releases checked, problems): every installed release's chart must be pinned in the cache."""
    try:
        rels = json.loads(listing or "[]")
    except ValueError:
        return 0, [f"helmfile list printed no JSON: {listing[:200]!r}"]
    if not isinstance(rels, list):
        return 0, ["helmfile list did not return a list"]
    allowed = allowed_chart_paths(root, lock)
    problems, n = [], 0
    for r in rels:
        if not isinstance(r, dict) or r.get("installed") is False or r.get("enabled") is False:
            continue
        n += 1
        chart = str(r.get("chart") or "")
        name = r.get("name")
        if not chart.startswith(("./", "../", "/")):
            problems.append(f"release {name}: chart {chart!r} is not a local path into {config.CHARTS_DIR}/")
            continue
        resolved = (helmfile.parent / chart).resolve()
        if resolved not in allowed:
            shown = resolved.relative_to(root).as_posix() if resolved.is_relative_to(root) else str(resolved)
            problems.append(f"release {name}: chart {shown} is not a chart versions.lock.yaml pins in "
                            f"{config.CHARTS_DIR}/ (nor the lab glue chart)")
    if n == 0:
        problems.append("helmfile list returned no installed release")
    return n, problems


def run_render(
    root: Path,
    tier_name: str,
    *,
    out: Optional[str] = None,
    helmfile: Optional[str] = None,
    environment: Optional[str] = None,
    runner: proc.Runner = proc.run,
    which=proc.which,
) -> dict:
    checks: list = []
    doc = {"command": "render", "tier": tier_name, "ok": False, "checks": checks}

    def done() -> dict:
        doc["ok"] = bool(checks) and all(c["status"] == PASS for c in checks)
        return doc

    try:
        lock = config.load_yaml(root / config.VERSIONS_LOCK, runner)
        tiers_doc = config.load_yaml(root / config.TIERS, runner)
        tier = config.resolve_tier(tiers_doc, tier_name)
    except config.ConfigError as exc:
        checks.append(check("config", FAIL, str(exc)))
        return done()
    checks.append(check("config", PASS, f"tier {tier.name!r}: {len(tier.releases)} releases ({', '.join(tier.releases)})"))
    membership = tier_membership_problems(lock, tiers_doc, tier)
    if membership:
        checks.append(check("tier-membership", FAIL, f"{len(membership)} release(s) missing from tier {tier.name!r}",
                            f"{config.TIERS} and the `tier` of each chart in {config.VERSIONS_LOCK} must agree.",
                            {"problems": membership}))
        return done()

    count, problems = verify_chart_cache(root, lock)
    if problems:
        checks.append(check("chart-cache", FAIL, f"{len(problems)} problem(s) in {config.CHARTS_DIR}",
                            "Restore the cache from git; a new pin is a new file plus SHA256SUMS and lock lines.",
                            {"problems": problems}))
        return done()
    checks.append(check("chart-cache", PASS, f"{count} files match {config.CHARTS_DIR}/{SUMS_FILE} and {config.VERSIONS_LOCK}"))

    hf = Path(helmfile).resolve() if helmfile else root / config.HELMFILE
    if not hf.is_file():
        checks.append(check("helmfile", FAIL, f"{hf.relative_to(root) if hf.is_relative_to(root) else hf} not present",
                            "Render cannot run without it."))
        return done()
    for tool in ("helmfile", "helm"):
        if not which(tool):
            checks.append(check(f"tool:{tool}", FAIL, f"{tool} not on PATH", "Run `./lab doctor` for the pinned versions."))
    if any(c["status"] == FAIL for c in checks):
        return done()

    static = static_helmfile_problems(hf)
    if static:
        checks.append(check("helmfile-static", FAIL, f"{len(static)} network-capable construct(s) in the Helmfile",
                            "The render must not be able to reach the network: no `exec`, secret fetchers, "
                            "remote Helmfile composition or hook commands other than the cache check.",
                            {"problems": static}))
        return done()
    checks.append(check("helmfile-static", PASS, "no network-capable template function, hook or remote composition"))

    if out:
        out_dir = Path(out).resolve()
        if out_dir.exists() and any(out_dir.iterdir()):
            checks.append(check("output", FAIL, f"{out_dir} is not empty",
                                "Use an empty or new directory so stale manifests cannot pass the release check."))
            return done()
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = Path(tempfile.mkdtemp(prefix=f"lab-render-{tier.name}-"))
    doc["output_dir"] = str(out_dir)

    home = Path(tempfile.mkdtemp(prefix="lab-render-home-"))
    try:
        argv = helmfile_argv(hf, tier, out_dir, environment)
        doc["argv"] = argv
        res = runner(argv, cwd=str(root), env=hermetic_env(home), timeout=HELMFILE_TIMEOUT_S)
    finally:
        shutil.rmtree(home, ignore_errors=True)
    if not res.ok:
        why = "timed out" if res.timed_out else ("not found" if res.missing else f"exit {res.returncode}")
        checks.append(check("helmfile-template", FAIL, f"helmfile template {why} (offline, from the chart cache)",
                            "Read the output tail. The render is offline (proxies point at a closed port), so a "
                            "chart or value that needs the network fails here; every chart must come from "
                            "platform/charts/.",
                            {"output_tail": res.output_tail()}))
        return done()
    checks.append(check("helmfile-template", PASS, "helmfile template exited 0 with no network"))

    home = Path(tempfile.mkdtemp(prefix="lab-render-home-"))
    try:
        res = runner(list_argv(hf, environment or tier.name), cwd=str(root), env=hermetic_env(home),
                     timeout=HELMFILE_TIMEOUT_S)
    finally:
        shutil.rmtree(home, ignore_errors=True)
    if not res.ok:
        checks.append(check("chart-source", FAIL, f"helmfile list failed (exit {res.returncode})",
                            detail={"output_tail": res.output_tail()}))
        return done()
    n_rel, src_problems = chart_source_problems(root, hf, lock, res.stdout)
    if src_problems:
        checks.append(check("chart-source", FAIL, f"{len(src_problems)} release(s) use a chart outside the cache",
                            "Every chart must be an archive or directory pinned in versions.lock.yaml under "
                            f"{config.CHARTS_DIR}/ (checked against SHA256SUMS above).",
                            {"problems": src_problems}))
        return done()
    checks.append(check("chart-source", PASS, f"{n_rel} release(s): every chart is pinned in {config.CHARTS_DIR}/"))

    rendered = sorted(p.name for p in out_dir.iterdir() if p.is_dir())
    names = list(tier.releases) + [r for r in rendered if r not in tier.releases]
    outputs = collect_outputs(out_dir, names)
    doc["releases"] = outputs
    owners = known_releases(tiers_doc)
    for rel in names:
        info = outputs[rel]
        empty = info["files"] == 0 or info["objects"] == 0
        if rel in tier.releases:
            if empty:
                checks.append(check(f"release:{rel}", FAIL, "rendered nothing",
                                    f"Is it defined in the helmfile and `installed` when tier={tier.name}?"))
            else:
                checks.append(check(f"release:{rel}", PASS, f"{info['objects']} objects in {info['files']} files"))
        elif rel in owners:
            checks.append(check(f"release:{rel}", FAIL,
                                f"rendered, but tier {tier.name!r} does not enable it (only: {', '.join(owners[rel])})",
                                "The helmfile must set `installed:` from the `tier` state value."))
        else:
            checks.append(check(f"release:{rel}", PASS,
                                f"{info['objects']} objects in {info['files']} files (lab glue; not named in {config.TIERS})"))
    return done()
