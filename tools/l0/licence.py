# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `licence`: classify every vendored artefact by READING ITS LICENCE TEXT.

Policy (not legal advice)
- Allowed: Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC; MPL-2.0 (file-level
  copyleft: allowed and RECORDED); the Red Hat UBI EULA (allowed and RECORDED:
  ship it unmodified with every image or bundle that contains UBI).
- Denied: BUSL/BSL, SSPL, AGPL, Elastic, the Confluent Community licence,   # brand-fence: allow
  RCL (Redpanda Community) and the Redis source-available licences, Commons   # brand-fence: allow
  Clause, any other End-User Licence Agreement, anything unclassified, and
  an artefact with no licence at all (unlicensed).
- Runtime-only licences: DENIED when vendored (any file in the tree). ALLOWED
  only for an artefact the lab references at runtime by a real digest in
  versions.lock.yaml with an `obligations` field that says what using it
  requires (a placeholder such as "-" or an all-zero digest fails):
  - OSL-3.0.

What is read
A. Chart cache (platform/charts): every archive is unpacked (stdlib tarfile,
   no links, no absolute or `..` paths) and every chart in it, nested
   subcharts included, is classified. A top-level chart needs licence TEXT:
   its own LICENSE/COPYING/EULA file, or the upstream LICENSE text that
   versions.lock.yaml points at (`license.upstream_text`, for archives that
   ship none). A nested subchart may instead rely on the lock's
   `license_text`, SPDX tags in its own files, its Chart.yaml licence
   annotation, or the lock's declared licence (in that order); every source
   found must be allowed. No evidence at all = unlicensed = FAIL.
A2. Full-text sweep: EVERY file in every cached chart (subchart archives
   unpacked recursively) and every file in the tree except .git/, tests/ and
   this gate is read (UTF-16 and NUL-containing text too) for the title
   phrases of the denied families, whatever its name or SPDX header says;
   licence-named files (LICENSE, COPYING, NOTICE, EULA, ...) are also read
   for short ids (an "AGPL-3.0-only" rider in an Apache LICENSE).
A3. A Helm chart (Chart.yaml) anywhere outside platform/charts/ (but the lab
   glue chart) fails, and so does any archive outside the cache. A tree with
   no chart cache, or no versions.lock.yaml, fails.
B. Every file under platform/ and LICENSES/: the licence `reuse spdx`
   resolves for it (SPDX headers and REUSE.toml), and a missing one fails.
C. Every licence-text file under platform/ and LICENSES/ (and every licence
   file found in A): classified from its text.
D. versions.lock.yaml: every chart, subchart and image licence the lab
   records, against the same policy (runtime references; OSL rule above).

Text classification: Google licenseclassifier via `trivy fs --scanners
license --license-full` (offline; borrowed, it recognises full canonical
texts), plus a stdlib table of distinctive title phrases for the denied
families and EULAs that a short or modified text would not match (and that
trivy does not report as denied by itself). A denial from either wins; a
text neither recognises fails as unclassified.

  licence.py [--root DIR]
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import tarfile
import tempfile
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

ALLOWED = {"Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC"}
RECORDED = {"MPL-2.0": "file-level copyleft: modified MPL files must be offered under MPL-2.0",
            "LicenseRef-UBI-EULA": "ship the unmodified UBI EULA with every image/bundle containing UBI; "
                                   "no Red Hat endorsement or marks"}
OSL = "OSL-3.0"
# Licences allowed only as a recorded runtime reference (see the policy above).
RUNTIME_ONLY = {OSL: "runtime reference only; obligations recorded in versions.lock.yaml"}
DENIED_HINT = {
    "BUSL-1.1": "Business Source License", "SSPL-1.0": "Server Side Public License",
    "AGPL-3.0": "GNU Affero GPL", "OSL-3.0": "Open Software License (vendored)",
    "Elastic-2.0": "Elastic License", "LicenseRef-Confluent-Community": "Confluent Community License",  # brand-fence: allow
    "LicenseRef-RCL": "Redpanda Community License", "LicenseRef-RSAL": "Redis Source Available License",  # brand-fence: allow
    "LicenseRef-Commons-Clause": "Commons Clause", "LicenseRef-EULA": "End-User License Agreement",
}
# Distinctive phrases -> SPDX id. Order matters: the UBI EULA is matched before
# the generic EULA rule. Phrases are matched on whitespace-collapsed, lowercased text.
SIGNATURES = [
    ("BUSL-1.1", [r"business source license"]),
    ("SSPL-1.0", [r"server side public license"]),
    ("AGPL-3.0", [r"gnu affero general public license"]),
    ("OSL-3.0", [r"open software license"]),
    ("Elastic-2.0", [r"elastic license"]),
    ("LicenseRef-Confluent-Community", [r"confluent community license"]),  # brand-fence: allow
    ("LicenseRef-RCL", [r"redpanda community license"]),  # brand-fence: allow
    ("LicenseRef-RSAL", [r"redis source available license", r"\brsalv2\b"]),
    ("LicenseRef-Commons-Clause", [r"commons clause"]),
    ("LicenseRef-UBI-EULA", [r"universal base image.{0,400}end user license agreement",
                             r"end user license agreement.{0,400}universal base image"]),
    ("LicenseRef-EULA", [r"end[- ]user licen[cs]e agreement", r"^\W*eula\b"]),
    ("GPL", [r"gnu general public license", r"gnu lesser general public license"]),
    ("Apache-2.0", [r"apache license,? version 2\.0", r"apache license\s+version 2\.0"]),
    ("MPL-2.0", [r"mozilla public license,? (?:version|v\.?) ?2\.0"]),
    ("MIT", [r"permission is hereby granted, free of charge, to any person obtaining a copy"]),
    ("ISC", [r"permission to use, copy, modify, and/or distribute this software for any purpose with or "
             r"without fee is hereby granted"]),
    ("BSD-3-Clause", [r"redistribution and use in source and binary forms.{0,1200}neither the name"]),
    ("BSD-2-Clause", [r"redistribution and use in source and binary forms"]),
]
SIG_RE = [(i, [re.compile(p, re.S) for p in ps]) for i, ps in SIGNATURES]
# Short forms (an SPDX id or abbreviation) of the denied families. Read in
# licence-named files only (LICENSE, COPYING, NOTICE, EULA, ...): an Apache
# LICENSE with "... distributed under AGPL-3.0-only" appended is not Apache.
SHORT_IDS = [
    ("AGPL-3.0", r"\bagpl(?:-?v?3(?:\.0)?)?\b|\baffero\b"),
    ("BUSL-1.1", r"\bbusl(?:-1\.1)?\b|\bbsl[- ]?1\.1\b"),
    ("SSPL-1.0", r"\bsspl(?:-?v?1(?:\.0)?)?\b"),
    ("OSL-3.0", r"\bosl-?3\.0\b"),
    ("Elastic-2.0", r"\belastic-2\.0\b|\belv2\b"),
    ("LicenseRef-Commons-Clause", r"\bcommons[- ]clause\b"),
    ("GPL", r"\b(?:l?gpl)-?v?[23](?:\.0)?(?:-only|-or-later|\+)?\b"),
]
SHORT_RE = [(i, re.compile(p, re.I)) for i, p in SHORT_IDS]
# Licence-shaped file names for the short-id rule (wider than LICENCE_NAME).
LEGAL_NAME = re.compile(r"^(?:LICEN[CS]E|COPYING|EULA|UNLICENSE|NOTICE|LEGAL|TERMS|PATENTS)", re.I)
# Archives the gate cannot classify in place: outside the chart cache they fail.
ARCHIVE_SUFFIXES = (".tgz", ".tar.gz", ".tar", ".zip", ".jar", ".whl", ".war", ".tar.xz", ".tar.bz2")
# Excluded from the full-text sweep: git metadata, the planted fixtures (the
# selftest runs them on purpose) and this gate itself (its phrase table names
# every denied licence). Everything else, first-party prose included, is
# swept: none of it may carry denied licence text.
SWEEP_EXEMPT_DIRS = {".git", "tests"}
SWEEP_EXEMPT_FILES = {"tools/l0/licence.py"}
# The lab's own glue chart: first party, outside the cache by design.
GLUE_CHARTS = {"platform/components/lab-objects"}
OBLIGATIONS_MIN = 40
# Families that make every other match in the same text irrelevant (an
# Apache text with a Commons Clause rider is not Apache-2.0).
DENY_FIRST = {"BUSL-1.1", "SSPL-1.0", "AGPL-3.0", "OSL-3.0", "Elastic-2.0", "LicenseRef-Confluent-Community",  # brand-fence: allow
              "LicenseRef-RCL", "LicenseRef-RSAL", "LicenseRef-Commons-Clause", "LicenseRef-EULA", "GPL"}
TRIVY_MAP = {"AGPL-3.0-only": "AGPL-3.0", "AGPL-3.0-or-later": "AGPL-3.0", "AGPL-3.0": "AGPL-3.0",
             "BUSL-1.1": "BUSL-1.1", "SSPL-1.0": "SSPL-1.0", "OSL-3.0": "OSL-3.0", "Elastic-2.0": "Elastic-2.0",
             "MPL-2.0": "MPL-2.0", "MPL-2.0-no-copyleft-exception": "MPL-2.0"}
TRIVY_IGNORE = {"Copyright", ""}
LICENCE_NAME = re.compile(r"^(?:LICEN[CS]E|COPYING|EULA|UNLICENSE)(?:[._-].*)?$|\.LICEN[CS]E$", re.I)
# REUSE-IgnoreStart
SPDX_TAG = re.compile(r"SPDX-License-Identifier:\s*([A-Za-z0-9.+()\s-]+?)\s*(?:-->|\*/|\"|$)", re.M)
# REUSE-IgnoreEnd
SPDX_CASE = {i.lower(): i for i in list(ALLOWED) + list(RECORDED) + list(DENIED_HINT) + ["GPL-2.0", "GPL-3.0"]}


# ------------------------------------------------------------ classification


def spdx_ids(expr: str) -> list:
    """Ids in an SPDX expression (AND/OR/WITH and parentheses dropped; conservative)."""
    toks = re.split(r"[\s()]+", expr or "")
    out = []
    for t in toks:
        if not t or t.upper() in ("AND", "OR", "WITH"):
            continue
        out.append(SPDX_CASE.get(t.lower(), t))
    return out


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def signature_ids(text: str) -> list:
    t = norm(text)
    found = [i for i, rxs in SIG_RE if any(rx.search(t) for rx in rxs)]
    if "LicenseRef-UBI-EULA" in found and "LicenseRef-EULA" in found:
        found.remove("LicenseRef-EULA")
    if "BSD-3-Clause" in found and "BSD-2-Clause" in found:
        found.remove("BSD-2-Clause")
    if "LicenseRef-UBI-EULA" in found:
        found = [f for f in found if f not in ("GPL",)]  # the UBI EULA names the GPL for its components
    deny = [f for f in found if f in DENY_FIRST]
    return deny or found


def trivy_classify(files: list) -> dict:
    """{index: [spdx ids]} for files classified by trivy's licenseclassifier (offline)."""
    if not files:
        return {}
    stage = Path(tempfile.mkdtemp(prefix="l0-licence-trivy-"))
    try:
        for i, f in enumerate(files):
            d = stage / f"f{i:05d}"
            d.mkdir()
            shutil.copyfile(f, d / "LICENSE")
        res = c.run(["trivy", "fs", "--scanners", "license", "--license-full", "--skip-db-update",
                     "--skip-java-db-update", "--offline-scan", "--disable-telemetry", "--format", "json",
                     "--quiet", str(stage)], timeout=600)
        if res.returncode != 0 and "--disable-telemetry" in res.stderr:
            res = c.run(["trivy", "fs", "--scanners", "license", "--license-full", "--skip-db-update",
                         "--skip-java-db-update", "--offline-scan", "--format", "json", "--quiet", str(stage)],
                        timeout=600)
        if res.returncode != 0:
            raise c.GateError(f"trivy licence scan failed (exit {res.returncode}): {res.stderr.strip()[:400]}")
        out: dict = {}
        for r in (json.loads(res.stdout or "{}").get("Results") or []):
            for lic in r.get("Licenses") or []:
                fp = lic.get("FilePath") or ""
                m = re.match(r"f(\d{5})/", fp)
                name = lic.get("Name") or ""
                # licenseclassifier also reports a "Copyright" notice match; it is not a licence.
                if m and float(lic.get("Confidence") or 0) >= 0.8 and name not in TRIVY_IGNORE:
                    out.setdefault(int(m.group(1)), []).append(TRIVY_MAP.get(name, name))
        return out
    finally:
        shutil.rmtree(stage, ignore_errors=True)


class Classifier:
    """Batch text classification: register files, then resolve once (one trivy run)."""

    def __init__(self) -> None:
        self.files: list = []
        self.result: dict = {}

    def add(self, path: Path) -> int:
        self.files.append(Path(path))
        return len(self.files) - 1

    def resolve(self) -> None:
        tv = trivy_classify(self.files)
        for i, f in enumerate(self.files):
            sig = signature_ids(f.read_text(errors="replace"))
            ids = list(dict.fromkeys((tv.get(i) or []) + sig))
            denied = [x for x in ids if x in DENY_FIRST or x == OSL]
            self.result[i] = (denied or ids, "trivy" if tv.get(i) else ("phrases" if sig else "none"))

    def ids(self, i: int) -> tuple:
        return self.result[i]


def verdict(ids: list, *, vendored: bool, runtime_ok_osl: bool = False) -> tuple:
    """(ok, reason, recorded notes) for a set of licence ids under the policy."""
    if not ids:
        return False, "no licence (unlicensed)", []
    notes, bad = [], []
    for i in ids:
        if i in ALLOWED:
            continue
        if i in RECORDED:
            notes.append(f"{i}: {RECORDED[i]}")
            continue
        if i in RUNTIME_ONLY and not vendored and runtime_ok_osl:
            notes.append(f"{i}: {RUNTIME_ONLY[i]}")
            continue
        if i in RUNTIME_ONLY:
            bad.append(f"{i} ({'vendored' if vendored else 'runtime reference without digest + obligations'})")
            continue
        bad.append(f"{i} ({DENIED_HINT.get(i, 'not on the allow list')})")
    if bad:
        return False, "denied: " + ", ".join(bad), notes
    return True, ", ".join(ids), notes


# ------------------------------------------------------------------ helpers


def safe_extract(archive: Path, dest: Path) -> None:
    with tarfile.open(archive, "r:*") as tf:
        for m in tf.getmembers():
            p = Path(m.name)
            if p.is_absolute() or ".." in p.parts:
                raise c.GateError(f"{archive.name}: unsafe member path {m.name!r}")
            if m.isdir():
                (dest / p).mkdir(parents=True, exist_ok=True)
            elif m.isfile():
                (dest / p).parent.mkdir(parents=True, exist_ok=True)
                with tf.extractfile(m) as src, open(dest / p, "wb") as out:
                    shutil.copyfileobj(src, out)
            # links, devices and fifos are never extracted


def extract_nested(top: Path) -> int:
    """Unpack every chart archive vendored inside an unpacked chart (charts/*.tgz),
    recursively, beside it as Helm would load it. Returns how many."""
    n = 0
    while True:
        found = [a for a in top.rglob("*") if a.is_file() and a.name.endswith((".tgz", ".tar.gz"))
                 and a.parent.name == "charts"]
        if not found:
            return n
        for a in found:
            dest = a.parent / (a.name + ".unpacked")
            dest.mkdir()
            safe_extract(a, dest)
            a.unlink()
            n += 1


def denied_phrases(text: str) -> list:
    """Denied families (and any EULA other than the UBI EULA) whose title
    phrase appears anywhere in the text."""
    t = norm(text)
    found = [i for i, rxs in SIG_RE if any(rx.search(t) for rx in rxs)]
    if "LicenseRef-UBI-EULA" in found:  # allowed and recorded; it names the GPL for its components
        found = [f for f in found if f not in ("GPL", "LicenseRef-EULA")]
    return [i for i in found if i in DENY_FIRST or i == OSL]


def short_ids(text: str) -> list:
    return [i for i, rx in SHORT_RE if rx.search(text)]


def sweep_file(f: Path, label: str, rep, legal: bool) -> bool:
    """Full-text sweep of one vendored file. True when clean."""
    text, how = c.read_text_any(f)
    if text is None:
        return True
    hits = denied_phrases(text)
    if legal:
        hits += [i for i in short_ids(text) if i not in hits]
    if hits:
        why = ", ".join(f"{h} ({DENIED_HINT.get(h, 'copyleft not on the allow list')})" for h in hits)
        rep.fail(f"{label}: names denied licence text: {why} [full-text sweep{', ' + how if how != 'text' else ''}]")
        return False
    return True


def chart_dirs(top: Path) -> list:
    return sorted({p.parent for p in top.rglob("Chart.yaml")}, key=lambda p: len(p.parts))


def own_files(chart: Path) -> list:
    """Files of this chart, excluding nested subcharts under charts/."""
    out = []
    for p in sorted(chart.rglob("*")):
        r = p.relative_to(chart).parts
        if p.is_file() and not (r and r[0] == "charts" and len(r) > 1):
            out.append(p)
    return out


def chart_meta(chart: Path) -> dict:
    return c.yaml_load(chart / "Chart.yaml") or {}


def lock_index(lock: dict) -> dict:
    return {str(e.get("file")): e for e in (lock.get("charts") or []) if isinstance(e, dict) and e.get("file")}


# --------------------------------------------------------------------- main


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("licence")
    lock_path = root / "versions.lock.yaml"
    if not lock_path.is_file():
        rep.fail("versions.lock.yaml not found: the runtime references cannot be checked")
    lock = (c.yaml_load(lock_path) or {}) if lock_path.is_file() else {}
    by_file = lock_index(lock)
    cls = Classifier()
    work = Path(tempfile.mkdtemp(prefix="l0-licence-"))
    pending = []   # (label, kind, payload) evaluated after the single classifier run
    try:
        # ---- A: chart cache ------------------------------------------------
        charts_dir = root / "platform/charts"
        tops = []
        if charts_dir.is_dir():
            for p in sorted(charts_dir.iterdir()):
                relp = c.rel(p, root)
                if p.is_file() and p.name.endswith((".tgz", ".tar.gz")):
                    dest = work / p.name
                    dest.mkdir()
                    safe_extract(p, dest)
                    nested = extract_nested(dest)
                    if nested:
                        rep.note(f"{relp}: {nested} vendored subchart archive(s) unpacked and classified")
                    for top in [d for d in dest.iterdir() if d.is_dir()]:
                        tops.append((relp, top, by_file.get(relp)))
                elif p.is_dir() and (p / "Chart.yaml").is_file():
                    tops.append((relp, p, by_file.get(relp)))
        if not tops:
            rep.fail("no chart cache: platform/charts holds no chart archive or chart directory "
                     "(the lab renders only from that cache, so there is nothing to vouch for)")
        # ---- A2: full-text sweep of every file in every cached chart ---------
        swept = clean = 0
        for relp, top, entry in tops:
            for f in sorted(top.rglob("*")):
                if f.is_file():
                    swept += 1
                    inner = f.relative_to(top.parent).as_posix().replace(".tgz.unpacked/", ".tgz > ")
                    clean += sweep_file(f, f"{relp} > {inner}", rep, bool(LEGAL_NAME.search(f.name)))
        for relp, top, entry in tops:
            for chart in chart_dirs(top):
                is_top = chart == top
                meta = chart_meta(chart)
                label = f"{relp}" + ("" if is_top else f" > {chart.relative_to(top).as_posix()}")
                label += f" ({meta.get('name')} {meta.get('version')})"
                texts = [f for f in own_files(chart) if LICENCE_NAME.search(f.name)]
                ev = {"texts": [(f.relative_to(chart).as_posix(), cls.add(f)) for f in texts], "lock_text": None,
                      "tags": set(), "annotation": [], "declared": [], "is_top": is_top, "entry": entry}
                lic = (entry or {}).get("license") or {}
                if is_top and lic.get("upstream_text"):
                    ut = root / str(lic["upstream_text"])
                    if not ut.is_file():
                        rep.fail(f"{label}: versions.lock.yaml upstream_text {lic['upstream_text']} is missing")
                    else:
                        ev["lock_text"] = (str(lic["upstream_text"]), cls.add(ut), lic.get("upstream_spdx"))
                if not is_top:
                    for sub in (entry or {}).get("subcharts") or []:
                        names = {sub.get("name"), sub.get("alias")}
                        # Matched by name/alias (a dependency version may be a range such as
                        # "0.0"); version drift is the chart_app gate's job.
                        if meta.get("name") in names and chart.parent.parent == top:
                            if sub.get("license_text") and (root / sub["license_text"]).is_file():
                                ev["lock_text"] = (sub["license_text"], cls.add(root / sub["license_text"]),
                                                   sub.get("license"))
                            if sub.get("license"):
                                ev["declared"] = spdx_ids(str(sub["license"]))
                    for f in own_files(chart):
                        if f.suffix in (".yaml", ".yml", ".tpl", ".txt", ".json") and not c.is_binary(f):
                            for m in SPDX_TAG.finditer(f.read_text(errors="replace")):
                                ev["tags"].update(spdx_ids(m.group(1)))
                    ann = (meta.get("annotations") or {})
                    for k in ("licenses", "artifacthub.io/license"):
                        if ann.get(k):
                            ev["annotation"] += spdx_ids(str(ann[k]))
                pending.append((label, "chart", ev))

        # ---- A3: everything vendored outside the chart cache ----------------
        # Charts and archives may live only in the cache (and the glue chart);
        # every other file outside first-party prose is swept for denied text,
        # whatever its SPDX header says.
        for dirpath, dirnames, filenames in __import__("os").walk(root):
            d = Path(dirpath)
            reld = c.rel(d, root) if d != root else ""
            dirnames[:] = sorted(n for n in dirnames if not (d == root and n in SWEEP_EXEMPT_DIRS))
            if reld == "platform/charts" or reld.startswith("platform/charts/"):
                dirnames[:] = []
                continue
            for name in sorted(filenames):
                f = d / name
                r = c.rel(f, root)
                if r in SWEEP_EXEMPT_FILES or "__pycache__" in Path(r).parts:
                    continue
                if name == "Chart.yaml" and reld not in GLUE_CHARTS:
                    rep.fail(f"{r}: a Helm chart outside platform/charts/ (only the cache is licence-checked "
                             "and rendered; the render gate refuses it too)")
                if name.endswith(ARCHIVE_SUFFIXES):
                    rep.fail(f"{r}: an archive outside platform/charts/ cannot be classified in place; "
                             "vendor its contents or move it into the cache with a lock entry")
                    continue
                swept += 1
                clean += sweep_file(f, r, rep, bool(LEGAL_NAME.search(name)))
        if swept and swept == clean:
            rep.ok(f"{swept} file(s) swept "
                   "(every file in the chart cache and in the tree, but .git/, tests/ and this gate): "
                   "no denied licence text anywhere, whatever the file name or header")

        # ---- C: licence texts under platform/ and LICENSES/ (outside archives) ----
        for base in ("platform", "LICENSES"):
            d = root / base
            if not d.is_dir():
                continue
            for p in c.walk_files(d):
                if LICENCE_NAME.search(p.name) or base == "LICENSES":
                    if c.is_binary(p):
                        continue
                    pending.append((c.rel(p, root), "text", cls.add(p)))

        cls.resolve()

        n_ok = 0
        for label, kind, payload in pending:
            if kind == "text":
                ids, how = cls.ids(payload)
                if label.startswith("LICENSES/"):
                    want = Path(label).stem
                    if want not in ALLOWED and want not in RECORDED:
                        rep.fail(f"{label}: licence {want} is not on the allow list")
                        continue
                    if ids and want not in ids and not (set(ids) & DENY_FIRST):
                        rep.fail(f"{label}: file name says {want}, text reads as {', '.join(ids)}")
                        continue
                ok, why, notes = verdict(ids, vendored=True)
                if not ok and not ids:
                    why = "licence text not recognised (unclassified)"
                if ok:
                    n_ok += 1
                    for nt in notes:
                        rep.note(f"recorded: {label}: {nt}")
                else:
                    rep.fail(f"{label}: {why} [{how}]")
                continue
            ev = payload
            sources, ids = [], []
            for name, idx in ev["texts"]:
                t_ids, how = cls.ids(idx)
                sources.append(f"{name}:{'/'.join(t_ids) or 'unclassified'}[{how}]")
                ids += t_ids or ["LicenseRef-unclassified"]
            if ev["lock_text"]:
                name, idx, declared = ev["lock_text"]
                t_ids, how = cls.ids(idx)
                sources.append(f"lock text {name}:{'/'.join(t_ids) or 'unclassified'}[{how}]")
                ids += t_ids or ["LicenseRef-unclassified"]
                if declared and t_ids and set(spdx_ids(str(declared))) != set(t_ids):
                    rep.fail(f"{label}: versions.lock.yaml declares {declared} but {name} reads as "
                             f"{', '.join(t_ids)}")
            has_text = bool(ev["texts"] or ev["lock_text"])
            if not ev["is_top"] and not has_text:
                for key, vals in (("SPDX tags", sorted(ev["tags"])), ("Chart.yaml annotation", ev["annotation"]),
                                  ("lock declared", ev["declared"])):
                    if vals:
                        sources.append(f"{key}:{'/'.join(vals)}")
                        ids += vals
            if ev["is_top"] and not has_text:
                rep.fail(f"{label}: no licence text in the chart and no upstream_text in versions.lock.yaml "
                         "(unlicensed)")
                continue
            ids = list(dict.fromkeys(ids))
            ok, why, notes = verdict(ids, vendored=True)
            if ok:
                n_ok += 1
                for nt in notes:
                    rep.note(f"recorded: {label}: {nt}")
            else:
                rep.fail(f"{label}: {why} <- {'; '.join(sources) or 'no evidence'}")
        if pending:
            rep.ok(f"{n_ok}/{len(pending)} vendored chart(s) and licence text(s) classified and allowed")

        # ---- B: per-file licences under platform/ and LICENSES/ via `reuse spdx` ----
        check_reuse_spdx(root, rep)

        # ---- D: what the lock records (runtime references) ------------------
        if lock:
            check_lock(lock, rep)
        else:
            rep.note("no versions.lock.yaml in this tree; runtime references not checked")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return rep.finish()


def check_reuse_spdx(root: Path, rep: c.Report) -> None:
    res = c.run(["reuse", "--root", str(root), "spdx"], timeout=600)
    if res.returncode != 0:
        raise c.GateError(f"`reuse spdx` failed: {res.stderr.strip()[:400]}")
    files, cur = {}, None
    for line in res.stdout.splitlines():
        if line.startswith("FileName: "):
            cur = line[len("FileName: "):].lstrip("./") if line[len("FileName: "):].startswith("./") else \
                line[len("FileName: "):]
            files[cur] = []
        elif cur and line.startswith("LicenseInfoInFile: "):
            v = line.split(": ", 1)[1].strip()
            if v not in ("NOASSERTION", "NONE"):
                files[cur] += spdx_ids(v)
    scope = {k: v for k, v in files.items() if k.startswith(("platform/", "LICENSES/"))}
    bad = 0
    for f, ids in sorted(scope.items()):
        ok, why, notes = verdict(ids, vendored=True)
        if not ok:
            bad += 1
            rep.fail(f"{f}: {why} (from SPDX headers / REUSE.toml)")
        for nt in notes:
            rep.note(f"recorded: {f}: {nt}")
    if not scope:
        rep.note("no files under platform/ or LICENSES/ in this tree")
    elif not bad:
        rep.ok(f"{len(scope)} file(s) under platform/ and LICENSES/: every declared licence allowed")


def check_lock(lock: dict, rep: c.Report) -> None:
    bad = n = 0
    for e in lock.get("charts") or []:
        if not isinstance(e, dict) or e.get("use") == "denied":
            continue
        lic = e.get("license") or {}
        ids = []
        if lic.get("chart_spdx") and str(lic["chart_spdx"]) != "unlicensed":
            ids += spdx_ids(str(lic["chart_spdx"]))
        if lic.get("upstream_spdx"):
            ids += spdx_ids(str(lic["upstream_spdx"]))
        for sub in e.get("subcharts") or []:
            if sub.get("license"):
                ids += spdx_ids(str(sub["license"]))
        n += 1
        ok, why, notes = verdict(list(dict.fromkeys(ids)), vendored=True)
        if not ok:
            bad += 1
            rep.fail(f"versions.lock.yaml chart {e.get('name')}: {why}")
    for i in lock.get("images") or []:
        if not isinstance(i, dict) or i.get("use") == "denied":
            continue
        lic = i.get("license") or {}
        ids = spdx_ids(str(lic.get("spdx") or ""))
        text = norm(" ".join(str(v) for v in lic.values()))
        if "ubi" in text and "eula" in text:
            ids.append("LicenseRef-UBI-EULA")
        obligations = i.get("obligations") or lic.get("obligations")
        digest = str(i.get("digest") or "")
        # A placeholder is not a record: the obligations must say something,
        # and the digest must be a real one (not one repeated hex digit).
        obligations_ok = isinstance(obligations, str) and len(obligations.strip()) >= OBLIGATIONS_MIN \
            and len(re.findall(r"[A-Za-z]{3,}", obligations)) >= 5
        digest_ok = bool(c.DIGEST_RE.match(digest)) and len(set(digest[7:])) > 4
        restricted = sorted(set(ids) & set(RUNTIME_ONLY))
        if restricted and obligations and not obligations_ok:
            rep.fail(f"versions.lock.yaml image {i.get('id')}: {restricted[0]} obligations {str(obligations)[:40]!r} "
                     f"are a placeholder (write what using it requires, {OBLIGATIONS_MIN}+ characters)")
        if restricted and c.DIGEST_RE.match(digest) and not digest_ok:
            rep.fail(f"versions.lock.yaml image {i.get('id')}: digest {digest[:20]}... is a placeholder")
        n += 1
        ok, why, notes = verdict(list(dict.fromkeys(ids)), vendored=False,
                                 runtime_ok_osl=obligations_ok and digest_ok)
        if not ok:
            bad += 1
            rep.fail(f"versions.lock.yaml image {i.get('id')} ({i.get('ref')}): {why}")
        for nt in notes:
            extra = f": {str(obligations)[:160]}" if nt.split(":", 1)[0] in RUNTIME_ONLY else ""
            rep.note(f"recorded: image {i.get('id')} ({i.get('use')}): {nt}{extra}")
    if n and not bad:
        rep.ok(f"versions.lock.yaml: {n} chart/image licence record(s) allowed (runtime references)")
    elif not n:
        rep.note("versions.lock.yaml has no chart or image licence records")


if __name__ == "__main__":
    c.main_wrapper(main)
