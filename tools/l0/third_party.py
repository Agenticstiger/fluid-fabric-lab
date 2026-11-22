# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `third_party`: THIRD-PARTY.md and NOTICE cover every vendored chart and
every image the lab pulls.

1. THIRD-PARTY.md is generated from the lock. Each `<!-- yq: EXPR -->` comment
   is run as `yq --string-interpolation=false -r 'EXPR' versions.lock.yaml`
   (the invocation the file's header gives) and its output lines are compared
   with the table rows that follow the comment. A missing or an extra row
   fails and is named. The four tables are the vendored charts, their
   subcharts, the images the lab runs and the images the lab builds.
2. The chart cache and the lock agree: every chart archive or chart directory
   listed in platform/charts/SHA256SUMS is a lock `.charts[].file`, and every
   lock chart file exists.
3. Every NOTICE file inside a vendored chart (an archive member or a file in a
   chart directory whose name starts with NOTICE, at any depth, nested
   archives included) is reproduced in the repository's NOTICE: every
   non-empty line, stripped of indentation, appears there.

  third_party.py [--root DIR]

The root holds versions.lock.yaml, THIRD-PARTY.md, NOTICE and platform/charts/.
"""

from __future__ import annotations

import argparse
import io
import re
import tarfile
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

YQ_COMMENT = re.compile(r"^<!--\s*yq:\s*(.+?)\s*-->\s*$")
MIN_TABLES = 4


def yq_lines(expr: str, root: Path) -> list:
    # The expression comes from a repository file: it may read the lock and nothing else (no env, no load()).
    res = c.run(["yq", "--security-disable-env-ops", "--security-disable-file-ops", "--string-interpolation=false",
                 "-r", expr, "versions.lock.yaml"], cwd=root, timeout=120)
    if res.returncode != 0:
        raise c.GateError(f"yq could not run `{expr[:80]}` on versions.lock.yaml: {res.stderr.strip()[:300]}")
    return [ln for ln in res.stdout.splitlines() if ln.strip()]


def table_rows(lines: list, after: int) -> list:
    """Data rows of the first table below line index `after` (header and separator skipped)."""
    i = after + 1
    while i < len(lines) and not lines[i].strip():
        i += 1
    block = []
    while i < len(lines) and lines[i].startswith("|"):
        block.append(lines[i].rstrip())
        i += 1
    return block[2:]


def check_tables(root: Path, rep: c.Report) -> None:
    doc = root / "THIRD-PARTY.md"
    if not doc.is_file():
        rep.fail("THIRD-PARTY.md is missing")
        return
    lines = doc.read_text().splitlines()
    comments = [(i, m.group(1)) for i, ln in enumerate(lines) if (m := YQ_COMMENT.match(ln))]
    if len(comments) < MIN_TABLES:
        rep.fail(f"THIRD-PARTY.md has {len(comments)} `<!-- yq: ... -->` table comment(s); expected at least "
                 f"{MIN_TABLES} (charts, subcharts, images, built images)")
    for n, (i, expr) in enumerate(comments, 1):
        want = yq_lines(expr, root)
        have = table_rows(lines, i)
        missing = [r for r in want if r not in have]
        extra = [r for r in have if r not in want]
        for r in missing:
            rep.fail(f"THIRD-PARTY.md table {n} (line {i + 1}): the lock gives a row that is missing: {r[:160]}")
        for r in extra:
            rep.fail(f"THIRD-PARTY.md table {n} (line {i + 1}): row has no lock entry: {r[:160]}")
        if not missing and not extra:
            rep.ok(f"THIRD-PARTY.md table {n}: {len(want)} row(s) match the lock")


def cache_entries(charts: Path) -> list:
    """Chart archives and chart directories listed in SHA256SUMS (top-level names)."""
    sums = charts / "SHA256SUMS"
    if not sums.is_file():
        raise c.GateError(f"{sums} not found")
    names = []
    for ln in sums.read_text().splitlines():
        parts = ln.split(None, 1)
        if len(parts) != 2:
            continue
        top = parts[1].strip().split("/")[0]
        if top in ("upstream-licenses", "README.md", "REUSE.toml") or top.endswith(".prov"):
            continue
        if top not in names:
            names.append(top)
    return names


def lock_chart_files(root: Path) -> list:
    return yq_lines(".charts[].file", root)


def check_cache(root: Path, rep: c.Report) -> list:
    """Returns the lock's chart files (as paths) for the NOTICE check."""
    files = lock_chart_files(root)
    cached = [f"platform/charts/{n}" for n in cache_entries(root / "platform" / "charts")]
    bad = 0
    for f in cached:
        if f not in files:
            bad += 1
            rep.fail(f"{f}: in the chart cache (SHA256SUMS) but no lock `.charts[].file` names it")
    charts = (root / "platform" / "charts").resolve()
    for f in files:
        target = (root / f).resolve()
        if target != charts and charts not in target.parents:
            bad += 1
            rep.fail(f"{f}: a lock chart file outside platform/charts/")
        elif not target.exists():
            bad += 1
            rep.fail(f"{f}: a lock chart file that does not exist")
    if not files:
        rep.fail("versions.lock.yaml lists no charts")
    elif not bad:
        rep.ok(f"chart cache and lock agree: {len(files)} chart(s)")
    return files


def notices_in_tar(data: bytes, label: str) -> list:
    """(label, lines) for every NOTICE* file in an archive, nested archives included."""
    out = []
    try:
        tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:*")
    except tarfile.TarError as exc:
        raise c.GateError(f"{label}: cannot read archive: {exc}") from exc
    with tf:
        for m in tf.getmembers():
            if not m.isfile():
                continue
            base = Path(m.name).name
            raw = tf.extractfile(m).read()
            if base.upper().startswith("NOTICE"):
                out.append((f"{label} > {m.name}", raw.decode("utf-8", "replace").splitlines()))
            elif base.endswith((".tgz", ".tar.gz")):
                out += notices_in_tar(raw, f"{label} > {m.name}")
    return out


def chart_notices(root: Path, files: list) -> list:
    found = []
    charts = (root / "platform" / "charts").resolve()
    for f in files:
        p = (root / f).resolve()
        if charts not in p.parents:
            continue      # check_cache reports a path outside platform/charts/; nothing outside it is read
        if p.is_dir():
            for q in sorted(p.rglob("*")):
                if q.is_file() and q.name.upper().startswith("NOTICE"):
                    found.append((c.rel(q, root), q.read_text(errors="replace").splitlines()))
        elif p.is_file():
            found += notices_in_tar(p.read_bytes(), f)
    return found


def check_notice(root: Path, files: list, rep: c.Report) -> None:
    notice = root / "NOTICE"
    if not notice.is_file():
        rep.fail("NOTICE is missing")
        return
    have = {ln.strip() for ln in notice.read_text(errors="replace").splitlines() if ln.strip()}
    found = chart_notices(root, files)
    bad = 0
    for label, lines in found:
        lost = [ln.strip() for ln in lines if ln.strip() and ln.strip() not in have]
        if lost:
            bad += 1
            rep.fail(f"{label}: {len(lost)} line(s) not reproduced in NOTICE, first: {lost[0][:120]!r}")
    if not bad:
        rep.ok(f"NOTICE reproduces the {len(found)} NOTICE file(s) inside vendored charts")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    args = ap.parse_args()
    root = Path(args.root).resolve()
    if not (root / "versions.lock.yaml").is_file():
        raise c.GateError(f"{root / 'versions.lock.yaml'} not found")
    rep = c.Report("third_party")
    check_tables(root, rep)
    files = check_cache(root, rep)
    check_notice(root, files, rep)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
