# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `secret_scan`: gitleaks finds no secret in the tree (or its history).

Runs gitleaks (pinned in versions.lock.yaml) offline with this repository's
.gitleaks.toml (gitleaks' default rules plus the lab canary rule):
- `gitleaks dir` over the working tree (every file, committed or not);
- `gitleaks git` over the history, when the tree is a git repository with at
  least one commit (a repository with no commits has no history to scan; the
  working-tree scan above still covers every file).
Findings are printed redacted. Named `secret_scan`, not `secrets`, so it
cannot shadow the standard-library module of that name.

  secret_scan.py [--root DIR] [--config FILE]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402


def run_gitleaks(argv: list, root: Path, what: str, rep: c.Report, n_files: int = 0) -> None:
    res = c.run(argv, cwd=root, timeout=900)
    if res.returncode not in (0, 1):
        raise c.GateError(f"gitleaks {what} failed (exit {res.returncode}): {res.stderr.strip()[-400:]}")
    try:
        findings = json.loads(res.stdout or "[]")
    except ValueError as exc:
        raise c.GateError(f"gitleaks {what}: unreadable report: {res.stdout[:200]!r}") from exc
    if res.returncode == 1 and not findings:
        raise c.GateError(f"gitleaks {what} exited 1 with no findings: {res.stderr.strip()[-300:]}")
    for f in findings:
        where = f"{f.get('File')}:{f.get('StartLine')}" + (f" @{f.get('Commit', '')[:10]}" if f.get("Commit") else "")
        rep.fail(f"{what}: {f.get('RuleID')} at {where} ({f.get('Description', '')})")
    if not findings:
        rep.ok(f"gitleaks {what}: no findings" + (f" ({n_files} file(s) in the tree)" if what == "working tree" else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--config", default=str(c.REPO / ".gitleaks.toml"))
    args = ap.parse_args()
    root = Path(args.root).resolve()
    cfg = Path(args.config).resolve()
    if not cfg.is_file():
        raise c.GateError(f"gitleaks config {cfg} not found")
    rep = c.Report("secret_scan")
    if not root.is_dir():
        raise c.GateError(f"--root {root} is not a directory")
    n_files = sum(1 for _ in c.walk_files(root, (".git",)))
    if n_files == 0:
        rep.fail(f"{root}: no files to scan (an empty tree proves nothing)")
        return rep.finish()
    common = ["--no-banner", "--no-color", "--config", str(cfg), "--redact", "--report-format", "json",
              "--report-path", "-", "--log-level", "error"]
    run_gitleaks(["gitleaks", "dir", *common, "."], root, "working tree", rep, n_files)
    head = c.run(["git", "-C", str(root), "rev-parse", "--verify", "--quiet", "HEAD"]) \
        if (root / ".git").exists() else None
    if head is not None and head.returncode == 0:
        run_gitleaks(["gitleaks", "git", *common, "."], root, "git history", rep)
    else:
        rep.note("no git history at this root (no commits yet); the working-tree scan covered every file")
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
