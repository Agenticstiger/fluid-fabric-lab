# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `reuse_lint`: REUSE compliance of the whole tree, and the values-only
rule for profiles/stackable (OSL-3.0).

1. `reuse lint` on the whole tree (REUSE Specification 3.3; the reuse tool,
   pinned in versions.lock.yaml, is run, never vendored).
2. `reuse lint-file` on every file in profiles/stackable/.
3. profiles/stackable/ may hold only README.md and Agentics-authored Helm
   values files, each with its OWN header naming Agentics Transformation
   Limited and Apache-2.0 (a REUSE.toml annotation does not count there).
   Anything else fails: Kubernetes objects (apiVersion + kind, e.g. a copied
   CRD), kustomizations or post-renderer patches, templates, charts,
   archives, sub-directories, symlinks.

  reuse_lint.py [--root DIR]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

OVERLAY = "profiles/stackable"  # brand-fence: allow
# REUSE-IgnoreStart
HEADER_COPY = re.compile(r"SPDX-FileCopyrightText:\s*(?:\d{4}(?:-\d{4})?\s+)?Agentics Transformation Limited\s*$", re.M)
HEADER_LIC = re.compile(r"SPDX-License-Identifier:\s*Apache-2\.0\s*$", re.M)
BAD_NAME = re.compile(r"(kustomiz|patch|post-?render|crd|chart\.ya?ml$|\.tpl$|\.tgz$|\.tar)", re.I)
# REUSE-IgnoreEnd
TOP_KEY = re.compile(r"^(apiVersion|kind|resources|patches|patchesStrategicMerge|patchesJson6902|bases)\s*:", re.M)


def check_overlay(root: Path, rep: c.Report) -> None:
    d = root / OVERLAY
    if not d.is_dir():
        rep.fail(f"{OVERLAY}/ is missing (the REUSE check must run on it)")
        return
    files, bad = [], 0
    for p in sorted(d.rglob("*")):
        r = c.rel(p, root)
        if p.is_symlink():
            bad += 1
            rep.fail(f"{r}: symlink not allowed in the values-only overlay")
            continue
        if p.is_dir():
            bad += 1
            rep.fail(f"{r}/: sub-directory not allowed (values files only, flat)")
            continue
        files.append(p)
        if p.name == "README.md":
            continue
        if p.suffix not in (".yaml", ".yml"):
            bad += 1
            rep.fail(f"{r}: only README.md and Helm values files (*.yaml) are allowed here")
            continue
        if BAD_NAME.search(p.name):
            bad += 1
            rep.fail(f"{r}: name looks like a kustomization, patch, CRD or chart file, not a values file")
            continue
        text = p.read_text(errors="replace")
        head = "\n".join(text.splitlines()[:15])
        if not (HEADER_COPY.search(head) and HEADER_LIC.search(head)):
            bad += 1
            # REUSE-IgnoreStart
            rep.fail(f"{r}: needs its own header 'SPDX-FileCopyrightText: <year> Agentics Transformation "
                     "Limited' and 'SPDX-License-Identifier: Apache-2.0' (Agentics-authored values only)")
            # REUSE-IgnoreEnd
        for doc in [x for x in re.split(r"^---\s*$", text, flags=re.M) if x.strip()]:
            keys = set(TOP_KEY.findall(doc))
            if {"apiVersion", "kind"} <= keys or keys & {"resources", "patches", "patchesStrategicMerge",
                                                          "patchesJson6902", "bases"}:
                bad += 1
                rep.fail(f"{r}: contains a Kubernetes object or kustomization ({', '.join(sorted(keys))}); "
                         "copied CRDs and patches of rendered templates are derivative works")
                break
    if files:
        res = c.run(["reuse", "--root", str(root), "lint-file", *[str(f) for f in files]])
        if res.returncode != 0:
            bad += 1
            rep.fail(f"reuse lint-file {OVERLAY}: {(res.stdout + res.stderr).strip()[-600:]}")
    if not bad:
        rep.ok(f"{OVERLAY}/: {len(files)} file(s), README and Agentics-authored values files only, REUSE-clean")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("reuse_lint")
    # --lines: one "path: problem" line per finding (reuse's own per-line format).
    res = c.run(["reuse", "--root", str(root), "lint", "--lines"], timeout=900)
    if res.returncode == 0:
        rep.ok("reuse lint: the tree is compliant with REUSE 3.3")
    else:
        lines = [ln.strip() for ln in (res.stdout + res.stderr).splitlines() if ln.strip()]
        for ln in lines[:40]:
            rep.fail(f"reuse lint: {c.rel(Path(ln.split(':', 1)[0]), root)}:{ln.split(':', 1)[1] if ':' in ln else ''}")
        if len(lines) > 40:
            rep.fail(f"reuse lint: ... {len(lines) - 40} more finding(s)")
        if not lines:
            rep.fail(f"reuse lint exit {res.returncode} with no output")
    check_overlay(root, rep)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
