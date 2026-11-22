# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `schema`: rendered manifests validate against the VENDORED schemas only.

1. `platform/schemas/regenerate.sh verify` (offline SHA256SUMS check of the
   vendored schemas; the schemas' own tool, reused).
2. kubeconform with `-strict` and only the local `-schema-location` templates
   from versions.lock.yaml (`schemas.kubeconform_flags`). The default remote
   location is never used: kubeconform only falls back to it when no
   -schema-location is given, and any non-local location is refused here. A
   resource with no schema is an error, not a skip (no -ignore-missing-schemas).
   Kinds listed in platform/schemas/kubernetes/MISSING.txt (upstream publishes
   no schema; reviewed) are passed to -skip and reported by name.

Site profiles are validated by the separate `profile_schema` gate.

  schema.py [--root DIR] [--tier T] [--manifests DIR]
"""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402


def kubeconform_flags(root: Path) -> list:
    lock = c.yaml_load(root / "versions.lock.yaml")
    raw = ((lock or {}).get("schemas") or {}).get("kubeconform_flags")
    if not raw:
        raise c.GateError("versions.lock.yaml schemas.kubeconform_flags is missing")
    flags = shlex.split(raw)
    locs = [flags[i + 1] for i, f in enumerate(flags) if f == "-schema-location" and i + 1 < len(flags)]
    if not locs:
        raise c.GateError("kubeconform_flags has no -schema-location: kubeconform would use its REMOTE default")
    for loc in locs:
        if loc == "default" or "://" in loc:
            raise c.GateError(f"kubeconform schema location {loc!r} is not a local path")
    for bad in ("-ignore-missing-schemas", "-schema-location=default"):
        if bad in flags:
            raise c.GateError(f"kubeconform_flags must not contain {bad}")
    if "-strict" not in flags:
        raise c.GateError("kubeconform_flags must contain -strict")
    return flags


def missing_kinds(root: Path) -> list:
    f = root / "platform/schemas/kubernetes/MISSING.txt"
    kinds = []
    if f.is_file():
        for line in f.read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2 and not line.startswith("#"):
                kinds.append(parts[1])
    return kinds


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO), help="tree holding versions.lock.yaml and platform/schemas")
    ap.add_argument("--tier")
    ap.add_argument("--manifests", help="validate this directory instead of rendering the tiers")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("schema")

    verify = root / "platform/schemas/regenerate.sh"
    if not verify.is_file():
        raise c.GateError(f"{verify} not found")
    res = c.run(["bash", str(verify), "verify"], cwd=root)
    if res.returncode != 0:
        rep.fail(f"vendored schemas do not match platform/schemas/SHA256SUMS: {(res.stdout + res.stderr).strip()[-400:]}")
    else:
        rep.ok("vendored schemas match platform/schemas/SHA256SUMS (offline)")

    flags = kubeconform_flags(root)
    skip = missing_kinds(root)
    if skip:
        rep.note(f"reviewed exception (no upstream schema, listed in MISSING.txt): -skip {','.join(skip)}")
    base, tiers = c.manifests_by_tier(args, root)
    try:
        for tier, d in tiers.items():
            argv = ["kubeconform", *flags, "-output", "json", "-verbose"]
            if skip:
                argv += ["-skip", ",".join(skip)]
            argv.append(str(d))
            res = c.run(argv, cwd=root)
            try:
                out = json.loads(res.stdout or "{}")
            except ValueError:
                rep.fail(f"{tier}: kubeconform output unreadable (exit {res.returncode}): {res.stderr.strip()[:300]}")
                continue
            resources = [r for r in out.get("resources", []) if r.get("kind")]
            bad = [r for r in resources if r.get("status") in ("statusInvalid", "statusError")]
            skipped = [r for r in resources if r.get("status") == "statusSkipped" and r.get("kind") not in skip]
            for r in bad:
                # kubeconform names the schema by its file:// URL. Shorten it to a
                # repository path BEFORE truncating, or a long checkout path pushes
                # the decisive part of the message ("additional properties ...")
                # past the cut.
                msg = (r.get("msg") or "").replace(root.resolve().as_uri() + "/", "")
                rep.fail(f"{tier}: {r.get('kind')}/{r.get('name')} ({c.rel(Path(r['filename']), d)}): "
                         f"{r.get('status')[6:].lower()}: {msg[:300]}")
            for r in skipped:
                rep.fail(f"{tier}: {r.get('kind')}/{r.get('name')} was skipped but is not a reviewed exception")
            if res.returncode != 0 and not bad:
                rep.fail(f"{tier}: kubeconform exit {res.returncode}: {res.stderr.strip()[:300]}")
            if not resources:
                rep.fail(f"{tier}: no resources found to validate in {d}")
            elif not bad and not skipped and res.returncode == 0:
                valid = sum(1 for r in resources if r.get("status") == "statusValid")
                rep.ok(f"{tier}: {valid} resource(s) valid against vendored schemas (strict); "
                       f"{len(resources) - valid} reviewed-exception skip(s)")
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
