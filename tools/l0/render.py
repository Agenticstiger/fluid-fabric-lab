# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `render`: every tier renders from the committed chart cache, offline.

For each tier in tiers.yaml this runs `helmfile template` through the
launcher's own render (labctl/render.py, reused rather than copied): chart
cache checked against platform/charts/SHA256SUMS and versions.lock.yaml, a
throwaway HOME and Helm dirs, every proxy variable pointed at a closed port,
`--skip-deps`, and a per-release "rendered something" check. It also renders
the one enableable optional add-on (kafbat UI on the streaming tier).

  render.py [--root DIR] [--tier T] [--helmfile FILE]

--helmfile renders a different Helmfile against the same tree (used by the
selftest to prove a remote chart cannot render offline).
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

ADDON_RUNS = [("streaming", "kafbat-ui", "kafka-ui")]


def render_addon(root: Path, tier: str, addon: str, release: str, rep: c.Report) -> None:
    render = c._labctl_render()
    from labctl import config  # noqa: E402  (on sys.path after _labctl_render)

    t = config.resolve_tier(c.yaml_load(root / "tiers.yaml"), tier)
    out = Path(tempfile.mkdtemp(prefix=f"l0-render-addon-{addon}-"))
    home = Path(tempfile.mkdtemp(prefix="l0-render-home-"))
    try:
        argv = render.helmfile_argv(root / "platform/helmfile.yaml.gotmpl", t, out, tier)
        i = argv.index("template")
        argv[i:i] = ["--state-values-set", f"addons.{addon}=true"]
        res = c.run(argv, cwd=root, env=render.hermetic_env(home), timeout=900)
        if res.returncode != 0:
            rep.fail(f"tier {tier} + add-on {addon}: helmfile template exit {res.returncode}: "
                     f"{(res.stdout + res.stderr).strip()[-400:]}")
            return
        files = c.manifest_files(out / release)
        if not files:
            rep.fail(f"tier {tier} + add-on {addon}: release {release} rendered nothing")
        else:
            rep.ok(f"tier {tier} + add-on {addon}: release {release} rendered {len(files)} file(s) offline")
    finally:
        shutil.rmtree(home, ignore_errors=True)
        shutil.rmtree(out, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--tier", help="render one tier only (default: every tier in tiers.yaml)")
    ap.add_argument("--helmfile", help="render this Helmfile instead of platform/helmfile.yaml.gotmpl")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("render")
    render = c._labctl_render()
    tiers = [args.tier] if args.tier else c.tier_names(root)
    for t in tiers:
        out = Path(tempfile.mkdtemp(prefix=f"l0-render-{t}-"))
        try:
            doc = render.run_render(root, t, out=str(out / "m"), helmfile=args.helmfile, environment=t)
        finally:
            shutil.rmtree(out, ignore_errors=True)
        failed = [ch for ch in doc.get("checks", []) if ch.get("status") != "pass"]
        if doc.get("ok") and not failed:
            n = sum(1 for ch in doc["checks"] if ch["id"].startswith("release:"))
            rep.ok(f"tier {t}: {n} release(s) rendered from the chart cache with no network")
        else:
            for ch in failed or [{"id": "render", "message": "not ok"}]:
                detail = ch.get("detail") or {}
                tail = detail.get("output_tail") or "; ".join(detail.get("problems") or [])
                rep.fail(f"tier {t}: {ch['id']}: {ch['message']}" + (f" | {tail.strip()[-500:]}" if tail else ""))
    if not args.helmfile and not args.tier:
        for tier, addon, release in ADDON_RUNS:
            render_addon(root, tier, addon, release, rep)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
