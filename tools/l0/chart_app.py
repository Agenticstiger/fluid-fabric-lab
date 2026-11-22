# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `chart_app`: chart appVersion, rendered images and versions.lock.yaml agree.

Chart/app version mismatches are a lint error unless
allowlisted with a reason. Per chart in versions.lock.yaml (not `use: denied`):

1. The cached chart's Chart.yaml (read from the archive or directory) has the
   lock's name, `version` and `appVersion`.
2. `runs` (what the lab runs) equals `app_version`, or the lock carries
   `app_version_mismatch: {allow: true, reason: ...}`.
3. Every vendored subchart is recorded under `subcharts` with its version
   (a recorded range such as "0.0" matching 0.0.5 is accepted with a note).
4. From the offline render of every tier: each release of the chart carries
   `helm.sh/chart: <name>-<lock version>`, and some container in it runs the
   image tag `runs`. A chart the lab uses (`use: used`) that renders nowhere
   fails.
5. Every rendered image reference (container images, and images handed to
   operators through CR fields, env vars, ConfigMaps and args) matches a lock
   image entry: same repository, same tag and same digest. A container
   running an image the lock marks `default-unused` or `denied`, or one the
   lock does not list, fails. Operator catalogue entries the lab does not
   select (another Kafka version, tag only) are reported as notes.

  chart_app.py [--root DIR] [--tier T] [--manifests DIR]
"""

from __future__ import annotations

import argparse
import io
import re
import shutil
import tempfile
import tarfile
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

REF_RE = re.compile(r"(?<![A-Za-z0-9./-])((?:[a-z0-9-]+\.)+[a-z]{2,}(?::[0-9]+)?/[a-z0-9._/-]+"
                    r"(?::[A-Za-z0-9._-]+)?(?:@sha256:[0-9a-f]{64})?)")
IMAGE_KEY = re.compile(r"image", re.I)


def read_chart_yaml(root: Path, file: str) -> dict:
    p = root / file
    if p.is_dir():
        return c.yaml_load(p / "Chart.yaml") or {}
    if not p.is_file():
        raise c.GateError(f"{file}: not in the chart cache")
    with tarfile.open(p, "r:*") as tf:
        names = [m for m in tf.getmembers() if m.isfile() and re.fullmatch(r"[^/]+/Chart\.yaml", m.name)]
        if len(names) != 1:
            raise c.GateError(f"{file}: expected one top-level Chart.yaml, found {len(names)}")
        tmp = Path(tempfile.mkdtemp(prefix="l0-chartapp-"))
        try:
            data = tf.extractfile(names[0]).read()
            (tmp / "Chart.yaml").write_bytes(data)
            return c.yaml_load(tmp / "Chart.yaml") or {}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


def _archive_top_chart(data: bytes) -> dict:
    """Chart.yaml of the top chart in a (nested) chart archive."""
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as inner:
        names = [m for m in inner.getmembers() if m.isfile() and re.fullmatch(r"[^/]+/Chart\.yaml", m.name)]
        if len(names) != 1:
            return {"name": None, "version": None}
        tmp = Path(tempfile.mkdtemp(prefix="l0-chartapp-"))
        try:
            (tmp / "Chart.yaml").write_bytes(inner.extractfile(names[0]).read())
            return c.yaml_load(tmp / "Chart.yaml") or {}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


def subchart_versions(root: Path, file: str) -> dict:
    """{dir or archive name: (name, version)} of first-level subcharts, whether
    vendored as a directory (charts/<name>/) or as an archive (charts/<name>.tgz),
    as Helm loads both."""
    p = root / file
    out = {}
    if p.is_dir():
        for cy in p.glob("charts/*/Chart.yaml"):
            meta = c.yaml_load(cy) or {}
            out[cy.parent.name] = (meta.get("name"), str(meta.get("version")))
        for a in p.glob("charts/*.tgz"):
            meta = _archive_top_chart(a.read_bytes())
            out[a.name] = (meta.get("name"), str(meta.get("version")))
        return out
    with tarfile.open(p, "r:*") as tf:
        tmp = Path(tempfile.mkdtemp(prefix="l0-chartapp-"))
        try:
            for m in tf.getmembers():
                if m.isfile() and re.fullmatch(r"[^/]+/charts/[^/]+/Chart\.yaml", m.name):
                    d = m.name.split("/")[2]
                    (tmp / f"{d}.yaml").write_bytes(tf.extractfile(m).read())
                    meta = c.yaml_load(tmp / f"{d}.yaml") or {}
                    out[d] = (meta.get("name"), str(meta.get("version")))
                elif m.isfile() and re.fullmatch(r"[^/]+/charts/[^/]+\.(?:tgz|tar\.gz)", m.name):
                    meta = _archive_top_chart(tf.extractfile(m).read())
                    out[m.name.split("/")[2]] = (meta.get("name"), str(meta.get("version")))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return out


def tag_matches(tag: str, runs: str) -> bool:
    if not tag:
        return False
    t, r = tag.lstrip("v"), str(runs).lstrip("v")
    return t == r


def image_refs(doc: dict) -> list:
    """(where, ref, is_container) for every image reference in one object."""
    out = []
    ps = c.pod_spec(doc)
    seen_paths = set()
    if ps is not None:
        for ct in c.containers(ps):
            if isinstance(ct.get("image"), str):
                out.append((f"container {ct.get('name')}", ct["image"], True))
            for e in ct.get("env") or []:
                if isinstance(e.get("value"), str) and IMAGE_KEY.search(e.get("name", "")):
                    for ref in REF_RE.findall(e["value"]):
                        out.append((f"container {ct.get('name')} env {e['name']}", ref, False))
            for a in (ct.get("args") or []) + (ct.get("command") or []):
                if isinstance(a, str) and "image" in a.lower():
                    for ref in REF_RE.findall(a):
                        out.append((f"container {ct.get('name')} arg", ref, False))
        seen_paths.add(".spec")
    if doc.get("kind") == "ConfigMap":
        for k, v in (doc.get("data") or {}).items():
            if IMAGE_KEY.search(k) and isinstance(v, str):
                for ref in REF_RE.findall(v):
                    out.append((f"data {k}", ref, False))
        return out
    if ps is None:
        for path, s in c.iter_strings(doc.get("spec") or {}, ".spec"):
            last = re.split(r"[.\[]", path)[-1]
            if IMAGE_KEY.search(last):
                for ref in REF_RE.findall(s):
                    out.append((path, ref, False))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--tier")
    ap.add_argument("--manifests", help="check this directory (one sub-directory per release) instead of rendering")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("chart_app")
    lock = c.yaml_load(root / "versions.lock.yaml") or {}
    charts = [e for e in lock.get("charts") or [] if isinstance(e, dict) and e.get("use") != "denied"]
    if not charts:
        raise c.GateError("versions.lock.yaml lists no charts")

    # ---- 1-3: cache vs lock --------------------------------------------------
    for e in charts:
        name, file = e.get("name"), str(e.get("file"))
        try:
            meta = read_chart_yaml(root, file)
        except c.GateError as exc:
            rep.fail(f"{name}: {exc}")
            continue
        problems = []
        if meta.get("name") != name:
            problems.append(f"Chart.yaml name {meta.get('name')!r} != lock name {name!r}")
        if str(meta.get("version")) != str(e.get("version")):
            problems.append(f"Chart.yaml version {meta.get('version')} != lock version {e.get('version')}")
        if str(meta.get("appVersion")) != str(e.get("app_version")):
            problems.append(f"Chart.yaml appVersion {meta.get('appVersion')} != lock app_version {e.get('app_version')}")
        if str(e.get("runs")) != str(e.get("app_version")):
            mm = e.get("app_version_mismatch") or {}
            if not (mm.get("allow") is True and str(mm.get("reason") or "").strip()):
                problems.append(f"runs {e.get('runs')} != appVersion {e.get('app_version')} and no "
                                "app_version_mismatch {allow: true, reason} in the lock")
            else:
                rep.note(f"{name}: runs {e.get('runs')} on appVersion {e.get('app_version')} "
                         f"(allowlisted: {str(mm['reason'])[:120]})")
        recorded = {}
        for s in e.get("subcharts") or []:
            for k in (s.get("name"), s.get("alias")):
                if k:
                    recorded[k] = s
        for d, (sname, sver) in sorted(subchart_versions(root, file).items()):
            s = recorded.get(d) or recorded.get(sname)
            if not s:
                problems.append(f"vendored subchart {d} ({sname} {sver}) is not recorded under subcharts")
            elif str(s.get("version")) != sver:
                lv = str(s.get("version"))
                if sver.startswith(lv + "."):
                    rep.note(f"{name}: subchart {d} recorded as range {lv!r}; archive ships {sver} "
                             "(record the exact version)")
                else:
                    problems.append(f"subchart {d}: archive ships {sver}, lock records {lv}")
        if problems:
            for p in problems:
                rep.fail(f"{name}: {p}")
        else:
            rep.ok(f"{name}: Chart.yaml {meta.get('version')} / appVersion {meta.get('appVersion')} match the lock; "
                   f"runs {e.get('runs')}")

    # ---- 4-5: rendered vs lock ---------------------------------------------
    img_index: dict = {}
    for i in lock.get("images") or []:
        if not isinstance(i, dict) or not i.get("ref"):
            continue
        p = c.parse_image(i["ref"])
        img_index.setdefault(p["name"], []).append({"tag": p["tag"], "digest": i.get("digest"),
                                                    "use": i.get("use"), "id": i.get("id")})
    for b in lock.get("built_images") or []:
        if b and b.get("digest") and b.get("repository"):
            p = c.parse_image(b["repository"])
            img_index.setdefault(p["name"], []).append({"tag": None, "digest": b["digest"], "use": b.get("use"),
                                                        "id": b.get("id")})

    base, tiers = c.manifests_by_tier(args, root)
    rendered_charts: dict = {}     # chart name -> set of tags run by its containers
    reported: set = set()
    try:
        for tier, d in tiers.items():
            docs = c.yaml_docs(c.manifest_files(d))
            if not docs:
                rep.fail(f"{tier}: no manifests found")
                continue
            for f, doc in docs:
                release = Path(c.rel(Path(f), d)).parts[0]
                label = ((doc.get("metadata") or {}).get("labels") or {}).get("helm.sh/chart") or ""
                owners = set()
                for e in charts:
                    vlabel = str(e.get("version")).replace("+", "_")
                    if label.startswith(f"{e['name']}-"):
                        owners.add(e["name"])
                        if label != f"{e['name']}-{vlabel}" and (tier, release, label) not in reported:
                            reported.add((tier, release, label))
                            rep.fail(f"{tier}/{release}: renders helm.sh/chart {label}, lock pins "
                                     f"{e['name']} {e.get('version')}")
                    elif release == e["name"]:
                        owners.add(e["name"])
                for where, ref, is_container in image_refs(doc):
                    key = (tier, c.obj_name(doc), where, ref)
                    if key in reported:
                        continue
                    reported.add(key)
                    msg = check_ref(ref, is_container, img_index)
                    obj = f"{tier}: {c.obj_name(doc)} {where}"
                    if msg and msg.startswith("NOTE"):
                        if ("note", ref) not in reported:
                            reported.add(("note", ref))
                            rep.note(f"{obj}: {ref}: {msg[5:]}")
                    elif msg:
                        rep.fail(f"{obj}: {ref}: {msg}")
                    if is_container:
                        for o in owners:
                            rendered_charts.setdefault(o, set()).add(effective_tag(ref, img_index))
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)

    for e in charts:
        tags = rendered_charts.get(e["name"])
        if tags is None:
            if e.get("use") == "used" and not args.manifests:
                rep.fail(f"{e['name']}: `use: used` in the lock but no tier renders it")
            elif not args.manifests:
                rep.note(f"{e['name']}: `use: {e.get('use')}`, not rendered by any tier (add-on off by default); "
                         "Chart.yaml checks only")
            continue
        if any(tag_matches(t, e.get("runs")) for t in tags):
            rep.ok(f"{e['name']}: a rendered container runs {e.get('runs')} (lock `runs`)")
        else:
            rep.fail(f"{e['name']}: no rendered container runs {e.get('runs')} (lock `runs`); "
                     f"rendered tags: {', '.join(sorted(t or '<none>' for t in tags))}")
    return rep.finish()


def effective_tag(ref: str, idx: dict) -> str:
    """The rendered tag, or for a digest-only reference the tag the lock pins for that digest."""
    p = c.parse_image(ref)
    if p["tag"]:
        return p["tag"]
    for e in idx.get(p["name"]) or []:
        if p["digest"] and e["digest"] == p["digest"] and e["tag"]:
            return e["tag"]
    return ""


def check_ref(ref: str, is_container: bool, idx: dict) -> str:
    p = c.parse_image(ref)
    entries = idx.get(p["name"])
    if not entries:
        return "not in versions.lock.yaml (unpinned image)" if is_container else \
            "NOTE image repository not in versions.lock.yaml (operator catalogue entry the lab does not select)"
    if p["digest"]:
        match = [e for e in entries if e["digest"] == p["digest"]]
        if not match:
            return f"digest {p['digest'][:19]}... matches no lock entry for {p['name']} (drift)"
        if p["tag"] and not any(e["tag"] == p["tag"] for e in match):
            return f"tag {p['tag']} with the digest of {match[0]['tag']} (lock {match[0]['id']}): drift"
        e = next((m for m in match if not p["tag"] or m["tag"] == p["tag"]), match[0])
    else:
        same_tag = [e for e in entries if e["tag"] == p["tag"]]
        if is_container:
            return "no digest (the lock pins " + (same_tag[0]["digest"] or "?")[:19] + "...)" if same_tag else \
                f"tag {p['tag']} is not pinned in versions.lock.yaml (lock has {', '.join(str(e['tag']) for e in entries)})"
        return f"NOTE operator catalogue entry without digest (tag {p['tag']}); the lab's CRs pin their image"
    if is_container and e["use"] not in ("used", "optional"):
        return f"lock entry {e['id']} is `use: {e['use']}`: the lab must not run it"
    return ""


if __name__ == "__main__":
    c.main_wrapper(main)
