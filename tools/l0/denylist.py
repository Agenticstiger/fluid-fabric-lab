# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `denylist`: no denied image or chart source can reach the lab.  # brand-fence: allow

Denied: Bitnami images (bitnami/*, bitnamilegacy/*,
docker.io/bitnami*), pgsty/silo and pgsty/mc (AGPL), quay.io/vakamo/
lakekeeper-plus (licensed edition), Redpanda images, Ilum images and the  # brand-fence: allow
Ilum chart repository, minio/minio, HashiCorp Vault (BUSL), MongoDB (SSPL),  # brand-fence: allow
and floating references (`:latest`, or neither tag nor digest).

What is scanned
1. Rendered manifests of every tier (`helmfile template`, offline). The deny
   patterns are matched against EVERY string value, so an image passed to an
   operator through an env var, a CR field or a ConfigMap is caught too; the
   patterns ignore the registry, so a mirror, a port or another registry in
   front of a denied repository does not hide it. Base64 values (Secret
   data, ConfigMap binaryData, any base64-looking string) are decoded and
   matched too, and a denied organisation named on its own (an image split
   into org and name keys) fails. Every container `image:` (every built-in
   workload kind, ReplicationController included) and every image-named
   field of a custom resource (`image`, `imageName`, ...) must be pinned by
   digest and must not be floating.
2. versions.lock.yaml: an image the lab runs (`use: used` or `optional`, and
   built_images bases) must not match a deny rule or float. A `use: denied`
   record must match a deny rule (else it is mislabelled); it is never
   rendered (checked by 1).
3. The Helmfile, component values, scripts and CI workflows, as text, for the
   deny patterns (catches e.g. an Ilum chart repository URL). Comment lines  # brand-fence: allow
   are not references and are not matched.

  denylist.py [--root DIR] [--tier T] [--manifests DIR] [--lock FILE] [--no-text]
"""

from __future__ import annotations

import argparse
import base64
import re
import shutil
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

# (rule id, regex over a whole string value). Matched case-insensitively.
DENY = [
    ("bitnami", r"(?<![a-z0-9.-])(?:[a-z0-9.-]+/)?bitnami(?:legacy|charts|secure)?/[a-z0-9._/-]+"),
    ("bitnami-registry", r"docker\.io/bitnami[a-z0-9]*(?:/|\b)"),
    # silo/mc with any suffix (silo-arm64 is the same AGPL image).
    ("pgsty-agpl", r"(?<![a-z0-9.-])(?:[a-z0-9.-]+/)?pgsty/(?:silo|mc)(?![a-z0-9])"),
    # The licensed edition, behind any registry, mirror or port.
    ("lakekeeper-plus", r"(?<![a-z0-9-])lakekeeper-plus(?![a-z0-9])"),
    ("redpanda", r"(?<![a-z0-9.-])(?:[a-z0-9.-]+/)?redpandadata/[a-z0-9._/-]+|docker\.redpanda\.com/"),  # brand-fence: allow
    ("ilum", r"(?<![a-z0-9.-])(?:[a-z0-9.-]+/)?ilum/[a-z0-9._/-]+|charts\.ilum\.cloud"),  # brand-fence: allow
    ("minio", r"(?<![a-z0-9.-])(?:(?:docker\.io|quay\.io)/)?minio/minio(?![a-z0-9-])"),
    ("vault-busl", r"(?<![a-z0-9.-])(?:docker\.io/)?hashicorp/vault(?:-enterprise)?(?![a-z0-9-])"),
    ("mongodb-sspl", r"(?<![a-z0-9.-])(?:docker\.io/)?(?:library/)?mongo(?:db)?(?::|@|/|$)"
                     r"|percona-server-mongodb|mongodb/mongodb-(?:community|enterprise)|mongodb-enterprise"),
    # Lakekeeper's internal-OpenFGA rebuild of the Bitnami postgresql image
    # (Debian 11, LTS ended 2026-08-31; needs allowInsecureImages).
    ("bitnami-rebuild", r"(?<![a-z0-9.-])lakekeeper/postgresql(?![a-z0-9-])"),
    # Scarf download-analytics pull-through proxy (Superset chart default): the
    # lab never pulls through a telemetry proxy (versions.lock.yaml superset note).
    ("scarf-proxy", r"\.scarf\.sh/"),
]
DENY_RE = [(rid, re.compile(rx, re.I)) for rid, rx in DENY]
# In RENDERED values only: a denied organisation named on its own, e.g. an
# operator given IMAGE_ORG and IMAGE_NAME as two keys (a split reference).
ORG_TOKEN = re.compile(r"(?<![a-z0-9])(bitnami(?:legacy|charts|secure)?|pgsty|vakamo|redpandadata)(?![a-z0-9])",  # brand-fence: allow
                       re.I)
B64ISH = re.compile(r"^[A-Za-z0-9+/_-]{16,}={0,2}$")
IMAGE_KEY = re.compile(r"image", re.I)
# A string that looks like an image reference (for the floating-tag rule on lock refs).
IMAGE_SHAPE = re.compile(r"^[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9._/-]+(?::[A-Za-z0-9._-]+)?(?:@sha256:[0-9a-f]{64})?$")
TEXT_GLOBS = ["platform/helmfile.yaml.gotmpl", "platform/**/*.yaml", "platform/**/*.yml",
              "platform/**/*.sh", "platform/**/*.gotmpl", "platform/**/*.hcl", "platform/**/Dockerfile",
              ".github/**/*.yml", ".github/**/*.yaml", "scripts/**", "tools/**/*.sh", "lab", "labctl/**/*.py"]


def deny_hits(s: str) -> list:
    return [rid for rid, rx in DENY_RE if rx.search(s)]


def floating(ref: str) -> str:
    p = c.parse_image(ref)
    if p["tag"] == "latest" and not p["digest"]:
        return "floating tag :latest without a digest"
    if p["tag"] == "latest":
        return "tag :latest (pin a real tag and the digest)"
    if not p["tag"] and not p["digest"]:
        return "no tag and no digest"
    return ""


def decoded(s: str) -> str:
    """The text a base64 value holds, if it is mostly printable; else ''."""
    s = s.strip()
    if not B64ISH.match(s.replace("\n", "")):
        return ""
    try:
        raw = base64.b64decode(s + "=" * (-len(s) % 4), altchars=b"-_" if ("-" in s or "_" in s) else None)
    except (ValueError, TypeError):
        return ""
    text = raw.decode("utf-8", "replace")
    printable = sum(ch.isprintable() or ch in "\n\t" for ch in text)
    return text if text and printable / len(text) > 0.9 else ""


def value_hits(s: str) -> list:
    """Deny rules a rendered string value matches (also after base64 decoding)."""
    out = [(rid, s) for rid in deny_hits(s)]
    m = ORG_TOKEN.search(s)
    if m and not out:
        out.append(("denied-org", s))
    d = decoded(s)
    if d:
        out += [(f"{rid}, base64", d) for rid in deny_hits(d)]
        m = ORG_TOKEN.search(d)
        if m and not any(r.endswith("base64") for r, _ in out):
            out.append(("denied-org, base64", d))
    return out


def image_shaped(s: str) -> bool:
    s = s.strip()
    return bool(s) and " " not in s and "\n" not in s and "/" in s and not s.startswith(("http:", "https:", "/"))


def scan_manifests(tier: str, d: Path, rep: c.Report) -> None:
    docs = c.yaml_docs(c.manifest_files(d))
    if not docs:
        rep.fail(f"{tier}: no manifests found in {d}")
        return
    n_img = 0
    problems = 0
    for f, doc in docs:
        where = f"{tier}: {c.obj_name(doc)} ({c.rel(Path(f), d)})"
        values = list(c.iter_strings(doc))
        if doc.get("kind") in ("Secret", "ConfigMap"):
            # Secret data and ConfigMap binaryData are base64 by definition.
            for k in ("data", "binaryData"):
                for path, s in c.iter_strings(doc.get(k) or {}, f".{k}"):
                    txt = decoded(s) or (base64.b64decode(s, validate=False).decode("utf-8", "replace")
                                         if doc.get("kind") == "Secret" or k == "binaryData" else "")
                    if txt and txt != s:
                        values.append((path + "(base64)", txt))
        for path, s in values:
            for rid, shown in value_hits(s):
                problems += 1
                rep.fail(f"{where}{path}: denied ({rid}): {shown.strip()[:160]}")
        ps = c.pod_spec(doc)
        if ps is None:
            # Images handed to an operator through a CR field (Keycloak `image`,
            # CNPG `imageName`, Strimzi `image`) must be pinned the same way.
            if doc.get("kind") in ("ConfigMap", "Secret", "CustomResourceDefinition"):
                continue
            for path, s in c.iter_strings(doc.get("spec") or {}, ".spec"):
                last = re.split(r"[.\[]", path)[-1]
                if not IMAGE_KEY.search(last) or not image_shaped(s):
                    continue
                n_img += 1
                why = floating(s)
                p = c.parse_image(s)
                if why:
                    problems += 1
                    rep.fail(f"{where}{path}: image {s}: {why}")
                elif not p["digest"] or not c.DIGEST_RE.match(p["digest"]):
                    problems += 1
                    rep.fail(f"{where}{path}: image {s} is not pinned by sha256 digest")
            continue
        for ct in c.containers(ps):
            img = ct.get("image")
            if not isinstance(img, str) or not img:
                problems += 1
                rep.fail(f"{where}: container {ct.get('name')} has no image")
                continue
            n_img += 1
            why = floating(img)
            if why:
                problems += 1
                rep.fail(f"{where}: container {ct.get('name')} image {img}: {why}")
            elif not c.parse_image(img)["digest"] or not c.DIGEST_RE.match(c.parse_image(img)["digest"]):
                problems += 1
                rep.fail(f"{where}: container {ct.get('name')} image {img} is not pinned by sha256 digest")
    if not problems:
        rep.ok(f"{tier}: {len(docs)} object(s), {n_img} image reference(s): none denied, all digest-pinned")


def lock_refs(lock: dict) -> list:
    """(where, ref, use, entry) for every image reference in the lock."""
    out = []
    for i in lock.get("images") or []:
        if isinstance(i, dict) and i.get("ref"):
            ref = i["ref"]
            if i.get("digest") and "@" not in ref:
                ref = f"{ref}@{i['digest']}"
            out.append((f"images[{i.get('id')}]", ref, str(i.get("use")), i))
    for b in lock.get("built_images") or []:
        base = (b or {}).get("base") or {}
        if base.get("ref"):
            ref = base["ref"] + (f"@{base['digest']}" if base.get("digest") else "")
            out.append((f"built_images[{b.get('id')}].base", ref, str(b.get("use")), b))
    for cand in lock.get("candidates") or []:
        for img in (cand or {}).get("images") or []:
            if img.get("ref"):
                ref = img["ref"] + (f"@{img['digest']}" if img.get("digest") else "")
                out.append((f"candidates[{cand.get('component')}]", ref, "candidate", img))
    return out


def scan_lock(path: Path, rep: c.Report) -> None:
    lock = c.yaml_load(path) or {}
    refs = lock_refs(lock)
    if not refs:
        rep.fail(f"{path.name}: no image references found")
        return
    problems = 0
    for where, ref, use, entry in refs:
        hits = deny_hits(ref)
        if use == "denied":
            if not hits:
                problems += 1
                rep.fail(f"{path.name} {where}: `use: denied` but {ref} matches no deny rule (mislabelled?)")
            continue
        if use in ("used", "optional", "candidate"):
            if hits:
                problems += 1
                rep.fail(f"{path.name} {where}: {ref} is denied ({', '.join(hits)}) but use is {use}")
            why = floating(ref)
            if why:
                problems += 1
                rep.fail(f"{path.name} {where}: {ref}: {why}")
            elif use != "candidate" and not c.parse_image(ref)["digest"]:
                problems += 1
                rep.fail(f"{path.name} {where}: {ref} has no digest")
        elif hits and use not in ("default-unused",):
            problems += 1
            rep.fail(f"{path.name} {where}: {ref} is denied ({', '.join(hits)}) with use {use}")
        elif hits:
            problems += 1
            rep.fail(f"{path.name} {where}: {ref} is denied ({', '.join(hits)}); record it as `use: denied`")
    # Free-text values elsewhere in the lock that look like image refs (e.g. a tool image).
    for path_s, s in c.iter_strings({k: v for k, v in lock.items() if k not in ("images", "built_images",
                                                                                 "candidates", "not_pinned")}):
        if IMAGE_SHAPE.match(s) and deny_hits(s):
            problems += 1
            rep.fail(f"{path.name} {path_s}: denied image reference {s}")
    if not problems:
        used = sum(1 for _, _, u, _ in refs if u in ("used", "optional"))
        denied = sum(1 for _, _, u, _ in refs if u == "denied")
        rep.ok(f"{path.name}: {len(refs)} image reference(s); {used} run by the lab, none denied or floating; "
               f"{denied} `use: denied` record(s) all match a deny rule")


def scan_text(root: Path, rep: c.Report) -> None:
    pats = [c.glob_to_regex(g) for g in TEXT_GLOBS]
    n = 0
    problems = 0
    for p in c.walk_files(root, (".git", "charts", "schemas", "node_modules")):
        r = c.rel(p, root)
        if not any(rx.match(r) for rx in pats) or c.is_binary(p):
            continue
        n += 1
        for ln, line in enumerate(p.read_text(errors="replace").splitlines(), 1):
            # Comments that explain what the lab turns off are not references;
            # the rendered manifests (scan 1) are the authority for what runs.
            if "denylist: allow" in line or line.lstrip().startswith(("#", "//", "{{/*", "{{- /*")):
                continue
            for rid in deny_hits(line):
                problems += 1
                rep.fail(f"{r}:{ln}: denied ({rid}): {line.strip()[:160]}")
    if n == 0:
        rep.fail("no Helmfile/values/script/workflow files found to scan")
    elif not problems:
        rep.ok(f"{n} Helmfile/values/script/workflow file(s): no denied image or chart source")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--tier")
    ap.add_argument("--manifests", help="scan this directory instead of rendering the tiers")
    ap.add_argument("--lock", help="lock file to scan (default <root>/versions.lock.yaml); 'none' is "
                                   "accepted only with --manifests (fixture runs)")
    ap.add_argument("--no-text", action="store_true",
                    help="do not scan Helmfile/script text (fixture runs; the real-repo run always scans)")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    rep = c.Report("denylist")
    lock = args.lock or str(root / "versions.lock.yaml")
    if lock == "none":
        if not args.manifests:
            raise c.GateError("--lock none is only allowed with --manifests")
    else:
        scan_lock(Path(lock).resolve(), rep)
    if not args.no_text:
        scan_text(root, rep)
    base, tiers = c.manifests_by_tier(args, root)
    try:
        for tier, d in tiers.items():
            scan_manifests(tier, d, rep)
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
