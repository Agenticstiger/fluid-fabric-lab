# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `profile_schema`: every site profile and expectations ledger is valid.

- Every profiles/**/site.yaml validates against profiles/schema/site.schema.json.
- Every profiles/**/expected/*.yaml validates against
  profiles/schema/expectations.schema.json and names its own profile directory.
- The cross-field rules the schema lists under `x-crossFieldRules` (which JSON
  Schema cannot express) are enforced here. A rule in the schema that this
  gate does not implement FAILS, so the list cannot grow silently.
- Credential references are never resolved: names only, no environment,
  file or secret is read.

Validation uses tools/l0/_jsonschema.py, a stdlib validator for the keyword
subset the schemas declare (it refuses any other keyword). YAML is read with yq.

  profile_schema.py [--root DIR] [--schema-dir DIR]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402
from _jsonschema import UnsupportedKeyword, Validator  # noqa: E402

# Each implemented rule, keyed by the opening words of its x-crossFieldRules text.
RULES = {
    "facts.streaming.connector_api equals": "connector_api",
    "facts.kubernetes.api is `absent` exactly when": "kubernetes_api",
    "facts.opa.bundle_transport equals": "opa_bundles",
    "facts.identity.trino_groups equals": "trino_groups",
    "facts.keys.state_encryption openbao_transit requires": "key_service",
    "tiers.default is a key of tiers.supported": "tiers",
    "Credential references never resolve": "no_resolve",
}


def load_schema(path: Path) -> tuple:
    if not path.is_file():
        raise c.GateError(f"schema {path} not found")
    schema = json.loads(path.read_text())
    try:
        return schema, Validator(schema)
    except UnsupportedKeyword as exc:
        raise c.GateError(f"{path.name}: {exc} (move this schema to a full JSON Schema validator)") from exc


def cross_field(site: dict, prof_dir: Path, rules: list, exp_validator: Validator, rep_errs: list,
                validated: set) -> None:
    facts = site.get("facts") or {}
    ep = site.get("endpoints") or {}
    for rule in rules:
        key = next((v for k, v in RULES.items() if rule.startswith(k)), None)
        if key is None:
            rep_errs.append(f"x-crossFieldRules entry not implemented by the L0 gate: {rule[:120]!r}")
        elif key == "connector_api" and "streaming.connector_api" in facts:
            api, connect = facts["streaming.connector_api"], ep.get("connect") or {}
            if api == "absent":
                if connect:
                    rep_errs.append("facts.streaming.connector_api is `absent` but endpoints.connect declares domains")
            else:
                for d, spec in connect.items():
                    if (spec or {}).get("api") != api:
                        rep_errs.append(f"endpoints.connect.{d}.api {(spec or {}).get('api')!r} != "
                                        f"facts.streaming.connector_api {api!r}")
        elif key == "kubernetes_api" and "kubernetes.api" in facts:
            absent_ep = (ep.get("kubernetes") or {}).get("absent") is True
            if (facts["kubernetes.api"] == "absent") != absent_ep:
                rep_errs.append(f"facts.kubernetes.api {facts['kubernetes.api']!r} disagrees with endpoints.kubernetes")
        elif key == "opa_bundles":
            ob = ep.get("opaBundles") or {}
            if "opa.bundle_transport" in facts and ob.get("transport") and \
                    facts["opa.bundle_transport"] != ob["transport"]:
                rep_errs.append(f"facts.opa.bundle_transport {facts['opa.bundle_transport']!r} != "
                                f"endpoints.opaBundles.transport {ob['transport']!r}")
            if facts.get("opa.bundle_credentials") == "static_read_only" and not ob.get("readCredential"):
                rep_errs.append("opa.bundle_credentials static_read_only needs endpoints.opaBundles.readCredential")
        elif key == "trino_groups":
            gl = ep.get("groupLookup") or {}
            if "identity.trino_groups" in facts and gl.get("route") and facts["identity.trino_groups"] != gl["route"]:
                rep_errs.append(f"facts.identity.trino_groups {facts['identity.trino_groups']!r} != "
                                f"endpoints.groupLookup.route {gl['route']!r}")
        elif key == "key_service":
            if facts.get("keys.state_encryption") == "openbao_transit" and not ep.get("keyService"):
                rep_errs.append("keys.state_encryption openbao_transit needs endpoints.keyService")
        elif key == "tiers":
            tiers = site.get("tiers") or {}
            sup = tiers.get("supported") or {}
            if tiers.get("default") not in sup:
                rep_errs.append(f"tiers.default {tiers.get('default')!r} is not in tiers.supported")
            for t, spec in sup.items():
                f = prof_dir / str((spec or {}).get("expectations"))
                if not f.is_file():
                    rep_errs.append(f"tiers.supported.{t}.expectations {(spec or {}).get('expectations')} is missing")
                    continue
                validated.add(f.resolve())
                doc = c.yaml_load(f)
                errs = exp_validator.errors(doc)
                rep_errs += [f"{f.relative_to(prof_dir).as_posix()}: {e}" for e in errs]
                if isinstance(doc, dict):
                    if doc.get("tier") != t:
                        rep_errs.append(f"{f.relative_to(prof_dir).as_posix()}: declares tier {doc.get('tier')!r}, "
                                        f"site.yaml uses it for {t!r}")
                    if doc.get("profile") != prof_dir.name:
                        rep_errs.append(f"{f.relative_to(prof_dir).as_posix()}: declares profile "
                                        f"{doc.get('profile')!r}, directory is {prof_dir.name!r}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--schema-dir", default=None, help="default: <root>/profiles/schema, else this repo's")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    sd = Path(args.schema_dir).resolve() if args.schema_dir else (
        root / "profiles/schema" if (root / "profiles/schema").is_dir() else c.REPO / "profiles/schema")
    site_schema, site_v = load_schema(sd / "site.schema.json")
    _, exp_v = load_schema(sd / "expectations.schema.json")
    rules = list(site_schema.get("x-crossFieldRules") or [])
    rep = c.Report("profile_schema")
    profiles = root / "profiles"
    sites = sorted(p for p in profiles.rglob("site.yaml") if "schema" not in p.relative_to(profiles).parts) \
        if profiles.is_dir() else []
    if not sites:
        rep.fail(f"no profiles/**/site.yaml under {root}")
    validated: set = set()
    for site_path in sites:
        prof_dir = site_path.parent
        label = c.rel(site_path, root)
        site = c.yaml_load(site_path)
        errs = site_v.errors(site)
        if isinstance(site, dict) and not errs:
            cross_field(site, prof_dir, rules, exp_v, errs, validated)
        for e in errs:
            rep.fail(f"{label}: {e}")
        if not errs:
            rep.ok(f"{label}: valid ({len(rules)} cross-field rule(s), "
                   f"{len((site.get('tiers') or {}).get('supported') or {})} expectations ledger(s))")
    # Ledgers no site.yaml references are still validated.
    for f in sorted(profiles.rglob("expected/*.yaml")) if profiles.is_dir() else []:
        if f.resolve() in validated:
            continue
        doc = c.yaml_load(f)
        errs = exp_v.errors(doc)
        if isinstance(doc, dict) and doc.get("profile") != f.parent.parent.name:
            errs.append(f"declares profile {doc.get('profile')!r}, directory is {f.parent.parent.name!r}")
        for e in errs:
            rep.fail(f"{c.rel(f, root)}: {e}")
        if not errs:
            rep.ok(f"{c.rel(f, root)}: valid (not referenced by a site.yaml)")
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
