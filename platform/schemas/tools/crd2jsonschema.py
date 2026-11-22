#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-FileCopyrightText: 2017 Gareth Rushgrove, Yann Hamon and the kubeconform contributors
# SPDX-License-Identifier: Apache-2.0
"""Convert CustomResourceDefinitions into kubeconform JSON schemas, offline.

Standard-library port of yannh/kubeconform scripts/openapi2jsonschema.py at
tag v0.8.0 (Apache-2.0; itself derived from instrumenta/openapi2jsonschema).
The transformation is kept identical so the schemas behave exactly like the
ones kubeconform users generate upstream:

* every object schema that has ``properties`` and no ``additionalProperties``
  gets ``additionalProperties: false``, except the root schema (upstream's
  default, i.e. DENY_ROOT_ADDITIONAL_PROPERTIES unset);
* ``format: int-or-string`` becomes ``oneOf: [string, integer]``.

Differences from upstream, all about I/O only:

* input is CRDs as JSON, one document per line (PyYAML is not stdlib; the
  pinned ``yq`` does the YAML -> JSON step, see ../regenerate.sh);
* output goes to ``<out>/<group>/<kind>_<version>.json`` (lower-case kind),
  the layout of datreeio/CRDs-catalog that kubeconform documents for
  ``-schema-location '<dir>/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'``;
* JSON is written compact, with sorted keys and a trailing newline, so
  regeneration is byte-for-byte reproducible and the vendored tree stays small.

Usage: yq -o=json -I=0 'select(.kind == "CustomResourceDefinition")' crds.yaml \
         | crd2jsonschema.py OUT_DIR
Exit codes: 0 written; 1 no CRD found or bad input.
"""
import json
import os
import sys


def additional_properties(data, skip=False):
    """Recreates kubectl's behaviour (see upstream docstring)."""
    if isinstance(data, dict):
        if "properties" in data and not skip:
            if "additionalProperties" not in data:
                data["additionalProperties"] = False
        for _, v in data.items():
            additional_properties(v)
    return data


def replace_int_or_string(data):
    new = {}
    try:
        for k, v in iter(data.items()):
            new_v = v
            if isinstance(v, dict):
                if "format" in v and v["format"] == "int-or-string":
                    new_v = {"oneOf": [{"type": "string"}, {"type": "integer"}]}
                else:
                    new_v = replace_int_or_string(v)
            elif isinstance(v, list):
                new_v = [replace_int_or_string(x) for x in v]
            new[k] = new_v
        return new
    except AttributeError:
        return data


def schemas_for(crd):
    spec = crd.get("spec") or {}
    group = spec["group"]
    kind = spec["names"]["kind"].lower()
    for version in spec.get("versions") or []:
        schema = (version.get("schema") or {}).get("openAPIV3Schema")
        if schema is None:
            schema = (spec.get("validation") or {}).get("openAPIV3Schema")
        if schema is None:
            continue
        yield group, kind, version["name"], schema


def main(argv):
    if len(argv) != 2:
        print("usage: crd2jsonschema.py OUT_DIR  (CRDs as JSON lines on stdin)", file=sys.stderr)
        return 1
    out = argv[1]
    written = 0
    for lineno, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line or line == "null":
            continue
        try:
            doc = json.loads(line)
        except json.JSONDecodeError as exc:
            print(f"crd2jsonschema: line {lineno}: not JSON ({exc})", file=sys.stderr)
            return 1
        docs = doc.get("items", [doc]) if isinstance(doc, dict) else []
        for crd in docs:
            if not isinstance(crd, dict) or crd.get("kind") != "CustomResourceDefinition":
                continue
            for group, kind, version, schema in schemas_for(crd):
                schema = additional_properties(schema, skip=True)
                schema = replace_int_or_string(schema)
                path = os.path.join(out, group, f"{kind}_{version}.json")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(schema, fh, sort_keys=True, separators=(",", ":"))
                    fh.write("\n")
                written += 1
    if not written:
        print("crd2jsonschema: no CustomResourceDefinition with an openAPIV3Schema on stdin", file=sys.stderr)
        return 1
    print(f"crd2jsonschema: wrote {written} schema(s) under {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
