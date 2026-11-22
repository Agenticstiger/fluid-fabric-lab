# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""A small JSON Schema (draft 2020-12) validator for the keyword SUBSET the lab's
profile schemas use. Standard library only. Not a gate.

Why not a library: the gates are stdlib-only and no JSON Schema CLI
(check-jsonschema, ajv) is installed in the toolchain. The schemas in
profiles/schema/ declare in their `$comment` that they keep to this subset so
a stdlib checker can evaluate them. The validator REFUSES any keyword outside
the subset (raising UnsupportedKeyword) instead of silently ignoring it, so a
schema that grows past the subset fails L0 loudly and can be moved to a real
validator then.

Supported: type, enum, const, pattern, minimum, maximum, minLength, maxLength,
properties, patternProperties, additionalProperties, propertyNames, required,
minProperties, maxProperties, items, minItems, maxItems, uniqueItems, oneOf,
anyOf, allOf, not, if/then/else, $ref (local "#/..." pointers only).
Annotations ignored: $schema, $id, $comment, $defs, title, description,
examples, default, deprecated, readOnly, writeOnly, and any x-* key.
Semantics follow the 2020-12 core and validation specs; regular expressions
use Python `re.search` (the schemas' patterns are ECMA-262-compatible).
"""

from __future__ import annotations

import re
from typing import Any

ANNOTATIONS = {"$schema", "$id", "$comment", "$defs", "definitions", "title", "description", "examples",
               "default", "deprecated", "readOnly", "writeOnly"}
SUPPORTED = {"type", "enum", "const", "pattern", "minimum", "maximum", "minLength", "maxLength", "properties",
             "patternProperties", "additionalProperties", "propertyNames", "required", "minProperties",
             "maxProperties", "items", "minItems", "maxItems", "uniqueItems", "oneOf", "anyOf", "allOf", "not",
             "if", "then", "else", "$ref"}


class UnsupportedKeyword(Exception):
    pass


def _type_ok(v: Any, t: str) -> bool:
    if t == "object":
        return isinstance(v, dict)
    if t == "array":
        return isinstance(v, list)
    if t == "string":
        return isinstance(v, str)
    if t == "boolean":
        return isinstance(v, bool)
    if t == "null":
        return v is None
    if t == "integer":
        return (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, float) and v.is_integer())
    if t == "number":
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    raise UnsupportedKeyword(f"type {t!r}")


def json_equal(a: Any, b: Any) -> bool:
    """JSON equality: booleans never equal numbers; 1 == 1.0."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(json_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(json_equal(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


class Validator:
    def __init__(self, schema: dict) -> None:
        self.root = schema
        self._re: dict = {}
        self.check_schema(schema, "#")

    def check_schema(self, s: Any, where: str) -> None:
        """Refuse keywords outside the subset anywhere in the schema."""
        if isinstance(s, bool):
            return
        if not isinstance(s, dict):
            raise UnsupportedKeyword(f"{where}: schema must be an object")
        for k, v in s.items():
            if k.startswith("x-") or k in ANNOTATIONS - {"$defs", "definitions"}:
                continue
            if k in ("$defs", "definitions"):
                for name, sub in v.items():
                    self.check_schema(sub, f"{where}/{k}/{name}")
                continue
            if k not in SUPPORTED:
                raise UnsupportedKeyword(f"{where}: keyword {k!r} is outside the supported subset")
            if k in ("properties", "patternProperties"):
                for name, sub in v.items():
                    self.check_schema(sub, f"{where}/{k}/{name}")
            elif k in ("additionalProperties", "propertyNames", "items", "not", "if", "then", "else"):
                self.check_schema(v, f"{where}/{k}")
            elif k in ("oneOf", "anyOf", "allOf"):
                for i, sub in enumerate(v):
                    self.check_schema(sub, f"{where}/{k}/{i}")
            elif k == "$ref" and not str(v).startswith("#"):
                raise UnsupportedKeyword(f"{where}: only local $ref is supported, got {v!r}")

    def _resolve(self, ref: str) -> Any:
        node: Any = self.root
        for part in ref.lstrip("#").split("/")[1:]:
            part = part.replace("~1", "/").replace("~0", "~")
            node = node[part]
        return node

    def _rx(self, p: str) -> re.Pattern:
        if p not in self._re:
            self._re[p] = re.compile(p)
        return self._re[p]

    def errors(self, inst: Any, schema: Any = None, path: str = "$") -> list:
        s = self.root if schema is None else schema
        if s is True:
            return []
        if s is False:
            return [f"{path}: not allowed here"]
        errs: list = []
        if "$ref" in s:
            errs += self.errors(inst, self._resolve(s["$ref"]), path)
        if "type" in s:
            types = s["type"] if isinstance(s["type"], list) else [s["type"]]
            if not any(_type_ok(inst, t) for t in types):
                return errs + [f"{path}: expected type {'/'.join(types)}, got {type(inst).__name__}"]
        if "enum" in s and not any(json_equal(inst, e) for e in s["enum"]):
            errs.append(f"{path}: {inst!r} is not one of {s['enum']}")
        if "const" in s and not json_equal(inst, s["const"]):
            errs.append(f"{path}: must be {s['const']!r}, got {inst!r}")
        if isinstance(inst, str):
            if "pattern" in s and not self._rx(s["pattern"]).search(inst):
                errs.append(f"{path}: {inst!r} does not match {s['pattern']}")
            if "minLength" in s and len(inst) < s["minLength"]:
                errs.append(f"{path}: shorter than {s['minLength']}")
            if "maxLength" in s and len(inst) > s["maxLength"]:
                errs.append(f"{path}: longer than {s['maxLength']}")
        if isinstance(inst, (int, float)) and not isinstance(inst, bool):
            if "minimum" in s and inst < s["minimum"]:
                errs.append(f"{path}: {inst} < minimum {s['minimum']}")
            if "maximum" in s and inst > s["maximum"]:
                errs.append(f"{path}: {inst} > maximum {s['maximum']}")
        if isinstance(inst, dict):
            props, pprops = s.get("properties") or {}, s.get("patternProperties") or {}
            for r in s.get("required") or []:
                if r not in inst:
                    errs.append(f"{path}: missing required property {r!r}")
            if "minProperties" in s and len(inst) < s["minProperties"]:
                errs.append(f"{path}: fewer than {s['minProperties']} properties")
            if "maxProperties" in s and len(inst) > s["maxProperties"]:
                errs.append(f"{path}: more than {s['maxProperties']} properties")
            for k, v in inst.items():
                sub = f"{path}.{k}"
                if "propertyNames" in s:
                    errs += [f"{sub}: property name: {e.split(': ', 1)[-1]}" for e in
                             self.errors(k, s["propertyNames"], sub)]
                matched = False
                if k in props:
                    matched = True
                    errs += self.errors(v, props[k], sub)
                for pat, ps in pprops.items():
                    if self._rx(pat).search(k):
                        matched = True
                        errs += self.errors(v, ps, sub)
                if not matched and "additionalProperties" in s:
                    ap = s["additionalProperties"]
                    if ap is False:
                        errs.append(f"{path}: unknown property {k!r} (not allowed by the schema)")
                    else:
                        errs += self.errors(v, ap, sub)
        if isinstance(inst, list):
            if "minItems" in s and len(inst) < s["minItems"]:
                errs.append(f"{path}: fewer than {s['minItems']} items")
            if "maxItems" in s and len(inst) > s["maxItems"]:
                errs.append(f"{path}: more than {s['maxItems']} items")
            if s.get("uniqueItems"):
                for i in range(len(inst)):
                    if any(json_equal(inst[i], inst[j]) for j in range(i)):
                        errs.append(f"{path}[{i}]: duplicate item")
            if "items" in s:
                for i, v in enumerate(inst):
                    errs += self.errors(v, s["items"], f"{path}[{i}]")
        for sub in s.get("allOf") or []:
            errs += self.errors(inst, sub, path)
        if "anyOf" in s and not any(not self.errors(inst, sub, path) for sub in s["anyOf"]):
            errs.append(f"{path}: matches none of anyOf")
        if "oneOf" in s:
            results = [self.errors(inst, sub, path) for sub in s["oneOf"]]
            ok = sum(1 for r in results if not r)
            if ok != 1:
                closest = min(results, key=len) if results else []
                errs.append(f"{path}: must match exactly one of oneOf (matched {ok})" +
                            (f"; closest: {closest[0]}" if ok == 0 and closest else ""))
        if "not" in s and not self.errors(inst, s["not"], path):
            errs.append(f"{path}: matches a schema it must not match")
        if "if" in s:
            if not self.errors(inst, s["if"], path):
                if "then" in s:
                    errs += self.errors(inst, s["then"], path)
            elif "else" in s:
                errs += self.errors(inst, s["else"], path)
        return errs
