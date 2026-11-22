# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Repository paths, YAML loading and tier resolution.

YAML is read by converting it to JSON with `yq` (mikefarah/yq, pinned in
versions.lock.yaml) rather than by a hand-written parser: the standard library
has no YAML reader and the lab already depends on yq (borrow before build).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from labctl import proc

DEFAULT_ROOT = Path(__file__).resolve().parent.parent

VERSIONS_LOCK = "versions.lock.yaml"
TIERS = "tiers.yaml"
CHARTS_DIR = "platform/charts"
HELMFILE = "platform/helmfile.yaml.gotmpl"
L0_DIR = "tools/l0"


class ConfigError(Exception):
    """A config file is missing, unreadable or malformed."""


def load_yaml(path: Path, runner: proc.Runner = proc.run) -> dict:
    """Parse a YAML file into a dict through `yq -o=json`."""
    if not path.is_file():
        raise ConfigError(f"{path} not found")
    res = runner(["yq", "-o=json", ".", str(path)], timeout=60)
    if res.missing:
        raise ConfigError(f"yq is required to read {path.name} and is not on PATH (pinned in {VERSIONS_LOCK})")
    if not res.ok:
        raise ConfigError(f"yq could not parse {path}: {res.output_tail(600).strip()}")
    try:
        data = json.loads(res.stdout or "null")
    except ValueError as exc:
        raise ConfigError(f"yq returned invalid JSON for {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a mapping at the top level")
    return data


@dataclass
class Tier:
    name: str
    memory_target_gib: float
    outside_cluster_gib: float
    releases: list = field(default_factory=list)
    requires: list = field(default_factory=list)
    description: str = ""


def tier_names(tiers_doc: dict) -> list:
    tiers = tiers_doc.get("tiers")
    return list(tiers) if isinstance(tiers, dict) else []


def resolve_tier(tiers_doc: dict, name: str) -> Tier:
    """Return tier `name` with the releases of everything it `requires` folded in.

    `memory_target_gib` already includes required tiers (tiers.yaml states the
    add-on targets as core + add-on), so it is taken as written.
    """
    tiers = tiers_doc.get("tiers")
    if not isinstance(tiers, dict) or not tiers:
        raise ConfigError(f"{TIERS} has no `tiers` mapping")
    if name not in tiers:
        raise ConfigError(f"unknown tier {name!r}; {TIERS} defines: {', '.join(tiers)}")

    releases: list = []

    def visit(tier_name: str, stack: tuple) -> None:
        if tier_name in stack:
            raise ConfigError(f"{TIERS}: `requires` cycle: {' -> '.join(stack + (tier_name,))}")
        spec = tiers.get(tier_name)
        if not isinstance(spec, dict):
            raise ConfigError(f"{TIERS}: tier {tier_name!r} (required by {stack[-1] if stack else '?'}) is not defined")
        for req in spec.get("requires") or []:
            visit(req, stack + (tier_name,))
        for rel in spec.get("releases") or []:
            if not isinstance(rel, str):
                raise ConfigError(f"{TIERS}: tier {tier_name!r} has a non-string release {rel!r}")
            if rel not in releases:
                releases.append(rel)

    visit(name, ())
    spec = tiers[name]
    target = spec.get("memory_target_gib")
    if not isinstance(target, (int, float)) or isinstance(target, bool) or target <= 0:
        raise ConfigError(f"{TIERS}: tier {name!r} needs a positive numeric memory_target_gib")
    outside = spec.get("outside_cluster_gib", 0) or 0
    if not isinstance(outside, (int, float)) or isinstance(outside, bool):
        raise ConfigError(f"{TIERS}: tier {name!r} outside_cluster_gib must be a number")
    if not releases:
        raise ConfigError(f"{TIERS}: tier {name!r} enables no releases")
    return Tier(
        name=name,
        memory_target_gib=float(target),
        outside_cluster_gib=float(outside),
        releases=releases,
        requires=list(spec.get("requires") or []),
        description=str(spec.get("description") or ""),
    )


def find_root(explicit: Optional[str]) -> Path:
    return Path(explicit).resolve() if explicit else DEFAULT_ROOT
