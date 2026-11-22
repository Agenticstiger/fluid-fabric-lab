# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Shared helpers for the launcher tests (standard library only).

Run: python3 -m unittest discover -s tests/launcher
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import os  # noqa: E402
import subprocess  # noqa: E402

from labctl import proc  # noqa: E402

# Unit tests must never reach the container runtime or a cluster: a test that
# calls a real `lab up` or `lab down` would create or delete real containers,
# volumes and state. Every launcher call goes through subprocess.run (proc.run),
# so refuse these tools there; tests use FakeRunner instead.
_FORBIDDEN = {"docker", "podman", "kind", "kubectl", "crane"}
_real_subprocess_run = subprocess.run


def _guarded_run(args, *a, **kw):
    argv = args if isinstance(args, (list, tuple)) else [args]
    if argv and os.path.basename(str(argv[0])) in _FORBIDDEN:
        raise AssertionError(f"unit tests must not run {argv[0]!r}; inject a FakeRunner")
    return _real_subprocess_run(args, *a, **kw)


subprocess.run = _guarded_run

TIERS_DOC = {
    "tiers_version": 1,
    "host_guidance": {"laptop_16gb": "core plus one add-on", "machine_32gb": "full"},
    "tiers": {
        "core": {"memory_target_gib": 8, "outside_cluster_gib": 0.25, "requires": [],
                 "releases": ["cert-manager", "trino"]},
        "streaming": {"addon": True, "memory_target_gib": 13, "requires": ["core"],
                      "releases": ["strimzi-kafka-operator"]},
        "full": {"memory_target_gib": 16, "requires": ["core", "streaming"],
                 "releases": ["cert-manager", "trino", "strimzi-kafka-operator"]},
    },
}

LOCK_DOC = {
    "lock_version": 1,
    "charts": [],
    "tools": [
        {"name": "helm", "version": "v3.22.0"},
        {"name": "helm-diff", "version": "v3.15.15"},
        {"name": "yq", "version": "v4.54.1"},
    ],
}

GIB = 1024**3


class FakeRunner:
    """Stands in for proc.run. Answers yq from in-memory docs and other argv
    prefixes from a table; records every call."""

    def __init__(self, docs=None, table=None):
        self.docs = docs or {}
        self.table = table or {}
        self.calls = []

    def __call__(self, argv, **kwargs):
        if isinstance(argv, str):
            raise TypeError("string argv")
        argv = list(argv)
        self.calls.append((argv, kwargs))
        if argv[0] == "yq" and "-o=json" in argv and "yq" not in self.table:
            name = Path(argv[-1]).name
            if name not in self.docs:
                return proc.Result(argv, 1, "", f"no fixture for {name}")
            return proc.Result(argv, 0, json.dumps(self.docs[name]), "")
        for n in range(len(argv), 0, -1):
            key = tuple(argv[:n])
            if key in self.table or (n == 1 and argv[0] in self.table):
                val = self.table.get(key, self.table.get(argv[0]))
                if callable(val):
                    return val(argv, **kwargs)
                rc, out = val
                if rc is None:
                    return proc.Result(argv, 127, "", "not found", missing=True)
                return proc.Result(argv, rc, out, "")
        return proc.Result(argv, 127, "", "not found", missing=True)


def make_root(tmp: Path, lock=None, tiers=None) -> tuple:
    """A minimal repo root with config files; returns (root, docs for FakeRunner)."""
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "versions.lock.yaml").write_text("# fixture\n")
    (tmp / "tiers.yaml").write_text("# fixture\n")
    return tmp, {"versions.lock.yaml": lock if lock is not None else LOCK_DOC,
                 "tiers.yaml": tiers if tiers is not None else TIERS_DOC}


def docker_info(mem_gib: float, server="29.4.1") -> str:
    return json.dumps({"ServerVersion": server, "OperatingSystem": "Docker Desktop", "NCPU": 8,
                       "MemTotal": int(mem_gib * GIB)})


class DiskUsage:
    def __init__(self, free_gib: float):
        self.free = int(free_gib * GIB)
        self.total = self.free * 2
        self.used = self.free


def tools_ok_table(extra=None) -> dict:
    table = {
        ("helm", "version", "--short"): (0, "v3.22.0+gabc\n"),
        ("helm", "plugin", "list"): (0, "NAME\tVERSION\tDESCRIPTION\ndiff\t3.15.15\tPreview helm upgrade\n"),
        ("yq", "--version"): (0, "yq (https://github.com/mikefarah/yq/) version v4.54.1\n"),
    }
    table.update(extra or {})
    return table
