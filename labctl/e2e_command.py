# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab e2e`: bring the e2e tier up and run its end-to-end product, in one command (docs/e2e.md).

This is the entry point another repository calls (for example from a `make lab-e2e` target). It runs, in order,
and stops at the first step that fails:

  mirror        `lab mirror --tier e2e` (skipped with --skip-mirror): every pinned image, by digest
  up            `lab up --tier e2e`: the cluster (created or reused), core, Kafka, the spark tier's
                objects and the e2e topic
  build x2      `lab build fluid-runner` and `lab build spark-iceberg`: forge's runner and the open
                Spark runtime, built on this machine (cached after the first run) and loaded into the node
  up again      `lab up --tier e2e`, now that both images are recorded: fluid-host starts on them
  check         `lab check --tier e2e --only ready,capacity,runner,e2e`: the lab is healthy, and the
                product ran through forge with exactly this run's rows (labctl/e2e.py)
  down          `lab down` (only with --down), keeping the mirror and the state directory

Its exit code is the check's: 0 only when every record passed. The other criteria of
`lab check` (a to j) are not this command's to judge; `lab check --tier e2e` runs them all.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from labctl import build, check, down, mirror, state, up
from labctl.output import FAIL, check as record

TIER = "e2e"
RECORDS = ("ready", "capacity", "runner", "e2e")
IMAGES = ("fluid-runner", "spark-iceberg")


def run(root: Path, *, name: str = state.DEFAULT_NAME, https_port: int = 443, mirror_port: int = 5001,
        mirror: bool = True, down_after: bool = False, log: Callable[[str], None] = lambda _m: None,
        steps=None) -> dict:
    """Every step of the module docstring; one doc whose checks are the steps' own records, in order."""
    steps = steps or Steps(root, name=name, https_port=https_port, mirror_port=mirror_port, log=log)
    checks: list = []
    doc = {"command": "e2e", "name": name, "tier": TIER, "ok": False, "checks": checks, "steps": []}

    def step(what: str, call) -> bool:
        log(f"lab e2e: {what} ...")
        try:
            sub = call()
        except state.StateError as exc:
            sub = {"ok": False, "checks": [record(f"e2e:{what}", FAIL, str(exc))]}
        checks.extend(sub.get("checks") or [])
        doc["steps"].append({"step": what, "ok": bool(sub.get("ok"))})
        if not sub.get("ok") and not sub.get("checks"):
            checks.append(record(f"e2e:{what}", FAIL, f"`lab {what}` failed and reported nothing"))
        return bool(sub.get("ok"))

    plan = ([("mirror", steps.mirror)] if mirror else []) + [("up", steps.up)]
    plan += [(f"build {image}", (lambda image=image: steps.build(image))) for image in IMAGES]
    plan += [("up", steps.up), ("check", steps.check)]
    ok = True
    for what, call in plan:
        if not step(what, call):
            ok = False
            break
    if down_after:
        step("down", steps.down)
    doc["ok"] = ok
    return doc


class Steps:
    """The launcher's own commands, called in-process with the same arguments a person would type."""

    def __init__(self, root: Path, *, name: str, https_port: int, mirror_port: int, log: Callable[[str], None]):
        self.root, self.name, self.https_port, self.mirror_port, self.log = root, name, https_port, mirror_port, log

    def mirror(self) -> dict:
        return mirror.run_mirror(self.root, TIER, port=self.mirror_port)

    def up(self) -> dict:
        return up.run_up(self.root, TIER, name=self.name, https_port=self.https_port, mirror_port=self.mirror_port,
                         log=self.log)

    def build(self, image: str) -> dict:
        return build.run_build(self.root, image, name=self.name, log=self.log)

    def check(self) -> dict:
        return check.run_check(self.root, TIER, name=self.name, log=self.log, only=list(RECORDS))

    def down(self) -> dict:
        return down.run_down(self.root, name=self.name, purge=False)
