# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab l0`: run the L0 gates in tools/l0/ as subprocesses.

The gates live in tools/l0/; this module only finds and runs them.

Gate contract (what this runner assumes):
- A gate is a top-level file in tools/l0/ that is either a Python script with
  an `if __name__ == "__main__":` block, or an executable without extension or
  with `.sh`. Names starting with `_`, `.` or `test_`, aggregator scripts
  (run_all.py, all.py, main.py, __main__.py), directories and documentation or
  configuration files (*.md, *.conf, *.toml, *.json, *.txt) are not gates.
- Every gate is registered in tools/l0/gates.conf (one name per line). A file
  that looks like a gate but is unregistered, a registered gate that is
  missing, a .py whose main guard is not the standard form (so it would
  silently never run) and a non-executable .sh all FAIL: a check cannot be
  added, dropped or hidden without the registry changing in review.
- It is run with no arguments from the repository root: Python gates with this
  interpreter, others directly (no shell).
- Exit 0 means pass only when the gate also printed its summary line
  `gate <name>: PASS (N check(s))` with N >= 1 (tools/l0/_common.Report); a
  bare `sys.exit(0)` stub fails. Anything else means fail. A gate that is not
  implemented must exit non-zero.
- The environment carries LAB_ROOT, LAB_OFFLINE=1 and every proxy variable
  pointed at a closed loopback port, so a gate that needs the network fails
  (no L0 check needs the network).

A missing tools/l0/, an empty one, or a named gate that does not exist is a
failure, never a skip.
"""

from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from labctl import config, proc
from labctl.output import FAIL, PASS, check

AGGREGATORS = {"run_all.py", "run-all.py", "all.py", "main.py", "__main__.py"}
NOT_GATE_SUFFIXES = (".md", ".conf", ".toml", ".json", ".txt")
REGISTRY = "gates.conf"
DEFAULT_TIMEOUT_S = 900
_MAIN_GUARD = re.compile(r"""^if\s+__name__\s*==\s*['"]__main__['"]\s*:""", re.M)


@dataclass
class Gate:
    name: str
    path: Path
    argv: list


def discover(l0_dir: Path, python: str = sys.executable) -> list:
    return inventory(l0_dir, python)[0]


def read_registry(l0_dir: Path) -> Optional[list]:
    f = l0_dir / REGISTRY
    if not f.is_file():
        return None
    names = []
    for line in f.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.append(line)
    return names


def inventory(l0_dir: Path, python: str = sys.executable) -> tuple:
    """(gates, problems). Problems are files that would silently not run, and
    disagreements with the registry (tools/l0/gates.conf)."""
    gates, problems = [], []
    for p in sorted(l0_dir.iterdir()):
        if p.is_dir() or p.name.startswith(("_", ".", "test_")) or p.name in AGGREGATORS \
                or p.name.endswith(NOT_GATE_SUFFIXES):
            continue
        if p.suffix == ".py":
            try:
                text = p.read_text(errors="replace")
            except OSError as exc:
                problems.append(f"{p.name}: unreadable ({exc})")
                continue
            if _MAIN_GUARD.search(text):
                gates.append(Gate(p.stem, p, [python, str(p)]))
            elif "__main__" in text:
                problems.append(f"{p.name}: has a non-standard __main__ guard, so it would never be run; "
                                'write `if __name__ == "__main__":`')
            else:
                problems.append(f"{p.name}: neither a gate (no __main__ block) nor a helper (helpers start with _)")
        elif p.suffix in ("", ".sh"):
            if os.access(p, os.X_OK):
                gates.append(Gate(p.stem, p, [str(p)]))
            else:
                problems.append(f"{p.name}: not executable, so it would never be run (chmod +x, or rename _{p.name})")
        else:
            problems.append(f"{p.name}: unexpected file in {config.L0_DIR}/ (not a gate, helper or config)")
    registered = read_registry(l0_dir)
    if registered is None:
        problems.append(f"{config.L0_DIR}/{REGISTRY} is missing: the registry of gates is required")
    else:
        found = {g.name for g in gates}
        for g in sorted(found - set(registered)):
            problems.append(f"gate {g} is not registered in {config.L0_DIR}/{REGISTRY} "
                            "(register it, with selftest cases and a CI leg)")
        for g in sorted(set(registered) - found):
            problems.append(f"{config.L0_DIR}/{REGISTRY} lists {g}, but no runnable gate {g} exists")
    return gates, problems


_SUMMARY_RE = re.compile(r"^gate (\S+): PASS \(([0-9]+) check")


def printed_pass_summary(name: str, output: str) -> bool:
    for line in output.splitlines():
        m = _SUMMARY_RE.match(line.strip())
        if m and m.group(1) == name and int(m.group(2)) >= 1:
            return True
    return False


def run_l0(
    root: Path,
    names: Optional[list] = None,
    *,
    list_only: bool = False,
    timeout: float = DEFAULT_TIMEOUT_S,
    runner: proc.Runner = proc.run,
    python: str = sys.executable,
) -> dict:
    checks: list = []
    doc = {"command": "l0", "ok": False, "checks": checks, "gates": []}
    l0_dir = root / config.L0_DIR
    if not l0_dir.is_dir():
        checks.append(check("l0", FAIL, f"{config.L0_DIR}/ not present",
                            "The gates live there; nothing was checked, so this is a failure."))
        return doc
    gates, problems = inventory(l0_dir, python)
    doc["gates"] = [g.name for g in gates]
    if not gates:
        checks.append(check("l0", FAIL, f"no gates found in {config.L0_DIR}/",
                            "A gate is a Python script with a __main__ block, or an executable."))
        return doc
    if problems:
        checks.append(check("l0-inventory", FAIL, f"{len(problems)} problem(s) in {config.L0_DIR}/",
                            "; ".join(problems)[:600], {"problems": problems}))
        if not names:
            return doc
    if names:
        by_name = {g.name: g for g in gates}
        unknown = [n for n in names if n not in by_name]
        if unknown:
            checks.append(check("l0", FAIL, f"unknown gate(s): {', '.join(unknown)}",
                                f"Available: {', '.join(by_name)}"))
            return doc
        gates = [by_name[n] for n in names]
    if list_only:
        checks.append(check("l0", PASS, f"{len(gates)} gate(s): {', '.join(g.name for g in gates)}"))
        doc["ok"] = True
        return doc

    env = proc.offline_env(None, LAB_ROOT=str(root), LAB_OFFLINE="1")
    for g in gates:
        started = time.monotonic()
        res = runner(g.argv, cwd=str(root), env=env, timeout=timeout)
        secs = round(time.monotonic() - started, 2)
        detail = {"argv": g.argv, "exit": res.returncode, "seconds": secs}
        if res.ok and not printed_pass_summary(g.name, res.stdout or ""):
            detail["output_tail"] = res.output_tail()
            checks.append(check(f"gate:{g.name}", FAIL, f"exit 0 in {secs}s but no `gate {g.name}: PASS (N check(s))` "
                                "summary with N >= 1: a gate that checked nothing has not passed", detail=detail))
        elif res.ok:
            checks.append(check(f"gate:{g.name}", PASS, f"exit 0 in {secs}s", detail=detail))
        else:
            why = "timed out" if res.timed_out else f"exit {res.returncode}"
            detail["output_tail"] = res.output_tail()
            last = next((ln for ln in reversed(res.output_tail().splitlines()) if ln.strip()), "")
            checks.append(check(f"gate:{g.name}", FAIL, f"{why}" + (f": {last.strip()[:200]}" if last else ""),
                                detail=detail))
    doc["ok"] = all(c["status"] == PASS for c in checks)
    return doc
