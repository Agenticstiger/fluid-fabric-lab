# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Check records and their human / JSON rendering.

A check has exactly one of four statuses: pass, warn, fail or skip. Anything
the launcher cannot evaluate is a failure with a reason. "skip" is narrower:
only a record whose prerequisite the operator chose not to provide (an add-on
tier's licence token, say) is skipped, and its message says what was not
checked and why. A skip proves nothing and is never a pass: a run with a
skipped record does not pass (`passed`), so its command exits 1.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Optional, TextIO

PASS, WARN, FAIL, SKIP = "pass", "warn", "fail", "skip"
STATUSES = (PASS, WARN, FAIL, SKIP)


def check(cid: str, status: str, message: str, advice: str = "", detail: Optional[dict] = None) -> dict:
    if status not in STATUSES:
        raise ValueError(f"bad status {status!r}")
    rec: dict[str, Any] = {"id": cid, "status": status, "message": message}
    if advice:
        rec["advice"] = advice
    if detail:
        rec["detail"] = detail
    return rec


def passed(checks: list) -> bool:
    """Every record passed or warned: a failure or a skip means the run did not pass."""
    return all(c["status"] in (PASS, WARN) for c in checks)


def summarise(checks: list) -> dict:
    return {s: sum(1 for c in checks if c["status"] == s) for s in STATUSES}


def emit_json(doc: dict, stream: Optional[TextIO] = None) -> None:
    stream = stream or sys.stdout
    stream.write(json.dumps(doc, indent=2, sort_keys=False) + "\n")


def emit_human(title: str, checks: list, ok: bool, stream: Optional[TextIO] = None) -> None:
    stream = stream or sys.stdout
    stream.write(title + "\n")
    width = max((len(c["id"]) for c in checks), default=0)
    for c in checks:
        stream.write(f"  {c['status'].upper():4}  {c['id']:<{width}}  {c['message']}\n")
        if c.get("advice"):
            stream.write(f"        {'':<{width}}  -> {c['advice']}\n")
    counts = summarise(checks)
    verdict = "OK" if ok else ("INCOMPLETE" if counts[SKIP] and not counts[FAIL] else "FAIL")
    skipped = f", {counts[SKIP]} skipped" if counts[SKIP] else ""
    stream.write(f"Result: {verdict} ({counts[PASS]} passed, {counts[WARN]} warnings, {counts[FAIL]} failed"
                 f"{skipped})\n")
