# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Sibling containers (the mirror registry, OpenBao): look one up by name."""

from __future__ import annotations

import json
import re
from typing import Optional

from labctl import proc


class DockerError(Exception):
    """Docker could not answer, so whether an object exists is unknown."""


# Docker's own "this object does not exist" answers (Docker 29.4.1, 2026-11-18):
#   No such container: x | No such object: x | get x: no such volume | network x not found
_NOT_FOUND = re.compile(r"no such (container|object|volume|network|image)\b|\bnetwork \S+ not found", re.IGNORECASE)
# ...and answers that only mean Docker could not be asked. A missing socket
# says "dial unix ...: no such file or directory", which must never read as
# "the object is gone".
_UNREACHABLE = re.compile(r"cannot connect to the docker daemon|error during connect|dial unix|"
                          r"is the docker daemon running", re.IGNORECASE)


def absent(res: proc.Result, name: str = "") -> bool:
    """True only when Docker answered that the object (`name`, when given) does not exist."""
    if res.ok or res.missing or res.timed_out:
        return False
    text = res.output_tail(600)
    if _UNREACHABLE.search(text) or not _NOT_FOUND.search(text):
        return False
    return not name or name in text


def inspect(runner: proc.Runner, name: str) -> Optional[dict]:
    """`docker inspect` of container `name`; None only when Docker says it does not exist.

    Any other failure (daemon down, timeout, CLI missing) raises DockerError:
    "could not look" must never be read as "not there".
    """
    res = runner(["docker", "inspect", "--type", "container", name], timeout=30)
    if absent(res, name):
        return None
    if not res.ok:
        raise DockerError(f"docker inspect {name}: {res.output_tail(300).strip() or 'no answer'}")
    try:
        data = json.loads(res.stdout)
    except ValueError as exc:
        raise DockerError(f"docker inspect {name}: unreadable answer ({exc})") from exc
    return data[0] if data else None


def running_image(found: Optional[dict]) -> str:
    return ((found or {}).get("Config") or {}).get("Image", "")


def is_running(found: Optional[dict]) -> bool:
    return bool(((found or {}).get("State") or {}).get("Running"))


def host_port(runner: proc.Runner, name: str, container_port: str) -> Optional[int]:
    """The 127.0.0.1 host port Docker published for `container_port` (e.g. "8200/tcp")."""
    res = runner(["docker", "port", name, container_port], timeout=30)
    for line in (res.stdout or "").splitlines():
        host, _, port = line.strip().rpartition(":")
        if host == "127.0.0.1" and port.isdigit():
            return int(port)
    return None
