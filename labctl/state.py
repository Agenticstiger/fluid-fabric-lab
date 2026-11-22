# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Per-lab state on the host: the lab name, its state directory, private files.

One machine can hold several labs side by side. A lab NAME prefixes every
object the launcher creates (kind cluster, sibling containers, volumes) and
selects its state directory, so `lab down --name a` can never touch lab `b`.
The mirror registry is the one exception: it is a cache shared by every lab on
the machine (`labctl/mirror.py`).

The state directory holds the lab CA, the OpenBao init record and the
kubeconfig. It is created mode 0700 and every secret file in it mode 0600.
It lives outside the repository so nothing in it can be committed:
`$LAB_STATE_DIR` when set, else `$XDG_STATE_HOME/fluid-fabric-lab/<name>`,
else `~/.local/state/fluid-fabric-lab/<name>`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional

DEFAULT_NAME = "fluid-fabric-lab"
# Docker label on every container, volume and network object the lab creates.
# An Agentics-owned prefix: never fluid.io (README, "Naming").
LABEL = "com.agentics.fluid-fabric-lab.name"
_NAME = re.compile(r"^[a-z][a-z0-9-]{0,30}[a-z0-9]$")


class StateError(Exception):
    """The lab name or the state directory is unusable."""


# kind's own default cluster name: a lab of that name would act on a cluster
# the user made with plain `kind create cluster`.
RESERVED_NAMES = {"kind"}


def check_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME.match(name) or "--" in name:
        raise StateError(
            f"lab name {name!r} is not valid: use 2-32 characters, lowercase letters, digits "
            "and single hyphens, starting with a letter (it becomes a kind cluster and "
            "container name prefix)")
    if name in RESERVED_NAMES:
        raise StateError(f"lab name {name!r} is reserved: it is kind's default cluster name")
    return name


def state_root(env: Optional[Mapping[str, str]] = None) -> Path:
    env = os.environ if env is None else env
    if env.get("XDG_STATE_HOME"):
        return Path(env["XDG_STATE_HOME"]) / "fluid-fabric-lab"
    return Path(env.get("HOME") or Path.home()) / ".local" / "state" / "fluid-fabric-lab"


@dataclass(frozen=True)
class Lab:
    """Names and paths of one lab instance."""

    name: str
    root: Path        # repository root
    state: Path       # state directory (0700)

    @property
    def cluster(self) -> str:
        return self.name

    @property
    def context(self) -> str:
        return f"kind-{self.name}"

    @property
    def node(self) -> str:
        return f"{self.name}-control-plane"

    @property
    def kubeconfig(self) -> Path:
        return self.state / "kubeconfig"

    @property
    def openbao(self) -> str:
        return f"{self.name}-openbao"

    @property
    def openbao_volume(self) -> str:
        return f"{self.name}-openbao-data"

    @property
    def images_record(self) -> Path:
        """What `lab build` loaded into this lab's node (`lab down` drops it with the cluster)."""
        return self.state / "images.json"

    @property
    def network(self) -> str:
        """The lab's own Docker network (IPv4 only); kind, OpenBao and the mirror join it."""
        return f"{self.name}-net"


def lab_for(root: Path, name: str = DEFAULT_NAME, env: Optional[Mapping[str, str]] = None) -> Lab:
    env = os.environ if env is None else env
    name = check_name(name)
    if env.get("LAB_STATE_DIR"):
        state = Path(env["LAB_STATE_DIR"]).expanduser()
    else:
        state = state_root(env) / name
    state = state.resolve()
    if state == root.resolve() or root.resolve() in state.parents:
        raise StateError(f"state directory {state} is inside the repository; it holds keys and must stay outside")
    return Lab(name=name, root=root, state=state)


def ensure_dir(path: Path) -> Path:
    """Create `path` (and parents) and make it 0700."""
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def write_private(path: Path, text: str) -> Path:
    """Write `text` to `path` atomically, mode 0600 from the first byte.

    A temporary file next to the target is written, flushed to disk and then
    renamed over it, so a kill or a full disk can never leave a truncated
    file behind. That matters for openbao/init.json, the only copy of the
    unseal shares.
    """
    ensure_dir(path.parent)
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return path


def write_json_private(path: Path, data: object) -> Path:
    return write_private(path, json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Optional[dict]:
    """The JSON object in `path`, or None when the file does not exist."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    data = json.loads(text)
    if not isinstance(data, dict):
        raise StateError(f"{path} does not hold a JSON object")
    return data
