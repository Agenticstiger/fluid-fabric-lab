# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Subprocess helpers: list argv only, never a shell.

Every external call in the launcher goes through `run`. It refuses a string
argv, so `shell=True`-style command strings cannot creep in, and it turns
"tool not installed" and "tool hung" into results instead of exceptions so
callers can report them as failures.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence

# An address nothing listens on (TCP "discard" port on loopback). Pointing every
# proxy variable at it makes any HTTP(S) attempt by a Go or Python tool fail
# fast instead of silently using the network, which is how `render` and `l0`
# prove they work offline. Go's ProxyFromEnvironment and Python's urllib both
# honour these variables.
OFFLINE_PROXY = "http://127.0.0.1:9"
_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
_NO_PROXY_VARS = ("NO_PROXY", "no_proxy")


@dataclass
class Result:
    argv: list
    returncode: int
    stdout: str = ""
    stderr: str = ""
    missing: bool = False  # executable not found
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.missing and not self.timed_out

    def output_tail(self, limit: int = 4000) -> str:
        text = (self.stdout or "") + (self.stderr or "")
        return text[-limit:]


Runner = Callable[..., Result]


def run(
    argv: Sequence[str],
    *,
    cwd: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    timeout: Optional[float] = None,
    input: Optional[str] = None,  # noqa: A002 - mirrors subprocess.run
) -> Result:
    """Run `argv` (a list of strings) without a shell and capture its output.

    `input` is written to the child's stdin. Secrets travel this way (for
    example `kubectl apply -f -`), never on the command line, where `ps` and
    shell history would show them.
    """
    if isinstance(argv, (str, bytes)) or not all(isinstance(a, str) for a in argv):
        raise TypeError("argv must be a list of strings; command strings are not accepted")
    args = list(argv)
    if not args:
        raise ValueError("argv is empty")
    try:
        done = subprocess.run(  # noqa: S603 - list argv, shell=False
            args,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            input=input,
            stdin=None if input is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            timeout=timeout,
            shell=False,
            check=False,
        )
    except FileNotFoundError:
        return Result(args, 127, "", f"{args[0]}: not found", missing=True)
    except PermissionError as exc:
        return Result(args, 126, "", f"{args[0]}: {exc}")
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        err = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return Result(args, 124, out, err + f"\n{args[0]}: timed out after {timeout}s", timed_out=True)
    return Result(args, done.returncode, done.stdout, done.stderr)


def which(name: str) -> Optional[str]:
    return shutil.which(name)


def offline_env(base: Optional[Mapping[str, str]] = None, **extra: str) -> dict:
    """Copy `base` (default: os.environ) with every proxy variable poisoned."""
    env = dict(os.environ if base is None else base)
    for var in _PROXY_VARS:
        env[var] = OFFLINE_PROXY
    for var in _NO_PROXY_VARS:
        env[var] = ""
    env.update(extra)
    return env
