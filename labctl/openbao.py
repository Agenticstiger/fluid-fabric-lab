# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""OpenBao beside the cluster: run it, initialise, unseal, configure, KV.

The container (`run_container`; server config platform/keys/openbao/config.hcl):
persistent Raft storage on a named volume, a TLS listener with a certificate
from the lab CA, a read-only root file system, no capabilities, 256 MiB, and a
host port bound to 127.0.0.1 only (an ephemeral one, so a local Vault or
OpenBao on 8200 never clashes). Dev mode is never used.

The launcher speaks the HTTP API (stdlib `labctl.http`, verified against the
lab CA), not the `bao` CLI: unseal keys and tokens then travel in request
bodies, never on a command line where `ps` would show them.

Lab-only convenience: unseal shares and the lab-admin password live in
`<state>/openbao/init.json` (0600). A production Key Service never keeps
unseal material next to the server (PRODUCTION-READINESS.md, Substitutes).
The root token is revoked as soon as `configure` has created the scoped
`lab-admin` login, so no root token survives `lab up`.

Pods reach OpenBao as `https://openbao.lab.test:8200`: CoreDNS rewrites that
name to the container's name on the lab's Docker network (`labctl/cluster.py`), so
chart values never depend on the lab name. (Lakekeeper's KV2 backend cannot
use it yet: platform/components/lakekeeper/values.yaml says why.)
"""

from __future__ import annotations

import secrets
import time
from pathlib import Path
from typing import Optional

from labctl import containers, http, mirror, proc
from labctl.state import LABEL, Lab, read_json, write_json_private

CONTAINER_PORT = 8200
STABLE_HOST = "openbao.lab.test"
SHARES, THRESHOLD = 3, 2
ADMIN_USER = "lab-admin"
KV_MOUNT = "kv"                 # lab secrets: kv/data/lab/<name>

ADMIN_POLICY = """
# lab-admin: what `lab up` needs after the root token is revoked. No wildcard
# over the whole server.
path "kv/data/lab/*"            { capabilities = ["create", "read", "update", "delete"] }
path "kv/metadata/lab/*"        { capabilities = ["read", "list", "delete"] }
path "sys/mounts"               { capabilities = ["read"] }
path "sys/mounts/kv"            { capabilities = ["create", "read", "update"] }
path "sys/mounts/transit"       { capabilities = ["create", "read", "update"] }
"""


class BaoError(Exception):
    pass


# --------------------------------------------------------------------------- container


def host_port(runner: proc.Runner, name: str) -> int:
    port = containers.host_port(runner, name, f"{CONTAINER_PORT}/tcp")
    if port is None:
        raise BaoError(f"{name} publishes no 127.0.0.1 port for {CONTAINER_PORT}")
    return port


def run_container(runner: proc.Runner, lab: Lab, lock: dict, tls_dir: Path, config_file: Path,
                  mirror_port: int = mirror.DEFAULT_PORT) -> str:
    """Start (or reuse) OpenBao. Returns a one-line description; raises BaoError."""
    entry = mirror.lock_image(lock, "openbao")
    image = mirror.host_digest_ref(entry, mirror_port)
    found = containers.inspect(runner, lab.openbao)
    if found is not None:
        if entry["digest"] not in containers.running_image(found):
            raise BaoError(f"container {lab.openbao} exists but does not run the pinned {entry['ref']}; "
                           f"remove it (`docker rm -f {lab.openbao}`; volume {lab.openbao_volume} is kept)")
        if not containers.is_running(found):
            res = runner(["docker", "start", lab.openbao], timeout=60)
            if not res.ok:
                raise BaoError(f"could not start {lab.openbao}: {res.output_tail(400).strip()}")
        return f"{lab.openbao} running ({entry['ref']})"
    # The image's own data directory /openbao/file belongs to its openbao user
    # (uid 100). A new, empty named volume mounted there inherits that
    # ownership, so the non-root server can write its Raft data without any
    # root helper.
    res = runner(["docker", "volume", "create", "--label", f"{LABEL}={lab.name}", lab.openbao_volume], timeout=60)
    if not res.ok:
        raise BaoError(f"could not create volume {lab.openbao_volume}: {res.output_tail(300).strip()}")
    argv = [
        "docker", "run", "-d", "--name", lab.openbao,
        "--label", f"{LABEL}={lab.name}",
        "--network", lab.network,
        "--restart", "unless-stopped",
        "-p", f"127.0.0.1::{CONTAINER_PORT}",
        "--read-only", "--tmpfs", "/tmp",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--memory", "256m",
        "-v", f"{lab.openbao_volume}:/openbao/file",
        "-v", f"{config_file}:/openbao/config/config.hcl:ro",
        "-v", f"{tls_dir}:/openbao/tls:ro",
        "--entrypoint", "bao",
        image, "server", "-config=/openbao/config/config.hcl",
    ]
    res = runner(argv, timeout=300)
    if not res.ok:
        raise BaoError(f"could not start {lab.openbao}: {res.output_tail(600).strip()}")
    return f"started {lab.openbao} ({entry['ref']})"


# --------------------------------------------------------------------------- API


class Bao:
    """A thin client for the OpenBao HTTP API on the host's loopback port."""

    def __init__(self, port: int, cafile: str, token: Optional[str] = None):
        self.port = port
        self.base = f"https://127.0.0.1:{port}"
        self.cafile = cafile
        self.token = token

    def call(self, method: str, path: str, body=None, token: Optional[str] = None,
             ok=(200, 204)) -> http.Response:
        headers = {}
        tok = token or self.token
        if tok:
            headers["X-Vault-Token"] = tok
        resp = http.request(method, self.base + path, cafile=self.cafile, headers=headers, body=body, timeout=30)
        if resp.status not in ok:
            raise BaoError(f"{method} {path}: HTTP {resp.status} {_errors(resp)}")
        return resp

    def health(self) -> dict:
        # These query flags make every state answer 200, so the body decides.
        resp = self.call("GET", "/v1/sys/health?standbyok=true&sealedcode=200&uninitcode=200")
        return resp.json()

    def wait_up(self, timeout: float = 90) -> dict:
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                return self.health()
            except (http.HTTPError, BaoError, ValueError) as exc:
                last = str(exc)
                time.sleep(2)
        raise BaoError(f"OpenBao did not answer within {timeout:.0f}s: {last}")

    def init(self) -> dict:
        return self.call("PUT", "/v1/sys/init", {"secret_shares": SHARES, "secret_threshold": THRESHOLD}).json()

    def unseal(self, keys: list) -> dict:
        state = self.call("GET", "/v1/sys/seal-status").json()
        for key in keys:
            if not state.get("sealed"):
                break
            state = self.call("PUT", "/v1/sys/unseal", {"key": key}).json()
        if state.get("sealed"):
            raise BaoError("OpenBao is still sealed after every stored share")
        return state

    def login(self, user: str, password: str) -> str:
        resp = self.call("POST", f"/v1/auth/userpass/login/{user}", {"password": password}, token="")
        token = (resp.json().get("auth") or {}).get("client_token")
        if not token:
            raise BaoError(f"login as {user} returned no token")
        return token

    def mounts(self) -> dict:
        data = self.call("GET", "/v1/sys/mounts").json()
        return data.get("data") or data

    def enable_kv2(self, path: str) -> bool:
        if f"{path}/" in self.mounts():
            return False
        self.call("POST", f"/v1/sys/mounts/{path}", {"type": "kv", "options": {"version": "2"}})
        return True

    def enable_userpass(self) -> bool:
        data = self.call("GET", "/v1/sys/auth").json()
        if "userpass/" in (data.get("data") or data):
            return False
        self.call("POST", "/v1/sys/auth/userpass", {"type": "userpass"})
        return True

    def put_policy(self, name: str, hcl: str) -> None:
        self.call("PUT", f"/v1/sys/policies/acl/{name}", {"policy": hcl})

    def put_user(self, name: str, password: str, policies: list) -> None:
        self.call("POST", f"/v1/auth/userpass/users/{name}",
                  {"password": password, "token_policies": policies, "token_ttl": "1h", "token_max_ttl": "24h"})

    def kv_get(self, mount: str, path: str) -> Optional[dict]:
        resp = _past_kv_upgrade(lambda: self.call("GET", f"/v1/{mount}/data/{path}", ok=(200, 404)))
        if resp.status == 404:
            return None
        return ((resp.json().get("data") or {}).get("data")) or {}

    def kv_put(self, mount: str, path: str, data: dict) -> None:
        _past_kv_upgrade(lambda: self.call("POST", f"/v1/{mount}/data/{path}", {"data": data}))

    def token_is_valid(self, token: str) -> bool:
        """True when `token` is live, False on 403 (revoked or unknown); other answers raise."""
        resp = self.call("GET", "/v1/auth/token/lookup-self", token=token, ok=(200, 403))
        return resp.status == 200

    def wait_active(self, timeout: float = 60) -> dict:
        """Wait until the node is unsealed AND active (a fresh Raft node elects itself first)."""
        deadline = time.monotonic() + timeout
        health: dict = {}
        while time.monotonic() < deadline:
            try:
                health = self.health()
            except (http.HTTPError, BaoError, ValueError):
                health = {}
            if health.get("initialized") and not health.get("sealed") and not health.get("standby"):
                return health
            time.sleep(1)
        raise BaoError(f"OpenBao did not become active within {timeout:.0f}s (last health: {health})")

    def revoke_self(self, token: str) -> None:
        self.call("POST", "/v1/auth/token/revoke-self", token=token)


def _past_kv_upgrade(call, timeout: float = 30) -> http.Response:
    """Run a KV v2 call, waiting out a mount's first-use upgrade.

    A KV v2 mount answers 400 "Upgrading from non-versioned to versioned data"
    for a moment after it is enabled (measured 2026-11-18 on a fresh OpenBao
    2.7.1). Only that answer is retried.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            return call()
        except BaoError as exc:
            if "Upgrading from non-versioned to versioned data" not in str(exc) or time.monotonic() > deadline:
                raise
            time.sleep(1)


def _errors(resp: http.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        body = None
    errs = body.get("errors") if isinstance(body, dict) else None
    return "; ".join(map(str, errs)) if errs else resp.text(300)


# --------------------------------------------------------------------------- lifecycle


def init_path(lab: Lab) -> Path:
    return lab.state / "openbao" / "init.json"


def bring_up(bao: Bao, lab: Lab) -> str:
    """Initialise once, unseal, configure, and leave `bao.token` as lab-admin.

    Idempotent and resumable. The unseal shares are written the moment `init`
    returns (losing them would lose the server). The root token is kept in the
    same 0600 record only until the lab-admin login exists and the root token is
    revoked, so a run that dies in between can finish the job on the next run
    instead of being locked out.
    """
    bao.wait_up()
    health = bao.health()
    record = read_json(init_path(lab))
    steps = []
    if not health.get("initialized"):
        if record is not None:
            raise BaoError(f"OpenBao in {lab.openbao_volume} is not initialised but {init_path(lab)} exists: "
                           "the volume was replaced. Move the old init.json away to start fresh.")
        out = bao.init()
        record = {
            "keys_b64": out["keys_base64"],
            "threshold": THRESHOLD,
            "admin_user": ADMIN_USER,
            "admin_password": secrets.token_urlsafe(32),
            "root_token_pending": out["root_token"],
            "note": "lab-only: unseal shares next to the server; never do this in production",
        }
        write_json_private(init_path(lab), record)
        steps.append("initialised (3 shares, threshold 2)")
    elif record is None:
        raise BaoError(f"OpenBao is initialised but {init_path(lab)} is missing; the lab cannot unseal it")
    if bao.health().get("sealed"):
        bao.unseal(record["keys_b64"])
        steps.append("unsealed from stored shares")
    bao.wait_active()
    if record.get("root_token_pending"):
        root = record["root_token_pending"]
        if bao.token_is_valid(root):
            _configure_root(bao, root, record)   # its own errors surface as they are
            bao.revoke_self(root)                # a failure keeps the token for the next run
        else:
            # An earlier run revoked it and died before rewriting the record.
            bao.login(record["admin_user"], record["admin_password"])
        record.pop("root_token_pending")
        write_json_private(init_path(lab), record)
        steps.append("lab-admin login created, root token revoked")
    bao.token = bao.login(record["admin_user"], record["admin_password"])
    _configure_admin(bao)
    steps.append("configured")
    return ", ".join(steps)


def open_existing(bao: Bao, lab: Lab) -> str:
    """Log in as lab-admin to an OpenBao `lab up` already set up; unseal it if it is sealed.

    Never initialises and never reconfigures: `lab check` reads with this, so a
    lab whose OpenBao was replaced fails instead of getting a new, empty one.
    """
    bao.wait_up()
    health = bao.health()
    record = read_json(init_path(lab))
    if not health.get("initialized") or record is None:
        raise BaoError(f"OpenBao {lab.openbao} is not set up the way `lab up` leaves it "
                       f"(initialised: {bool(health.get('initialized'))}, record {init_path(lab)}: "
                       f"{'present' if record else 'missing'}); run `lab up`")
    if record.get("root_token_pending"):
        raise BaoError(f"OpenBao {lab.openbao}'s setup did not finish (its root token is not revoked yet); "
                       "run `lab up` to finish it")
    step = "unsealed"
    if health.get("sealed"):
        bao.unseal(record["keys_b64"])
        step = "unsealed from stored shares"
    bao.wait_active()
    bao.token = bao.login(record["admin_user"], record["admin_password"])
    return step


def _configure_root(bao: Bao, root: str, record: dict) -> None:
    prev, bao.token = bao.token, root
    try:
        bao.enable_userpass()
        bao.put_policy("lab-admin", ADMIN_POLICY)
        bao.put_user(record["admin_user"], record["admin_password"], ["lab-admin"])
    finally:
        bao.token = prev


def _configure_admin(bao: Bao) -> None:
    """Everything after the root token is gone (idempotent)."""
    bao.enable_kv2(KV_MOUNT)
