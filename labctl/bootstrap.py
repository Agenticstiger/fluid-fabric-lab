# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""What `lab up` does once the charts run: the warehouse bucket, Lakekeeper's
bootstrap and its warehouse. Also the HTTPS client for lab names that `lab up`
(and later `lab check`) use from the host.

The client reaches the lab Gateway through `kubectl port-forward` to its
Service (labctl.kube.PortForward), never through the host port kind maps, and
verifies every answer against the lab CA with the real `*.lab.test` name.
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from labctl import credentials, http, kube

REALM = "fluid"
WAREHOUSE = "sales-wh"
TOKEN_WAIT_S = 300


class BootstrapError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


@dataclass
class LabClient:
    """HTTPS to `*.lab.test` names through an open Gateway port-forward."""

    cafile: Path
    port: int
    creds: dict

    def https(self, method: str, host: str, path: str, **kw) -> http.Response:
        return http.request(method, f"https://{host}{path}", cafile=str(self.cafile),
                            connect_to=("127.0.0.1", self.port), **kw)

    def token(self, client: str, scope: Optional[str] = "lakekeeper") -> str:
        """A client-credentials token for one realm client (scope `lakekeeper` unless told; None sends none)."""
        form = {"grant_type": "client_credentials", "client_id": client,
                "client_secret": self.creds[f"client.{client}"]}
        if scope:
            form["scope"] = scope
        resp = self.https("POST", "keycloak.lab.test", f"/realms/{REALM}/protocol/openid-connect/token", form=form)
        if resp.status != 200:
            raise BootstrapError(f"Keycloak refused a token for {client}: HTTP {resp.status} {resp.text(300)}",
                                 "Check the realm import: `kubectl -n keycloak get keycloakrealmimports`.")
        return resp.json()["access_token"]

    def wait_token(self, client: str, timeout_s: float = TOKEN_WAIT_S) -> str:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                return self.token(client)
            except (BootstrapError, http.HTTPError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(5)


def gateway(kubectl: kube.Kube) -> kube.PortForward:
    return kube.PortForward(kubectl, "envoy-gateway-system", "svc/lab-gateway", 443)


def bucket(kubectl: kube.Kube, name: str) -> str:
    """Create a SeaweedFS bucket through `weed shell` in the master pod (idempotent)."""
    pod = kubectl.first_pod("seaweedfs", "app.kubernetes.io/component=master")
    res = kubectl.exec("seaweedfs", pod, ["weed", "shell", "-master=localhost:9333",
                                          "-filer=seaweedfs-filer-client.seaweedfs:8888"],
                       input=f"s3.bucket.create -name {name}\nexit\n", timeout=120)
    out = res.output_tail(600)
    if not res.ok and "already exist" not in out:
        raise BootstrapError(f"bucket {name} could not be created: {out.strip()}")
    return f"bucket {name}"


def warehouse_bucket(kubectl: kube.Kube) -> str:
    return bucket(kubectl, credentials.WAREHOUSE_BUCKET)


def artifacts_bucket(kubectl: kube.Kube) -> str:
    """The spark tier's bucket for what forge uploads (runner, job.json); its identity reaches nothing else."""
    return bucket(kubectl, credentials.ARTIFACTS_BUCKET)


def _warehouse_body(creds: dict) -> dict:
    return {
        "warehouse-name": WAREHOUSE,
        "storage-profile": {
            "type": "s3", "bucket": credentials.WAREHOUSE_BUCKET, "key-prefix": WAREHOUSE,
            "endpoint": "https://s3.lab.test", "region": "us-east-1", "path-style-access": True,
            "flavor": "s3-compat", "sts-enabled": True, "sts-role-arn": credentials.STS_ROLE_ARN,
        },
        "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": creds["s3.lakekeeper.access"],
            "aws-secret-access-key": creds["s3.lakekeeper.secret"],
        },
        "delete-profile": {"type": "hard"},
    }


def lakekeeper(client: LabClient) -> str:
    """Bootstrap Lakekeeper (lab-admin becomes its server admin) and create the warehouse."""
    auth = {"Authorization": f"Bearer {client.wait_token('lab-admin')}"}
    info = client.https("GET", "lakekeeper.lab.test", "/management/v1/info", headers=auth)
    if info.status != 200:
        raise BootstrapError(f"Lakekeeper /management/v1/info: HTTP {info.status} {info.text(300)}")
    steps = []
    if not info.json().get("bootstrapped"):
        resp = client.https("POST", "lakekeeper.lab.test", "/management/v1/bootstrap", headers=auth,
                            body={"accept-terms-of-use": True, "is-operator": True})
        if resp.status not in (200, 204):
            raise BootstrapError(f"Lakekeeper bootstrap: HTTP {resp.status} {resp.text(400)}")
        steps.append("bootstrapped (server admin: lab-admin)")
    listing = client.https("GET", "lakekeeper.lab.test", "/management/v1/warehouse", headers=auth)
    if listing.status != 200:
        raise BootstrapError(f"Lakekeeper warehouse list: HTTP {listing.status} {listing.text(300)}")
    if WAREHOUSE in [w.get("name") for w in listing.json().get("warehouses") or []]:
        steps.append(f"warehouse {WAREHOUSE} present")
        return "Lakekeeper " + ", ".join(steps)
    resp = client.https("POST", "lakekeeper.lab.test", "/management/v1/warehouse", headers=auth,
                        body=_warehouse_body(client.creds), timeout=120)
    if resp.status not in (200, 201):
        raise BootstrapError(f"Lakekeeper refused warehouse {WAREHOUSE}: HTTP {resp.status} {resp.text(600)}",
                             "The storage check needs the bucket, the S3 route and STS on SeaweedFS.")
    steps.append(f"warehouse {WAREHOUSE} created (s3-compat, STS vending)")
    return "Lakekeeper " + ", ".join(steps)


PIPELINE_CLIENT = "fluid-pipeline"
TRINO_SCOPE = "trino"
TRINO_AUDIENCE_MAPPER = "Add trino Audience"
KEYCLOAK = "keycloak.lab.test"


def _admin_headers(client: LabClient, kubectl: kube.Kube) -> dict:
    """A master-realm admin token from Secret keycloak/keycloak-initial-admin (the operator makes it).

    Neither the password nor the token is returned, logged or put in an error message.
    """
    secret = kubectl.get("secret", "keycloak-initial-admin", "keycloak")
    data = (secret or {}).get("data") or {}
    try:
        user = base64.b64decode(data["username"]).decode()
        password = base64.b64decode(data["password"]).decode()
    except (KeyError, ValueError) as exc:
        raise BootstrapError("Secret keycloak/keycloak-initial-admin has no username and password",
                             "The Keycloak operator creates it with the Keycloak CR: "
                             "`kubectl -n keycloak get secret keycloak-initial-admin`.") from exc
    resp = client.https("POST", KEYCLOAK, "/realms/master/protocol/openid-connect/token",
                        form={"grant_type": "password", "client_id": "admin-cli", "username": user,
                              "password": password})
    token = resp.json().get("access_token") if resp.status == 200 else None
    if not token:
        raise BootstrapError(f"Keycloak refused a master-realm admin token: HTTP {resp.status}")
    return {"Authorization": f"Bearer {token}"}


def _admin(client: LabClient, headers: dict, method: str, path: str, body=None, ok=(200,)) -> http.Response:
    resp = client.https(method, KEYCLOAK, f"/admin/realms/{REALM}{path}", headers=headers, body=body)
    if resp.status not in ok:
        raise BootstrapError(f"Keycloak admin API {method} {path.split('?')[0]}: HTTP {resp.status} {resp.text(200)}")
    return resp


def _trino_scope_body() -> dict:
    return {"name": TRINO_SCOPE, "description": "Client of Trino (JWT audience)", "protocol": "openid-connect",
            "attributes": {"include.in.token.scope": "false", "display.on.consent.screen": "false"}}


def _audience_mapper() -> dict:
    return {"name": TRINO_AUDIENCE_MAPPER, "protocol": "openid-connect", "protocolMapper": "oidc-audience-mapper",
            "consentRequired": False,
            "config": {"included.client.audience": TRINO_SCOPE, "id.token.claim": "false",
                       "lightweight.claim": "false", "introspection.token.claim": "true",
                       "access.token.claim": "true", "userinfo.token.claim": "false"}}


def _pipeline_client_body(secret: str) -> dict:
    return {"clientId": PIPELINE_CLIENT, "name": PIPELINE_CLIENT, "enabled": True, "protocol": "openid-connect",
            "description": "A pipeline identity: dbt-trino through Trino (client credentials, scope trino)",
            "publicClient": False, "clientAuthenticatorType": "client-secret", "secret": secret,
            "serviceAccountsEnabled": True, "standardFlowEnabled": False, "directAccessGrantsEnabled": False,
            "implicitFlowEnabled": False,
            "attributes": {"client_credentials.use_refresh_token": "false", "use.refresh.tokens": "false",
                           "access.token.lifespan": "3600"}}


def keycloak_reconcile(client: LabClient, kubectl: kube.Kube) -> str:
    """Make realm `fluid` have client `fluid-pipeline` and scope `trino`, whatever the import did.

    The realm import runs once, so a lab created before this client existed never gets it.
    Each step is guarded by a GET and writes only what is missing; the scope is linked as an
    optional scope of `fluid-pipeline` only (never a realm default).
    """
    secret = client.creds.get(f"client.{PIPELINE_CLIENT}")
    if not secret:
        raise BootstrapError(f"no credential for Keycloak client {PIPELINE_CLIENT}", "Run `./lab up` again.")
    admin = _admin_headers(client, kubectl)
    did = []
    scopes = {s.get("name"): s.get("id") for s in _admin(client, admin, "GET", "/client-scopes").json() or []}
    if TRINO_SCOPE not in scopes:
        _admin(client, admin, "POST", "/client-scopes", _trino_scope_body(), ok=(201, 409))
        scopes = {s.get("name"): s.get("id") for s in _admin(client, admin, "GET", "/client-scopes").json() or []}
        did.append(f"scope {TRINO_SCOPE}")
    scope_id = scopes.get(TRINO_SCOPE)
    if not scope_id:
        raise BootstrapError(f"Keycloak has no client scope {TRINO_SCOPE} after creating it")
    mappers = _admin(client, admin, "GET", f"/client-scopes/{scope_id}/protocol-mappers/models").json() or []
    if TRINO_AUDIENCE_MAPPER not in [m.get("name") for m in mappers]:
        _admin(client, admin, "POST", f"/client-scopes/{scope_id}/protocol-mappers/models", _audience_mapper(),
               ok=(201, 409))
        did.append("its audience mapper")
    found = _admin(client, admin, "GET", f"/clients?clientId={PIPELINE_CLIENT}").json() or []
    if not found:
        _admin(client, admin, "POST", "/clients", _pipeline_client_body(secret), ok=(201, 409))
        found = _admin(client, admin, "GET", f"/clients?clientId={PIPELINE_CLIENT}").json() or []
        did.append(f"client {PIPELINE_CLIENT}")
    if not found:
        raise BootstrapError(f"Keycloak has no client {PIPELINE_CLIENT} after creating it")
    cid = found[0]["id"]
    linked = _admin(client, admin, "GET", f"/clients/{cid}/optional-client-scopes").json() or []
    if scope_id not in [s.get("id") for s in linked]:
        _admin(client, admin, "PUT", f"/clients/{cid}/optional-client-scopes/{scope_id}", ok=(200, 204))
        did.append(f"scope {TRINO_SCOPE} linked to {PIPELINE_CLIENT} only")
    return ("Keycloak " + (", ".join(did) + " added" if did else f"already has {PIPELINE_CLIENT} and scope {TRINO_SCOPE}"))


SPARK_CLIENT = "spark"
SPARK_GRANTS = ("create",)   # create namespaces, which it then owns; nothing else (measured enough, 2026-11-18)
TRINO_CLIENT = "trino"
# Trino is the trusted engine: Lakekeeper lets it do what OPA allows the user to do (docs/trino-auth.md).
TRINO_GRANTS = ("describe", "select", "create", "modify")


def warehouse_grant(client: LabClient, who: str, grants: tuple) -> tuple:
    """Provision realm client `who` in Lakekeeper and give it `grants` on the warehouse.

    Returns (user id, grants it was missing). Idempotent: `update-if-exists` so a
    rerun finds the user provisioned (409 UserAlreadyExists otherwise), and only
    grants not yet held are written.
    """
    token = {"Authorization": f"Bearer {client.wait_token(who)}"}
    resp = client.https("POST", "lakekeeper.lab.test", "/management/v1/user", headers=token,
                        body={"update-if-exists": True})
    if resp.status not in (200, 201):
        raise BootstrapError(f"Lakekeeper did not provision the {who} client: HTTP {resp.status} {resp.text(300)}")
    me = client.https("GET", "lakekeeper.lab.test", "/management/v1/whoami", headers=token)
    user = (me.json() or {}).get("id") if me.status == 200 else None
    if not user:
        raise BootstrapError(f"Lakekeeper /management/v1/whoami for {who}: HTTP {me.status} {me.text(300)}")
    admin = {"Authorization": f"Bearer {client.token('lab-admin')}"}
    listing = client.https("GET", "lakekeeper.lab.test", "/management/v1/warehouse", headers=admin)
    found = [w for w in (listing.json() or {}).get("warehouses") or [] if w.get("name") == WAREHOUSE] \
        if listing.status == 200 else []
    if not found:
        raise BootstrapError(f"warehouse {WAREHOUSE} not found (HTTP {listing.status}); the core bootstrap makes it")
    path = f"/management/v1/permissions/warehouse/{found[0]['id']}/assignments"
    have = client.https("GET", "lakekeeper.lab.test", path, headers=admin)
    if have.status != 200:
        raise BootstrapError(f"Lakekeeper warehouse assignments: HTTP {have.status} {have.text(300)}")
    held = {(a.get("type"), a.get("user")) for a in (have.json() or {}).get("assignments") or []}
    missing = [g for g in grants if (g, user) not in held]
    if missing:
        resp = client.https("POST", "lakekeeper.lab.test", path, headers=admin,
                            body={"writes": [{"type": g, "user": user} for g in missing]})
        if resp.status not in (200, 204):
            raise BootstrapError(f"Lakekeeper refused the {who} grants {missing}: HTTP {resp.status} {resp.text(300)}")
    return user, missing


def spark_grant(client: LabClient) -> str:
    """Provision the `spark` client in Lakekeeper and let it create namespaces in the warehouse.

    Nothing broader: on OpenFGA a namespace's creator owns it, so the spark tier
    can write its own tables and nothing else. `describe` is not needed: the
    smoke run passes on `create` alone (2026-11-18). Idempotent.
    """
    user, missing = warehouse_grant(client, SPARK_CLIENT, SPARK_GRANTS)
    return (f"spark client ({user}) may {' and '.join(SPARK_GRANTS)} in {WAREHOUSE}"
            + ("" if missing else " (already granted)"))


def trino_grant(client: LabClient) -> str:
    """Let the `trino` client describe, select, create and modify across the warehouse.

    Trino is the trusted engine: it reaches Lakekeeper as itself, and OPA
    (platform/components/trino, docs/trino-auth.md) decides per user what a
    query may do. Idempotent.
    """
    user, missing = warehouse_grant(client, TRINO_CLIENT, TRINO_GRANTS)
    return (f"trino client ({user}) may {', '.join(TRINO_GRANTS)} in {WAREHOUSE}"
            + ("" if missing else " (already granted)"))


def host_port_reaches_gateway(cafile: Path, port: int) -> Optional[str]:
    """None when the host port kind maps answers for *.lab.test; else why not."""
    try:
        resp = http.request("GET", f"https://keycloak.lab.test/realms/{REALM}/.well-known/openid-configuration",
                            cafile=str(cafile), connect_to=("127.0.0.1", port), timeout=10)
    except http.HTTPError as exc:
        return str(exc)
    return None if resp.status == 200 else f"HTTP {resp.status}"


def wait_keycloak(kubectl: kube.Kube, import_timeout_s: float = 600) -> None:
    """Keycloak Ready, then realm import `fluid` Done.

    The import is polled rather than `kubectl wait`ed so that a timeout can
    name the operator's last HasErrors message. HasErrors alone is not fatal:
    the operator also sets it while the import Job's Deployment is still
    starting ("Deployment not yet ready", seen 2026-11-18).
    """
    try:
        kubectl.wait("condition=Ready", "keycloak/keycloak", "keycloak", timeout_s=900)
    except kube.KubeError as exc:
        raise BootstrapError(f"Keycloak did not become ready: {exc}",
                             "`kubectl -n keycloak describe keycloak keycloak` shows why.") from exc
    deadline = time.monotonic() + import_timeout_s
    while True:
        found = kubectl.get("keycloakrealmimport", REALM, "keycloak") or {}
        conds = {c.get("type"): c for c in (found.get("status") or {}).get("conditions") or []}
        if (conds.get("Done") or {}).get("status") == "True":
            return
        if time.monotonic() > deadline:
            errors = conds.get("HasErrors") or {}
            last = f"; last error: {errors.get('message', '')[:300]}" if errors.get("status") == "True" else ""
            raise BootstrapError(f"the realm import did not finish within {import_timeout_s:.0f} s{last}",
                                 "`kubectl -n keycloak get keycloakrealmimport fluid -o yaml` shows its status.")
        time.sleep(5)
