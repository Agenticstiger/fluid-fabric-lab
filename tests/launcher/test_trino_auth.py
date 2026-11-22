# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Trino JWT authentication and the first OPA policy: the realm client and its
audience scope, Trino's configuration, the grants' shape and the policy that
reads them, Lakekeeper's grants for the trino client, `lab-dbt`, and the
`b-jwt` record (judged strictly, with its positive controls).

Rego is not evaluated here (no OPA in the tests); the grants and the policy's
structure are."""

import importlib.machinery
import importlib.util
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT

from labctl import bootstrap, check, config, credentials, http

import test_check
from test_check import FakeClient, make_ctx

PLATFORM = REPO_ROOT / "platform"
REALM = config.load_yaml(PLATFORM / "identity/realm-import.yaml")["objects"]["30-realm-import"]["spec"]
TRINO = config.load_yaml(PLATFORM / "components/trino/values.yaml")
GRANTS = json.loads((PLATFORM / "policy/trino-lab/data.json").read_text())
REGO = (PLATFORM / "policy/trino-lab/lab.rego").read_text()
LEVELS = {"read", "write"}


def load_lab_dbt():
    path = str(PLATFORM / "dbt/lab-dbt")
    loader = importlib.machinery.SourceFileLoader("lab_dbt", path)
    spec = importlib.util.spec_from_loader("lab_dbt", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


class Realm(unittest.TestCase):
    def client(self, name):
        return next(c for c in REALM["realm"]["clients"] if c["clientId"] == name)

    def test_fluid_pipeline_is_a_confidential_service_account_with_client_credentials_only(self):
        c = self.client("fluid-pipeline")
        self.assertFalse(c["publicClient"])
        self.assertTrue(c["serviceAccountsEnabled"])
        for flow in ("standardFlowEnabled", "directAccessGrantsEnabled", "implicitFlowEnabled"):
            self.assertFalse(c[flow], flow)
        self.assertEqual(c["secret"], "${FLUID_PIPELINE_CLIENT_SECRET}")
        self.assertEqual(c["optionalClientScopes"], ["trino"])   # asked for by name; no other audience
        self.assertNotIn("lakekeeper", c["defaultClientScopes"] + c["optionalClientScopes"])

    def test_the_secret_placeholder_reads_the_credential_lab_up_writes(self):
        placeholder = REALM["placeholders"]["FLUID_PIPELINE_CLIENT_SECRET"]["secret"]
        self.assertEqual(placeholder, {"name": "keycloak-realm-clients", "key": "fluid-pipeline"})
        self.assertIn("fluid-pipeline", credentials.CLIENTS)

    def test_scope_trino_puts_aud_trino_in_the_access_token(self):
        scope = next(s for s in REALM["realm"]["clientScopes"] if s["name"] == "trino")
        (mapper,) = scope["protocolMappers"]
        self.assertEqual(mapper["protocolMapper"], "oidc-audience-mapper")
        self.assertEqual(mapper["config"]["included.client.audience"], "trino")
        self.assertEqual(mapper["config"]["access.token.claim"], "true")
        # Only fluid-pipeline may ask for it: the scope is never a realm default.
        self.assertNotIn("trino", REALM["realm"]["defaultOptionalClientScopes"])
        self.assertNotIn("trino", REALM["realm"].get("defaultDefaultClientScopes", []))

    def test_no_other_client_is_given_the_trino_scope(self):
        for c in REALM["realm"]["clients"]:
            if c["clientId"] != "fluid-pipeline":
                self.assertNotIn("trino", c.get("defaultClientScopes", []) + c.get("optionalClientScopes", []))


class TrinoConfig(unittest.TestCase):
    PROPS = TRINO["additionalConfigProperties"]

    def test_jwt_over_https_with_keycloaks_keys_issuer_audience_and_client_principal(self):
        self.assertEqual(TRINO["server"]["config"]["authenticationType"], "JWT")
        self.assertTrue(TRINO["server"]["config"]["https"]["enabled"])
        want = {
            "http-server.authentication.jwt.key-file":
                "https://keycloak.lab.test/realms/fluid/protocol/openid-connect/certs",
            "http-server.authentication.jwt.required-issuer": "https://keycloak.lab.test/realms/fluid",
            "http-server.authentication.jwt.required-audience": "trino",
            "http-server.authentication.jwt.principal-field": "azp",
        }
        have = dict(p.split("=", 1) for p in self.PROPS)
        for key, value in want.items():
            self.assertEqual(have.get(key), value, key)

    def test_the_shared_secret_and_keystore_password_come_from_the_environment(self):
        text = "\n".join(self.PROPS)
        self.assertIn("internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}", text)
        self.assertIn("http-server.https.keystore.key=${ENV:TRINO_KEYSTORE_PASSWORD}", text)
        self.assertIn({"secretRef": {"name": "trino-server-secrets"}}, TRINO["envFrom"])
        self.assertNotIn("allow-insecure-over-http", text)   # plain HTTP never authenticates

    def test_opa_reads_the_grants_configmap_and_the_default_deny_policy_is_gone(self):
        self.assertEqual(TRINO["opa"]["policyConfigMap"], "trino-grants")
        self.assertNotIn("policy", TRINO["opa"])
        self.assertFalse((PLATFORM / "components/trino/values.yaml").read_text().count("lab-default-deny.rego") > 1)
        extras = (PLATFORM / "components/trino/lab-extras.yaml.gotmpl").read_text()
        self.assertIn("name: trino-grants", extras)
        self.assertIn("lab.rego", extras)
        self.assertIn("data.json", extras)
        self.assertIn("name: lab-ca, kind: ClusterIssuer", extras)

    def test_the_gateway_reaches_the_https_port_and_verifies_it_against_the_lab_ca(self):
        objects = config.load_yaml(PLATFORM / "components/lab-gateway/values.yaml")["objects"]
        self.assertEqual(objects["54-route-trino"]["spec"]["rules"][0]["backendRefs"][0]["port"], 8443)
        policy = objects["55-backendtls-trino"]["spec"]["validation"]
        self.assertEqual(policy["hostname"], "trino.trino.svc")
        self.assertEqual(policy["caCertificateRefs"][0]["name"], "lab-ca-bundle")

    def test_the_policy_files_survive_the_charts_tpl_and_helmfile_ships_the_extras(self):
        for f in ("lab.rego", "data.json"):
            self.assertNotIn("{{", (PLATFORM / "policy/trino-lab" / f).read_text(), f)
        self.assertIn("components/trino/lab-extras.yaml.gotmpl", (PLATFORM / "helmfile.yaml.gotmpl").read_text())


class Grants(unittest.TestCase):
    def test_the_shape_is_principal_catalog_schema_level(self):
        self.assertEqual(set(GRANTS), {"principals"})
        for principal, catalogs in GRANTS["principals"].items():
            self.assertTrue(principal)
            for catalog, schemas in catalogs.items():
                self.assertTrue(catalog)
                for schema, level in schemas.items():
                    self.assertTrue(schema)
                    self.assertIn(level, LEVELS, f"{principal}.{catalog}.{schema}")

    def test_fluid_pipeline_reads_raw_and_curated_and_writes_only_marts(self):
        grants = GRANTS["principals"]["fluid-pipeline"]["lakehouse"]
        self.assertEqual(grants, {"events_raw": "read", "events_curated": "read", "events_marts": "write"})
        self.assertNotIn("lab_check", grants)   # the schema `b-jwt` must see refused

    def test_the_check_refuses_a_schema_that_no_principal_may_read(self):
        denied = check.TRINO_DENIED.rsplit(".", 1)[-1]
        for catalogs in GRANTS["principals"].values():
            for schemas in catalogs.values():
                self.assertNotIn(denied, schemas)


class Policy(unittest.TestCase):
    def test_it_is_package_trino_default_deny_and_reads_the_grants_from_data(self):
        self.assertIn("\npackage trino\n", REGO)
        self.assertIn("default allow := false", REGO)
        self.assertIn("data.principals[principal]", REGO)
        self.assertNotIn("allow := true", REGO)
        self.assertTrue(REGO.startswith("# SPDX-FileCopyrightText"))

    def test_every_operation_dbt_trino_needs_is_decided_and_nothing_else(self):
        decided = {"ExecuteQuery", "ExecuteFunction", "AccessCatalog", "ShowSchemas", "ShowTables", "ShowColumns",
                   "SelectFromColumns", "CreateSchema", "CreateTable", "DropTable", "InsertIntoTable", "RenameTable",
                   "FilterCatalogs", "FilterSchemas", "FilterTables", "FilterColumns"}
        import re
        named = set(re.findall(r'"([A-Z][A-Za-z]+)"', REGO))
        self.assertEqual(named, decided)
        for forbidden in ("GrantTablePrivilege", "ExecuteProcedure", "DeleteFromTable", "DropSchema", "CreateCatalog",
                          "ImpersonateUser", "WriteSystemInformation"):
            self.assertNotIn(forbidden, REGO)

    def test_writes_need_the_write_level_and_a_rename_needs_it_at_both_ends(self):
        self.assertIn('level(catalog, schema) == "write"', REGO)
        rename = REGO[REGO.index('op == "RenameTable"'):]
        self.assertIn("targetResource.table", rename.split("# ----")[0])
        self.assertEqual(rename.split("# ----")[0].count("can_write("), 2)

    def test_the_other_endpoints_exist_and_are_empty_for_now(self):
        for line in ("rowFilters := []", "columnMask := {}", "batchColumnMasks := []"):
            self.assertIn(line, REGO)
        self.assertIn("batch contains i if", REGO)
        for url in ("allow", "batch", "rowFilters", "columnMask", "batchColumnMasks"):
            self.assertIn(f"/v1/data/trino/{url}", TRINO["accessControl"]["properties"])


class RegoTests(unittest.TestCase):
    def test_opa_test_passes_on_the_policy(self):
        # No skip when opa is missing: it is a pinned CI tool (versions.lock.yaml tools: opa).
        import subprocess
        res = subprocess.run(["opa", "test", str(PLATFORM / "policy/trino-lab")], capture_output=True, text=True,
                             timeout=120)
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assertIn("PASS", res.stdout)

    def test_the_test_file_is_not_shipped_in_the_configmap(self):
        extras = (PLATFORM / "components/trino/lab-extras.yaml.gotmpl").read_text()
        self.assertNotIn("_test", extras)

    def test_the_lock_pins_opa_and_ci_installs_it(self):
        lock = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
        (opa,) = [t for t in lock["tools"] if t["name"] == "opa"]
        self.assertRegex(opa["linux_amd64"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertIn("releases/download/" + opa["version"] + "/", opa["checksum_url"])
        workflow = (REPO_ROOT / ".github/workflows/l0.yml").read_text()
        self.assertIn("opa test platform/policy/trino-lab", workflow)
        self.assertIn('opa) "$bin/opa" version', workflow)


class KeycloakReconcile(unittest.TestCase):
    """The realm import runs once; this brings an older lab to the same client and scope."""

    PASSWORD = "admin-pass-value"

    class Kube:
        def __init__(self, secret):
            self.secret = secret

        def get(self, kind, name=None, namespace=None, **_kw):
            return self.secret if (kind, name, namespace) == ("secret", "keycloak-initial-admin", "keycloak") else None

    def admin_secret(self):
        import base64
        enc = lambda v: base64.b64encode(v.encode()).decode()   # noqa: E731
        return {"data": {"username": enc("temp-admin"), "password": enc(self.PASSWORD)}}

    def fake(self, scopes=None, mappers=None, clients=None, linked=None, token_status=200):
        """A Keycloak admin API in memory; every call is recorded."""
        calls = []
        state = {"scopes": list(scopes or []), "mappers": list(mappers or []), "clients": list(clients or []),
                 "linked": list(linked or [])}

        class Client:
            creds = {"client.fluid-pipeline": "pipeline-secret-value"}

            def https(self, method, host, path, headers=None, body=None, form=None, **_kw):
                calls.append((method, path, headers, body, form))
                if path == "/realms/master/protocol/openid-connect/token":
                    ok = token_status == 200
                    return http.Response(token_status, json.dumps({"access_token": "admin-token"} if ok else {}).encode())
                assert (headers or {}).get("Authorization") == "Bearer admin-token", path
                base = "/admin/realms/fluid"
                reply = lambda status, doc=None: http.Response(status, json.dumps(doc if doc is not None else {}).encode())  # noqa: E731
                if (method, path) == ("GET", base + "/client-scopes"):
                    return reply(200, state["scopes"])
                if (method, path) == ("POST", base + "/client-scopes"):
                    state["scopes"].append({"id": "scope-1", "name": body["name"]})
                    return reply(201)
                if path == base + "/client-scopes/scope-1/protocol-mappers/models":
                    if method == "GET":
                        return reply(200, state["mappers"])
                    state["mappers"].append({"name": body["name"]})
                    return reply(201)
                if (method, path) == ("GET", base + "/clients?clientId=fluid-pipeline"):
                    return reply(200, state["clients"])
                if (method, path) == ("POST", base + "/clients"):
                    state["clients"].append({"id": "client-1", "clientId": body["clientId"]})
                    return reply(201)
                if path == base + "/clients/client-1/optional-client-scopes":
                    return reply(200, state["linked"])
                if (method, path) == ("PUT", base + "/clients/client-1/optional-client-scopes/scope-1"):
                    state["linked"].append({"id": "scope-1"})
                    return reply(204)
                return reply(599)

        return Client(), calls, state

    def run_it(self, **kw):
        client, calls, state = self.fake(**kw)
        message = bootstrap.keycloak_reconcile(client, self.Kube(self.admin_secret()))
        return message, calls, state

    def writes(self, calls):
        return [(m, p) for m, p, _h, _b, _f in calls if m in ("POST", "PUT") and "/admin/" in p]

    def test_a_lab_without_either_gets_both_and_the_link(self):
        message, calls, state = self.run_it()
        self.assertEqual(self.writes(calls), [
            ("POST", "/admin/realms/fluid/client-scopes"),
            ("POST", "/admin/realms/fluid/client-scopes/scope-1/protocol-mappers/models"),
            ("POST", "/admin/realms/fluid/clients"),
            ("PUT", "/admin/realms/fluid/clients/client-1/optional-client-scopes/scope-1")])
        self.assertIn("scope trino", message)
        mapper = next(b for m, p, _h, b, _f in calls if p.endswith("/protocol-mappers/models") and m == "POST")
        self.assertEqual(mapper["protocolMapper"], "oidc-audience-mapper")
        self.assertEqual(mapper["config"]["included.client.audience"], "trino")
        client = next(b for m, p, _h, b, _f in calls if (m, p) == ("POST", "/admin/realms/fluid/clients"))
        self.assertEqual((client["clientId"], client["publicClient"], client["serviceAccountsEnabled"]),
                         ("fluid-pipeline", False, True))
        self.assertFalse(client["standardFlowEnabled"] or client["directAccessGrantsEnabled"])
        self.assertEqual(client["secret"], "pipeline-secret-value")

    def test_a_second_run_writes_nothing(self):
        _m, _c, state = self.run_it()
        message, calls, _ = self.run_it(scopes=state["scopes"], mappers=state["mappers"], clients=state["clients"],
                                        linked=state["linked"])
        self.assertEqual(self.writes(calls), [])
        self.assertIn("already has", message)

    def test_each_step_is_guarded_on_its_own(self):
        _m, calls, _s = self.run_it(scopes=[{"id": "scope-1", "name": "trino"}], mappers=[{"name": bootstrap.TRINO_AUDIENCE_MAPPER}],
                                    clients=[{"id": "client-1", "clientId": "fluid-pipeline"}])
        self.assertEqual(self.writes(calls), [("PUT", "/admin/realms/fluid/clients/client-1/optional-client-scopes/scope-1")])
        _m, calls, _s = self.run_it(scopes=[{"id": "scope-1", "name": "trino"}], mappers=[],
                                    clients=[{"id": "client-1", "clientId": "fluid-pipeline"}], linked=[{"id": "scope-1"}])
        self.assertEqual(self.writes(calls), [("POST", "/admin/realms/fluid/client-scopes/scope-1/protocol-mappers/models")])

    def test_the_scope_is_never_made_a_realm_default(self):
        _m, calls, _s = self.run_it()
        self.assertFalse([p for _m2, p, *_ in calls if "default-" in p or "default-default" in p])

    def test_neither_the_admin_password_nor_a_token_reaches_the_result_or_an_error(self):
        message, _c, _s = self.run_it()
        for secret in (self.PASSWORD, "admin-token", "pipeline-secret-value"):
            self.assertNotIn(secret, message)
        client, _calls, _state = self.fake(token_status=401)
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.keycloak_reconcile(client, self.Kube(self.admin_secret()))
        for secret in (self.PASSWORD, "admin-token", "pipeline-secret-value"):
            self.assertNotIn(secret, str(caught.exception))

    def test_a_missing_admin_secret_or_credential_says_what_to_do(self):
        client, _c, _s = self.fake()
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.keycloak_reconcile(client, self.Kube(None))
        self.assertIn("keycloak-initial-admin", str(caught.exception))
        client.creds = {}
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.keycloak_reconcile(client, self.Kube(self.admin_secret()))

    def test_up_runs_it_before_the_trino_grant(self):
        text = (REPO_ROOT / "labctl/up.py").read_text()
        self.assertLess(text.index("keycloak_reconcile"), text.index("trino_grant"))


class Credentials(unittest.TestCase):
    def test_the_pipeline_secret_lands_in_spark_beside_the_spark_client_secret(self):
        creds, _ = credentials.generate({})
        objs = credentials.secrets_for(creds, {"spark", "trino", "keycloak"})
        by = {(o["metadata"]["namespace"], o["metadata"]["name"]): o["stringData"] for o in objs}
        self.assertEqual(by[("spark", "fluid-pipeline-client")],
                         {"FLUID_CLIENT_ID": "fluid-pipeline", "FLUID_CLIENT_SECRET": creds["client.fluid-pipeline"]})
        self.assertEqual(by[("keycloak", "keycloak-realm-clients")]["fluid-pipeline"], creds["client.fluid-pipeline"])
        server = by[("trino", "trino-server-secrets")]
        self.assertEqual(set(server), {"TRINO_SHARED_SECRET", "TRINO_KEYSTORE_PASSWORD"})
        self.assertNotEqual(server["TRINO_SHARED_SECRET"], server["TRINO_KEYSTORE_PASSWORD"])

    def test_the_certificate_is_sealed_with_the_password_the_secret_carries(self):
        extras = (PLATFORM / "components/trino/lab-extras.yaml.gotmpl").read_text()
        self.assertIn("passwordSecretRef: {name: trino-server-secrets, key: TRINO_KEYSTORE_PASSWORD}", extras)


class TrinoGrant(unittest.TestCase):
    def client(self, assignments):
        calls = []

        class Client:
            def wait_token(self, c):
                return f"t-{c}"

            def token(self, c, scope="lakekeeper"):
                return f"t-{c}"

            def https(self, method, host, path, headers=None, body=None, **_kw):
                calls.append((method, path, body))
                answers = {("POST", "/management/v1/user"): (201, {}),
                           ("GET", "/management/v1/whoami"): (200, {"id": "oidc~trino-sub"}),
                           ("GET", "/management/v1/warehouse"): (200, {"warehouses": [{"name": "sales-wh", "id": "w1"}]}),
                           ("GET", "/management/v1/permissions/warehouse/w1/assignments"):
                               (200, {"assignments": assignments}),
                           ("POST", "/management/v1/permissions/warehouse/w1/assignments"): (204, {})}
                status, payload = answers.get((method, path), (599, {}))
                return http.Response(status, json.dumps(payload).encode())

        return Client(), calls

    def test_the_four_grants_are_written_once_and_a_rerun_writes_nothing(self):
        client, calls = self.client([])
        self.assertIn("may describe, select, create, modify in sales-wh", bootstrap.trino_grant(client))
        self.assertEqual([b for m, p, b in calls if (m, p) == ("POST", "/management/v1/user")],
                         [{"update-if-exists": True}])
        (writes,) = [b for m, p, b in calls if p.endswith("/assignments") and m == "POST"]
        self.assertEqual([w["type"] for w in writes["writes"]], ["describe", "select", "create", "modify"])
        held = [{"type": t, "user": "oidc~trino-sub"} for t in bootstrap.TRINO_GRANTS]
        client, calls = self.client(held)
        self.assertIn("already granted", bootstrap.trino_grant(client))
        self.assertFalse([1 for m, p, _ in calls if p.endswith("/assignments") and m == "POST"])

    def test_only_missing_grants_are_written(self):
        client, calls = self.client([{"type": "select", "user": "oidc~trino-sub"}])
        bootstrap.trino_grant(client)
        (writes,) = [b for m, p, b in calls if p.endswith("/assignments") and m == "POST"]
        self.assertEqual([w["type"] for w in writes["writes"]], ["describe", "create", "modify"])

    def test_the_token_helper_sends_a_scope_only_when_asked(self):
        sent = []

        class Lab(bootstrap.LabClient):
            def https(self, method, host, path, **kw):
                sent.append(kw["form"])
                return http.Response(200, b'{"access_token": "x"}')

        lab = Lab(Path("ca"), 1, {"client.fluid-pipeline": "s"})
        lab.token("fluid-pipeline", scope="trino")
        lab.token("fluid-pipeline", scope=None)
        lab.token("fluid-pipeline")
        self.assertEqual([f.get("scope") for f in sent], ["trino", None, "lakekeeper"])


class LabDbt(unittest.TestCase):
    ENV = {"FLUID_CLIENT_ID": "fluid-pipeline", "FLUID_CLIENT_SECRET": "s3cr3t-value", "PATH": "/bin"}

    def setUp(self):
        self.dbt = load_lab_dbt()

    def opener(self, status=200, body=b'{"access_token": "jwt-value"}'):
        seen = []

        class Resp:
            def __init__(self):
                self.status = status

            def __enter__(self):
                return self

            def __exit__(self, *_a):
                return False

            def read(self):
                return body

        class Opener:
            def open(self, request, timeout=None):
                seen.append(request)
                if status != 200:
                    raise urllib.error.HTTPError(request.full_url, status, "x", {}, __import__("io").BytesIO(body))
                return Resp()

        return Opener(), seen

    def test_it_asks_keycloak_for_the_trino_scope_and_the_secret_stays_in_the_body(self):
        opener, seen = self.opener()
        self.assertEqual(self.dbt.fetch_token(self.ENV, opener), "jwt-value")
        (request,) = seen
        self.assertEqual(request.full_url, self.dbt.TOKEN_URL)
        self.assertIn(b"scope=trino", request.data)
        self.assertIn(b"grant_type=client_credentials", request.data)
        self.assertNotIn("s3cr3t-value", request.full_url)

    def test_it_execs_dbt_with_the_same_argv_the_token_for_the_child_only_and_no_secret(self):
        calls = []
        rc = self.dbt.main(["run", "--select", "events_hourly"], dict(self.ENV),
                           execvpe=lambda *a: calls.append(a), fetch=lambda _env: "jwt-value")
        self.assertEqual(rc, 0)
        ((file, argv, env),) = calls
        self.assertEqual((file, argv), ("dbt", ["dbt", "run", "--select", "events_hourly"]))
        self.assertEqual(env["DBT_ENV_SECRET_FLUID_TRINO_JWT"], "jwt-value")
        self.assertNotIn("FLUID_CLIENT_SECRET", env)
        self.assertNotIn("jwt-value", " ".join(argv))
        self.assertTrue(Path(env["DBT_PROFILES_DIR"]).samefile(PLATFORM / "dbt"))

    def test_a_refusal_or_missing_credentials_stop_without_exec_and_print_no_secret(self):
        import contextlib
        import io
        for env, fetch in (({}, None), (self.ENV, lambda e: self.dbt.fetch_token(e, self.opener(401, b'{"error": "unauthorized_client"}')[0]))):
            calls, err = [], io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = self.dbt.main(["run"], dict(env), execvpe=lambda *a: calls.append(a),
                                   **({"fetch": fetch} if fetch else {}))
            self.assertEqual(rc, 1)
            self.assertFalse(calls)
            self.assertNotIn("s3cr3t-value", err.getvalue())
        self.assertIn("unauthorized_client", err.getvalue())

    def test_a_token_url_that_is_not_https_is_refused_before_the_secret_is_sent(self):
        opener, seen = self.opener()
        env = dict(self.ENV, LAB_TOKEN_URL="http://keycloak.lab.test/realms/fluid/protocol/openid-connect/token")
        with self.assertRaises(self.dbt.TokenError) as caught:
            self.dbt.fetch_token(env, opener)
        self.assertIn("https", str(caught.exception))
        self.assertFalse(seen)

    def test_the_token_variable_is_one_dbt_scrubs_from_its_logs(self):
        self.assertTrue(self.dbt.child_env({}, "t")["DBT_ENV_SECRET_FLUID_TRINO_JWT"] == "t")
        self.assertTrue("DBT_ENV_SECRET_FLUID_TRINO_JWT".startswith("DBT_ENV_SECRET_"))

    def test_the_profile_is_the_events_pipeline_jwt_profile_for_the_marts_schema(self):
        text = (PLATFORM / "dbt/profiles.yml").read_text()
        profile = config.load_yaml(PLATFORM / "dbt/profiles.yml")["events_pipeline"]
        out = profile["outputs"][profile["target"]]
        self.assertEqual((profile["target"], out["method"], out["host"], out["port"], out["database"], out["schema"]),
                         ("local", "jwt", "trino.trino.svc", 8443, "lakehouse", "events_marts"))
        self.assertEqual(out["jwt_token"], "{{ env_var('DBT_ENV_SECRET_FLUID_TRINO_JWT') }}")
        self.assertEqual(out["cert"], "/etc/lab-ca/ca.crt")
        self.assertNotIn("s3cr3t", text)

    def test_the_configmap_ships_both_files_and_escapes_the_profiles_braces(self):
        listed = (PLATFORM / "components/lab-spark/values.yaml.gotmpl").read_text()
        for needle in ("name: lab-dbt", "dbt/lab-dbt", "dbt/profiles.yml", 'replace "{{" "{{ `{{` }}"'):
            self.assertIn(needle, listed)
        self.assertNotIn("{{", (PLATFORM / "dbt/lab-dbt").read_text())


class TrinoQuery(unittest.TestCase):
    """b-jwt against a scripted Trino: every answer is what Trino's REST protocol returns."""

    def trino(self, **over):
        """Scripts the Gateway's Trino. `over` changes one behaviour."""
        calls = []
        statements = {
            "SELECT 1": {"data": [[1]]},
            check.TRINO_SHOW_SCHEMAS: {"data": [["information_schema"], ["events_marts"]]},
            check.TRINO_RAW_SCHEMA: {"data": [["events"]]},
            check.TRINO_LIST_RELATIONS: {"data": []},
            check.TRINO_DENIED: over.get("denied", {"error": {
                "message": f"Access Denied: Cannot show tables of schema lakehouse.{check.NAMESPACE}",
                "errorName": "PERMISSION_DENIED"}}),
        }
        statements.update(over.get("statements", {}))
        state = {}

        class Client(FakeClient):
            def https(self, method, host, path, headers=None, body=None, **_kw):
                headers = headers or {}
                calls.append((method, host, path, headers.get("Authorization"), body))
                if host == "trino.lab.test" and method == "POST" and path == "/v1/statement":
                    auth = headers.get("Authorization")
                    if auth != "Bearer token-fluid-pipeline-trino":
                        return http.Response(over.get("anon_status", 401), b"", {"www-authenticate": over.get(
                            "challenge", 'Bearer realm="Trino", token_type="JWT"')})
                    state["sql"] = body
                    return http.Response(200, json.dumps({"id": "q", "nextUri": "https://trino.lab.test/v1/statement/executing/q/1/x",
                                                          "stats": {"state": "QUEUED"}}).encode())
                if method == "GET" and path.startswith("/v1/statement/executing/q/1"):
                    answer = statements[state["sql"]]
                    doc = {"id": "q", "stats": {"state": "FINISHED" if "error" not in answer else "FAILED"}}
                    doc.update(answer)
                    return http.Response(200, json.dumps(doc).encode())
                return http.Response(599, b"no fake answer")

            def token(self, client, scope="lakekeeper"):
                return f"token-{client}-{scope}" if scope else f"token-{client}-none"

        return Client([]), calls

    def run_check(self, **over):
        client, calls = self.trino(**over)
        with tempfile.TemporaryDirectory() as tmp:
            ctx = make_ctx(Path(tmp), client=client, creds={f"client.{check.PIPELINE}": "x"})
            return check.b_jwt(ctx), calls

    def fails(self, said, **over):
        with self.assertRaises(check.CheckError) as caught:
            self.run_check(**over)
        self.assertIn(said, str(caught.exception))

    def test_every_leg_passes_and_the_record_says_what_is_not_proven(self):
        message, calls = self.run_check()
        self.assertIn("SELECT 1 ran to FINISHED", message)
        self.assertIn("Access Denied", message)
        self.assertIn("b-oauth2", message)
        self.assertNotIn("token-", message)   # a token never reaches a record
        posts = [c for c in calls if c[0] == "POST" and c[2] == "/v1/statement"]
        auths = [c[3] for c in posts]
        self.assertIn(None, auths)                               # the request without a token
        self.assertIn("Bearer token-lab-probe-lakekeeper", auths)  # another client
        self.assertIn("Bearer token-fluid-pipeline-none", auths)   # no aud: trino
        self.assertTrue(all(c[1] == "trino.lab.test" for c in calls))

    def test_an_anonymous_request_that_is_not_trinos_401_fails(self):
        self.fails("not Trino's 401", anon_status=200)
        self.fails("not Trino's 401", anon_status=401, challenge="")
        self.fails("insecure", anon_status=403)

    def test_a_denied_schema_must_say_access_denied_not_merely_fail(self):
        self.fails("not with Access Denied", denied={"error": {"message": "Schema 'x' does not exist"}})
        self.fails("let a schema it has no grant for through", denied={"data": [["probe"]]})

    def test_the_granted_read_is_the_positive_control(self):
        self.fails("the deny below would prove nothing", statements={check.TRINO_SHOW_SCHEMAS: {"error": {"message": "Access Denied: x"}}})

    def test_select_1_must_finish_with_its_row(self):
        self.fails("did not run SELECT 1", statements={"SELECT 1": {"data": []}})

    def test_dbts_own_queries_must_not_be_refused(self):
        denied = {"error": {"message": "Access Denied: Cannot access catalog system", "errorName": "PERMISSION_DENIED"}}
        self.fails("OPA refused a query dbt-trino runs", statements={check.TRINO_LIST_RELATIONS: denied})
        self.fails("OPA refused a query dbt-trino runs", statements={check.TRINO_RAW_SCHEMA: denied})

    def test_a_missing_schema_is_fine_but_another_failure_is_not(self):
        missing = {"error": {"message": "Schema 'events_raw' does not exist", "errorName": "SCHEMA_NOT_FOUND"}}
        message, _calls = self.run_check(statements={check.TRINO_RAW_SCHEMA: missing})
        self.assertIn("list-relations", message)
        broken = {"error": {"message": "boom", "errorName": "GENERIC_INTERNAL_ERROR"}}
        self.fails("ended FAILED", statements={check.TRINO_LIST_RELATIONS: broken})

    def test_the_list_relations_query_is_the_one_dbt_trino_sends(self):
        sql = check.TRINO_LIST_RELATIONS
        self.assertIn("left join (select * from system.metadata.materialized_views", sql)
        self.assertIn("lakehouse.information_schema.tables", sql)

    def test_only_the_path_of_a_nexturi_is_followed_and_every_request_goes_to_trino(self):
        seen = []

        class Rewritten(FakeClient):
            def https(self, method, host, path, headers=None, **_kw):
                seen.append((method, host, path))
                if method == "POST":
                    return http.Response(200, json.dumps({"nextUri": "http://trino:8080/v1/statement/executing/q/1?x=y"}).encode())
                return http.Response(200, json.dumps({"data": [[1]], "stats": {"state": "FINISHED"}}).encode())

        out = check.trino_run(Rewritten([]), "tok", "SELECT 1", sleep=lambda _s: None)
        self.assertEqual(out["rows"], [[1]])
        self.assertEqual(seen[-1], ("GET", "trino.lab.test", "/v1/statement/executing/q/1?x=y"))
        self.assertEqual({h for _m, h, _p in seen}, {"trino.lab.test"})

    def test_a_lab_without_the_pipeline_credential_says_what_to_do(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = make_ctx(Path(tmp), client=FakeClient([]), creds={"x": "y"})
            with self.assertRaises(check.CheckError) as caught:
                check.b_jwt(ctx)
        self.assertIn("no credential for Keycloak client fluid-pipeline", str(caught.exception))

    def test_the_records_are_split_and_only_the_browser_half_is_not_yet(self):
        self.assertIn("b-jwt", check.CHECKS)
        self.assertIn("b-oauth2", check.NOT_YET)
        self.assertNotIn("b", check.CRITERIA)
        self.assertIn("lab_check", check.TRINO_DENIED)
        self.assertEqual(test_check.WAREHOUSE_ID[:2], "01")   # the shared fixture still loads


if __name__ == "__main__":
    unittest.main()
