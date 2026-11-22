# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab check`: every criterion yields exactly one record, an unproven one
FAILs with its reason, and the probes judge what the lab answers strictly."""

import json
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT, TIERS_DOC, FakeRunner

from labctl import check, cluster, config, down, http, openbao, proc, state
from labctl.output import FAIL

WAREHOUSE_ID = "01a1f000-4910-7611-b2a8-e8cb3b74e670"
CONFIG = {"overrides": {"uri": "https://lakekeeper.lab.test/catalog"}, "defaults": {"prefix": WAREHOUSE_ID}}
BASE = f"/catalog/v1/{WAREHOUSE_ID}/namespaces"
CATALOG_CM = {"metadata": {"name": "trino-catalog"}, "data": {
    "lakehouse.properties": "connector.name=iceberg\niceberg.rest-catalog.vended-credentials-enabled=true\n"}}


class FakeClient:
    """Stands in for bootstrap.LabClient: answers by (method, path prefix), records every call."""

    port = 4443

    def __init__(self, answers: list):
        self.answers = answers  # [(method, path prefix, (status, body) or callable(headers))]
        self.calls = []

    def token(self, client: str, scope="lakekeeper") -> str:
        return f"token-{client}"

    def https(self, method, host, path, headers=None, body=None, **_kw):
        self.calls.append((method, path, (headers or {}).get("Authorization"), body))
        for m, prefix, answer in self.answers:
            if m == method and path.startswith(prefix):
                status, payload = answer(headers or {}) if callable(answer) else answer
                return http.Response(status, json.dumps(payload).encode())
        return http.Response(599, b"no fake answer")


class FakeKube:
    def __init__(self, answers=None):
        self.answers = answers or {}

    def run(self, *args, **_kw):
        key = tuple(a for a in args if a not in ("-o", "json"))
        answer = self.answers.get(key, {"items": []})
        return proc.Result(list(args), 0, answer if isinstance(answer, str) else json.dumps(answer), "")


def make_ctx(tmp: Path, *, client=None, kube=None, creds=None) -> check.Ctx:
    root = tmp / "repo"
    root.mkdir(exist_ok=True)
    lab = state.lab_for(root, "ffl-x", {"LAB_STATE_DIR": str(tmp / "state")})
    ctx = check.Ctx(root, lab, FakeRunner(), kube or FakeKube(), {}, TIERS_DOC, "core",
                    config.resolve_tier(TIERS_DOC, "core"), creds=creds or {"s3.lakekeeper.access": "STATICKEY"})
    ctx.client = client
    return ctx


def pod(ns, name, *, phase="Running", ready=True, owner="ReplicaSet"):
    return {"metadata": {"namespace": ns, "name": name, "ownerReferences": [{"kind": owner}]},
            "status": {"phase": phase, "containerStatuses": [{"ready": ready}]}}


class Helpers(unittest.TestCase):
    def test_not_ready_counts_each_kind_in_the_tier_only(self):
        objects = [
            {"kind": "Deployment", "metadata": {"namespace": "trino", "name": "coord"}, "spec": {"replicas": 1},
             "status": {"readyReplicas": 1}},
            {"kind": "StatefulSet", "metadata": {"namespace": "lab-db", "name": "pg"}, "spec": {"replicas": 1},
             "status": {}},
            {"kind": "DaemonSet", "metadata": {"namespace": "trino", "name": "ds"},
             "status": {"desiredNumberScheduled": 1, "numberReady": 0}},
            {"kind": "Deployment", "metadata": {"namespace": "elsewhere", "name": "x"}, "spec": {"replicas": 1},
             "status": {}},
        ]
        self.assertEqual(check.not_ready(objects, {"trino", "lab-db"}),
                         ["lab-db/statefulset/pg 0/1", "trino/daemonset/ds 0/1"])

    def test_pods_cover_operator_made_workloads_jobs_and_empty_namespaces(self):
        nss = {"lab-db", "trino", "openfga"}
        pods = [pod("lab-db", "lab-pg-1", owner="Cluster", ready=False),          # CNPG's Postgres, down
                pod("trino", "coord-1"),
                pod("trino", "migrate-x", phase="Failed", owner="Job")]            # judged by its Job
        jobs = [{"metadata": {"namespace": "trino", "name": "migrate"}, "status": {"failed": 1}},
                {"metadata": {"namespace": "trino", "name": "ok"}, "status": {"succeeded": 1, "failed": 2}}]
        self.assertEqual(check.pod_problems(pods, jobs, nss),
                         ["pod lab-db/lab-pg-1 Running, not ready", "job trino/migrate failed",
                          "namespace openfga has no running workload"])
        self.assertEqual(check.pod_problems([pod("trino", "a")], [], {"trino"}), [])
        self.assertEqual(check.pod_problems([pod("trino", "a")], [], {"trino", "spark"}, frozenset({"spark"})), [])
        evicted = pod("trino", "old", phase="Failed", ready=False)          # its ReplicaSet replaced it
        self.assertEqual(check.pod_problems([pod("trino", "a"), evicted], [], {"trino"}), [])
        stray = pod("trino", "stray", phase="Failed", ready=False, owner="Nothing")
        self.assertEqual(check.pod_problems([pod("trino", "a"), stray], [], {"trino"}),
                         ["pod trino/stray Failed, not ready"])

    def test_an_image_counts_as_pinned_only_by_its_own_repositorys_digest(self):
        lock = {"images": [{"registry": "docker.io", "repository": "trinodb/trino", "digest": "sha256:" + "a" * 64}]}
        images = [("trino", "docker.io/trinodb/trino@sha256:" + "a" * 64),
                  ("trino", "docker.io/trinodb/trino:483"),
                  ("evil", "docker.io/someone/else:1@sha256:" + "a" * 64),        # a lock digest, wrong repository
                  ("kube-system", "registry.k8s.io/coredns/coredns:v1.14.2"),
                  ("kube-system", "docker.io/library/busybox:1.37"),              # not one of kind's images
                  ("runner", "lab.local/fluid-runner:0.32.0")]
        self.assertEqual(check.unpinned(images, lock, {"lab.local/fluid-runner:0.32.0"}), [
            "evil: docker.io/someone/else:1@sha256:" + "a" * 64,
            "kube-system: docker.io/library/busybox:1.37 (not one of kind's own images)",
            "trino: docker.io/trinodb/trino:483"])
        self.assertEqual(check.repository("quay.io/jetstack/cert-manager-controller:v1.21.2@sha256:x"),
                         "quay.io/jetstack/cert-manager-controller")

    def test_the_node_listing_parses_back_into_what_lab_up_writes(self):
        lock = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
        tiers = config.load_yaml(REPO_ROOT / config.TIERS)
        want = check.expected_hosts(lock, tiers)
        self.assertEqual(want["_default"], cluster.default_hosts_toml())
        self.assertEqual(want["docker.io"], cluster.hosts_toml("docker.io"))
        listing = "".join(f"=== /etc/containerd/certs.d/{k}/hosts.toml\n{v}" for k, v in want.items())
        self.assertEqual(check.parse_hosts(listing), want)

    def test_pod_security_must_match_the_record_and_carry_its_exemption(self):
        recorded = {"a": {"pod_security": "restricted"}, "b": {"pod_security": "baseline", "exemption": "why"},
                    "c": {"pod_security": "baseline"}, "d": {"pod_security": "restricted"}}
        live = {"a": {"labels": {"pod-security.kubernetes.io/enforce": "restricted"}},
                "b": {"labels": {"pod-security.kubernetes.io/enforce": "baseline"}},
                "c": {"labels": {"pod-security.kubernetes.io/enforce": "baseline"}},
                "d": {"labels": {"pod-security.kubernetes.io/enforce": "privileged"}}}
        self.assertEqual(check.pss_problems(live, recorded),
                         ["c enforces baseline with no recorded exemption", "d enforces privileged, recorded restricted"])

    def test_an_endpoint_must_answer_what_its_backend_answers(self):
        good = {"keycloak.lab.test": 200, "lakekeeper.lab.test": 200, "s3.lab.test": 403, "trino.lab.test": 200}
        self.assertEqual(check.endpoint_problems(good), [])
        self.assertEqual(check.endpoint_problems({**good, "trino.lab.test": 404}),
                         ["trino.lab.test answered HTTP 404, expected 200"])

    def test_vended_credentials_are_found_in_config_or_storage_credentials(self):
        flat = {"config": {"s3.access-key-id": "A", "s3.secret-access-key": "S", "s3.session-token": "T"}}
        nested = {"storage-credentials": [{"prefix": "s3://lake/", "config": flat["config"]},
                                          {"prefix": "s3://lake/x", "config": {"s3.access-key-id": "B"}}]}
        self.assertTrue(set(check.VENDED_KEYS) <= check.vended_keys(flat))
        self.assertEqual(check.vended_access_keys(nested), {"A", "B"})  # every entry, not just the first

    def test_the_warehouse_prefix_comes_from_overrides_or_defaults(self):
        for body in (CONFIG, {"overrides": {"prefix": WAREHOUSE_ID}}):
            client = FakeClient([("GET", "/catalog/v1/config", (200, body))])
            self.assertEqual(check._prefix(client, {}), WAREHOUSE_ID)
        with self.assertRaises(check.CheckError):
            check._prefix(FakeClient([("GET", "/catalog/v1/config", (200, {"defaults": {}}))]), {})

    def test_the_kv2_failure_is_recorded_where_e_says_it_is(self):
        self.assertTrue(check._kv2_recorded(REPO_ROOT))
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(check._kv2_recorded(Path(tmp)))


class Criteria(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_every_criterion_has_a_probe_or_a_reason_never_both(self):
        self.assertEqual(set(check.CRITERIA), set(check.CHECKS) | set(check.NOT_YET))
        self.assertFalse(set(check.CHECKS) & set(check.NOT_YET))

    def test_an_unproven_criterion_names_what_it_needs_without_a_schedule(self):
        for cid, reason in check.NOT_YET.items():
            with self.subTest(criterion=cid):
                self.assertNotRegex(reason.lower(), r"phase|later|\bsoon\b|\bnext\b")

    def test_an_add_on_record_belongs_to_the_tier_that_includes_the_add_on(self):
        tiers = config.load_yaml(REPO_ROOT / config.TIERS)
        core = check.records_for(config.resolve_tier(tiers, "core"))
        with_spark = check.records_for(config.resolve_tier(tiers, "spark"))
        self.assertNotIn("spark", core)
        self.assertEqual(with_spark, core + ["spark"])

    def test_an_unproven_criterion_fails_with_its_reason(self):
        rec = check._record("b-oauth2", None, make_ctx(self.dir))
        self.assertEqual(rec["status"], FAIL)
        self.assertIn(check.NOT_YET["b-oauth2"], rec["message"])
        self.assertEqual(rec["detail"]["criterion"], check.CRITERIA["b-oauth2"])

    def test_an_unexpected_error_is_one_failure_record_not_a_traceback(self):
        for exc in (KeyError("x"), AttributeError("y")):
            def broken(_ctx, exc=exc):
                raise exc

            rec = check._record("ready", broken, make_ctx(self.dir))
            self.assertEqual(rec["status"], FAIL)
            self.assertTrue(rec["message"].startswith("unexpected"), rec["message"])

    def test_ready_fails_when_the_tier_runs_nothing(self):
        ctx = make_ctx(self.dir)
        (ctx.root / "platform").mkdir()
        (ctx.root / "platform/namespaces.yaml").write_text("# fixture\n")
        ctx.runner = FakeRunner(docs={"namespaces.yaml": {"namespaces": {"trino": {"tier": "core"}}}})
        rec = check._record("ready", check.ready, ctx)
        self.assertEqual(rec["status"], FAIL)
        self.assertIn("namespace trino has no running workload", rec["message"])

    def test_a_runner_record_without_layers_is_not_a_pass(self):
        ctx = make_ctx(self.dir)
        down.mark_state_dir(ctx.lab)
        state.write_json_private(ctx.lab.images_record, {"fluid-runner": {"tag": "lab.local/fluid-runner:1"}})
        rec = check._record("runner", check.runner_image, ctx)
        self.assertEqual(rec["status"], FAIL)
        self.assertIn("no usable fluid-runner record", rec["message"])

    def openfga_client(self, probe_status: int, probe_body: dict, backend="openfga") -> FakeClient:
        return FakeClient([
            ("GET", "/management/v1/info", (200, {"authz-backend": backend})),
            ("GET", "/catalog/v1/config", (200, CONFIG)),
            ("POST", BASE, lambda h: (probe_status, probe_body) if h.get("Authorization") == "Bearer token-lab-probe"
             else (200, {})),
            ("DELETE", f"{BASE}/{check.ADMIN_NAMESPACE}", (204, {})),
            ("GET", f"{BASE}/{check.DENIED_NAMESPACE}", (404, {"error": {"type": "NoSuchNamespaceException"}})),
            ("DELETE", f"{BASE}/{check.DENIED_NAMESPACE}", (204, {})),
        ])

    def test_a_refusal_by_403_or_a_hidden_warehouse_passes_with_a_positive_control(self):
        hidden = {"error": {"type": "NoSuchWarehouseException", "code": 404}}
        for status, body in ((403, {"error": {"type": "Forbidden"}}), (404, hidden)):
            client = self.openfga_client(status, body)
            message = check.g_openfga(make_ctx(self.dir, client=client))
            self.assertIn("authz backend openfga", message, status)
            admin_calls = [(m, p) for m, p, auth, _ in client.calls if auth == "Bearer token-lab-admin"]
            self.assertIn(("POST", BASE), admin_calls)                                 # the positive control
            self.assertIn(("DELETE", f"{BASE}/{check.ADMIN_NAMESPACE}"), admin_calls)

    def test_a_bare_refusal_or_another_backend_is_not_evidence_of_a_deny(self):
        for client in (self.openfga_client(404, {}), self.openfga_client(403, {}),   # no Lakekeeper error body
                       self.openfga_client(403, {"error": {"type": "Forbidden"}}, backend="allow-all")):
            with self.assertRaises(check.CheckError):
                check.g_openfga(make_ctx(self.dir, client=client))

    def test_an_allowed_create_fails_and_is_cleaned_up(self):
        client = self.openfga_client(200, {"namespace": [check.DENIED_NAMESPACE]})
        with self.assertRaises(check.CheckError) as caught:
            check.g_openfga(make_ctx(self.dir, client=client))
        self.assertIn("authorization is not enforced", str(caught.exception))
        self.assertIn(("DELETE", f"{BASE}/{check.DENIED_NAMESPACE}"), [(c[0], c[1]) for c in client.calls])

    def test_a_refused_token_is_not_evidence_of_a_deny(self):
        with self.assertRaises(check.CheckError):
            check.g_openfga(make_ctx(self.dir, client=self.openfga_client(401, {})))

    def catalog_client(self, table_config: dict) -> FakeClient:
        return FakeClient([
            ("GET", "/catalog/v1/config", (200, CONFIG)),
            ("POST", f"{BASE}/{check.NAMESPACE}/tables", (409, {})),
            ("POST", BASE, (409, {})),
            ("GET", f"{BASE}/{check.NAMESPACE}/tables/{check.TABLE}", (200, {"config": table_config})),
        ])

    def test_the_catalog_probe_needs_temporary_credentials_and_no_static_keys(self):
        temporary = {"s3.access-key-id": "TEMP", "s3.secret-access-key": "S", "s3.session-token": "T"}
        good = FakeKube({("get", "configmaps", "-n", "trino"): {"items": [CATALOG_CM]}})
        message = check.a_catalog(make_ctx(self.dir, client=self.catalog_client(temporary), kube=good))
        self.assertIn("temporary credential", message)
        self.assertNotIn("TEMP", message)  # credential values never reach a record
        for bad in ({**temporary, "s3.access-key-id": "STATICKEY"}, {"s3.access-key-id": "TEMP"}):
            with self.assertRaises(check.CheckError):
                check.a_catalog(make_ctx(self.dir, client=self.catalog_client(bad), kube=good))
        static_cm = {"metadata": {"name": "x"}, "data": {"lakehouse.properties": "s3.aws-access-key=AKIA\n"}}
        pods = {"items": [{"metadata": {"name": "coord"}, "spec": {"containers": [
            {"envFrom": [{"secretRef": {"name": "trino-env"}}]}]}}]}
        cases = {
            "no lakehouse catalog": FakeKube({}),
            "static S3 keys": FakeKube({("get", "configmaps", "-n", "trino"): {"items": [CATALOG_CM, static_cm]}}),
            "Secret trino-env key AWS_SECRET_ACCESS_KEY": FakeKube({
                ("get", "configmaps", "-n", "trino"): {"items": [CATALOG_CM]}, ("get", "pods", "-n", "trino"): pods,
                ("get", "secret", "trino-env", "-n", "trino", check.SECRET_KEYS): "AWS_SECRET_ACCESS_KEY\nOTHER\n"}),
            "ConfigMap y/hive.properties": FakeKube({("get", "configmaps", "-n", "trino"): {"items": [CATALOG_CM, {
                "metadata": {"name": "y"}, "data": {"hive.properties": "hive.s3.aws-access-key=AKIA\n"}}]}}),
        }
        for said, kube in cases.items():
            with self.assertRaises(check.CheckError) as caught:
                check.a_catalog(make_ctx(self.dir, client=self.catalog_client(temporary), kube=kube))
            self.assertIn(said, str(caught.exception))
            self.assertNotIn("AKIA", str(caught.exception))  # values never reach a record

    def test_a_probe_without_its_prerequisite_says_which_one(self):
        ctx = make_ctx(self.dir)
        ctx.missing["gateway"] = "the port-forward to the Gateway: no pod"
        rec = check._record("i", check.i_tls, ctx)
        self.assertEqual(rec["status"], FAIL)
        self.assertIn("needs gateway", rec["message"])


class OpenBaoForCheck(unittest.TestCase):
    def test_check_never_initialises_an_openbao_lab_up_did_not_set_up(self):
        class Fresh:
            def wait_up(self):
                return {}

            def health(self):
                return {"initialized": False, "sealed": True}

        with tempfile.TemporaryDirectory() as tmp:
            lab = state.lab_for(Path(tmp) / "repo", "ffl-x", {"LAB_STATE_DIR": str(Path(tmp) / "state")})
            with self.assertRaises(openbao.BaoError) as caught:
                openbao.open_existing(Fresh(), lab)
        self.assertIn("run `lab up`", str(caught.exception))

    def test_an_empty_error_body_is_a_message_not_a_crash(self):
        self.assertEqual(openbao._errors(http.Response(503, b"")), "")


class RunCheck(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        for name in ("versions.lock.yaml", "tiers.yaml"):
            (self.root / name).write_text("# fixture\n")
        self.env = {"LAB_STATE_DIR": str(Path(self.tmp.name) / "state")}
        self.lab = state.lab_for(self.root, "ffl-x", self.env)

    def tearDown(self):
        self.tmp.cleanup()

    def run_check(self, table):
        runner = FakeRunner(docs={"versions.lock.yaml": {}, "tiers.yaml": TIERS_DOC}, table=table)
        return check.run_check(self.root, "core", name="ffl-x", runner=runner, env=self.env)

    def test_a_lab_that_lab_up_never_made_gets_one_failure(self):
        doc = self.run_check({})
        self.assertEqual([(c["id"], c["status"]) for c in doc["checks"]], [("check:lab", FAIL)])
        self.assertFalse(doc["ok"])

    def test_a_lab_without_a_cluster_or_without_kind_gets_one_failure(self):
        down.mark_state_dir(self.lab)
        for clusters, said in (((0, ""), "no kind cluster"), ((None, ""), "kind get clusters failed")):
            doc = self.run_check({("kind", "get", "clusters"): clusters})
            self.assertEqual(len(doc["checks"]), 1, doc)
            self.assertIn(said, doc["checks"][0]["message"])


if __name__ == "__main__":
    unittest.main()
