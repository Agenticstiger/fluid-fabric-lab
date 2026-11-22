# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Launcher pieces that need no container runtime: names and state,
mirror selection and layout, containerd and CoreDNS config, credentials and
the Secrets built from them, and `lab down`'s refusal to delete a directory
it cannot prove is a lab's."""

import json
import os
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import FakeRunner

from labctl import cluster, credentials, down, mirror, openbao, proc, render, state
from labctl.output import FAIL, PASS

LOCK = {
    "charts": [
        {"name": "trino", "component": "trino", "tier": "core"},
        {"name": "strimzi-kafka-operator", "component": "strimzi", "tier": "streaming"},
    ],
    "images": [
        {"id": "kind-node", "component": "cluster", "use": "used", "registry": "docker.io",
         "repository": "kindest/node", "tag": "v1.36.4", "digest": "sha256:" + "a" * 64,
         "ref": "docker.io/kindest/node:v1.36.4"},
        {"id": "trino", "component": "trino", "use": "used", "registry": "docker.io",
         "repository": "trinodb/trino", "tag": "483", "digest": "sha256:" + "b" * 64,
         "ref": "docker.io/trinodb/trino:483"},
        {"id": "strimzi-kafka", "component": "strimzi", "use": "used", "registry": "quay.io",
         "repository": "strimzi/kafka", "tag": "1.2.0", "digest": "sha256:" + "c" * 64,
         "ref": "quay.io/strimzi/kafka:1.2.0"},
        {"id": "openbao", "component": "openbao", "use": "used", "registry": "quay.io",
         "repository": "openbao/openbao", "tag": "2.7.1", "digest": "sha256:" + "d" * 64,
         "ref": "quay.io/openbao/openbao:2.7.1"},
        {"id": "lab-registry", "component": "mirror", "use": "used", "registry": "docker.io",
         "repository": "library/registry", "tag": "3.1.2", "digest": "sha256:" + "e" * 64,
         "ref": "docker.io/library/registry:3.1.2"},
        {"id": "denied", "component": "trino", "use": "denied", "registry": "docker.io",
         "repository": "bitnamilegacy/x", "tag": "1", "digest": "sha256:" + "f" * 64, "ref": "x"},
        {"id": "kafbat-ui", "component": "kafbat-ui", "use": "optional", "registry": "ghcr.io",
         "repository": "kafbat/kafka-ui", "tag": "v1.5.0", "digest": "sha256:" + "9" * 64,
         "ref": "ghcr.io/kafbat/kafka-ui:v1.5.0"},
    ],
}
TIERS = {"tiers": {
    "core": {"memory_target_gib": 8, "releases": ["trino"]},
    "streaming": {"memory_target_gib": 13, "requires": ["core"], "releases": ["strimzi-kafka-operator"]},
    "full": {"memory_target_gib": 16, "requires": ["core", "streaming"], "releases": ["trino"]},
}}


class Names(unittest.TestCase):
    def test_valid_and_invalid_lab_names(self):
        for good in ("fluid-fabric-lab", "ffl-e", "a1"):
            self.assertEqual(state.check_name(good), good)
        for bad in ("", "A", "-x", "x-", "a--b", "a_b", "1abc", "a" * 33, "../x", "kind"):
            with self.subTest(bad=bad), self.assertRaises(state.StateError):
                state.check_name(bad)

    def test_state_dir_follows_xdg_and_never_sits_in_the_repo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            lab = state.lab_for(root, "ffl-e", {"XDG_STATE_HOME": str(Path(tmp) / "xdg")})
            self.assertEqual(lab.state, (Path(tmp) / "xdg" / "fluid-fabric-lab" / "ffl-e").resolve())
            self.assertEqual((lab.openbao, lab.openbao_volume, lab.context, lab.node),
                             ("ffl-e-openbao", "ffl-e-openbao-data", "kind-ffl-e", "ffl-e-control-plane"))
            with self.assertRaisesRegex(state.StateError, "inside the repository"):
                state.lab_for(root, "ffl-e", {"LAB_STATE_DIR": str(root / "state")})

    def test_private_files_are_0600_and_dirs_0700(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = state.write_private(Path(tmp) / "d" / "secret.json", "{}")
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(path.parent).st_mode & 0o777, 0o700)


class MirrorSelection(unittest.TestCase):
    def test_core_takes_used_images_of_core_components_only(self):
        ids = [e["id"] for e in mirror.tier_images(LOCK, TIERS, "core")]
        self.assertEqual(ids, ["kind-node", "trino", "openbao"])

    def test_streaming_adds_its_components_and_full_takes_everything_used(self):
        self.assertIn("strimzi-kafka", [e["id"] for e in mirror.tier_images(LOCK, TIERS, "streaming")])
        ids = [e["id"] for e in mirror.tier_images(LOCK, TIERS, "full")]
        self.assertNotIn("denied", ids)
        self.assertNotIn("lab-registry", ids, "the registry is the mirror, not an image in it")
        self.assertNotIn("kafbat-ui", ids)
        self.assertIn("kafbat-ui", [e["id"] for e in mirror.tier_images(LOCK, TIERS, "core", with_optional=True)])

    def test_an_image_without_a_digest_or_owner_fails(self):
        lock = json.loads(json.dumps(LOCK))
        lock["images"][1]["digest"] = "latest"
        with self.assertRaisesRegex(mirror.MirrorError, "not a sha256 digest"):
            mirror.tier_images(lock, TIERS, "core")
        lock = json.loads(json.dumps(LOCK))
        lock["images"][1]["component"] = "nobody"
        with self.assertRaisesRegex(mirror.MirrorError, "no tier"):
            mirror.tier_images(lock, TIERS, "core")

    def test_layout_keeps_the_upstream_host_as_a_path_prefix(self):
        entry = LOCK["images"][2]
        self.assertEqual(mirror.host_ref(entry), "localhost:5001/quay.io/strimzi/kafka:1.2.0")
        self.assertEqual(mirror.node_mirror_url("quay.io"), "http://fluid-fabric-lab-registry:5000/v2/quay.io")
        self.assertEqual(mirror.upstream_hosts(LOCK["images"][:3]), ["docker.io", "quay.io"])

    def test_copy_is_verified_by_digest_and_a_cache_hit_is_verified_too(self):
        entry = LOCK["images"][1]
        hit = FakeRunner(table={("crane", "digest"): (0, entry["digest"] + "\n")})
        self.assertEqual(mirror.copy_one(hit, entry)["status"], PASS)
        self.assertFalse(any(c[0][:2] == ["crane", "copy"] for c in hit.calls))
        moved = FakeRunner(table={("crane", "digest"): (0, "sha256:" + "0" * 64), ("crane", "copy"): (0, "")})
        result = mirror.copy_one(moved, entry)
        self.assertEqual(result["status"], FAIL)
        self.assertIn("lock says", result["message"])


class NodeConfig(unittest.TestCase):
    def test_hosts_toml_has_no_upstream_fallback(self):
        text = cluster.hosts_toml("quay.io")
        self.assertIn('server = "http://fluid-fabric-lab-registry:5000/v2/quay.io"', text)
        self.assertNotIn("https://quay.io", text)
        self.assertEqual(text.count("override_path = true"), 2)
        self.assertIn('server = "http://127.0.0.1:9"', cluster.default_hosts_toml())

    def test_a_registry_name_that_is_not_a_host_is_refused_before_any_shell(self):
        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        runner = FakeRunner()
        for bad in ('quay.io"; rm -rf /', "quay.io/x", "../etc", "QUAY.IO", "localhost"):
            with self.subTest(bad=bad), self.assertRaises(cluster.ClusterError):
                cluster.pin_pulls_to_mirror(runner, lab, [bad])
        self.assertEqual(runner.calls, [])

    def test_corefile_rewrites_are_inserted_once_before_kubernetes(self):
        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        core = ".:53 {\n    errors\n    ready\n    kubernetes cluster.local in-addr.arpa {\n    }\n}\n"
        once = cluster.corefile_with_rewrites(core, lab)
        self.assertEqual(once, cluster.corefile_with_rewrites(once, lab))
        lines = once.splitlines()
        rewrites = [i for i, ln in enumerate(lines) if "rewrite" in ln]
        kube_line = next(i for i, ln in enumerate(lines) if ln.strip().startswith("kubernetes "))
        self.assertEqual(len(rewrites), 2)
        self.assertTrue(all(i < kube_line for i in rewrites))
        self.assertIn("openbao.lab.test ffl-e-openbao", once)
        with self.assertRaises(cluster.ClusterError):
            cluster.corefile_with_rewrites(".:53 {\n errors\n}\n", lab)

    def test_namespace_labels_enforce_and_record_exemptions(self):
        ns = cluster.namespace("trino", "baseline", "chart runs as root")
        labels = ns["metadata"]["labels"]
        self.assertEqual(labels["pod-security.kubernetes.io/enforce"], "baseline")
        self.assertEqual(labels["pod-security.kubernetes.io/warn"], "restricted")
        self.assertIn("chart runs as root", json.dumps(ns))


class Credentials(unittest.TestCase):
    def test_generate_fills_only_what_is_missing(self):
        creds, changed = credentials.generate({})
        self.assertTrue(changed)
        again, changed_again = credentials.generate(creds)
        self.assertFalse(changed_again)
        self.assertEqual(creds, again)
        partial = dict(creds)
        partial.pop("pg.keycloak")
        refilled, changed = credentials.generate(partial)
        self.assertTrue(changed)
        self.assertEqual({k: v for k, v in refilled.items() if k != "pg.keycloak"},
                         {k: v for k, v in creds.items() if k != "pg.keycloak"})

    def test_secrets_go_only_to_existing_namespaces_and_hold_the_values(self):
        creds, _ = credentials.generate({})
        objs = credentials.secrets_for(creds, {"lab-db", "lakekeeper", "openfga", "seaweedfs"})
        where = {(o["metadata"]["namespace"], o["metadata"]["name"]) for o in objs}
        self.assertIn(("lab-db", "lab-pg-keycloak"), where)
        self.assertNotIn(("keycloak", "lab-pg-keycloak"), where, "keycloak namespace not in this tier")
        self.assertIn(("openfga", "lab-pg-openfga-uri"), where)
        uri = next(o for o in objs if o["metadata"]["name"] == "lab-pg-openfga-uri")["stringData"]["uri"]
        self.assertIn(creds["pg.openfga"], uri)
        self.assertIn("sslmode=require", uri)
        pg = next(o for o in objs if o["metadata"]["name"] == "lab-pg-lakekeeper")
        self.assertEqual(pg["type"], "kubernetes.io/basic-auth")

    def test_sts_role_reaches_only_the_warehouse_bucket(self):
        creds, _ = credentials.generate({})
        doc = credentials.seaweedfs_config(creds)
        statement = doc["policies"][0]["document"]["Statement"][0]
        self.assertEqual(statement["Resource"], ["arn:aws:s3:::lake", "arn:aws:s3:::lake/*"])
        lk = next(i for i in doc["identities"] if i["name"] == "lakekeeper")
        self.assertTrue(all(a.endswith(":lake") for a in lk["actions"]))
        self.assertEqual(doc["roles"][0]["roleArn"], credentials.STS_ROLE_ARN)


class Down(unittest.TestCase):
    def _lab(self, tmp):
        root = Path(tmp) / "repo"
        root.mkdir()
        env = {"LAB_STATE_DIR": str(Path(tmp) / "state")}
        return root, env, state.lab_for(root, "ffl-e", env)

    def test_purge_deletes_only_a_directory_marked_for_this_lab(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, _, lab = self._lab(tmp)
            state.ensure_dir(lab.state)
            (lab.state / "keep.txt").write_text("not a lab file")
            self.assertEqual(down._purge_state(lab)["status"], FAIL)
            (lab.state / down.STATE_MARKER).write_text("other-lab\n")
            self.assertEqual(down._purge_state(lab)["status"], FAIL)
            self.assertTrue((lab.state / "keep.txt").exists())
            (lab.state / down.STATE_MARKER).write_text("ffl-e\n")
            self.assertEqual(down._purge_state(lab)["status"], PASS)
            self.assertFalse(lab.state.exists())

    def test_up_never_marks_a_non_empty_directory_it_did_not_create(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, _, lab = self._lab(tmp)
            state.ensure_dir(lab.state)
            (lab.state / "notes.txt").write_text("someone else's files")
            with self.assertRaises(down.NotALab):
                down.mark_state_dir(lab)
            self.assertFalse((lab.state / down.STATE_MARKER).exists())
            (lab.state / "notes.txt").unlink()
            down.mark_state_dir(lab)                  # empty: fine
            down.mark_state_dir(lab)                  # already ours: fine
            self.assertTrue(down.owns_state(lab))



class DownRemoval(unittest.TestCase):
    _lab = Down._lab

    def test_down_without_this_labs_state_removes_nothing(self):
        runner = FakeRunner(table={("kind", "get", "clusters"): (0, "ffl-e\n")})
        with tempfile.TemporaryDirectory() as tmp:
            root, env, _ = self._lab(tmp)
            doc = down.run_down(root, name="ffl-e", runner=runner, env=env)
        self.assertFalse(doc["ok"])
        self.assertEqual(runner.calls, [], "not even a listing: there is nothing of ours to find")

    def test_down_touches_only_this_labs_labelled_objects(self):
        def inspect(argv, **_kw):
            labels = {state.LABEL: "ffl-e"} if argv[-1] == "ffl-e-openbao" else {}
            return proc.Result(argv, 0, json.dumps([{"Config": {"Labels": labels}}]), "")
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (0, "other\nffl-e\n"),
            ("kind", "delete"): (0, ""),
            ("docker", "inspect"): inspect,
            ("docker", "rm"): (0, ""),
            ("docker", "network", "inspect"): (1, "Error response from daemon: No such network: ffl-e-net"),
        })
        with tempfile.TemporaryDirectory() as tmp:
            root, env, lab = self._lab(tmp)
            down.mark_state_dir(lab)
            doc = down.run_down(root, name="ffl-e", runner=runner, env=env)
        self.assertTrue(doc["ok"], doc)
        argvs = [c[0] for c in runner.calls]
        self.assertIn(["kind", "delete", "cluster", "--name", "ffl-e", "--kubeconfig", str(lab.kubeconfig)], argvs)
        self.assertIn(["docker", "rm", "-f", "ffl-e-openbao"], argvs)
        self.assertFalse(any("volume" in a for argv in argvs for a in argv), "no volume without --purge")
        self.assertFalse(any(a in ("prune", "compose") for argv in argvs for a in argv))

    def test_a_container_without_this_labs_label_is_left_alone(self):
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (0, ""),
            ("docker", "inspect"): (0, json.dumps([{"Config": {"Labels": {}}}])),
        })
        with tempfile.TemporaryDirectory() as tmp:
            root, env, lab = self._lab(tmp)
            down.mark_state_dir(lab)
            doc = down.run_down(root, name="ffl-e", runner=runner, env=env)
        self.assertFalse(doc["ok"])
        self.assertFalse(any(c[0][:2] == ["docker", "rm"] for c in runner.calls))


class FakeBao:
    """Stands in for openbao.Bao: a server that can be fresh, sealed or half-configured."""

    def __init__(self, initialized=False, sealed=True, fail_configure_once=False):
        self.initialized, self.sealed = initialized, sealed
        self.fail_configure_once = fail_configure_once
        self.users, self.revoked, self.token = {}, [], None

    def wait_up(self):
        return {}

    def wait_active(self):
        return {}

    def token_is_valid(self, token):
        return token not in self.revoked

    def health(self):
        return {"initialized": self.initialized, "sealed": self.sealed}

    def init(self):
        self.initialized = True
        return {"keys_base64": ["k1", "k2", "k3"], "root_token": "root"}

    def unseal(self, keys):
        self.sealed = False

    def enable_userpass(self):
        if self.fail_configure_once:
            self.fail_configure_once = False
            raise openbao.BaoError("connection reset")

    def put_policy(self, name, hcl):
        pass

    def put_user(self, name, password, policies):
        self.users[name] = password

    def revoke_self(self, token):
        if token in self.revoked:
            raise openbao.BaoError("HTTP 403 permission denied")
        self.revoked.append(token)

    def login(self, user, password):
        if self.users.get(user) != password:
            raise openbao.BaoError("HTTP 400 invalid username or password")
        return "admin-token"

    def enable_kv2(self, path):
        return False


class OpenBaoLifecycle(unittest.TestCase):
    def test_a_run_that_dies_after_init_is_finished_by_the_next_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab = state.Lab("ffl-e", Path(tmp) / "repo", Path(tmp) / "state")
            bao = FakeBao(fail_configure_once=True)
            with self.assertRaises(openbao.BaoError):
                openbao.bring_up(bao, lab)
            record = state.read_json(openbao.init_path(lab))
            self.assertEqual(record["keys_b64"], ["k1", "k2", "k3"], "shares saved the moment init returned")
            self.assertIn("root_token_pending", record)
            bao.sealed = True                       # e.g. the container restarted
            steps = openbao.bring_up(bao, lab)
            self.assertIn("root token revoked", steps)
            self.assertEqual(bao.revoked, ["root"])
            self.assertNotIn("root_token_pending", state.read_json(openbao.init_path(lab)))
            self.assertEqual(os.stat(openbao.init_path(lab)).st_mode & 0o777, 0o600)

    def test_a_dead_pending_root_token_is_dropped_once_lab_admin_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab = state.Lab("ffl-e", Path(tmp) / "repo", Path(tmp) / "state")
            bao = FakeBao(initialized=True, sealed=False)
            record = {"keys_b64": ["k1"], "threshold": 2, "admin_user": "lab-admin", "admin_password": "pw",
                      "root_token_pending": "root"}
            state.write_json_private(openbao.init_path(lab), record)
            bao.users["lab-admin"] = "pw"
            bao.revoked.append("root")              # an earlier run revoked it, then died
            openbao.bring_up(bao, lab)
            self.assertNotIn("root_token_pending", state.read_json(openbao.init_path(lab)))


class StepDriver(unittest.TestCase):
    def test_up_stops_at_the_first_failing_step_and_says_what_to_do(self):
        from labctl import up

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            run = up.Up(root, "core", name="ffl-e", env={"LAB_STATE_DIR": str(Path(tmp) / "state")},
                        runner=FakeRunner())
            calls = []
            run.preflight = lambda: calls.append("preflight") or "ok"
            run.mirror = lambda: calls.append("mirror") or "ok"

            def broken():
                calls.append("pki")
                raise up.StepError("the lab CA could not be created", "install openssl")
            run.pki = broken
            run.cluster = lambda: calls.append("cluster") or "ok"
            doc = run.run()
        self.assertFalse(doc["ok"])
        self.assertEqual(calls, ["preflight", "mirror", "pki"])
        last = doc["checks"][-1]
        self.assertEqual((last["id"], last["status"]), ("up:pki", FAIL))
        self.assertEqual(last["advice"], "install openssl")


class SecretsOffArgv(unittest.TestCase):
    def test_kube_apply_sends_secrets_on_stdin_with_server_side_apply(self):
        from labctl import kube

        runner = FakeRunner(table={("kubectl",): (0, "applied")})
        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        kube.Kube(lab, runner).apply([kube.secret("x", "ns", {"password": "hunter2-value"})])
        argv, kwargs = runner.calls[0]
        self.assertNotIn("hunter2-value", " ".join(argv))
        self.assertIn("hunter2-value", kwargs["input"])
        self.assertIn("--server-side", argv)
        self.assertEqual(argv[:5], ["kubectl", "--kubeconfig", "/s/kubeconfig", "--context", "kind-ffl-e"])


class ProcInput(unittest.TestCase):
    def test_stdin_is_the_given_input_or_closed_and_never_the_argv(self):
        script = "import sys; print(repr(sys.stdin.read()[::-1]))"
        for given, seen in (("terces", "'secret'"), (None, "''")):
            with self.subTest(given=given):
                res = proc.run(["python3", "-c", script], input=given, timeout=30)
                self.assertTrue(res.ok)
                self.assertEqual(res.stdout.strip(), seen)
                self.assertNotIn("terces", " ".join(res.argv))


class TierClosure(unittest.TestCase):
    def test_closure_is_transitive(self):
        self.assertEqual(render.tier_closure(TIERS, "full"), {"full", "core", "streaming"})
        self.assertEqual(render.tier_closure(TIERS, "core"), {"core"})


class RuntimeGuard(unittest.TestCase):
    def test_unit_tests_cannot_reach_the_container_runtime(self):
        for tool in ("docker", "kind", "kubectl", "crane"):
            with self.subTest(tool=tool), self.assertRaisesRegex(AssertionError, "must not run"):
                proc.run([tool, "version"], timeout=5)


class WebhookWarmup(unittest.TestCase):
    def test_only_a_webhook_that_is_not_answering_is_retried(self):
        from labctl import up
        warm = ('Error: failed calling webhook "trust.cert-manager.io": failed to call webhook: '
                'Post "https://trust-manager.cert-manager.svc:443/validate": dial tcp 10.96.253.9:443: '
                'connect: connection refused')
        self.assertTrue(up.is_webhook_warmup(warm))
        self.assertTrue(up.is_webhook_warmup('failed calling webhook "x": no endpoints available for service "y"'))
        self.assertFalse(up.is_webhook_warmup('failed calling webhook "x": admission denied: bad spec'))
        self.assertFalse(up.is_webhook_warmup("Error: context deadline exceeded"))


class CliDefaults(unittest.TestCase):
    def test_cli_registry_port_matches_the_mirror(self):
        from labctl import cli
        self.assertEqual(cli.DEFAULT_REGISTRY_PORT, mirror.DEFAULT_PORT)
        self.assertEqual(cli.DEFAULT_NAME, state.DEFAULT_NAME)


class KVUpgradeWindow(unittest.TestCase):
    def test_only_the_kv_upgrade_answer_is_waited_out(self):
        bao = openbao.Bao(1, "/ca.crt")
        answers = [openbao.BaoError("GET /v1/kv/data/x: HTTP 400 Upgrading from non-versioned to versioned data."),
                   "ok"]

        def call(method, path, body=None, token=None, ok=(200, 204)):
            item = answers.pop(0)
            if isinstance(item, Exception):
                raise item
            return openbao.http.Response(200, b'{"data": {"data": {"k": "v"}}}')
        bao.call = call
        self.assertEqual(bao.kv_get("kv", "x"), {"k": "v"})
        bao.call = lambda *a, **k: (_ for _ in ()).throw(openbao.BaoError("HTTP 403 permission denied"))
        with self.assertRaisesRegex(openbao.BaoError, "403"):
            bao.kv_get("kv", "x")


class LabNetwork(unittest.TestCase):
    def test_the_network_is_created_ipv4_only_and_a_foreign_one_is_refused(self):
        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        fresh = FakeRunner(table={("docker", "network", "inspect"): (1, ""), ("docker", "network", "create"): (0, "id")})
        self.assertIn("created", cluster.ensure_network(fresh, lab))
        create = next(c[0] for c in fresh.calls if c[0][:3] == ["docker", "network", "create"])
        self.assertNotIn("--ipv6", create)
        self.assertIn(f"{state.LABEL}=ffl-e", create)
        foreign = FakeRunner(table={("docker", "network", "inspect"): (0, json.dumps({"Labels": {}}))})
        with self.assertRaisesRegex(cluster.ClusterError, "not this lab's"):
            cluster.ensure_network(foreign, lab)
        dual = FakeRunner(table={("docker", "network", "inspect"): (
            0, json.dumps({"Labels": {state.LABEL: "ffl-e"}, "EnableIPv6": True}))})
        with self.assertRaisesRegex(cluster.ClusterError, "IPv6"):
            cluster.ensure_network(dual, lab)

    def test_down_removes_the_network_only_when_just_the_mirror_is_left(self):
        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        def info(containers):
            return json.dumps({"Labels": {state.LABEL: "ffl-e"},
                               "Containers": {str(i): {"Name": n} for i, n in enumerate(containers)}})
        busy = FakeRunner(table={("docker", "network", "inspect"): (0, info([mirror.REGISTRY, "other"])),
                                 ("docker", "network", "disconnect"): (0, "")})
        self.assertEqual(down._remove_network(busy, lab)["status"], FAIL)
        self.assertFalse(any(c[0][:3] == ["docker", "network", "rm"] for c in busy.calls))
        idle = FakeRunner(table={("docker", "network", "inspect"): (0, info([mirror.REGISTRY])),
                                 ("docker", "network", "disconnect"): (0, ""), ("docker", "network", "rm"): (0, "")})
        self.assertEqual(down._remove_network(idle, lab)["status"], PASS)
        self.assertIn(["docker", "network", "rm", "ffl-e-net"], [c[0] for c in idle.calls])


class DownWhenDockerIsDown(unittest.TestCase):
    def test_purge_keeps_everything_when_the_runtime_cannot_answer(self):
        daemon_down = (1, "Cannot connect to the Docker daemon at unix:///var/run/docker.sock")
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (1, "ERROR: failed to list clusters"),
            ("docker",): daemon_down,
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            env = {"LAB_STATE_DIR": str(Path(tmp) / "state")}
            lab = state.lab_for(root, "ffl-e", env)
            down.mark_state_dir(lab)
            doc = down.run_down(root, name="ffl-e", purge=True, runner=runner, env=env)
            self.assertFalse(doc["ok"])
            self.assertTrue(lab.state.exists(), "the unseal shares must survive a failed down")
        self.assertFalse(any(c[0][:2] in (["docker", "rm"], ["docker", "volume"]) for c in runner.calls))

    def test_a_failed_removal_keeps_the_volume_and_the_state(self):
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (0, "ffl-e\n"),
            ("kind", "delete"): (1, "failed to delete cluster"),
            ("docker", "inspect"): (1, "Error: No such object: ffl-e-openbao"),
            ("docker", "network", "inspect"): (1, "Error response from daemon: No such network: ffl-e-net"),
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            env = {"LAB_STATE_DIR": str(Path(tmp) / "state")}
            lab = state.lab_for(root, "ffl-e", env)
            down.mark_state_dir(lab)
            doc = down.run_down(root, name="ffl-e", purge=True, runner=runner, env=env)
            self.assertFalse(doc["ok"])
            self.assertTrue(lab.state.exists())
        self.assertFalse(any(c[0][:2] == ["docker", "volume"] for c in runner.calls))

    def test_only_dockers_not_found_answers_mean_absent(self):
        from labctl import containers
        R = proc.Result
        # Docker 29.4.1's own wording for each object kind (measured 2026-11-18)
        self.assertTrue(containers.absent(R([], 1, "", "Error response from daemon: No such container: x"), "x"))
        self.assertTrue(containers.absent(R([], 1, "", "Error: No such object: x"), "x"))
        self.assertTrue(containers.absent(R([], 1, "", "Error response from daemon: get v: no such volume"), "v"))
        self.assertTrue(containers.absent(R([], 1, "", "Error response from daemon: network n-net not found"), "n-net"))
        # Docker could not be asked: never "absent"
        self.assertFalse(containers.absent(R([], 1, "", "Cannot connect to the Docker daemon")))
        self.assertFalse(containers.absent(R([], 1, "", "failed to connect to the docker API at unix:///x.sock; check "
                                                       "if the path is correct and if the daemon is running: dial "
                                                       "unix /x.sock: connect: no such file or directory"), "x"))
        # a not-found answer about another object does not count
        self.assertFalse(containers.absent(R([], 1, "", "No such container: other"), "x"))
        self.assertFalse(containers.absent(proc.Result([], 124, "", "timed out", timed_out=True)))
        self.assertFalse(containers.absent(proc.Result([], 127, "", "not found", missing=True)))
        with self.assertRaises(containers.DockerError):
            containers.inspect(FakeRunner(table={("docker",): (1, "Cannot connect to the Docker daemon")}), "x")


class PrivateWrites(unittest.TestCase):
    def test_writes_are_atomic_and_leave_no_temporary_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "d" / "init.json"
            state.write_json_private(path, {"keys_b64": ["a"]})
            state.write_json_private(path, {"keys_b64": ["b"]})
            self.assertEqual(state.read_json(path), {"keys_b64": ["b"]})
            self.assertEqual(sorted(p.name for p in path.parent.iterdir()), ["init.json"])
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)


class PortForwardDeadline(unittest.TestCase):
    def test_a_silent_kubectl_fails_within_the_timeout(self):
        import sys
        import time as _time
        from labctl import kube

        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        forward = kube.PortForward(kube.Kube(lab, FakeRunner()), "ns", "svc/x", 443, timeout_s=1)
        forward.argv = [sys.executable, "-c", "import time; time.sleep(30)"]   # never prints, never exits
        started = _time.monotonic()
        with self.assertRaisesRegex(kube.KubeError, "did not start"):
            forward.__enter__()
        self.assertLess(_time.monotonic() - started, 10)
        self.assertIsNotNone(forward.proc.poll(), "the silent child is stopped")


class RealmImportWait(unittest.TestCase):
    def test_a_transient_has_errors_is_waited_out_until_done(self):
        from labctl import bootstrap, kube

        answers = [
            {"status": {"conditions": [{"type": "HasErrors", "status": "True", "message": "Deployment not yet ready"},
                                       {"type": "Done", "status": "False"}]}},
            {"status": {"conditions": [{"type": "HasErrors", "status": "False"}, {"type": "Done", "status": "True"}]}},
        ]

        def kubectl(argv, **_kw):
            if "wait" in argv:
                return proc.Result(argv, 0, "condition met", "")
            return proc.Result(argv, 0, json.dumps(answers.pop(0)), "")

        lab = state.Lab("ffl-e", Path("/r"), Path("/s"))
        real_sleep = bootstrap.time.sleep
        bootstrap.time.sleep = lambda _s: None
        try:
            bootstrap.wait_keycloak(kube.Kube(lab, FakeRunner(table={("kubectl",): kubectl})))
        finally:
            bootstrap.time.sleep = real_sleep
        self.assertEqual(answers, [])


class MirrorWhenDockerIsDown(unittest.TestCase):
    def test_mirror_reports_a_dead_daemon_as_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "versions.lock.yaml").write_text("# fixture\n")
            (root / "tiers.yaml").write_text("# fixture\n")
            runner = FakeRunner(docs={"versions.lock.yaml": LOCK, "tiers.yaml": TIERS},
                                table={("docker",): (1, "Cannot connect to the Docker daemon at unix:///x.sock")})
            doc = mirror.run_mirror(root, "core", runner=runner)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "registry")
        self.assertIn("Cannot connect", doc["checks"][-1]["message"])


if __name__ == "__main__":
    unittest.main()
