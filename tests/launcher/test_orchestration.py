# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The orchestration add-on tier: the airflow-fluid image is held to the lock, the credentials and
SeaweedFS identities are bucket-limited, `lab up` prepares the S3 prefixes before Airflow starts, and
`lab check --tier orchestration` judges a run on its marker, with fakes for S3, the REST API and kubectl."""

import ast
import base64
import datetime
import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

import support  # noqa: F401
from support import REPO_ROOT, FakeRunner, TIERS_DOC

from labctl import airflow, airflow_image, bootstrap, build, check, cli, cluster, config, credentials, http, proc, \
    s3, state, up
from labctl.output import FAIL, PASS

AIRFLOW_DIR = REPO_ROOT / "platform" / "airflow"
IMAGE_DIR = REPO_ROOT / build.IMAGES["airflow-fluid"]
RUNNER_DIR = REPO_ROOT / build.IMAGES["fluid-runner"]
LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
TIERS = config.load_yaml(REPO_ROOT / config.TIERS)
PIN = build.forge_pin(build.lock_entry(LOCK, "fluid-runner")["contents"])
FORGE_LOCAL = PIN["local_version"]
TAG = f"lab.local/airflow-fluid:{PIN['version']}-{PIN['commit'][:8]}"
NONCE = "abc123def456"


def response(status, body=b"", headers=None):
    if not isinstance(body, bytes):
        body = json.dumps(body).encode()
    return http.Response(status, body, headers or {})


# --------------------------------------------------------------------------- the image


class ImageAgreesWithTheLock(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entry = build.lock_entry(LOCK, "airflow-fluid")
        cls.contents = cls.entry["contents"]
        cls.runner = build.lock_entry(LOCK, "fluid-runner")["contents"]

    def test_forge_dbt_and_duckdb_are_the_fluid_runner_pins_typed_once(self):
        raw = next(b for b in LOCK["built_images"] if b["id"] == "airflow-fluid")
        self.assertEqual(raw["contents"], {"pins_from": "fluid-runner", "airflow": "3.2.2"})
        for key in airflow_image.PINNED:
            self.assertEqual(self.contents[key], self.runner[key])
        self.assertEqual(build.forge_pin(self.contents)["local_version"], FORGE_LOCAL)
        self.assertEqual(build.tag_for("airflow-fluid", self.contents), TAG)

    def test_the_requirements_are_byte_for_byte_the_fluid_runners(self):
        # Both images run Python 3.13 (airflow 3.2.2: 3.13.13), so one set of hashes serves both.
        for name in ("requirements.in", "requirements.txt", "requirements-build.txt", "record_forge.py"):
            self.assertEqual((IMAGE_DIR / name).read_bytes(), (RUNNER_DIR / name).read_bytes(), name)

    def test_the_base_is_the_airflow_image_of_the_lock_by_digest(self):
        airflow_entry = next(i for i in LOCK["images"] if i["id"] == "airflow")
        text = (IMAGE_DIR / "Dockerfile").read_text()
        self.assertEqual(self.entry["base"], {"ref": airflow_entry["ref"], "digest": airflow_entry["digest"]})
        self.assertIn(f"ARG AIRFLOW_IMAGE={airflow_entry['ref']}@{airflow_entry['digest']}\n", text)
        self.assertEqual(airflow_entry["tag"], self.contents["airflow"])

    def test_forge_enters_only_through_build_args_and_the_checked_archive(self):
        text = (IMAGE_DIR / "Dockerfile").read_text()
        pin = build.forge_pin(self.contents)
        for name in ("FORGE_VERSION", "FORGE_COMMIT", "FORGE_SHA256"):
            self.assertRegex(text, rf"(?m)^ARG {name}$")
        self.assertIn("ADD --checksum=sha256:${FORGE_SHA256} "
                      "https://github.com/Agenticstiger/forge-cli/archive/${FORGE_COMMIT}.tar.gz", text)
        self.assertNotIn(pin["commit"], text)
        self.assertIn("record_forge.py", text)
        self.assertIn("--require-hashes", text)
        self.assertIn("--no-deps --no-build-isolation", text)
        argv = build.build_argv(Path("/r"), "airflow-fluid", "t", "arm64", self.contents)
        args = [argv[i + 1] for i, a in enumerate(argv) if a == "--build-arg"]
        self.assertEqual(args, [f"FORGE_VERSION={FORGE_LOCAL}", f"FORGE_COMMIT={pin['commit']}",
                                f"FORGE_SHA256={pin['sha256']}"])

    def test_the_venv_is_apart_from_airflow_and_only_fluid_and_dbt_are_linked(self):
        text = (IMAGE_DIR / "Dockerfile").read_text()
        self.assertIn("python3 -m venv /opt/fluid/venv", text)
        self.assertIn("ln -s /opt/fluid/venv/bin/fluid /usr/local/bin/fluid", text)
        self.assertIn("ln -s /opt/fluid/venv/bin/dbt /usr/local/bin/dbt", text)
        self.assertNotRegex(text, r"ln -s /opt/fluid/venv/bin/(python|pip)")
        self.assertRegex(text, r"(?m)^USER 50000$")
        lines = [ln for ln in (IMAGE_DIR / ".dockerignore").read_text().splitlines() if ln and not ln.startswith("#")]
        self.assertEqual(lines[0], "*")
        for kept in lines[1:]:
            self.assertTrue((IMAGE_DIR / kept[1:]).is_file(), kept)

    def test_a_lock_entry_that_names_no_source_or_airflow_is_refused(self):
        for bad in ({"pins_from": "elsewhere", "airflow": "3.2.2"}, {"pins_from": "fluid-runner"}, None):
            lock = json.loads(json.dumps(LOCK))
            next(b for b in lock["built_images"] if b["id"] == "airflow-fluid")["contents"] = bad
            with self.assertRaises(config.ConfigError):
                build.lock_entry(lock, "airflow-fluid")
        lock = json.loads(json.dumps(LOCK))
        next(b for b in lock["built_images"] if b["id"] == "airflow-fluid")["base"]["digest"] = "latest"
        with self.assertRaises(config.ConfigError):
            build.lock_entry(lock, "airflow-fluid")

    def test_the_cli_builds_it(self):
        self.assertEqual(cli.BUILDABLE, tuple(build.IMAGES))
        self.assertEqual(cli._parser().parse_args(["build", "airflow-fluid"]).image, "airflow-fluid")


def good_probe() -> dict:
    return {"uid": 50000, "env": dict(airflow_image.ENV),
            "versions": {"data-product-forge": FORGE_LOCAL, "dbt-core": "1.10.23", "dbt-trino": "1.10.6",
                         "duckdb": "1.5.6"},
            "forge_source": {"record": {"commit": PIN["commit"], "archive_sha256": PIN["sha256"],
                                        "wheel": "w.whl", "wheel_sha256": "a" * 64},
                             "wheel_sha256": "a" * 64, "checked": 40, "mismatched": []},
            "fluid_cli": {"rc": 0, "out": "FLUID Forge CLI v0.32.1.dev0"}, "dbt_cli": {"rc": 0, "out": "dbt"},
            "airflow": {"airflow": "3.2.2", "amazon": "9.29.0", "bundle": "S3DagBundle"}, "airflow_tail": None}


class Probe(unittest.TestCase):
    contents = build.lock_entry(LOCK, "airflow-fluid")["contents"]

    def problems(self, **change):
        found = {**good_probe(), **change}
        return build.compare(self.contents, found, "airflow-fluid")

    def test_a_matching_image_has_no_problems(self):
        self.assertEqual(self.problems(), [])

    def test_each_difference_is_reported(self):
        cases = {
            "uid": (self.problems(uid=0), "uid 0"),
            "forge": (self.problems(versions={**good_probe()["versions"], "data-product-forge": "0.32.1.dev0"}),
                      "data-product-forge"),
            "commit": (self.problems(forge_source={**good_probe()["forge_source"], "record": {**good_probe()["forge_source"]["record"], "commit": "f" * 40}}), "commit"),
            "fluid": (self.problems(fluid_cli={"rc": 127, "out": "not found"}), "`fluid --version` failed"),
            "dbt": (self.problems(dbt_cli={"rc": 1, "out": "boom"}), "`dbt --version` failed"),
            "airflow import": (self.problems(airflow=None, airflow_tail="ModuleNotFoundError"), "S3DagBundle"),
            "airflow version": (self.problems(airflow={"airflow": "3.1.0", "amazon": "9", "bundle": "S3DagBundle"}),
                                "Airflow 3.1.0"),
            "telemetry": (self.problems(env={**airflow_image.ENV, "DBT_VERSION_CHECK": "true"}), "DBT_VERSION_CHECK"),
        }
        for what, (found, needle) in cases.items():
            with self.subTest(what):
                self.assertTrue(any(needle in p for p in found), found)

    def test_the_probe_runs_the_venv_python_with_no_network_and_no_capabilities(self):
        argv = build.probe_argv("sha256:" + "9" * 64, "arm64", "probe-1", "airflow-fluid")
        self.assertEqual(argv[argv.index("--network") + 1], "none")
        self.assertEqual(argv[argv.index("--cap-drop") + 1], "ALL")
        self.assertEqual(argv[argv.index("--entrypoint") + 1], "/opt/fluid/venv/bin/python")
        self.assertEqual(argv[-1], "/opt/fluid/forge/forge.json")
        ast.parse(airflow_image.PROBE)

    def test_a_checked_image_is_loaded_and_recorded(self):
        layers = ["sha256:" + "1" * 64]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            env = {"LAB_STATE_DIR": str(Path(tmp) / "state")}
            lab = state.lab_for(root, "ffl-x", env)
            from labctl import down
            down.mark_state_dir(lab)
            node = {"info": {"imageSpec": {"rootfs": {"diff_ids": layers}}}}
            facts = {"Id": "sha256:" + "9" * 64, "RootFS": {"Layers": layers}, "Os": "linux", "Architecture": "arm64"}
            runner = FakeRunner(docs={"versions.lock.yaml": LOCK}, table={
                ("docker", "version"): (0, "arm64\n"), ("docker", "build"): (0, ""),
                ("docker", "image", "inspect"): (0, json.dumps(facts)),
                ("docker", "run"): (0, json.dumps(good_probe()) + "\n"), ("docker", "inspect"): (0, "node-id\n"),
                ("kind", "get", "clusters"): (0, "ffl-x\n"), ("kind", "load", "docker-image"): (0, ""),
                ("docker", "exec"): (0, json.dumps(node))})
            (root / "versions.lock.yaml").write_text("# fixture\n")
            # run_build checks the build input against the lock (input_problem), so the image dir must be there
            shutil.copytree(IMAGE_DIR, root / build.IMAGES["airflow-fluid"])
            doc = build.run_build(root, "airflow-fluid", name="ffl-x", runner=runner, env=env)
            self.assertTrue(doc["ok"], doc)
            self.assertEqual(doc["tag"], TAG)
            record = state.read_json(lab.images_record)["airflow-fluid"]
            self.assertEqual((record["layers"], record["versions"]["airflow"]), (layers, "3.2.2"))
            self.assertIn("S3DagBundle", doc["checks"][1]["message"])
            # The chart's pods find it by that tag; the image check reads the same record.
            ok = airflow.loaded_image(runner, lab, LOCK)
            self.assertEqual(ok["tag"], doc["tag"])
            state.write_json_private(lab.images_record, {"airflow-fluid": {**record, "tag": "lab.local/airflow-fluid:x"}})
            with self.assertRaises(airflow.AirflowError) as caught:
                airflow.loaded_image(runner, lab, LOCK)
            self.assertIn("lab build airflow-fluid", caught.exception.advice)


# --------------------------------------------------------------------------- credentials, S3, identities


class Credentials(unittest.TestCase):
    def setUp(self):
        self.creds, _ = credentials.generate({})

    def test_the_airflow_credentials_are_generated_once_and_kept(self):
        for key in ("airflow.fernet_key", "airflow.api_secret_key", "airflow.jwt_secret", "airflow.admin_password",
                    "s3.airflow.access", "s3.airflow.secret", "s3.airflow_publish.access", "s3.airflow_publish.secret"):
            self.assertTrue(self.creds[key], key)
        again, changed = credentials.generate(self.creds)
        self.assertEqual((again, changed), (self.creds, False))
        self.assertEqual(len(base64.urlsafe_b64decode(self.creds["airflow.fernet_key"])), 32)

    def test_the_seaweedfs_identities_reach_only_the_airflow_bucket(self):
        ids = {i["name"]: i for i in credentials.seaweedfs_config(self.creds)["identities"]}
        self.assertEqual(ids["airflow"]["actions"], ["Read:lab-airflow", "List:lab-airflow"])
        self.assertEqual(set(ids["airflow-publish"]["actions"]),
                         {"Read:lab-airflow", "List:lab-airflow", "Write:lab-airflow", "Tagging:lab-airflow"})
        for name in ("airflow", "airflow-publish"):
            self.assertTrue(all(a.endswith(":lab-airflow") for a in ids[name]["actions"]), name)
        self.assertEqual(ids["airflow"]["credentials"][0]["accessKey"], self.creds["s3.airflow.access"])
        self.assertNotEqual(ids["airflow"]["credentials"][0]["accessKey"], ids["airflow-publish"]["credentials"][0]["accessKey"])

    def test_the_secrets_carry_the_charts_key_names_in_namespace_airflow_only(self):
        objs = credentials.secrets_for(self.creds, {"airflow", "lab-db"})
        mine = {o["metadata"]["name"]: o for o in objs if o["metadata"]["namespace"] == "airflow"}
        self.assertEqual({k: sorted(v["stringData"]) for k, v in mine.items() if k.startswith("airflow-")},
                         {"airflow-metadata": ["connection"], "airflow-fernet-key": ["fernet-key"],
                          "airflow-api-secret-key": ["api-secret-key"], "airflow-jwt-secret": ["jwt-secret"],
                          "airflow-simple-auth": ["passwords.json"], "airflow-s3-connection": ["AIRFLOW_CONN_LAB_S3"]})
        self.assertIn("lab-pg-airflow", mine)
        self.assertFalse([o for o in credentials.secrets_for(self.creds, {"lab-db"})
                          if o["metadata"]["name"].startswith("airflow-")])

    def test_the_metadata_uri_quotes_the_password_and_names_the_cnpg_role(self):
        creds = {**self.creds, "pg.airflow": "p/w:@#?"}
        uri = credentials.airflow_secrets(creds)["airflow-metadata"]["connection"]
        self.assertEqual(uri, "postgresql://airflow:p%2Fw%3A%40%23%3F@lab-pg-rw.lab-db.svc:5432/airflow?sslmode=require")

    def test_the_connection_names_seaweedfs_path_style_and_the_lab_ca(self):
        conn = json.loads(credentials.airflow_secrets(self.creds)["airflow-s3-connection"]["AIRFLOW_CONN_LAB_S3"])
        self.assertEqual((conn["conn_type"], conn["login"]), ("aws", self.creds["s3.airflow.access"]))
        self.assertEqual(conn["extra"]["endpoint_url"], "https://s3.lab.test")
        self.assertEqual(conn["extra"]["verify"], "/etc/lab-ca/ca.crt")
        self.assertEqual(conn["extra"]["config_kwargs"], {"s3": {"addressing_style": "path"}})
        self.assertNotIn("airflow-publish", json.dumps(conn))
        self.assertNotIn(self.creds["s3.airflow_publish.secret"], json.dumps(conn))

    def test_the_one_login_is_the_generated_password_never_a_default(self):
        auth = json.loads(credentials.airflow_secrets(self.creds)["airflow-simple-auth"]["passwords.json"])
        self.assertEqual(auth, {"lab-admin": self.creds["airflow.admin_password"]})
        self.assertNotIn(auth["lab-admin"], ("admin", "airflow", ""))


class SigV4(unittest.TestCase):
    ACCESS, SECRET = "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    WHEN = datetime.datetime(2013, 5, 24, tzinfo=datetime.timezone.utc)

    def test_aws_documented_get_object_example(self):
        # "Authenticating Requests: Using the Authorization Header (AWS Signature Version 4)", example 1.
        headers = s3.sign("GET", "examplebucket.s3.amazonaws.com", "/test.txt", {}, {"Range": "bytes=0-9"},
                          s3.EMPTY_SHA256, self.ACCESS, self.SECRET, self.WHEN)
        self.assertIn("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature="
                      "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", headers["Authorization"])

    def test_aws_documented_list_objects_example(self):
        headers = s3.sign("GET", "examplebucket.s3.amazonaws.com", "/", {"max-keys": "2", "prefix": "J"}, {},
                          s3.EMPTY_SHA256, self.ACCESS, self.SECRET, self.WHEN)
        self.assertIn("Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
                      headers["Authorization"])


class FakeSeaweed:
    """SeaweedFS's S3 for a bucket: identities by access key, objects, path-style, SigV4 headers present."""

    SECRETS = {"AK": "SK"}   # access key -> secret, for the signature check (tests add the lab's)

    def __init__(self, identities, objects=None, deny_unknown=0, assume=()):
        self.identities, self.objects, self.deny = identities, dict(objects or {}), deny_unknown
        self.assume = set(assume)   # access keys the role's trust policy names
        self.calls = []

    def _verify(self, method, host, path, headers, body, service="s3"):
        """Recompute SigV4 over what arrived, like the server: a wrong path, query, body or key is a bad signature."""
        bucket_and_key, _, query = path.partition("?")
        auth = headers.get("Authorization", "")
        access = auth.split("Credential=")[-1].split("/")[0]
        params = {urllib.parse.unquote(k): urllib.parse.unquote(v) for k, _, v in
                  (p.partition("=") for p in query.split("&") if p)}
        signed = {k: v for k, v in headers.items() if k.lower() == "content-type"}
        when = datetime.datetime.strptime(headers["x-amz-date"], "%Y%m%dT%H%M%SZ").replace(
            tzinfo=datetime.timezone.utc)
        want = s3.sign(method, host, bucket_and_key, params, signed, hashlib.sha256(body or b"").hexdigest(), access,
                       self.SECRETS.get(access, "SK"), when, service=service)["Authorization"]
        return access, want == auth

    def __call__(self, method, host, path, *, headers=None, body=None, timeout=0):
        if method == "POST" and path == "/":
            return self._sts(host, headers, body)
        bucket_and_key, _, query = path.partition("?")
        access, good = self._verify(method, host, path, headers, body)
        self.calls.append((method, bucket_and_key, query, access))
        assert host == "s3.lab.test" and bucket_and_key.startswith("/lab-airflow"), (host, path)
        if not good:
            return response(403, b"<Error><Code>SignatureDoesNotMatch</Code></Error>")
        if access not in self.identities or self.deny:
            self.deny = max(0, self.deny - 1)
            return response(403, b"<Error><Code>AccessDenied</Code></Error>")
        allowed = self.identities[access]
        key = urllib.parse.unquote(bucket_and_key[len("/lab-airflow/"):]) if bucket_and_key != "/lab-airflow" else ""
        if method == "GET" and not key:
            if "list" not in allowed:
                return response(403, b"denied")
            prefix = urllib.parse.unquote(dict(p.split("=") for p in query.split("&")).get("prefix", ""))
            keys = "".join(f"<Contents><Key>{k}</Key></Contents>" for k in sorted(self.objects) if k.startswith(prefix))
            return response(200, f"<ListBucketResult>{keys}</ListBucketResult>".encode())
        if method == "GET":
            return response(200, self.objects[key]) if "read" in allowed and key in self.objects else response(404)
        if "write" not in allowed:
            return response(403, b"denied")
        if method == "PUT":
            self.objects[key] = body or b""
            return response(200)
        self.objects.pop(key, None)
        return response(204)


    def _sts(self, host, headers, body):
        access, good = self._verify("POST", host, "/", headers, body, service="sts")
        self.calls.append(("STS", access))
        if not good:
            return response(403, b"<Error><Code>SignatureDoesNotMatch</Code></Error>")
        if access not in self.assume:
            return response(403, b"<Error><Code>AccessDenied</Code></Error>")
        return response(200, b"<AssumeRoleResponse><Credentials><AccessKeyId>ASIAX</AccessKeyId></Credentials>"
                             b"</AssumeRoleResponse>")


class Bucket(unittest.TestCase):
    def test_keys_with_awkward_characters_are_signed_and_stored_as_written(self):
        fake = FakeSeaweed({"AK": {"read", "list", "write"}})
        bucket = s3.Bucket(fake, "lab-airflow", "AK", "SK")
        for key in ("dags/\u00fc \u00e4+b.py", "projects/a/b c", "projects/x&y=z~1.sql"):
            bucket.put(key, key.encode())
            self.assertEqual(bucket.get(key), key.encode(), key)
        self.assertEqual(sorted(bucket.keys("")), sorted(fake.objects))
        self.assertEqual(bucket.keys("projects/a/"), ["projects/a/b c"])

    def test_a_request_signed_with_the_wrong_secret_is_refused_by_the_fake(self):
        bucket = s3.Bucket(FakeSeaweed({"AK": {"read", "list", "write"}}), "lab-airflow", "AK", "WRONG")
        with self.assertRaises(s3.S3Error) as caught:
            bucket.put("dags/x.py", b"")
        self.assertIn("SignatureDoesNotMatch", str(caught.exception))

    def test_put_get_list_delete_as_one_identity(self):
        fake = FakeSeaweed({"AK": {"read", "list", "write"}})
        bucket = s3.Bucket(fake, "lab-airflow", "AK", "SK")
        bucket.put("dags/a b.py", b"x = 1")
        self.assertEqual(bucket.get("dags/a b.py"), b"x = 1")
        self.assertEqual(bucket.keys("dags/"), ["dags/a b.py"])
        bucket.delete("dags/a b.py")
        self.assertIsNone(bucket.get("dags/a b.py"))
        self.assertEqual(fake.calls[0][1], "/lab-airflow/dags/a%20b.py")
        self.assertEqual({c[3] for c in fake.calls}, {"AK"})

    def test_a_refusal_names_the_status_and_never_the_secret(self):
        bucket = s3.Bucket(FakeSeaweed({"AK": {"read"}}), "lab-airflow", "AK", "SK-SECRET-VALUE")
        with self.assertRaises(s3.S3Error) as caught:
            bucket.put("dags/x.py", b"")
        self.assertEqual(caught.exception.status, 403)
        self.assertNotIn("SK-SECRET-VALUE", str(caught.exception))


# --------------------------------------------------------------------------- lab up


class FakeKube:
    def __init__(self, exec_ok=True):
        self.calls, self.applied, self.exec_ok = [], [], exec_ok

    def first_pod(self, namespace, selector, timeout_s=300):
        return "seaweedfs-master-0"

    def exec(self, namespace, pod, command, *, input=None, **_kw):  # noqa: A002
        self.calls.append(("exec", namespace, pod, input))
        return proc.Result(["kubectl"], 0 if self.exec_ok else 1, "", "")

    def run(self, *args, **_kw):
        self.calls.append(("run",) + args)
        return proc.Result(["kubectl"], 0, "", "")

    def apply(self, objs):
        self.applied += list(objs)


class Client:
    def __init__(self, send):
        self.https = send


class Prepare(unittest.TestCase):
    def setUp(self):
        self.creds, _ = credentials.generate({})
        c = self.creds
        FakeSeaweed.SECRETS.update({c["s3.admin.access"]: c["s3.admin.secret"],
                                    c["s3.airflow.access"]: c["s3.airflow.secret"],
                                    c["s3.airflow_publish.access"]: c["s3.airflow_publish.secret"],
                                    c["s3.lakekeeper.access"]: c["s3.lakekeeper.secret"]})
        self.identities = {c["s3.admin.access"]: {"read", "list", "write"}, c["s3.airflow.access"]: {"read", "list"},
                           c["s3.airflow_publish.access"]: {"read", "list", "write"}}
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_it_makes_the_bucket_and_the_prefix_markers_and_proves_the_read_only_identity(self):
        fake, kube = FakeSeaweed(self.identities), FakeKube()
        message = airflow.prepare(kube, Client(fake), self.creds, self.root, sleep=lambda _s: None)
        self.assertEqual(sorted(fake.objects), ["dags/.lab-keep", "projects/.lab-keep"])
        self.assertIn("s3.bucket.create -name lab-airflow", kube.calls[0][3])
        self.assertIn("identity `airflow` reads them", message)
        again = airflow.prepare(kube, Client(fake), self.creds, self.root, sleep=lambda _s: None)
        self.assertIn("exist", again)
        self.assertEqual([c for c in fake.calls if c[0] == "PUT"].__len__(), 2, "markers are written once")

    def test_a_gateway_that_has_not_loaded_the_new_identities_is_restarted_once(self):
        # SeaweedFS reads identities at start: the airflow key is refused until its S3 gateway restarts.
        refused = {"n": 1}
        fake = FakeSeaweed(self.identities)
        real = fake.__call__

        def send(method, host, path, **kw):
            access = kw["headers"]["Authorization"].split("Credential=")[1].split("/")[0]
            if access == self.creds["s3.airflow.access"] and refused["n"]:
                refused["n"] -= 1
                return response(403, b"denied")
            return real(method, host, path, **kw)

        kube = FakeKube()
        airflow.prepare(kube, Client(send), self.creds, self.root, sleep=lambda _s: None)
        restarts = [c for c in kube.calls if c[:1] == ("run",) and "restart" in c]
        self.assertEqual(len(restarts), 1)
        self.assertIn("deploy/seaweedfs-s3", restarts[0])

    def test_an_identity_that_stays_refused_fails_with_what_to_do(self):
        fake = FakeSeaweed({k: v for k, v in self.identities.items() if k != self.creds["s3.airflow.access"]})
        with self.assertRaises(airflow.AirflowError) as caught:
            airflow.prepare(FakeKube(), Client(fake), self.creds, self.root, sleep=lambda _s: None)
        self.assertIn("cannot list", str(caught.exception))
        self.assertIn("lab up", caught.exception.advice)

    def test_a_bucket_that_cannot_be_made_stops_the_step(self):
        with self.assertRaises(bootstrap.BootstrapError):
            airflow.prepare(FakeKube(exec_ok=False), Client(FakeSeaweed(self.identities)), self.creds, self.root)

    def test_the_spark_submit_files_are_copied_into_airflows_namespace_when_the_checkout_has_them(self):
        self.assertIsNone(airflow.spark_configmap(self.root))
        spark = self.root / "platform" / "spark"
        spark.mkdir(parents=True)
        (spark / "spark-defaults.apache.conf").write_text("spark.a b\n")
        self.assertIsNone(airflow.spark_configmap(self.root), "no spark-submit, no copy")
        (spark / "lab-spark-submit").write_text("#!/bin/sh\n")
        cm = airflow.spark_configmap(self.root)
        self.assertEqual((cm["metadata"]["name"], cm["metadata"]["namespace"]), ("lab-spark", "airflow"))
        self.assertEqual(sorted(cm["data"]), ["lab-spark-submit", "spark-defaults.conf"])
        kube = FakeKube()
        airflow.prepare(kube, Client(FakeSeaweed(self.identities)), self.creds, self.root)
        self.assertEqual([o["metadata"]["name"] for o in kube.applied], ["lab-spark"])


class UpWaves(unittest.TestCase):
    def make(self, tier):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name) / "repo"
        root.mkdir()
        runner = FakeRunner(table={("helmfile",): (0, "")})
        run = up.Up(root, tier, name="ffl-e", env={"LAB_STATE_DIR": str(Path(tmp.name) / "state")}, runner=runner)
        run.tiers_doc, run.lock = TIERS, LOCK
        run.tier = config.resolve_tier(TIERS, tier)
        run.lab.state.mkdir(parents=True)
        return run, runner

    def selectors(self, runner):
        return [argv[argv.index("-l") + 1] for argv, _ in runner.calls if argv[0] == "helmfile"]

    def test_the_tier_is_supported(self):
        self.assertIn("orchestration", up.SUPPORTED_TIERS)
        self.assertNotIn("airflow", config.resolve_tier(TIERS, "core").releases)

    def test_airflow_is_the_last_wave_after_its_image_and_its_prefixes(self):
        run, runner = self.make("orchestration")
        order = []
        with mock.patch.object(airflow, "loaded_image",
                               side_effect=lambda *a: order.append("image") or {"tag": "lab.local/airflow-fluid:t"}), \
                mock.patch.object(airflow, "prepare", side_effect=lambda *a: order.append("prepare") or "prepared"), \
                mock.patch.object(bootstrap, "wait_keycloak", side_effect=lambda *a: order.append("keycloak")), \
                mock.patch.object(bootstrap, "gateway") as gateway:
            gateway.return_value.__enter__.return_value = mock.Mock(port=4443)
            run.helmfile()
        self.assertEqual(self.selectors(runner), ["component!=lakekeeper,component!=airflow", "component=lakekeeper",
                                                  "component=airflow"])
        self.assertEqual(order, ["image", "keycloak", "prepare"])
        # Every wave names the loaded image; the chart runs it, not the pinned base the offline render shows.
        for argv, _ in runner.calls:
            self.assertEqual(argv[argv.index("--state-values-set") + 1], "labImages.airflow=lab.local/airflow-fluid:t")

    def test_a_tier_without_airflow_keeps_its_two_waves_and_never_asks_for_the_image(self):
        run, runner = self.make("core")
        with mock.patch.object(airflow, "loaded_image") as image, mock.patch.object(bootstrap, "wait_keycloak"):
            run.helmfile()
        self.assertEqual(self.selectors(runner), ["component!=lakekeeper", "component=lakekeeper"])
        self.assertFalse([a for argv, _ in runner.calls for a in argv if "labImages" in a])
        image.assert_not_called()

    def test_a_missing_image_stops_before_any_helm_work(self):
        run, runner = self.make("orchestration")
        with mock.patch.object(airflow, "loaded_image", side_effect=airflow.AirflowError("no image", "build it")):
            with self.assertRaises(airflow.AirflowError):
                run.helmfile()
        self.assertEqual(self.selectors(runner), [])


class Namespaces(unittest.TestCase):
    def test_airflow_enforces_restricted_and_spark_exists_for_the_scheduler_role(self):
        doc = config.load_yaml(REPO_ROOT / "platform/namespaces.yaml")["namespaces"]
        self.assertEqual(doc["airflow"]["pod_security"], "restricted")
        self.assertNotIn("exemption", doc["airflow"])
        runner = FakeRunner(docs={"namespaces.yaml": {"namespaces": doc}})
        for tier, spark_there in (("core", False), ("spark", True), ("orchestration", True)):
            got = cluster.tier_namespaces(REPO_ROOT, runner, TIERS, tier)
            self.assertEqual("spark" in got, spark_there, tier)
            self.assertEqual("airflow" in got, tier == "orchestration", tier)
        self.assertEqual(doc["spark"]["workloads"], "on-demand")


# --------------------------------------------------------------------------- lab check


class FakeApi:
    """Airflow's REST API for one DAG: it lists after `listed_after` polls and runs to `outcome`."""

    def __init__(self, nonce=NONCE, forge=FORGE_LOCAL, outcome="success", listed_after=1, log="ok", xcom=True,
                 old_copy=0, drop=0, tag=NONCE):
        self.nonce, self.forge, self.outcome, self.listed_after, self.log, self.xcom = (nonce, forge, outcome,
                                                                                         listed_after, log, xcom)
        self.old_copy, self.drop, self.tag = old_copy, drop, tag   # polls an earlier upload's DAG answers; dropped calls
        self.calls, self.polls, self.deleted, self.ran = [], 0, False, False
        self.token = ""

    def line(self):
        return "LAB-ORCH " + json.dumps({"nonce": self.nonce, "forge": self.forge, "fluid": "FLUID Forge CLI v0.32.1.dev0",
                                         "project_dir": "/opt/fluid/projects", "dbt_profiles": "/opt/lab/dbt",
                                         "s3_endpoint": "https://s3.lab.test"})

    def call(self, method, path, body=None, accept="application/json", timeout=60):
        self.calls.append((method, path))
        if self.drop and method == "GET":
            self.drop -= 1
            raise http.HTTPError("connection reset by peer")
        dag = "/api/v2/dags/lab_check_orchestration"
        if method == "GET" and path == dag:
            self.polls += 1
            if self.deleted or self.polls <= self.listed_after:
                return response(404, {"detail": "not found"})
            # An earlier check's copy is listed (with its own nonce) until the bundle refreshes.
            tags = [{"name": "lab-check"}, {"name": f"nonce-{'old000' if self.polls <= self.listed_after + self.old_copy else self.tag}"}]
            # Once the run is over the test deletes the file; the bundle refresh then marks the DAG stale.
            return response(200, {"dag_id": "lab_check_orchestration", "bundle_name": "lab-s3-dags",
                                  "is_stale": self.ran, "tags": tags})
        if method == "PATCH":
            return response(200, {})
        if method == "POST":
            return response(200, {"dag_run_id": "manual__1"})
        if method == "DELETE":
            self.deleted = True
            return response(204)
        if path.endswith("/dagRuns/manual__1"):
            self.ran = True
            return response(200, {"state": self.outcome})
        if path.endswith("/taskInstances/fluid_version"):
            return response(200, {"try_number": 1, "state": self.outcome})
        if "/logs/" in path:
            if self.log is None:
                return response(404, {"detail": "no log"})
            if self.log == "unserved":
                return response(200, {"content": [{"event": "*** Could not read served logs: Name or service not known"}]})
            events = ["Task started", "LAB-ORCH-FLUID FLUID Forge CLI v0.32.1.dev0", self.line()] if self.log == "ok" \
                else ["Task failed: exit 1"]
            return response(200, {"content": [{"event": e, "level": "info"} for e in events]})
        if path.endswith("/xcomEntries/return_value"):
            return response(200, {"value": self.line()}) if self.xcom else response(404)
        raise AssertionError((method, path))


def probe(api, bucket=None, **kw):
    bucket = bucket or s3.Bucket(FakeSeaweed({"AK": {"read", "list", "write"}}), "lab-airflow", "AK", "SK")
    clock = iter(range(0, 100000, 1))
    return airflow.run_probe(api, bucket, b"dag", NONCE, FORGE_LOCAL, clock=lambda: next(clock), sleep=lambda _s: None,
                             **kw), bucket


class RunProbe(unittest.TestCase):
    def test_a_listed_triggered_successful_run_with_this_runs_marker_passes_and_cleans_up(self):
        api = FakeApi(listed_after=2)
        result, bucket = probe(api)
        self.assertEqual((result["run"], result["bundle"], result["evidence"]), ("manual__1", "lab-s3-dags", "task log"))
        self.assertEqual((result["forge"], result["project_dir"]), (FORGE_LOCAL, "/opt/fluid/projects"))
        self.assertIn("FLUID Forge CLI", result["fluid_version"])
        self.assertIn("deleted", result["cleanup"])
        self.assertEqual(bucket.keys("dags/"), [], "the probe DAG is gone from S3")
        self.assertTrue(api.deleted)
        methods = [c[0] for c in api.calls]
        self.assertLess(methods.index("PATCH"), methods.index("POST"), "unpaused before it is triggered")

    def test_a_log_the_api_cannot_serve_falls_back_to_the_tasks_xcom(self):
        result, _ = probe(FakeApi(log=None))
        self.assertIn("XCom", result["evidence"])

    def test_a_marker_from_another_run_or_another_forge_fails(self):
        for api, needle in ((FakeApi(nonce="someone-else"), "another run's nonce"),
                            (FakeApi(forge="0.32.0"), "the lock pins")):
            with self.subTest(needle), self.assertRaises(airflow.AirflowError) as caught:
                probe(api)
            self.assertIn(needle, str(caught.exception))
            self.assertIn("clean-up", str(caught.exception))

    def test_a_failed_run_fails_with_the_logs_tail_and_still_cleans_up(self):
        api = FakeApi(outcome="failed", log="boom")
        bucket = s3.Bucket(FakeSeaweed({"AK": {"read", "list", "write"}}, {"dags/lab-check/lab_check_orchestration.py": b"x"}),
                           "lab-airflow", "AK", "SK")
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(api, bucket)
        self.assertIn("ended failed", str(caught.exception))
        self.assertIn("Task failed", str(caught.exception))
        self.assertEqual(bucket.keys("dags/"), [])

    def test_a_dag_airflow_never_lists_times_out_and_the_upload_is_removed(self):
        api = FakeApi(listed_after=10**6)
        bucket = s3.Bucket(FakeSeaweed({"AK": {"read", "list", "write"}}), "lab-airflow", "AK", "SK")
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(api, bucket)
        self.assertIn("did not list lab_check_orchestration", str(caught.exception))
        self.assertIn("dag-processor", caught.exception.advice)
        self.assertEqual(bucket.keys("dags/"), [])

    def test_a_run_with_no_readable_output_is_not_a_pass(self):
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(FakeApi(log=None, xcom=False))
        self.assertIn("neither its log nor its XCom", str(caught.exception))

    def test_an_upload_the_identity_may_not_make_fails_and_leaves_nothing(self):
        bucket = s3.Bucket(FakeSeaweed({"AK": {"read", "list"}}), "lab-airflow", "AK", "SK")
        with self.assertRaises(s3.S3Error):
            probe(FakeApi(), bucket)

    def test_login_failure_names_the_user_and_hides_the_password(self):
        api = airflow.Api(1, send=lambda *a, **k: response(401, {"detail": "bad"}))
        with self.assertRaises(airflow.AirflowError) as caught:
            api.login("lab-admin", "hunter2-value")
        self.assertIn("lab-admin", str(caught.exception))
        self.assertNotIn("hunter2-value", str(caught.exception))
        seen = {}
        ok = airflow.Api(1, send=lambda method, url, **k: seen.update(url=url, headers=k["headers"]) or response(
            201, {"access_token": "t0k"}))
        ok.login("lab-admin", "pw")
        self.assertEqual(ok.token, "t0k")
        self.assertEqual(seen["url"], "http://127.0.0.1:1/auth/token")


class ProbeDag(unittest.TestCase):
    def test_the_dag_file_carries_exactly_one_nonce_line_and_compiles(self):
        text = airflow.probe_source(REPO_ROOT, NONCE).decode()
        tree = ast.parse(text)
        self.assertIn(f'NONCE = "{NONCE}"', text)
        self.assertNotIn('"LAB_NONCE"', text)
        self.assertIn("is_paused_upon_creation=False", text)
        self.assertIn("from airflow.sdk import DAG", text)
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        self.assertLessEqual({"BashOperator", "DAG"}, names)
        self.assertIn(f'dag_id="{airflow.DAG_ID}"', text)
        self.assertIn(f'task_id="{airflow.TASK_ID}"', text)

    def test_a_dag_file_without_the_nonce_line_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "platform/airflow").mkdir(parents=True)
            (Path(tmp) / airflow.PROBE_SOURCE).write_text("NONCE = 'x'\n")
            with self.assertRaises(airflow.AirflowError):
                airflow.probe_source(Path(tmp), NONCE)

    def test_the_marker_the_dag_prints_is_what_judge_reads(self):
        text = airflow.probe_source(REPO_ROOT, NONCE).decode()
        self.assertIn("'LAB-ORCH ' + json.dumps", text)
        self.assertIn("m.version('data-product-forge')", text)
        mark = {"nonce": NONCE, "forge": FORGE_LOCAL, "fluid": "FLUID Forge CLI v0.32.1.dev0"}
        found = airflow.judge(f"x\nLAB-ORCH-FLUID FLUID Forge CLI v0.32.1.dev0\nLAB-ORCH {json.dumps(mark)}\n",
                              NONCE, FORGE_LOCAL)
        self.assertEqual((found["forge"], found["fluid_version"]), (FORGE_LOCAL, "FLUID Forge CLI v0.32.1.dev0"))
        self.assertIn("exit 1", text, "a failing `fluid --version` fails the task")
        with self.assertRaises(airflow.AirflowError):
            airflow.judge(f"LAB-ORCH {json.dumps({'nonce': NONCE, 'forge': FORGE_LOCAL})}\n", NONCE, FORGE_LOCAL)


class Record(unittest.TestCase):
    def test_the_record_exists_only_on_tiers_that_include_airflow(self):
        core = check.records_for(config.resolve_tier(TIERS, "core"))
        orch = check.records_for(config.resolve_tier(TIERS, "orchestration"))
        spark = check.records_for(config.resolve_tier(TIERS, "spark"))
        self.assertNotIn("orchestration", core)
        self.assertNotIn("orchestration", spark)
        self.assertEqual(orch, core + ["orchestration"])
        self.assertIn("orchestration", check.records_for(config.resolve_tier(TIERS, "full")))
        self.assertIs(check.CHECKS["orchestration"], check.orchestration)

    def ctx(self, tmp, creds):
        root = Path(tmp) / "repo"
        root.mkdir(exist_ok=True)
        lab = state.lab_for(root, "ffl-x", {"LAB_STATE_DIR": str(Path(tmp) / "state")})
        return check.Ctx(root, lab, FakeRunner(), mock.Mock(), LOCK, TIERS_DOC, "orchestration",
                         config.resolve_tier(TIERS, "orchestration"), creds=creds, client=Client(FakeSeaweed({})))

    def test_it_fails_when_the_image_was_never_built(self):
        with tempfile.TemporaryDirectory() as tmp:
            rec = check._record("orchestration", check.orchestration, self.ctx(tmp, {}))
        self.assertEqual(rec["status"], FAIL)
        self.assertIn(TAG, rec["message"])
        self.assertIn("lab build airflow-fluid", rec["advice"])

    def test_it_fails_for_a_lab_that_predates_the_tier(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(airflow, "loaded_image", return_value={"tag": "lab.local/airflow-fluid:x"}):
                rec = check._record("orchestration", check.orchestration, self.ctx(tmp, {"airflow.admin_password": "p"}))
        self.assertEqual(rec["status"], FAIL)
        self.assertIn("predates the orchestration tier", rec["message"])

    def test_a_passing_probe_is_one_record_that_names_the_evidence_without_secrets(self):
        creds, _ = credentials.generate({})
        result = {"run": "manual__1", "bundle": "lab-s3-dags", "evidence": "task log", "fluid_version": "FLUID v1",
                  "forge": FORGE_LOCAL, "project_dir": "/opt/fluid/projects", "dbt_profiles": "/opt/lab/dbt",
                  "cleanup": "deleted"}
        session = mock.MagicMock()
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(airflow, "loaded_image", return_value={"tag": "lab.local/airflow-fluid:t"}), \
                mock.patch.object(airflow, "api_session", return_value=session), \
                mock.patch.object(airflow, "assumerole_refused", return_value="AssumeRole refused"), \
                mock.patch.object(airflow, "run_probe", return_value=result) as run:
            ctx = self.ctx(tmp, creds)
            (ctx.root / "platform/airflow").mkdir(parents=True)
            (ctx.root / airflow.PROBE_SOURCE).write_text('NONCE = "LAB_NONCE"\n')
            rec = check._record("orchestration", check.orchestration, ctx)
        self.assertEqual(rec["status"], PASS, rec)
        self.assertIn("manual__1", rec["message"])
        self.assertIn(FORGE_LOCAL, rec["message"])
        for secret in (creds["airflow.admin_password"], creds["s3.airflow_publish.secret"]):
            self.assertNotIn(secret, json.dumps(rec))
        bucket = run.call_args.args[1]
        self.assertEqual(bucket.access, creds["s3.airflow_publish.access"])
        self.assertEqual(run.call_args.args[4], FORGE_LOCAL)

    def test_an_airflow_error_is_one_failure_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = self.ctx(tmp, {})
            with mock.patch.object(airflow, "loaded_image", side_effect=airflow.AirflowError("x", "do y")):
                rec = check._record("orchestration", check.orchestration, ctx)
        self.assertEqual((rec["status"], rec["advice"]), (FAIL, "do y"))


# --------------------------------------------------------------------------- the platform files


class Values(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.values = config.load_yaml(REPO_ROOT / "platform/components/airflow/values.yaml")

    def test_localexecutor_with_the_three_components_and_nothing_else(self):
        v = self.values
        self.assertEqual(v["executor"], "LocalExecutor")
        for off in ("redis", "statsd", "postgresql", "pgbouncer", "triggerer", "createUserJob"):
            self.assertFalse(v[off]["enabled"], off)
        self.assertTrue(v["dagProcessor"]["enabled"])
        self.assertFalse(v["allowPodLaunching"])
        self.assertFalse(v["workers"]["persistence"]["enabled"])

    def test_the_dag_bundle_is_s3_on_the_lab_bucket_through_the_lab_connection(self):
        (bundle,) = self.values["dagProcessor"]["dagBundleConfigList"]
        self.assertEqual(bundle["classpath"], "airflow.providers.amazon.aws.bundles.s3.S3DagBundle")
        self.assertEqual((bundle["kwargs"]["bucket_name"], bundle["kwargs"]["prefix"], bundle["kwargs"]["aws_conn_id"]),
                         ("lab-airflow", "dags/", "lab_s3"))
        self.assertEqual(self.values["secret"][0]["envName"], "AIRFLOW_CONN_LAB_S3")

    def test_every_secret_is_named_never_written_here(self):
        text = (REPO_ROOT / "platform/components/airflow/values.yaml").read_text()
        v = self.values
        self.assertEqual((v["data"]["metadataSecretName"], v["fernetKeySecretName"], v["apiSecretKeySecretName"],
                          v["jwtSecretName"]), ("airflow-metadata", "airflow-fernet-key", "airflow-api-secret-key",
                                                "airflow-jwt-secret"))
        self.assertNotRegex(text, r"(?i)password\s*:\s*\S|secret_key\s*:\s*\S|fernetKey\s*:\s*\S")
        self.assertEqual(self.values["config"]["core"]["simple_auth_manager_users"], "lab-admin:admin")

    def test_the_pods_meet_the_restricted_profile(self):
        sc = self.values["securityContexts"]
        self.assertTrue(sc["pod"]["runAsNonRoot"])
        self.assertEqual(sc["pod"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertEqual((sc["containers"]["allowPrivilegeEscalation"], sc["containers"]["capabilities"]["drop"]),
                         (False, ["ALL"]))
        sidecar = self.values["scheduler"]["extraContainers"][0]
        self.assertEqual(sidecar["securityContext"], sc["containers"])

    def test_what_fluid_needs_passes_the_generated_dags_env_allowlist(self):
        # forge e37cdda5 fluid_build/schedulers/airflow/fluid_apply.py PASSTHROUGH_PREFIXES / PASSTHROUGH_NAMES (subset).
        prefixes = ("FLUID_", "AWS_", "DBT_", "TF_", "OTEL_")
        names = {"PATH", "HOME", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "TZ"}
        env = {e["name"]: e["value"] for e in self.values["scheduler"]["env"]}
        for name in ("FLUID_PROJECT_DIR", "FLUID_SPARK_SUBMIT_EXECUTABLE", "FLUID_SPARK_SUBMIT_PROPERTIES_FILE",
                     "FLUID_SPARK_LOCAL_ROOT", "AWS_ENDPOINT_URL_S3", "AWS_CA_BUNDLE", "DBT_PROFILES_DIR",
                     "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
            self.assertIn(name, env)
            self.assertTrue(name.startswith(prefixes) or name in names, name)
        # The Airflow connection (a credential) is not passed: its name starts with AIRFLOW, which never passes.
        self.assertFalse("AIRFLOW_CONN_LAB_S3".startswith(prefixes))
        self.assertEqual(env["FLUID_SPARK_SUBMIT_EXECUTABLE"], "/spark/lab-spark-submit")
        self.assertEqual(env["AWS_ENDPOINT_URL_S3"], "https://s3.lab.test")
        mounts = {m["name"]: m["mountPath"] for m in self.values["scheduler"]["extraVolumeMounts"]}
        self.assertEqual(mounts["lab-spark"], "/spark")
        spark = next(v for v in self.values["scheduler"]["extraVolumes"] if v["name"] == "lab-spark")
        self.assertTrue(spark["configMap"]["optional"])

    def test_the_image_is_the_pinned_base_offline_and_the_lab_built_one_when_lab_up_says_so(self):
        helmfile = (REPO_ROOT / "platform/helmfile.yaml.gotmpl").read_text()
        self.assertIn("--state-values-set labImages.<name>", helmfile)
        self.assertIn(".Values.labImages.airflow", helmfile)
        self.assertIn("pullPolicy: Never", helmfile)
        self.assertIn('(index $img "airflow").digest', helmfile, "offline renders keep the lock's digest")
        self.assertNotIn("defaultAirflowRepository", (REPO_ROOT / "platform/components/airflow/values.yaml").read_text())


class ObjectsAndScripts(unittest.TestCase):
    def test_the_scheduler_may_manage_pods_in_spark_and_nothing_else(self):
        text = (REPO_ROOT / "platform/components/lab-airflow/values.yaml.gotmpl").read_text()
        self.assertIn("verbs: [create, get, list, watch, delete]", text)
        self.assertIn("resources: [pods/log]\n        verbs: [get]", text)
        self.assertIn("{kind: ServiceAccount, name: airflow-scheduler, namespace: airflow}", text)
        self.assertNotIn("ClusterRole", text)
        self.assertNotIn("secrets", text)
        for name in ("profiles.yml", "sync_projects.py"):
            self.assertIn(f'"{name}"', text)
            self.assertNotIn("{{", (AIRFLOW_DIR / name).read_text(), name)

    def test_the_dbt_profile_is_trino_target_local_with_no_credential(self):
        profile = config.load_yaml(AIRFLOW_DIR / "profiles.yml")["fluid"]
        out = profile["outputs"]["local"]
        self.assertEqual(profile["target"], "local")
        self.assertEqual((out["type"], out["method"], out["host"], out["database"], out["user"]),
                         ("trino", "none", "trino.trino.svc", "lakehouse", "fluid-pipeline"))
        self.assertNotIn("password", out)

    def test_tiers_yaml_describes_the_tier_as_it_runs(self):
        tier = TIERS["tiers"]["orchestration"]
        self.assertEqual(tier["releases"], ["airflow"])
        self.assertIn("S3", tier["description"])


class SyncProjects(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("sync_projects", AIRFLOW_DIR / "sync_projects.py")
        cls.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.mod)

    class FakeS3:
        def __init__(self, objects):
            self.objects, self.fail = objects, False

        def get_paginator(self, _name):
            return self

        def paginate(self, Bucket, Prefix):  # noqa: N803
            if self.fail:
                raise OSError("endpoint down")
            yield {"Contents": [{"Key": k, "ETag": f'"{hashlib.md5(v).hexdigest()}"'} for k, v in sorted(
                self.objects.items()) if k.startswith(Prefix)]}

        def download_file(self, bucket, key, filename):
            Path(filename).write_bytes(self.objects[key])

    def test_a_pass_mirrors_the_prefix_updates_changes_and_deletes_what_left(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest, seen = Path(tmp) / "projects", {}
            s3c = self.FakeS3({"projects/.lab-keep": b"", "projects/contract.fluid.yaml": b"a",
                               "projects/sql/m.sql": b"select 1", "dags/other.py": b"no"})
            self.assertEqual(self.mod.sync(s3c, "lab-airflow", "projects/", dest, seen), {"downloaded": 2, "deleted": 0, "skipped": 0})
            self.assertEqual((dest / "sql/m.sql").read_text(), "select 1")
            self.assertFalse((dest / ".lab-keep").exists())
            self.assertFalse((dest / "other.py").exists())
            self.assertEqual(self.mod.sync(s3c, "lab-airflow", "projects/", dest, seen), {"downloaded": 0, "deleted": 0, "skipped": 0})
            s3c.objects["projects/contract.fluid.yaml"] = b"b"
            del s3c.objects["projects/sql/m.sql"]
            self.assertEqual(self.mod.sync(s3c, "lab-airflow", "projects/", dest, seen), {"downloaded": 1, "deleted": 1, "skipped": 0})
            self.assertEqual((dest / "contract.fluid.yaml").read_text(), "b")
            self.assertFalse((dest / "sql/m.sql").exists())
            self.assertFalse(list(dest.rglob("*.part")))

    def test_a_pass_that_cannot_list_deletes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest, seen = Path(tmp) / "p", {}
            s3c = self.FakeS3({"projects/a": b"1"})
            self.mod.sync(s3c, "b", "projects/", dest, seen)
            s3c.fail = True
            with self.assertRaises(OSError):
                self.mod.sync(s3c, "b", "projects/", dest, seen)
            self.assertTrue((dest / "a").is_file())

    def test_a_key_that_climbs_out_of_the_directory_is_skipped_and_the_pass_goes_on(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest, seen = Path(tmp) / "p", {}
            s3c = self.FakeS3({"projects/../../evil": b"x", "projects/a": b"1", "projects/a/b": b"2",
                               "projects/ok.sql": b"3"})
            done = self.mod.sync(s3c, "b", "projects/", dest, seen)
            self.assertEqual((done["skipped"], (dest / "ok.sql").read_text()), (2, "3"))
            self.assertFalse((Path(tmp) / "evil").exists())
            # The bad key neither stops the deletes: a file that left S3 goes even while a key keeps failing.
            del s3c.objects["projects/ok.sql"]
            self.assertEqual(self.mod.sync(s3c, "b", "projects/", dest, seen)["deleted"], 1)
            self.assertFalse((dest / "ok.sql").exists())

    def test_after_a_restart_files_deleted_from_s3_in_the_gap_are_removed_on_the_first_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "p"
            (dest / "old").mkdir(parents=True)
            (dest / "old/gone.sql").write_text("x")
            (dest / "stale.yaml.part").write_text("half")
            (dest / "keep.sql").write_text("k")
            s3c = self.FakeS3({"projects/keep.sql": b"k"})
            # A process with no memory of what it downloaded (`seen` empty), as after a sidecar restart.
            done = self.mod.sync(s3c, "b", "projects/", dest, {}, first=True)
            self.assertEqual(sorted(p.relative_to(dest).as_posix() for p in dest.rglob("*") if p.is_file()),
                             ["keep.sql"])
            self.assertEqual(done["deleted"], 2)
            (dest / "later.sql").write_text("y")   # later passes trust `seen`, and leave what they did not write
            self.mod.sync(s3c, "b", "projects/", dest, {"keep.sql": '"x"'}, first=False)
            self.assertTrue((dest / "later.sql").exists())

    def test_the_client_is_path_style_with_the_connections_endpoint_and_ca(self):
        boto3 = mock.MagicMock()
        botocore = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"boto3": boto3, "botocore": botocore, "botocore.config": botocore.config}):
            creds, _ = credentials.generate({})
            raw = credentials.airflow_secrets(creds)["airflow-s3-connection"]["AIRFLOW_CONN_LAB_S3"]
            self.mod.client_from_connection(raw)
        kwargs = boto3.client.call_args.kwargs
        self.assertEqual((kwargs["endpoint_url"], kwargs["verify"]), ("https://s3.lab.test", "/etc/lab-ca/ca.crt"))
        self.assertEqual(botocore.config.Config.call_args.kwargs, {"s3": {"addressing_style": "path"}})


class ReviewRound1(unittest.TestCase):
    def test_the_vended_roles_trust_names_lakekeeper_only_never_a_wildcard(self):
        creds, _ = credentials.generate({})
        (role,) = credentials.seaweedfs_config(creds)["roles"]
        (statement,) = role["trustPolicy"]["Statement"]
        self.assertNotEqual(statement["Principal"], "*")
        self.assertEqual(statement["Principal"], {"AWS": "arn:aws:iam::000000000000:user/lakekeeper"})
        self.assertEqual(statement["Principal"]["AWS"], credentials.LAKEKEEPER_PRINCIPAL)
        self.assertNotIn("*", json.dumps(role["trustPolicy"]))

    def assume(self, allowed):
        creds, _ = credentials.generate({})
        FakeSeaweed.SECRETS.update({creds[f"s3.{n}.access"]: creds[f"s3.{n}.secret"]
                                    for n in ("airflow", "airflow_publish", "lakekeeper")})
        fake = FakeSeaweed({}, assume={creds[f"s3.{n}.access"] for n in allowed})
        return creds, fake

    def test_airflows_keys_are_refused_assumerole_and_lakekeepers_is_the_positive_control(self):
        creds, fake = self.assume(["lakekeeper"])
        note = airflow.assumerole_refused(fake, creds)
        self.assertIn("refused for identities airflow and airflow-publish", note)
        self.assertEqual([c[0] for c in fake.calls], ["STS"] * 3)
        self.assertNotIn("ASIAX", note)

    def test_an_airflow_key_that_can_assume_the_role_fails_the_record(self):
        for leaky in ("airflow", "airflow_publish"):
            creds, fake = self.assume(["lakekeeper", leaky])
            with self.assertRaises(airflow.AirflowError) as caught:
                airflow.assumerole_refused(fake, creds)
            self.assertIn("whole warehouse", str(caught.exception))

    def test_a_refusal_proves_nothing_when_the_positive_control_fails(self):
        creds, fake = self.assume([])
        with self.assertRaises(airflow.AirflowError) as caught:
            airflow.assumerole_refused(fake, creds)
        self.assertIn("positive control failed", str(caught.exception))

    def test_a_listing_of_an_earlier_upload_is_not_this_uploads(self):
        api = FakeApi(old_copy=3)
        result, _ = probe(api)
        self.assertEqual(result["run"], "manual__1")
        self.assertGreater(api.polls, 3, "the earlier copy's listing was waited out")
        stuck = FakeApi(old_copy=10**6)
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(stuck)
        self.assertIn("not this upload", str(caught.exception))

    def test_the_probe_dag_carries_the_nonce_as_a_tag(self):
        self.assertIn('"nonce-" + NONCE', airflow.probe_source(REPO_ROOT, NONCE).decode())
        self.assertEqual(airflow.nonce_tag(NONCE), f"nonce-{NONCE}")

    def test_a_200_that_says_the_log_could_not_be_served_is_no_log_and_falls_back_to_xcom(self):
        result, _ = probe(FakeApi(log="unserved"))
        self.assertIn("XCom", result["evidence"])

    def test_a_failed_run_with_an_unserved_log_shows_the_scheduler_pods_output(self):
        api = FakeApi(outcome="failed", log="unserved", xcom=False)
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(api, scheduler_log=lambda: "ERROR task fluid_version exit 1\nboom")
        self.assertIn("scheduler pod log", str(caught.exception))
        self.assertIn("boom", str(caught.exception))

    def test_a_put_that_times_out_still_gets_its_object_deleted(self):
        store = FakeSeaweed({"AK": {"read", "list", "write"}})
        bucket = s3.Bucket(store, "lab-airflow", "AK", "SK")
        real = bucket.send

        def timing_out(method, host, path, **kw):
            res = real(method, host, path, **kw)
            if method == "PUT":
                raise http.HTTPError("timed out after the server stored it")
            return res

        bucket.send = timing_out
        with self.assertRaises(http.HTTPError):
            probe(FakeApi(), bucket)
        self.assertEqual(store.objects, {}, "the DAG the timed-out PUT left is deleted")

    def test_a_dropped_connection_while_polling_is_retried_and_named_if_it_never_comes_back(self):
        result, _ = probe(FakeApi(drop=3))
        self.assertEqual(result["run"], "manual__1")
        with self.assertRaises(airflow.AirflowError) as caught:
            probe(FakeApi(drop=10**6))
        self.assertIn("connection reset by peer", str(caught.exception))

    def test_cleanup_with_no_airflow_records_deletes_nothing_and_says_so(self):
        api = FakeApi()
        api.deleted = True    # GET answers 404 from the start of clean-up on
        note = airflow._cleanup(api, s3.Bucket(FakeSeaweed({"AK": {"write"}}), "lab-airflow", "AK", "SK"), True,
                                lambda: 0, lambda _s: None)
        self.assertIn("holds no records", note)
        self.assertNotIn("DELETE", [c[0] for c in api.calls])

    def test_the_task_log_host_is_an_ip_so_the_api_server_can_reach_the_scheduler(self):
        core = config.load_yaml(REPO_ROOT / "platform/components/airflow/values.yaml")["config"]["core"]
        self.assertEqual(core["hostname_callable"], "airflow.utils.net.get_host_ip_address")


if __name__ == "__main__":
    unittest.main()
