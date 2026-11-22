# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`fluid apply --env local` on the spark tier: lab-spark-submit (forge's spark-submit as one Pod on the
open Spark image), its in-pod entry script, the artifacts identities and bucket, and fluid-host.

lab-spark-submit talks to a fake Kubernetes API on a loopback port (its LAB_KUBE_API test seam): no
cluster, no network. The entry script runs under bash with a fake spark-submit.
"""

import importlib.machinery
import importlib.util
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlparse

import support  # noqa: F401
from support import REPO_ROOT

from labctl import bootstrap, check, config, credentials, proc, render, spark, up

LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
SPARK_DIR = REPO_ROOT / "platform" / "spark"
SUBMIT = SPARK_DIR / "lab-spark-submit"
ENTRY = SPARK_DIR / "lab-spark-entry.sh"
GOTMPL = REPO_ROOT / "platform" / "components" / "lab-spark" / "values.yaml.gotmpl"
SECRET = "sentinel-value-for-tests"   # stands for the OAuth2 client secret and the S3 keys
NAME = "fluid-sales-daily-1a2b3c4d"   # forge's run_name: fluid-<product>-<build>-<8 hex build key>
PROPS = "/opt/lab-spark/spark-defaults.conf"
APACHE_CONF = SPARK_DIR / "spark-defaults.apache.conf"
FORGE_ARGV = ["--name", NAME, "--deploy-mode", "client", "--properties-file", PROPS,
              "--conf", "spark.executor.memory=1g",
              "s3://lab-spark-artifacts/runs/runner/forge_spark_runner.py",
              "--config", "s3://lab-spark-artifacts/runs/job.json"]
STREAM_ARGV = FORGE_ARGV + ["--fluid-checkpoint", "ck1"]
T = ["2026-11-19T10:00:0%d.123456789Z" % i for i in range(1, 9)]


def load_submit():
    loader = importlib.machinery.SourceFileLoader("lab_spark_submit", str(SUBMIT))
    spec = importlib.util.spec_from_loader("lab_spark_submit", loader)
    module = importlib.util.module_from_spec(spec)
    keep = sys.dont_write_bytecode
    sys.dont_write_bytecode = True      # never leave a __pycache__ in platform/spark/, which test_spark.py reads
    try:
        loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = keep
    return module


submit = load_submit()
APACHE = "lab.local/spark-iceberg:3.5.8-iceberg1.12.0-c15dd816"
_TMP = tempfile.TemporaryDirectory()
IMAGE_FILE = Path(_TMP.name) / "spark-image"          # what the ConfigMap's file holds
IMAGE_FILE.write_text(APACHE)


class NoBytecodeInTheSourceTree(unittest.TestCase):
    def test_loading_the_wrapper_leaves_no_pycache_in_platform_spark(self):
        self.assertFalse((SPARK_DIR / "__pycache__").exists())


def tearDownModule():
    _TMP.cleanup()


class FakeKube:
    """The few Kubernetes API calls lab-spark-submit makes, on 127.0.0.1.

    `log` is a list of (stamp, bytes) lines; a stream returns the lines stamped at or after `sinceTime`,
    as the API server does, and `cut` closes the first stream after that many lines while the container runs on.
    """

    HOST = "fluid-host-abc"

    def __init__(self, exit_code=0, log=None, hang=False, waiting=None, refuse_create=None, cut=0,
                 existing=(), host=True):
        self.exit_code, self.hang, self.waiting, self.refuse_create, self.cut = exit_code, hang, waiting, refuse_create, cut
        self.log = log or [(T[0], b"driver line 1"), (T[1], b"driver line 2")]
        self.host = {"metadata": {"name": self.HOST, "uid": "uid-host-1"}} if host else None
        self.pods = {p["metadata"]["name"]: p for p in existing}
        self.deleted, self.requests, self.queries = [], [], []
        self.finished = threading.Event()
        self.log_started = threading.Event()
        self.gone = threading.Event()
        self.log_opens = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args):
                pass

            def _send(self, code, doc):
                body = json.dumps(doc).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                outer.requests.append(("POST", self.path, body))
                if outer.refuse_create:
                    return self._send(outer.refuse_create, {"message": "forbidden"})
                outer.pods[body["metadata"]["name"]] = body
                self._send(201, body)

            def do_DELETE(self):
                outer.requests.append(("DELETE", self.path, None))
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                name = self.path.rsplit("/", 1)[-1]
                if name not in outer.pods:
                    return self._send(404, {"message": "not found"})
                del outer.pods[name]
                outer.deleted.append(name)
                outer.gone.set()
                self._send(200, {"status": "Success"})

            def do_GET(self):
                url = urlparse(self.path)
                outer.requests.append(("GET", self.path, None))
                if url.path.endswith("/log"):
                    return self._log(parse_qs(url.query))
                if url.path.endswith("/pods"):
                    want = unquote(parse_qs(url.query)["labelSelector"][0]).split("=", 1)
                    items = [p for p in outer.pods.values() if p["metadata"]["labels"].get(want[0]) == want[1]]
                    return self._send(200, {"items": items})
                name = url.path.rsplit("/", 1)[-1]
                if name == outer.HOST and outer.host:
                    return self._send(200, outer.host)
                if name not in outer.pods:
                    return self._send(404, {"message": "not found"})
                self._send(200, outer.status(outer.pods[name]))

            def _log(self, query):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                outer.log_opens += 1
                outer.queries.append(query)
                since = (query.get("sinceTime") or [""])[0]
                lines = [(s, t) for s, t in outer.log if not since or s >= since]
                if outer.cut and outer.log_opens == 1:
                    lines = lines[:outer.cut]       # the stream drops; the container runs on
                for stamp, text in lines:
                    stamped = "timestamps" in query
                    self.wfile.write((stamp.encode() + b" " if stamped else b"") + text + b"\n")
                    self.wfile.flush()
                if outer.cut and outer.log_opens == 1:
                    return
                outer.log_started.set()
                if outer.hang:
                    outer.gone.wait(20)     # a followed log stays open until the pod is deleted
                outer.finished.set()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def status(self, pod):
        doc = json.loads(json.dumps(pod))
        if self.waiting:
            doc["status"] = {"phase": "Pending", "containerStatuses": [
                {"name": "spark", "state": {"waiting": {"reason": self.waiting, "message": "no such image"}}}]}
        elif self.finished.is_set():
            doc["status"] = {"phase": "Succeeded" if self.exit_code == 0 else "Failed", "containerStatuses": [
                {"name": "spark", "state": {"terminated": {"exitCode": self.exit_code}}}]}
        else:
            doc["status"] = {"phase": "Running", "containerStatuses": [
                {"name": "spark", "state": {"running": {}}}]}
        return doc

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.gone.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(10)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def methods(self):
        return [m for m, _, _ in self.requests]


def old_pod(name, build="1a2b3c4d"):
    return {"metadata": {"name": name, "labels": {"fluid-fabric-lab/spark-build": build,
                                                  "fluid-fabric-lab/spark-submit": "x"}}}


def wrapper_env(api: FakeKube, **extra) -> dict:
    env = {"PATH": os.environ.get("PATH", ""), "LAB_KUBE_API": api.url, "LAB_SPARK_IMAGE_FILE": str(IMAGE_FILE),
           "LAB_SPARK_POLL_S": "0.05", "HOSTNAME": FakeKube.HOST}
    env.update(extra)
    return env


def start(api: FakeKube, argv=None, **extra) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, str(SUBMIT), *(FORGE_ARGV if argv is None else argv)],
                            env=wrapper_env(api, **extra), stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def finish(child: subprocess.Popen, timeout=30) -> tuple:
    try:
        out, err = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        out, err = child.communicate()
        raise AssertionError(f"lab-spark-submit did not exit: {out!r} {err!r}")
    return child.returncode, out.decode(), err.decode()


def posted(api: FakeKube) -> dict:
    return [b for m, _, b in api.requests if m == "POST"][0]


class PodSpec(unittest.TestCase):
    def setUp(self):
        self.owner = {"name": "fluid-host-abc", "uid": "uid-host-1"}
        self.pod = submit.pod_spec("lab-spark-submit-1", APACHE, FORGE_ARGV, NAME, "fluid-host-abc",
                                   build="1a2b3c4d", owner=self.owner)
        self.spec = self.pod["spec"]
        self.container = self.spec["containers"][0]

    def test_it_is_one_restricted_pod_in_spark_on_the_lab_built_image_which_is_never_pulled(self):
        self.assertEqual(self.pod["metadata"]["namespace"], "spark")
        self.assertEqual(self.container["image"], APACHE)
        self.assertEqual(self.container["imagePullPolicy"], "Never")
        self.assertEqual(self.spec["restartPolicy"], "Never")
        sc, csc = self.spec["securityContext"], self.container["securityContext"]
        self.assertTrue(sc["runAsNonRoot"])
        self.assertEqual(sc["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertEqual((sc["runAsUser"], sc["runAsGroup"]), (185, 185))
        self.assertEqual((csc["runAsUser"], csc["runAsGroup"]), (185, 185))
        self.assertEqual((spark.APACHE_UID, spark.APACHE_GID), (submit.UID, submit.GID))
        self.assertIs(csc["allowPrivilegeEscalation"], False)
        self.assertEqual(csc["capabilities"], {"drop": ["ALL"]})
        self.assertEqual(self.container["args"], FORGE_ARGV)
        job = spark.job("x", APACHE, "seed.py")["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual((job["image"], job["imagePullPolicy"]), (APACHE, "Never"))
        self.assertEqual((job["securityContext"]["runAsUser"], job["securityContext"]["runAsGroup"]), (185, 185))

    def test_it_has_no_serviceaccount_token_and_the_smoke_jobs_memory_limits(self):
        self.assertIs(self.spec["automountServiceAccountToken"], False)
        job = spark.job("x", APACHE, "seed.py")["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(self.container["resources"], job["resources"])
        self.assertEqual(self.container["resources"]["limits"], {"memory": "2560Mi"})

    def test_the_entry_gets_forges_argv_unchanged(self):
        self.assertEqual(self.container["command"], ["bash", "/spark/lab-spark-entry.sh"])
        self.assertEqual(self.container["args"], FORGE_ARGV)

    def test_dollar_signs_are_escaped_because_kubernetes_expands_them_in_args(self):
        argv = ["--conf", "k=$(AWS_REGION)", "app.py", "--pattern", "^[a-z]+$", "--cost", "$$5"]
        args = submit.pod_spec("n", APACHE, argv, "r", "h")["spec"]["containers"][0]["args"]
        self.assertEqual(args, ["--conf", "k=$$(AWS_REGION)", "app.py", "--pattern", "^[a-z]+$$", "--cost", "$$$$5"])
        # What Kubernetes does to them (`$$` becomes `$`, then nothing else is expanded) gives argv back.
        self.assertEqual([a.replace("$$", "$") for a in args], argv)

    def test_every_pod_is_labelled_so_a_crashed_wrappers_pod_can_be_found_and_swept(self):
        labels = self.pod["metadata"]["labels"]
        self.assertEqual(labels["fluid-fabric-lab/spark-submit"], NAME)
        self.assertEqual(labels["fluid-fabric-lab/spark-build"], "1a2b3c4d")
        self.assertEqual(labels["fluid-fabric-lab/spark-host"], "fluid-host-abc")
        self.assertEqual(labels["app.kubernetes.io/part-of"], "fluid-fabric-lab")
        self.assertEqual(spark.SUBMIT_LABEL, submit.LABEL)

    def test_the_pod_is_owned_by_fluid_host_so_garbage_collection_removes_it_with_fluid_host(self):
        self.assertEqual(self.pod["metadata"]["ownerReferences"],
                         [{"apiVersion": "v1", "kind": "Pod", "name": "fluid-host-abc", "uid": "uid-host-1"}])

    def test_secrets_are_secret_references_only_and_build_pods_get_the_build_key(self):
        env = {e["name"]: e for e in self.container["env"]}
        for key in ("LAKEKEEPER_CLIENT_SECRET", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "LAKEKEEPER_CLIENT_ID"):
            self.assertNotIn("value", env[key], key)
            self.assertIn("secretKeyRef", env[key]["valueFrom"], key)
        self.assertEqual(env["AWS_SECRET_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"], "spark-build-s3")
        self.assertEqual(env["AWS_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"]["name"], "spark-build-s3")
        self.assertEqual(env["LAKEKEEPER_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]["name"], "spark-lakekeeper-client")

    def test_labels_and_build_keys(self):
        for raw, want in (("daily run/2026", "daily-run-2026"), ("", "unnamed"), ("--x--", "x"), ("a" * 90, "a" * 63)):
            self.assertEqual(submit.label_value(raw), want)
        self.assertEqual(submit.app_name(["--name=abc", "--conf", "k=v"]), "abc")
        self.assertEqual(submit.app_name(FORGE_ARGV), NAME)
        self.assertEqual(submit.build_key(NAME), "1a2b3c4d")
        self.assertEqual(submit.build_key("no-key-here"), "no-key-here")

    def test_the_pod_deadline_is_a_parameter(self):
        self.assertNotIn("activeDeadlineSeconds", self.spec)
        timed = submit.pod_spec("n", APACHE, [], "r", "h", 3600)["spec"]
        self.assertEqual(timed["activeDeadlineSeconds"], 3600)


class Log(unittest.TestCase):
    def feed(self, *chunks):
        out = io.BytesIO()
        log = submit.Log(out)
        for chunk in chunks:
            log.reopen()
            for raw in chunk:
                log.line(raw)
        return out.getvalue()

    def test_stamps_are_stripped_and_lines_come_out_in_order(self):
        self.assertEqual(self.feed([f"{T[0]} a".encode(), f"{T[1]} b c".encode()]), b"a\nb c\n")

    def test_a_reopened_stream_repeats_only_its_boundary_line_which_is_skipped(self):
        first = [f"{T[0]} a".encode(), f"{T[1]} b".encode()]
        second = [f"{T[1]} b".encode(), f"{T[2]} c".encode()]    # sinceTime returns the line at that stamp again
        self.assertEqual(self.feed(first, second), b"a\nb\nc\n")

    def test_identical_lines_at_one_stamp_are_each_written_once(self):
        first = [f"{T[0]} x".encode()]
        second = [f"{T[0]} x".encode(), f"{T[0]} x".encode(), f"{T[1]} y".encode()]
        self.assertEqual(self.feed(first, second), b"x\nx\ny\n")

    def test_stamps_compare_by_value_not_by_text(self):
        # Kubernetes trims trailing zeros: ".5Z" is later than ".49Z" and "05Z" earlier than "05.1Z".
        self.assertLess(submit.ts_key("2026-11-19T10:00:05Z"), submit.ts_key("2026-11-19T10:00:05.1Z"))
        self.assertLess(submit.ts_key("2026-11-19T10:00:05.49Z"), submit.ts_key("2026-11-19T10:00:05.5Z"))

    def test_a_line_with_no_stamp_passes_through(self):
        self.assertEqual(self.feed([b"plain text without a stamp"]), b"plain text without a stamp\n")


class Wrapper(unittest.TestCase):
    def test_exit_codes_and_logs_pass_through_and_the_pod_is_deleted(self):
        for code in (0, 3, 137):
            with self.subTest(code=code), FakeKube(exit_code=code) as api:
                child = start(api)
                rc, out, err = finish(child)
                self.assertEqual(rc, code, err)
                self.assertEqual(out, "driver line 1\ndriver line 2\n")
                self.assertEqual(api.pods, {}, "the pod was left behind")
                body = posted(api)
                self.assertEqual(body["spec"]["containers"][0]["args"], FORGE_ARGV)
                self.assertEqual(api.deleted, [body["metadata"]["name"]])

    def test_the_posted_pod_is_owned_by_fluid_host_and_a_batch_run_has_a_deadline(self):
        with FakeKube() as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, 0, err)
            body = posted(api)
            self.assertEqual(body["metadata"]["ownerReferences"][0]["uid"], "uid-host-1")
            self.assertEqual(body["spec"]["activeDeadlineSeconds"], 21600)
            self.assertEqual(body["metadata"]["labels"]["fluid-fabric-lab/spark-build"], "1a2b3c4d")

    def test_a_stream_has_no_deadline_unless_asked(self):
        with FakeKube() as api:
            finish(start(api, STREAM_ARGV))
            self.assertNotIn("activeDeadlineSeconds", posted(api)["spec"])
        with FakeKube() as api:
            finish(start(api, STREAM_ARGV, LAB_SPARK_POD_DEADLINE_S="60"))
            self.assertEqual(posted(api)["spec"]["activeDeadlineSeconds"], 60)
        with FakeKube() as api:
            finish(start(api, FORGE_ARGV, LAB_SPARK_POD_DEADLINE_S="0"))
            self.assertNotIn("activeDeadlineSeconds", posted(api)["spec"])

    def test_without_its_own_pod_it_refuses_rather_than_make_an_unowned_pod(self):
        with FakeKube(host=False) as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, submit.EXIT_FAILED)
            self.assertIn("ownerReference", err)
            self.assertNotIn("POST", api.methods())

    def test_an_earlier_pod_of_the_same_build_is_deleted_before_the_new_one_starts(self):
        mine, other = old_pod("lab-spark-submit-old"), old_pod("lab-spark-submit-else", build="ffffffff")
        with FakeKube(existing=[mine, other]) as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, 0, err)
            self.assertIn("lab-spark-submit-old", api.deleted)
            self.assertNotIn("lab-spark-submit-else", api.deleted, "another build's pod must stay")
            self.assertIn("lab-spark-submit-else", api.pods)
            order = [(m, p) for m, p, _ in api.requests if m in ("DELETE", "POST")]
            self.assertTrue(order[0][0] == "DELETE" and order[0][1].endswith("lab-spark-submit-old"), order)
            self.assertLess(api.methods().index("DELETE"), api.methods().index("POST"))

    def test_a_log_cut_mid_run_is_resumed_by_time_without_repeating_lines(self):
        # The fake returns only lines stamped at or after sinceTime, as the API server does after the
        # kubelet has rotated older ones away: a byte offset into the new stream would now be wrong.
        log = [(T[0], b"one"), (T[1], b"two"), (T[2], b"three")]
        with FakeKube(log=log, cut=2) as api:
            rc, out, err = finish(start(api))
            self.assertGreaterEqual(api.log_opens, 2)
            self.assertEqual(api.queries[0]["timestamps"], ["true"])
            self.assertEqual(api.queries[1]["sinceTime"], [T[1]])
            self.assertNotIn("sinceTime", api.queries[0])
        self.assertEqual((rc, out), (0, "one\ntwo\nthree\n"), err)

    def test_sigterm_deletes_the_pod_and_exits_143(self):
        self._signal(signal.SIGTERM, 143)

    def test_sigint_deletes_the_pod_and_exits_130(self):
        self._signal(signal.SIGINT, 130)

    def test_a_second_signal_during_cleanup_neither_skips_the_delete_nor_changes_the_exit(self):
        self._signal(signal.SIGTERM, 143, again=True)

    def _signal(self, sig, want, again=False):
        with FakeKube(hang=True) as api:
            child = start(api)
            self.assertTrue(api.log_started.wait(20), "the log stream never opened")
            self.assertEqual(len(api.pods), 1)
            time.sleep(0.5)     # the wrapper has read what the server wrote
            child.send_signal(sig)
            if again:
                child.send_signal(sig)
                child.send_signal(sig)
            rc, out, err = finish(child)
            self.assertEqual(rc, want, err)
            self.assertEqual(api.pods, {}, "the pod was left behind")
            self.assertEqual(len(api.deleted), 1)
            self.assertIn("driver line 1", out)

    def test_a_pod_that_cannot_start_fails_at_once_and_is_deleted(self):
        with FakeKube(waiting="ErrImageNeverPull") as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, submit.EXIT_FAILED)
            self.assertIn("ErrImageNeverPull", err)
            self.assertEqual(api.pods, {})

    def test_a_refused_create_exits_non_zero_without_a_traceback(self):
        with FakeKube(refuse_create=403) as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, submit.EXIT_FAILED)
            self.assertIn("HTTP 403", err)
            self.assertNotIn("Traceback", err)

    def test_the_image_is_the_lab_built_one_from_the_configmap_file_and_the_env_cannot_override_it(self):
        with FakeKube() as api:
            rc, out, err = finish(start(api))
            self.assertEqual(rc, 0, err)
            pod = posted(api)
            self.assertEqual(pod["spec"]["containers"][0]["image"], APACHE)
            self.assertEqual(pod["spec"]["containers"][0]["imagePullPolicy"], "Never")
            self.assertEqual(pod["spec"]["securityContext"]["runAsUser"], 185)
        bad = Path(_TMP.name) / "bad-image"
        for text in ("public.example.com/x/y:latest", "docker.io/apache/spark:3.5.8", "lab.local/fluid-runner:1",
                     "lab.local/spark-iceberg:", "lab.local/spark-iceberg@sha256:" + "0" * 64, ""):
            bad.write_text(text)
            with self.subTest(text=text), FakeKube() as api:
                rc, out, err = finish(start(api, LAB_SPARK_IMAGE_FILE=str(bad)))
                self.assertEqual(rc, submit.EXIT_FAILED)
                self.assertEqual(api.requests, [])
        with FakeKube() as api:       # the environment may only agree with the file, never replace it
            rc, out, err = finish(start(api, LAB_SPARK_IMAGE="lab.local/spark-iceberg:evil"))
            self.assertEqual(rc, submit.EXIT_FAILED)
            self.assertIn("differs", err)
            self.assertEqual(api.requests, [])
        with FakeKube() as api:
            rc, out, err = finish(start(api, LAB_SPARK_IMAGE=APACHE))
            self.assertEqual(rc, 0, err)

    def test_there_is_no_runtime_switch_left(self):
        self.assertFalse(hasattr(submit, "spark_runtime"))
        self.assertFalse(hasattr(submit, "RUNTIME_FILE"))
        with FakeKube() as api:       # a runtime in the environment is no longer read at all
            rc, out, err = finish(start(api, LAB_SPARK_RUNTIME="emr"))
            self.assertEqual(rc, 0, err)
            self.assertEqual(posted(api)["spec"]["containers"][0]["image"], APACHE)

    def test_the_file_path_override_exists_only_with_the_loopback_test_seam(self):
        self.assertEqual(submit.IMAGE_FILE, "/opt/lab-spark/spark-image")
        with self.assertRaises(submit.SubmitError):      # no LAB_KUBE_API: the real path, which is absent here
            submit.spark_image({"LAB_SPARK_IMAGE_FILE": str(IMAGE_FILE)})

    def test_a_properties_file_other_than_the_labs_is_refused_before_any_request(self):
        for argv in (["--properties-file", "/tmp/evil.conf", "app.py"], ["--properties-file=/tmp/evil.conf", "app.py"],
                     ["--properties-file"]):
            with self.subTest(argv=argv), FakeKube() as api:
                rc, out, err = finish(start(api, argv))
                self.assertEqual(rc, submit.EXIT_FAILED)
                self.assertIn(PROPS, err)
                self.assertEqual(api.requests, [])

    def test_the_test_seam_accepts_only_loopback(self):
        with self.assertRaises(submit.SubmitError):
            submit.Api({"LAB_KUBE_API": "http://10.0.0.1:8080"})

    def test_dollar_signs_reach_the_posted_pod_escaped(self):
        with FakeKube() as api:
            finish(start(api, ["--name", NAME, "app.py", "--conf", "x=$(AWS_REGION)", "--re", "^a$"]))
            self.assertEqual(posted(api)["spec"]["containers"][0]["args"][-4:],
                             ["--conf", "x=$$(AWS_REGION)", "--re", "^a$$"])

    def test_secrets_are_never_in_argv_the_pod_spec_or_the_logs(self):
        # fluid-host has its S3 key in its own environment; the wrapper must not forward it.
        with FakeKube() as api:
            child = start(api, AWS_SECRET_ACCESS_KEY=SECRET, LAKEKEEPER_CLIENT_SECRET=SECRET)
            rc, out, err = finish(child)
            self.assertEqual(rc, 0, err)
            self.assertNotIn(SECRET, json.dumps(api.requests))
            self.assertNotIn(SECRET, out + err)
            self.assertNotIn(SECRET, " ".join(child.args))
            wrapper = SUBMIT.read_text()
            self.assertNotIn("LAKEKEEPER_CLIENT_SECRET\"]", wrapper.replace(" ", ""))
            self.assertNotIn("environ[\"AWS_SECRET", wrapper)


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class Entry(unittest.TestCase):
    """lab-spark-entry.sh, with a fake spark-submit and a fake base conf."""
    CONF_FILE = APACHE_CONF

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        t = Path(self.tmp.name)
        (t / "base").mkdir()
        (t / "base" / "spark-defaults.conf").write_text("spark.base.line 1\n")
        (t / "mount").mkdir()
        (t / "mount" / "spark-defaults.conf").write_text(self.CONF_FILE.read_text())
        (t / "fake-submit").write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$OUT/argv"\n'
                                       'cp "$SPARK_CONF_DIR/spark-defaults.conf" "$OUT/props"\n')
        (t / "fake-submit").chmod(0o755)
        (t / "out").mkdir()
        self.t = t

    def tearDown(self):
        self.tmp.cleanup()

    def run_entry(self, *argv, **extra):
        env = {"PATH": os.environ["PATH"], "LAB_SPARK_SUBMIT": str(self.t / "fake-submit"),
               "LAB_SPARK_BASE_CONF": str(self.t / "base"), "LAB_SPARK_MOUNT": str(self.t / "mount"),
               "LAB_SPARK_CONF_DIR": str(self.t / "conf"), "OUT": str(self.t / "out"),
               "LAKEKEEPER_CLIENT_ID": "spark", "LAKEKEEPER_CLIENT_SECRET": SECRET}
        env.update(extra)
        res = subprocess.run(["bash", str(ENTRY), *argv], env=env, capture_output=True, text=True, timeout=60)
        argv_seen = (self.t / "out" / "argv").read_text().splitlines() if (self.t / "out" / "argv").exists() else None
        return res, argv_seen

    def test_the_argv_is_rewritten_to_local_master_and_the_pods_properties_file(self):
        res, got = self.run_entry(*FORGE_ARGV)
        self.assertEqual(res.returncode, 0, res.stderr)
        props = str(self.t / "conf" / "spark-defaults.conf")
        self.assertEqual(got, ["--master", "local[2]", "--properties-file", props,
                               "--name", NAME, "--deploy-mode", "client", "--conf", "spark.executor.memory=1g",
                               "s3://lab-spark-artifacts/runs/runner/forge_spark_runner.py",
                               "--config", "s3://lab-spark-artifacts/runs/job.json"])

    def test_a_master_forge_gave_is_dropped_but_the_applications_own_arguments_are_untouched(self):
        res, got = self.run_entry("--master", "yarn", "--name", "n", "app.py", "--master", "x",
                                  "--properties-file", "/elsewhere", "--conf", "spark.redaction.regex=.")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(got.count("--master"), 2)
        self.assertEqual(got[:2], ["--master", "local[2]"])
        self.assertNotIn("yarn", got)
        self.assertEqual(got[-7:], ["app.py", "--master", "x", "--properties-file", "/elsewhere",
                                    "--conf", "spark.redaction.regex=."])

    def test_confs_that_are_the_labs_are_refused_in_every_spelling(self):
        for conf in (["--conf", "spark.sql.catalog.fluid.uri=https://evil"], ["--conf=spark.sql.catalog.fluid.type=hive"],
                     ["-c", "spark.sql.catalog.fluid=x"], ["--conf", "spark.redaction.regex=nothing"],
                     ["--conf=spark.redaction.regex="]):
            with self.subTest(conf=conf):
                res, got = self.run_entry("--name", "n", *conf, "app.py")
                self.assertEqual(res.returncode, 65, res.stderr)
                self.assertIsNone(got, "spark-submit must not run")
                self.assertIn("refused", res.stderr)

    def test_other_confs_pass(self):
        res, got = self.run_entry("--conf", "spark.executor.memory=1g", "-c", "spark.sql.shuffle.partitions=4",
                                  "--conf=spark.sql.catalogImplementation=hive", "app.py")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("spark.sql.shuffle.partitions=4", got)

    def test_the_properties_file_is_the_base_then_the_labs_then_the_credential(self):
        res, _ = self.run_entry(*FORGE_ARGV)
        self.assertEqual(res.returncode, 0, res.stderr)
        props = (self.t / "out" / "props").read_text()
        self.assertTrue(props.startswith("spark.base.line 1\n"))
        self.assertIn("spark.sql.catalog.fluid.type", props)
        self.assertIn("spark.hadoop.fs.s3.impl", props)
        for key in ("spark.jars ", "spark.jars.ivySettings", "spark.jars.packages"):
            self.assertIn(key, props)
        self.assertTrue(props.rstrip().endswith(f"spark.sql.catalog.fluid.credential spark:{SECRET}"))
        self.assertLess(props.index("spark.base.line"), props.index("spark.sql.catalog.fluid.type"))
        mode = (self.t / "conf" / "spark-defaults.conf").stat().st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_the_secret_is_never_in_argv_stdout_stderr_or_the_mounted_copy(self):
        res, got = self.run_entry(*FORGE_ARGV)
        self.assertNotIn(SECRET, "\n".join(got))
        self.assertNotIn(SECRET, res.stdout + res.stderr)
        self.assertNotIn(SECRET, (self.t / "mount" / "spark-defaults.conf").read_text())
        self.assertNotIn(SECRET, APACHE_CONF.read_text())
        self.assertNotIn(SECRET, ENTRY.read_text())

    def test_a_properties_file_the_configmap_does_not_ship_is_refused(self):
        res, got = self.run_entry("--properties-file", "/etc/passwd.conf", "app.py")
        self.assertEqual(res.returncode, 64)
        self.assertIsNone(got)
        self.assertNotIn(SECRET, res.stdout + res.stderr)

    def test_no_properties_file_means_the_labs_defaults(self):
        res, got = self.run_entry("--name", "n", "app.py")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(got[:4], ["--master", "local[2]", "--properties-file", str(self.t / "conf" / "spark-defaults.conf")])

    def test_a_missing_credential_fails_instead_of_running_unauthenticated(self):
        res, got = self.run_entry("app.py", LAKEKEEPER_CLIENT_SECRET="")
        self.assertNotEqual(res.returncode, 0)
        self.assertIsNone(got)

    def test_the_entry_reads_no_runtime_file(self):
        self.assertNotIn("spark-runtime", ENTRY.read_text().replace("# ", ""))
        (self.t / "mount" / "spark-runtime").write_text("emr")       # a leftover file is ignored
        res, got = self.run_entry(*FORGE_ARGV)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIsNotNone(got)


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class EntryImageLayouts(unittest.TestCase):
    """Where the image keeps spark-submit and its base conf, on a fake filesystem root."""

    def layout(self, submit_path, conf_path, conf_files):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        t = Path(tmp.name)
        (t / "root" / submit_path).parent.mkdir(parents=True)
        if conf_files is not None:
            (t / "root" / conf_path).mkdir(parents=True)
        (t / "root" / submit_path).write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$OUT/argv"\n'
                                            'cp "$SPARK_CONF_DIR/spark-defaults.conf" "$OUT/props"\n'
                                            'ls "$SPARK_CONF_DIR" > "$OUT/conf-files"\n')
        (t / "root" / submit_path).chmod(0o755)
        for name, text in (conf_files or {}).items():
            (t / "root" / conf_path / name).write_text(text)
        (t / "mount").mkdir()
        (t / "mount" / "spark-defaults.conf").write_text("spark.lab.line 1\n")
        (t / "out").mkdir()
        env = {"PATH": os.environ["PATH"], "LAB_SPARK_FS_ROOT": str(t / "root"), "LAB_SPARK_MOUNT": str(t / "mount"),
               "LAB_SPARK_CONF_DIR": str(t / "conf"), "OUT": str(t / "out"), "LAKEKEEPER_CLIENT_ID": "spark",
               "LAKEKEEPER_CLIENT_SECRET": SECRET}
        res = subprocess.run(["bash", str(ENTRY), "--master", "yarn", "app.py"], env=env, capture_output=True,
                             text=True, timeout=60)
        return t, res

    def test_apache_uses_opt_spark_and_starts_from_templates_only(self):
        t, res = self.layout("opt/spark/bin/spark-submit", "opt/spark/conf",
                             {"spark-defaults.conf.template": "# template\n", "log4j2.properties.template": "# t\n"})
        self.assertEqual(res.returncode, 0, res.stderr)
        props = (t / "out" / "props").read_text()
        self.assertNotIn("template", props)       # no base spark-defaults.conf: the file is the lab's lines alone
        self.assertIn("spark.lab.line 1", props)
        self.assertIn("spark-defaults.conf.template", (t / "out" / "conf-files").read_text())

    def test_apache_works_when_the_image_has_no_conf_directory_at_all(self):
        # apache/spark:3.5.8 ships no /opt/spark/conf (the first live run failed on `cp`, 2026-11-22).
        t, res = self.layout("opt/spark/bin/spark-submit", "opt/spark/conf", None)
        self.assertEqual(res.returncode, 0, res.stderr)
        props = (t / "out" / "props").read_text()
        self.assertIn("spark.lab.line 1", props)
        self.assertTrue(props.rstrip().endswith(f"spark.sql.catalog.fluid.credential spark:{SECRET}"))

    def test_a_spark_submit_outside_opt_spark_is_not_found(self):
        t, res = self.layout("usr/lib/spark/bin/spark-submit", "opt/spark/conf", {})
        self.assertNotEqual(res.returncode, 0)    # the entry looks in /opt/spark, where this layout has none
        self.assertFalse((t / "out" / "argv").exists())


class ArtifactsAndHost(unittest.TestCase):
    def setUp(self):
        self.creds, _ = credentials.generate({})
        self.ids = {i["name"]: i for i in credentials.seaweedfs_config(self.creds)["identities"]}

    def test_the_hosts_identity_reaches_only_its_bucket_and_the_lab_keys_do_not_reach_it(self):
        art = self.ids["spark-artifacts"]
        self.assertEqual(art["credentials"], [{"accessKey": self.creds["s3.spark.access"],
                                               "secretKey": self.creds["s3.spark.secret"]}])
        self.assertTrue(art["actions"] and all(a.endswith(":lab-spark-artifacts") for a in art["actions"]))
        self.assertNotIn("Admin", art["actions"])
        self.assertTrue(all(a.endswith(":lake") for a in self.ids["lakekeeper"]["actions"]))
        self.assertNotIn(self.creds["s3.spark.access"], json.dumps(credentials.seaweedfs_config(self.creds)["policies"]))
        self.assertEqual(len({self.creds[k] for k in ("s3.spark.secret", "s3.sparkbuild.secret", "s3.lakekeeper.secret")}), 3)

    def test_build_pods_read_runs_and_write_only_checkpoints(self):
        build = self.ids["spark-build"]
        self.assertEqual(sorted(build["actions"]), sorted([
            "Read:lab-spark-artifacts/runs*", "List:lab-spark-artifacts/runs*",
            "Read:lab-spark-artifacts/_checkpoints*", "List:lab-spark-artifacts/_checkpoints*",
            "Write:lab-spark-artifacts/_checkpoints*"]))
        writes = [a for a in build["actions"] if a.startswith(("Write", "Admin", "Tagging"))]
        self.assertTrue(all("/_checkpoints" in a for a in writes))
        self.assertFalse([a for a in build["actions"] if "/runs" in a and not a.startswith(("Read", "List"))])
        self.assertTrue(all(a.split(":", 1)[1].startswith("lab-spark-artifacts/") for a in build["actions"]), "bucket-wide?")

    def test_the_sts_role_trusts_the_lakekeeper_identity_only(self):
        doc = credentials.seaweedfs_config(self.creds)
        statements = doc["roles"][0]["trustPolicy"]["Statement"]
        self.assertEqual(statements, [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                       "Principal": {"AWS": "arn:aws:iam::000000000000:user/lakekeeper"}}])
        self.assertNotIn("*", json.dumps(doc["roles"][0]["trustPolicy"]))
        # SeaweedFS names a static identity's principal user/<identity name>: the ARN is the lakekeeper identity's.
        self.assertIn("lakekeeper", self.ids)
        self.assertTrue(credentials.LAKEKEEPER_PRINCIPAL.endswith(":user/lakekeeper"))
        for name in ("spark-artifacts", "spark-build", "lab-admin"):
            self.assertNotEqual(credentials.LAKEKEEPER_PRINCIPAL.rsplit("/", 1)[1], name)

    def test_the_keys_reach_pods_through_secrets_in_spark_only(self):
        objs = credentials.secrets_for(self.creds, {"spark", "seaweedfs"})
        for name, prefix in (("spark-artifacts-s3", "s3.spark"), ("spark-build-s3", "s3.sparkbuild")):
            mine = [o for o in objs if o["metadata"]["name"] == name]
            self.assertEqual([o["metadata"]["namespace"] for o in mine], ["spark"])
            self.assertEqual(mine[0]["stringData"], {"AWS_ACCESS_KEY_ID": self.creds[prefix + ".access"],
                                                     "AWS_SECRET_ACCESS_KEY": self.creds[prefix + ".secret"]})
            self.assertEqual([o for o in credentials.secrets_for(self.creds, {"seaweedfs"})
                              if o["metadata"]["name"] == name], [])

    def test_a_rerun_keeps_the_generated_keys(self):
        again, changed = credentials.generate(self.creds)
        self.assertFalse(changed)
        self.assertEqual(again["s3.sparkbuild.secret"], self.creds["s3.sparkbuild.secret"])

    def test_the_bucket_is_made_like_the_warehouse_bucket(self):
        calls = []
        fake = SimpleNamespace(first_pod=lambda ns, sel: "master-0",
                               exec=lambda ns, pod, cmd, **kw: (calls.append((cmd, kw)), proc.Result(cmd, 0, "", ""))[1])
        self.assertEqual(bootstrap.artifacts_bucket(fake), "bucket lab-spark-artifacts")
        self.assertEqual(bootstrap.warehouse_bucket(fake), "bucket lake")
        self.assertIn("s3.bucket.create -name lab-spark-artifacts\n", calls[0][1]["input"])
        self.assertIn("s3.bucket.create -name lake\n", calls[1][1]["input"])

    def test_the_labs_defaults_read_s3_artifacts_through_s3a_for_that_bucket_only(self):
        lines = {ln.split(None, 1)[0]: ln.split(None, 1)[1].strip() for ln in APACHE_CONF.read_text().splitlines()
                 if ln.strip() and not ln.startswith("#")}
        b = "spark.hadoop.fs.s3a.bucket.lab-spark-artifacts."
        self.assertEqual(lines["spark.hadoop.fs.s3.impl"], "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.assertEqual(lines[b + "endpoint"], "https://s3.lab.test")
        self.assertEqual(lines[b + "path.style.access"], "true")
        self.assertIn("EnvironmentVariableCredentialsProvider", lines[b + "aws.credentials.provider"])
        self.assertFalse([k for k in lines if k.startswith("spark.hadoop.fs.s3a.") and not k.startswith(b)],
                         "every s3a setting is scoped to the artifacts bucket")
        self.assertEqual(lines["spark.sql.catalog.fluid.io-impl"], "org.apache.iceberg.aws.s3.S3FileIO")
        self.assertEqual(lines["spark.sql.catalog.fluid.header.X-Iceberg-Access-Delegation"], "vended-credentials")
        self.assertIn("credential", lines["spark.redaction.regex"])
        self.assertFalse([k for k in lines if "access" in k.lower() and "key" in k.lower()], "no key in the file")

    def test_the_file_carries_the_images_classpath(self):
        apache = {ln.split(None, 1)[0]: ln.split(None, 1)[1].strip() for ln in APACHE_CONF.read_text().splitlines()
                  if ln.strip() and not ln.startswith("#")}
        entry = next(b for b in LOCK["built_images"] if b["id"] == "spark-iceberg")
        paths = {f["id"]: f["path"] for f in entry["contents"]["files"]}
        self.assertEqual(apache["spark.jars"], f"{paths['iceberg-spark-runtime']},{paths['iceberg-aws-bundle']}")
        self.assertEqual(apache["spark.jars.packages"],
                         f"org.apache.spark:spark-sql-kafka-0-10_2.12:{entry['contents']['kafka-connector']}")
        self.assertEqual(apache["spark.jars.ivySettings"], "/opt/lab/ivysettings.xml")

    def test_the_runner_tag_comes_from_the_images_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = Path(tmp) / "images.json"
            self.assertEqual(spark.recorded_runner(record), "")
            record.write_text("not json")
            self.assertEqual(spark.recorded_runner(record), "")
            record.write_text(json.dumps({"fluid-runner": {"tag": "lab.local/fluid-runner:0.32.0", "layers": ["a"]}}))
            self.assertEqual(spark.recorded_runner(record), "lab.local/fluid-runner:0.32.0")
            record.write_text(json.dumps({"fluid-runner": {}}))
            self.assertEqual(spark.recorded_runner(record), "")

    def test_the_gotmpl_allowlists_equal_the_launchers(self):
        text = GOTMPL.read_text()
        for key, value in spark.OPERATOR_ALLOWLISTS.items():
            self.assertIn(f'name: {key}, value: "{value}"', text)
        self.assertNotIn("FLUID_SPARK_TARGET_ALLOWLIST", text)

    def test_the_new_files_ship_in_the_configmap(self):
        text = GOTMPL.read_text()
        self.assertIn('"lab-spark-submit"', text)
        self.assertIn('"lab-spark-entry.sh"', text)
        self.assertTrue(os.access(SUBMIT, os.X_OK))
        for f in (SUBMIT, ENTRY):
            self.assertNotIn("{{", f.read_text(), f.name)


class SeaweedfsRestart(unittest.TestCase):
    """A changed `seaweedfs-s3-config` must reach the S3 pod on an existing lab: the chart's own checksum does it."""

    def test_the_chart_stamps_the_s3_pod_with_the_live_secrets_checksum(self):
        with tarfile.open(REPO_ROOT / "platform/charts/seaweedfs-4.48.0.tgz") as tar:
            text = tar.extractfile("seaweedfs/templates/s3/s3-deployment.yaml").read().decode()
        self.assertIn("if .Values.s3.existingConfigSecret", text)
        self.assertIn('lookup "v1" "Secret" .Release.Namespace .Values.s3.existingConfigSecret', text)
        self.assertIn('"checksum/s3config"', text)

    def test_the_lab_uses_that_secret_and_writes_it_before_helmfile_runs(self):
        values = (REPO_ROOT / "platform/components/seaweedfs/values.yaml").read_text()
        self.assertIn("existingConfigSecret: seaweedfs-s3-config", values)
        self.assertEqual(credentials.secrets_for(credentials.generate({})[0], {"seaweedfs"})[-1]["metadata"]["name"],
                         "seaweedfs-s3-config")
        source = (REPO_ROOT / "labctl/up.py").read_text()
        steps = source[source.index("steps = [("):source.index("ok = True")]
        self.assertLess(steps.index('"secrets"'), steps.index('"helmfile"'))


class ReadyIgnoresBuildPods(unittest.TestCase):
    def pod(self, ns, name, labels=None, phase="Failed"):
        return {"metadata": {"namespace": ns, "name": name, "labels": labels or {}},
                "status": {"phase": phase, "containerStatuses": [{"ready": False}]}}

    def test_a_wrapper_pod_in_spark_is_not_a_problem(self):
        pods = [self.pod("spark", "lab-spark-submit-x", {"fluid-fabric-lab/spark-submit": "n"})]
        self.assertEqual(check.pod_problems(pods, [], {"spark"}, frozenset({"spark"})), [])

    def test_other_pods_and_other_namespaces_still_count(self):
        label = {"fluid-fabric-lab/spark-submit": "n"}
        self.assertEqual(len(check.pod_problems([self.pod("spark", "other")], [], {"spark"}, frozenset({"spark"}))), 1)
        self.assertEqual(len(check.pod_problems([self.pod("trino", "t", label)], [], {"trino"})), 1)

    def test_fluid_host_must_be_available_when_it_is_rendered(self):
        deployment = {"kind": "Deployment", "metadata": {"namespace": "spark", "name": "fluid-host"},
                      "spec": {"replicas": 1}, "status": {"readyReplicas": 0}}
        self.assertEqual(check.not_ready([deployment], {"spark"}), ["spark/deployment/fluid-host 0/1"])
        deployment["status"]["readyReplicas"] = 1
        self.assertEqual(check.not_ready([deployment], {"spark"}), [])


@unittest.skipUnless(shutil.which("helmfile"), "needs helmfile")
class RenderedHost(unittest.TestCase):
    """`helmfile template` of the spark tiers, offline, as the L0 render gate does it.

    `apache_offline` is tier spark with no record, `apache_runner_only` has a fluid-runner record and no
    spark-iceberg one, `apache` has both."""

    @classmethod
    def setUpClass(cls):
        cls.apache_offline = cls.docs_of(cls.render("spark", ""))
        cls.apache_runner_only = cls.docs_of(cls.render("spark", "lab.local/fluid-runner:9.9.9"))
        cls.apache = cls.docs_of(cls.render("spark", "lab.local/fluid-runner:9.9.9", APACHE))
        cls.built = cls.apache

    @staticmethod
    def docs_of(docs: list) -> dict:
        return {(d["kind"], d["metadata"]["name"]): d for d in docs}

    @staticmethod
    def render(tier: str, runner: str, image: str = "", expect_ok: bool = True):
        with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as out:
            env = render.hermetic_env(Path(home))
            for name, value in ((spark.RUNNER_ENV, runner), (spark.IMAGE_ENV, image)):
                if value:
                    env[name] = value
            res = subprocess.run(["helmfile", "-f", str(REPO_ROOT / "platform/helmfile.yaml.gotmpl"), "-e", tier,
                                  "--skip-deps", "--quiet", "template", "--output-dir", out], env=env, cwd=REPO_ROOT,
                                 capture_output=True, text=True, timeout=300)
            if not expect_ok:
                return res
            assert res.returncode == 0, res.stderr[-800:]
            docs = []
            for path in Path(out).rglob("objects.yaml"):
                if "lab-spark" not in str(path):
                    continue
                conv = subprocess.run(["yq", "-o=json", "-I=0", "-N", "."], input=path.read_text(), capture_output=True,
                                      text=True, timeout=60)
                docs += [json.loads(line) for line in conv.stdout.splitlines() if line.strip()]
            return docs

    def test_without_a_build_there_is_no_fluid_host_but_the_files_ship(self):
        docs = self.apache_offline
        self.assertNotIn(("Deployment", "fluid-host"), docs)
        data = docs[("ConfigMap", "lab-spark")]["data"]
        self.assertIn("lab-spark-submit", data)
        self.assertIn("lab-spark-entry.sh", data)
        self.assertEqual(data["lab-spark-submit"], SUBMIT.read_text())

    def test_the_configmap_ships_the_probe_contract_its_overlay_and_no_aws_files(self):
        data = self.apache[("ConfigMap", "lab-spark")]["data"]
        for name in ("probe.fluid.yaml", "probe.overlay.local.yaml", "seed.py", "verify.py", "lab-spark-submit",
                     "lab-spark-entry.sh", "spark-defaults.conf", "spark-image"):
            self.assertIn(name, data)
        for name in ("compile.sh", "extract.py", "smoke.sh", "spark-runtime"):
            self.assertNotIn(name, data)
        self.assertEqual(data["probe.fluid.yaml"], (SPARK_DIR / "probe.fluid.yaml").read_text())

    def test_tier_spark_ships_the_recorded_open_image(self):
        data = self.apache[("ConfigMap", "lab-spark")]["data"]
        self.assertEqual(data["spark-image"], APACHE)
        self.assertEqual(data["spark-defaults.conf"], APACHE_CONF.read_text())
        dep = self.apache[("Deployment", "fluid-host")]["spec"]["template"]["spec"]["containers"][0]
        env = {e["name"]: e["value"] for e in dep["env"] if "value" in e}
        self.assertEqual(env["LAB_SPARK_IMAGE"], APACHE)
        self.assertNotIn("LAB_SPARK_RUNTIME", env)
        self.assertEqual(env["FLUID_SPARK_SUBMIT_PROPERTIES_FILE"], PROPS)
        self.assertNotIn("public.ecr.aws", json.dumps(list(self.apache.values())))

    def test_without_a_spark_iceberg_record_the_files_render_but_no_fluid_host(self):
        for docs in (self.apache_offline, self.apache_runner_only):
            self.assertNotIn(("Deployment", "fluid-host"), docs)
            data = docs[("ConfigMap", "lab-spark")]["data"]
            self.assertEqual(data["spark-image"], "")
            self.assertIn("lab-spark-submit", data)

    def test_an_image_that_is_not_the_lab_built_one_fails_the_render(self):
        for image in ("docker.io/apache/spark:3.5.8", "public.example.com/x/y@sha256:" + "0" * 64,
                      "lab.local/spark-iceberg:", "lab.local/spark-iceberg:" + "a" * 129,
                      "lab.local/spark-iceberg:bad tag"):
            with self.subTest(image=image):
                res = self.render("spark", "", image, expect_ok=False)
                self.assertNotEqual(res.returncode, 0)
                self.assertIn("LAB_SPARK_IMAGE", res.stderr)

    def test_fluid_host_runs_the_recorded_runner_restricted_with_the_operators_environment(self):
        docs = self.built
        dep = docs[("Deployment", "fluid-host")]
        pod = dep["spec"]["template"]["spec"]
        c = pod["containers"][0]
        self.assertEqual(dep["spec"]["replicas"], 1)
        self.assertEqual(c["image"], "lab.local/fluid-runner:9.9.9")
        self.assertEqual(c["command"], ["sleep", "infinity"])
        self.assertIs(pod["shareProcessNamespace"], True, "the pause container is PID 1 and reaps orphans")
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(pod["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertIs(c["securityContext"]["allowPrivilegeEscalation"], False)
        self.assertEqual(c["securityContext"]["capabilities"], {"drop": ["ALL"]})
        self.assertEqual(pod["serviceAccountName"], "fluid-host")
        env = {e["name"]: e for e in c["env"]}
        self.assertEqual(env["FLUID_SPARK_SUBMIT_EXECUTABLE"]["value"], "/opt/lab-spark/lab-spark-submit")
        self.assertEqual(env["FLUID_SPARK_SUBMIT_PROPERTIES_FILE"]["value"], PROPS)
        self.assertEqual(env["FLUID_SPARK_LOCAL_ROOT"]["value"], "s3://lab-spark-artifacts/runs")
        self.assertEqual(env["AWS_ENDPOINT_URL_S3"]["value"], "https://s3.lab.test")
        for key, value in spark.OPERATOR_ALLOWLISTS.items():
            self.assertEqual(env[key]["value"], value)
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            self.assertEqual(env[key]["valueFrom"]["secretKeyRef"]["name"], "spark-artifacts-s3")
        self.assertTrue(env["AWS_CA_BUNDLE"]["value"].startswith("/etc/lab-ca/"))
        mounts = {m["name"]: m for m in c["volumeMounts"]}
        self.assertEqual(set(mounts), {"spark", "state", "lab-ca"})
        volumes = {v["name"]: v for v in pod["volumes"]}
        self.assertEqual(volumes["state"]["persistentVolumeClaim"]["claimName"], "fluid-host-state")
        self.assertEqual(volumes["spark"]["configMap"]["defaultMode"], 0o555)
        self.assertIn(("PersistentVolumeClaim", "fluid-host-state"), docs)

    def test_fluid_host_may_only_create_get_list_and_delete_pods_and_read_their_logs_in_spark(self):
        docs = self.built
        rules = docs[("Role", "fluid-host")]["rules"]
        self.assertEqual(sorted(map(json.dumps, rules)), sorted(map(json.dumps, [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "get", "list", "delete"]},
            {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]}])))
        binding = docs[("RoleBinding", "fluid-host")]
        self.assertEqual(binding["roleRef"]["name"], "fluid-host")
        self.assertEqual(binding["subjects"], [{"kind": "ServiceAccount", "name": "fluid-host", "namespace": "spark"}])
        self.assertEqual(docs[("Role", "fluid-host")]["metadata"]["namespace"], "spark")
        self.assertNotIn(("ClusterRole", "fluid-host"), docs)

    def test_every_call_the_wrapper_makes_is_a_verb_the_role_allows(self):
        source = SUBMIT.read_text()
        for needed in ('"POST", pods_path()', '"DELETE", pods_path(name)', "/log?container=", "labelSelector"):
            self.assertIn(needed, source)
        self.assertNotIn("watch=", source)


if __name__ == "__main__":
    unittest.main()
