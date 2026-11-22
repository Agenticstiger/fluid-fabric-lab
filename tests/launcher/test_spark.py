# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The spark add-on tier: the smoke Jobs are restricted and keep their secret in a
Secret, `fluid apply` runs in fluid-host behind the operator's allowlists, the
result is judged exactly and must be this run's write, a stuck or competing Job
fails fast and an unfinished one is deleted, the spark client's grant is
idempotent, and the open image carries no licence exception."""

import base64
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT, TIERS_DOC, FakeRunner

from labctl import accept_runner, bootstrap, check, config, http, mirror, proc, spark, state
from labctl.output import FAIL

LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
SPARK_DIR = REPO_ROOT / "platform" / "spark"
IMAGE = "lab.local/spark-iceberg:3.5.8-iceberg1.12.0-c15dd816"
SEED_LOGS = ("=== spark\nnoise\nLAB-SPARK-SEEDED {\"rows\": 3, \"sink_rows\": 0, \"sink_snapshots\": 4}\n")
VERIFY_LOGS = ("=== spark\nLAB-SPARK-RESULT {\"rows\": {\"2026-11-17\": 2, \"2026-11-18\": 1}, \"snapshots\": 5, "
               "\"spark\": \"3.5.8\"}\n")
LOGS = SEED_LOGS + VERIFY_LOGS


def shipped_files(directory: Path) -> list:
    """The regular source files of a ConfigMap directory: never a directory, `__pycache__` or bytecode."""
    return sorted(f for f in directory.iterdir() if f.is_file() and f.suffix != ".pyc" and not f.name.startswith("."))


class SmokeJob(unittest.TestCase):
    def setUp(self):
        self.job = spark.job("lab-check-spark-seed-1", IMAGE, "seed.py")
        self.pod = self.job["spec"]["template"]["spec"]

    def test_every_container_meets_pod_security_restricted(self):
        self.assertTrue(self.pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(self.pod["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertFalse(self.pod["automountServiceAccountToken"])
        self.assertNotIn("initContainers", self.pod)
        for c in self.pod["containers"]:
            sc = c["securityContext"]
            self.assertFalse(sc["allowPrivilegeEscalation"], c["name"])
            self.assertEqual(sc["capabilities"], {"drop": ["ALL"]}, c["name"])
            self.assertEqual((sc["runAsUser"], sc["runAsGroup"]), (185, 185), c["name"])
            self.assertIn("memory", c["resources"]["limits"], c["name"])

    def test_the_job_runs_the_script_on_the_lab_built_image_which_is_never_pulled(self):
        (run,) = self.pod["containers"]
        self.assertEqual((run["image"], run["imagePullPolicy"]), (IMAGE, "Never"))
        self.assertEqual(run["command"], ["bash", "/spark/lab-spark-entry.sh", "/spark/seed.py"])
        self.assertEqual(spark.job("x", IMAGE, "verify.py")["spec"]["template"]["spec"]["containers"][0]["command"][-1],
                         "/spark/verify.py")

    def test_the_client_secret_reaches_the_pod_only_through_a_secret_reference(self):
        env = {e["name"]: e for e in self.pod["containers"][0]["env"]}
        self.assertEqual(env["LAKEKEEPER_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]["name"], "spark-lakekeeper-client")
        self.assertNotIn("value", env["LAKEKEEPER_CLIENT_SECRET"])

    def test_the_smoke_files_survive_lab_objects_tpl(self):
        # lab-objects renders every object through `tpl`; a brace pair would be executed.
        listed = (REPO_ROOT / "platform/components/lab-spark/values.yaml.gotmpl").read_text()
        for f in shipped_files(SPARK_DIR):
            self.assertNotIn("{{", f.read_text(), f.name)
            if f.name == "spark-defaults.apache.conf":      # shipped under the key spark-defaults.conf
                self.assertIn(f.name, listed)
            else:
                self.assertIn(f'"{f.name}"', listed, f"{f.name} is not shipped in the lab-spark ConfigMap")

    def test_a_pycache_in_the_directory_is_not_a_smoke_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp) / "spark"
            shutil.copytree(SPARK_DIR, copy)
            (copy / "__pycache__").mkdir()
            (copy / "__pycache__" / "x.pyc").write_bytes(b"\0\1")
            (copy / "stale.pyc").write_bytes(b"\0")
            self.assertEqual([f.name for f in shipped_files(copy)], [f.name for f in shipped_files(SPARK_DIR)])
            for f in shipped_files(copy):
                f.read_text()       # no directory, no binary

    def test_the_seed_empties_the_sink_so_the_rows_are_this_runs_write(self):
        self.assertIn("DELETE FROM fluid.curated.events_daily", (SPARK_DIR / "seed.py").read_text())

    def test_the_compile_step_of_the_old_smoke_is_gone(self):
        for name in ("compile.sh", "extract.py", "smoke.sh", "spark-defaults.emr.conf"):
            self.assertFalse((SPARK_DIR / name).exists(), name)


class ProbeContract(unittest.TestCase):
    """The probe contract, the operator's allowlists for it and fluid-host's copy of them stay in step."""

    CONTRACT = config.load_yaml(SPARK_DIR / "probe.fluid.yaml")

    def test_it_is_a_local_batch_contract_over_the_lakekeeper_warehouse(self):
        c = self.CONTRACT
        self.assertEqual(c["id"], spark.PRODUCT_ID)
        self.assertEqual(c["environments"], {"local": {}})
        (build,) = c["builds"]
        self.assertEqual(build["id"], spark.BUILD_ID)
        self.assertEqual(build["properties"]["parameters"]["spark"], {"runtime": "local", "mode": "batch"})
        for expose in c["exposes"]:
            loc = expose["binding"]["location"]
            self.assertEqual((expose["binding"]["platform"], loc["catalog"], loc["warehouse"]),
                             ("local", "lakekeeper", "sales-wh"))
        self.assertIn("FROM raw.events GROUP BY", build["properties"]["sql"])

    def test_the_overlay_names_the_same_build(self):
        overlay = config.load_yaml(SPARK_DIR / "probe.overlay.local.yaml")
        self.assertEqual([b["id"] for b in overlay["builds"]], [spark.BUILD_ID])

    def test_the_sql_allowlist_pins_the_reviewed_sql_and_the_namespaces_are_the_exposes(self):
        (build,) = self.CONTRACT["builds"]
        digest = hashlib.sha256(build["properties"]["sql"].strip().encode("utf-8")).hexdigest()
        allow = spark.OPERATOR_ALLOWLISTS
        self.assertEqual(allow["FLUID_SPARK_CONTRACT_SQL_ALLOWLIST"],
                         f"{spark.PRODUCT_ID}/{spark.BUILD_ID}@sha256:{digest}",
                         "the probe's SQL changed: review it and list its new digest")
        self.assertEqual(set(allow["FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST"].split(",")),
                         {f"sales-wh/{e['binding']['location']['database']}" for e in self.CONTRACT["exposes"]})
        self.assertEqual(set(allow), {"FLUID_SPARK_CONTRACT_SQL_ALLOWLIST", "FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST"})

    def test_fluid_host_carries_the_same_allowlists(self):
        template = (REPO_ROOT / "platform/components/lab-spark/values.yaml.gotmpl").read_text()
        for key, value in spark.OPERATOR_ALLOWLISTS.items():
            self.assertIn(f'{{name: {key}, value: "{value}"}}', template)


class Judge(unittest.TestCase):
    def test_the_expected_rows_written_after_the_seed_pass(self):
        result = spark.judge(spark.marks(LOGS))
        self.assertEqual((result["spark"], result["snapshots"], result["seed_snapshots"]), ("3.5.8", 5, 4))

    def test_a_fresh_sink_with_no_snapshot_before_the_build_passes(self):
        # On a new lab the seed creates the sink, and its DELETE may add no snapshot.
        fresh = LOGS.replace('"sink_snapshots": 4', '"sink_snapshots": 0').replace('"snapshots": 5', '"snapshots": 1')
        self.assertEqual(spark.judge(spark.marks(fresh))["seed_snapshots"], 0)

    def test_wrong_rows_a_stale_sink_or_a_missing_step_fail(self):
        for logs in (LOGS.replace('"2026-11-18": 1', '"2026-11-18": 2'),     # wrong rows
                     LOGS.replace('"snapshots": 5', '"snapshots": 4'),       # no newer snapshot: nothing written
                     LOGS.replace('"sink_rows": 0', '"sink_rows": 2'),       # the seed did not empty the sink
                     SEED_LOGS,                                              # the read-back never ran
                     VERIFY_LOGS):                                           # the seed never ran
            with self.assertRaises(spark.SparkError):
                spark.judge(spark.marks(logs))

    def test_the_secret_is_redacted_raw_and_base64(self):
        text = "a s3cr3t b " + base64.b64encode(b"spark:s3cr3t").decode() + " c " + base64.b64encode(b"s3cr3t").decode()
        self.assertEqual(spark._redact(text, "s3cr3t"), "a <redacted> b <redacted> c <redacted>")


def waiting(container, reason, message="", init=False):
    """A pod whose container waits for `reason`."""
    key = "initContainerStatuses" if init else "containerStatuses"
    return {"status": {key: [{"name": container, "state": {"waiting": {"reason": reason, "message": message}}}]}}


class FakeKube:
    """Stands in for kube.Kube in the smoke Jobs. `statuses` and `pods` are scripts: the i-th poll of
    this run's Job sees item i, and the last item repeats. `jobs` are earlier smoke Jobs, and
    `old_pods` their pods by Job name."""

    def __init__(self, statuses=({},), pods=([],), jobs=(), old_pods=None, logs="", gone=False):
        self.statuses, self.pod_script, self.jobs = list(statuses), list(pods), list(jobs)
        self.old_pods, self.logs, self.gone = old_pods or {}, logs, gone
        self.polls, self.applied, self.deleted = 0, [], []

    @staticmethod
    def _at(script, i):
        return script[min(i, len(script) - 1)]

    @property
    def name(self):
        return self.applied[-1]["metadata"]["name"] if self.applied else None

    def logs_of(self, name):
        return self.logs

    def run(self, *args, **_kw):
        if args[0] == "delete":
            self.deleted.append(args[2] if args[1] == "job" else args[1])
        out = self.logs_of(args[1].split("/", 1)[-1]) if args[0] == "logs" else ""
        return proc.Result(list(args), 0, out, "" if out else "container is waiting to start: PodInitializing")

    def apply(self, objects):
        self.applied += objects

    def get(self, kind, name=None, namespace=None, selector=None):
        if kind == "jobs":
            return {"items": self.jobs}
        if kind == "pods":
            owner = (selector or "").split("=", 1)[-1]
            return {"items": self._at(self.pod_script, self.polls - 1) if owner == self.name
                    else self.old_pods.get(owner, [])}
        self.polls += 1
        return None if self.gone else {"status": self._at(self.statuses, self.polls - 1)}


def clock():
    return iter(range(0, 10 ** 6, 10)).__next__


def run_job(k, sleep=lambda _s: None):
    """The seed Job of the smoke run, to its end: (its name, its marks)."""
    return spark._run_job(k, "seed", IMAGE, lambda t: spark._redact(t, "s3cr3t"), clock(), sleep)


class SmokeJobLifecycle(unittest.TestCase):
    def test_a_completed_job_is_read_and_kept_for_its_logs(self):
        k = FakeKube(statuses=[{"active": 1}, {"succeeded": 1}], logs=LOGS)
        name, found = run_job(k)
        self.assertTrue(name.startswith("lab-check-spark-seed-"))
        self.assertIn("LAB-SPARK-RESULT", found)
        self.assertEqual(k.deleted, [])

    def test_a_failed_job_ends_the_wait_at_once_and_is_kept_for_its_logs(self):
        k = FakeKube(statuses=[{"conditions": [{"type": "Failed", "status": "True"}]}])
        with self.assertRaises(spark.SparkError) as caught:
            run_job(k)
        self.assertIn("failed", str(caught.exception))
        self.assertIn("PodInitializing", str(caught.exception))  # stderr is kept when there is no log
        self.assertIn("stays an hour", caught.exception.advice)
        self.assertEqual(k.deleted, [])

    def test_a_job_that_cannot_start_or_keeps_failing_to_pull_fails_and_is_deleted(self):
        for pod, says in ((waiting("spark", "ErrImageNeverPull", "image not present"),
                           "cannot start: container spark: ErrImageNeverPull"),         # at once
                          (waiting("spark", "ErrImagePull", "connection refused"),
                           f"still failing after {spark.PULL_GRACE_S} s")):            # after the grace
            with self.subTest(says):
                k = FakeKube(statuses=[{"active": 1}], pods=[[pod]])
                with self.assertRaises(spark.SparkError) as caught:
                    run_job(k)
                self.assertIn(says, str(caught.exception))
                self.assertIn("deleted", caught.exception.advice)
                self.assertEqual(k.deleted, [k.name])

    def test_a_pull_error_that_clears_within_the_grace_is_waited_out(self):
        backoff = waiting("spark", "ImagePullBackOff", "Back-off pulling image")
        k = FakeKube(statuses=[{"active": 1}] * 4 + [{"succeeded": 1}], pods=[[backoff]] * 3 + [[]], logs=LOGS)
        self.assertIn("LAB-SPARK-RESULT", run_job(k)[1])
        self.assertEqual(k.deleted, [])

    def test_an_interrupted_run_deletes_its_job(self):
        def interrupt(_s):
            raise KeyboardInterrupt

        class InterruptedApply(FakeKube):
            def apply(self, objects):
                super().apply(objects)   # the Job exists, then the interrupt lands
                raise KeyboardInterrupt
        for k, sleep in ((FakeKube(statuses=[{"active": 1}]), interrupt), (InterruptedApply(), lambda _s: None)):
            with self.subTest(type(k).__name__):
                with self.assertRaises(KeyboardInterrupt):
                    run_job(k, sleep=sleep)
                self.assertEqual(k.deleted, [k.name])


class Leftovers(unittest.TestCase):
    """Smoke Jobs an earlier or concurrent check left."""

    def test_an_unfinished_smoke_job_is_not_disturbed(self):
        # A running Job, and one created a moment ago whose status is still empty.
        for status in ({"active": 1}, {}):
            k = FakeKube(jobs=[{"metadata": {"name": "lab-check-spark-1"}, "status": status}])
            with self.assertRaises(spark.SparkError) as caught:
                run_job(k)
            self.assertIn("another spark smoke Job is unfinished (lab-check-spark-1)", str(caught.exception))
            self.assertEqual((k.applied, k.deleted), ([], []))

    def test_a_leftover_job_that_can_never_start_is_deleted_and_the_run_goes_on(self):
        old = {"metadata": {"name": "lab-check-spark-old"}, "status": {"active": 1}}
        k = FakeKube(statuses=[{"succeeded": 1}], jobs=[old], logs=LOGS,
                     old_pods={"lab-check-spark-old": [waiting("spark", "ErrImageNeverPull")]})
        self.assertIn("LAB-SPARK-RESULT", run_job(k)[1])
        self.assertEqual(k.deleted, ["lab-check-spark-old"])

    def test_finished_jobs_are_left_to_their_ttl_and_a_vanished_job_is_reported(self):
        k = FakeKube(jobs=[{"metadata": {"name": "old"}, "status": {"succeeded": 1}}], gone=True)
        with self.assertRaises(spark.SparkError) as caught:
            run_job(k)
        self.assertIn("disappeared", str(caught.exception))
        self.assertNotIn("old", k.deleted)


class FlowKube(FakeKube):
    """FakeKube plus the fluid-host pod: records every exec; `forge_rc` is what `fluid apply` exits with."""

    def __init__(self, forge_rc=0, **kw):
        super().__init__(statuses=[{"succeeded": 1}], **kw)
        self.forge_rc, self.execs = forge_rc, []

    def logs_of(self, name):
        return SEED_LOGS if "-seed-" in name else VERIFY_LOGS

    def first_pod(self, namespace, selector, timeout_s=300):
        self.execs.append(("first_pod", namespace, selector))
        return "fluid-host-0"

    def exec(self, namespace, pod, command, **kw):
        self.execs.append((namespace, pod, list(command)))
        is_forge = len(command) > 2 and command[2] == accept_runner.FORGE_WRAPPER
        if is_forge:
            return proc.Result(command, self.forge_rc, "", "boom" if self.forge_rc else "")
        return proc.Result(command, 0, "", "")


def run_smoke(k):
    return spark.run_smoke(k, IMAGE, secret="s3cr3t", sleep=lambda _s: None, clock=clock())


class RunSmoke(unittest.TestCase):
    def test_seed_then_apply_in_fluid_host_then_read_back_and_judge(self):
        k = FlowKube()
        result = run_smoke(k)
        self.assertEqual(result["rows"], spark.EXPECTED)
        self.assertEqual((result["image"], result["spark"]), (IMAGE, "3.5.8"))
        seed, verify = k.applied
        self.assertIn("-seed-", seed["metadata"]["name"])
        self.assertIn("-verify-", verify["metadata"]["name"])
        self.assertEqual(result["jobs"], [seed["metadata"]["name"], verify["metadata"]["name"]])
        for job in k.applied:
            self.assertEqual(job["spec"]["template"]["spec"]["containers"][0]["image"], IMAGE)
        execs = [e for e in k.execs if e[0] == "spark"]
        self.assertEqual(len(execs), 2)
        _, host, prepare = execs[0]
        self.assertEqual(host, "fluid-host-0")
        self.assertEqual(prepare[:3], ["sh", "-c", spark.PREPARE])
        self.assertEqual(prepare[-1], spark.WORKDIR)
        self.assertIn("/opt/lab-spark/probe.fluid.yaml", spark.PREPARE)
        self.assertIn("overlays/local.yaml", spark.PREPARE)

    def test_the_apply_is_strict_with_the_operators_allowlists_and_runs_the_scheduled_command(self):
        k = FlowKube()
        run_smoke(k)
        forge = [e[2] for e in k.execs if e[0] == "spark" and e[2][2] == accept_runner.FORGE_WRAPPER][0]
        self.assertEqual(forge[3:5], ["sh", spark.WORKDIR])
        argv = forge[6:]
        self.assertEqual(argv[0], "env")
        self.assertIn("VALIDATE_STRICT=true", argv)
        self.assertIn("FLUID_ENV=local", argv)
        for key, value in spark.OPERATOR_ALLOWLISTS.items():
            self.assertIn(f"{key}={value}", argv)
        self.assertEqual(argv[argv.index("fluid"):],
                         ["fluid", "apply", "contract.fluid.yaml", "--env", "local", "--mode", "amend-and-build",
                          "--build-id", "daily-curated", "--yes"])
        self.assertFalse([a for a in argv if "s3cr3t" in a])

    def test_a_failed_apply_fails_the_run_before_the_read_back(self):
        k = FlowKube(forge_rc=2)
        with self.assertRaises(spark.SparkError) as caught:
            run_smoke(k)
        self.assertIn("exited 2 in fluid-host", str(caught.exception))
        self.assertEqual(len(k.applied), 1)          # only the seed ran

    def test_a_wrong_result_fails_even_when_every_step_exited_0(self):
        k = FlowKube()
        k.logs_of = lambda name: SEED_LOGS if "-seed-" in name else VERIFY_LOGS.replace('"2026-11-18": 1', '"2026-11-18": 5')
        with self.assertRaises(spark.SparkError):
            run_smoke(k)


class SparkGrant(unittest.TestCase):
    def client(self, assignments):
        calls = []

        class Client:
            def wait_token(self, _c):
                return "t-spark"

            def token(self, _c):
                return "t-admin"

            def https(self, method, host, path, headers=None, body=None, **_kw):
                calls.append((method, path, body))
                answers = {("POST", "/management/v1/user"): (201, {}),
                           ("GET", "/management/v1/whoami"): (200, {"id": "oidc~spark-sub"}),
                           ("GET", "/management/v1/warehouse"): (200, {"warehouses": [{"name": "sales-wh", "id": "w1"}]}),
                           ("GET", "/management/v1/permissions/warehouse/w1/assignments"): (200, {"assignments": assignments}),
                           ("POST", "/management/v1/permissions/warehouse/w1/assignments"): (204, {})}
                status, payload = answers.get((method, path), (599, {}))
                return http.Response(status, json.dumps(payload).encode())

        return Client(), calls

    def test_the_grant_is_written_once_and_reruns_are_idempotent(self):
        client, calls = self.client([])
        self.assertIn("may create in sales-wh", bootstrap.spark_grant(client))
        provision = [b for m, p, b in calls if (m, p) == ("POST", "/management/v1/user")]
        self.assertEqual(provision, [{"update-if-exists": True}])  # a rerun must not get 409 UserAlreadyExists
        (writes,) = [b for m, p, b in calls if p.endswith("/assignments") and m == "POST"]
        self.assertEqual(writes, {"writes": [{"type": "create", "user": "oidc~spark-sub"}]})
        held = [{"type": t, "user": "oidc~spark-sub"} for t in bootstrap.SPARK_GRANTS]
        client, calls = self.client(held)
        self.assertIn("already granted", bootstrap.spark_grant(client))
        self.assertFalse([1 for m, p, _ in calls if p.endswith("/assignments") and m == "POST"])


class PullConfig(unittest.TestCase):
    def test_no_lab_image_is_pulled_with_an_operators_credentials(self):
        hosts = mirror.upstream_hosts(mirror.all_images(LOCK))
        self.assertNotIn("public.ecr.aws", hosts)
        self.assertNotIn(mirror.REGISTRY_IMAGE_ID, [e["id"] for e in mirror.all_images(LOCK)])
        self.assertFalse([i["id"] for i in LOCK["images"] if i.get("pull_auth")])
        self.assertFalse(hasattr(mirror, "PULL_AUTH"))


class SparkRecord(unittest.TestCase):
    def test_the_smoke_needs_the_runner_image_in_the_node(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            lab = state.lab_for(root, "ffl-x", {"LAB_STATE_DIR": str(Path(tmp) / "state")})
            ctx = check.Ctx(root, lab, FakeRunner(table={("docker", "exec"): (1, "no such image")}), None, LOCK,
                            TIERS_DOC, "core", config.resolve_tier(TIERS_DOC, "core"), creds={"client.spark": "x"})
            self.assertIn("has none", check._record("spark", check.spark_smoke, ctx)["message"])
            state.write_json_private(lab.images_record, {"fluid-runner": {"tag": "lab.local/fluid-runner:1",
                                                                           "layers": ["sha256:" + "1" * 64]}})
            rec = check._record("spark", check.spark_smoke, ctx)
            self.assertEqual(rec["status"], FAIL)
            self.assertIn("does not hold lab.local/fluid-runner:1", rec["message"])
            # No recorded layers: the node lookup also gives None, and that must not count as a match.
            state.write_json_private(lab.images_record, {"fluid-runner": {"tag": "lab.local/fluid-runner:1"}})
            rec = check._record("spark", check.spark_smoke, ctx)
            self.assertEqual(rec["status"], FAIL)
            self.assertIn("does not hold lab.local/fluid-runner:1", rec["message"])


class ApacheRecord(unittest.TestCase):
    """`lab check --tier spark` runs the spark-iceberg image `lab build` recorded, and judges the node first."""

    def ctx(self, tmp, runner, tier):
        root = Path(tmp) / "repo"
        root.mkdir()
        lab = state.lab_for(root, "ffl-x", {"LAB_STATE_DIR": str(Path(tmp) / "state")})
        tiers = config.load_yaml(REPO_ROOT / config.TIERS)
        return lab, check.Ctx(root, lab, runner, None, LOCK, tiers, tier, config.resolve_tier(tiers, tier),
                              creds={"client.spark": "x"})

    def test_the_spark_tier_needs_a_spark_iceberg_record_and_names_the_build_that_makes_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab, ctx = self.ctx(tmp, FakeRunner(table={("docker", "exec"): (1, "no such image")}), "spark")
            state.write_json_private(lab.images_record, {"fluid-runner": {"tag": "lab.local/fluid-runner:1",
                                                                           "layers": ["sha256:" + "1" * 64]}})
            runner = FakeRunner(table={("docker", "exec"): lambda argv, **_kw: proc.Result(
                argv, 0, json.dumps({"info": {"imageSpec": {"rootfs": {"diff_ids": ["sha256:" + "1" * 64]}}}}), "")})
            ctx.runner = runner
            rec = check._record("spark", check.spark_smoke, ctx)
            self.assertEqual(rec["status"], FAIL)
            self.assertIn("spark-iceberg image", rec["message"])
            self.assertIn("./lab build spark-iceberg --name ffl-x", rec["advice"])

    def test_the_h_mirror_check_accepts_the_recorded_spark_iceberg_image_and_nothing_else_unpinned(self):
        tag = "lab.local/spark-iceberg:3.5.8-iceberg1.12.0-c15dd816"
        self.assertEqual(check.unpinned([("spark", tag)], LOCK, {tag}), [])
        self.assertEqual(check.unpinned([("spark", tag)], LOCK, {"lab.local/fluid-runner:1"}), [f"spark: {tag}"])
        self.assertEqual(check.unpinned([("spark", "docker.io/apache/spark:3.5.8")], LOCK, {tag}),
                         ["spark: docker.io/apache/spark:3.5.8"])

    def test_the_images_the_pods_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = Path(tmp) / "images.json"
            self.assertEqual(spark.recorded_image(record, spark.APACHE_IMAGE_ID), "")     # no record, no image yet
            record.write_text(json.dumps({"spark-iceberg": {"tag": "lab.local/spark-iceberg:t", "layers": ["a"]},
                                          "fluid-runner": {"tag": "lab.local/fluid-runner:1"}}))
            self.assertEqual(spark.recorded_image(record, spark.APACHE_IMAGE_ID), "lab.local/spark-iceberg:t")
            self.assertEqual(spark.recorded_runner(record), "lab.local/fluid-runner:1")


class TierWiring(unittest.TestCase):
    TIERS = config.load_yaml(REPO_ROOT / config.TIERS)["tiers"]

    def test_spark_is_the_open_runtime_and_the_only_spark_tier(self):
        tier = self.TIERS["spark"]
        self.assertTrue(tier["opt_in"])
        self.assertEqual((tier["requires"], tier["releases"]), (["core"], ["lab-spark"]))
        self.assertIn("spark-iceberg", tier["description"])
        self.assertIn("open software only", tier["description"])
        self.assertFalse([t for t in self.TIERS if "emr" in t or "aws" in t or "localstack" in t])

    def test_lab_up_supports_the_tier_and_passes_the_image_to_the_helmfile(self):
        from labctl import up
        self.assertIn("spark", up.SUPPORTED_TIERS)
        self.assertFalse([t for t in up.SUPPORTED_TIERS if "emr" in t or "localstack" in t])
        source = (REPO_ROOT / "labctl/up.py").read_text()
        for name in ("spark.IMAGE_ENV", "spark.RUNNER_ENV"):
            self.assertIn(name, source)
        self.assertEqual((spark.IMAGE_ENV, spark.RUNNER_ENV), ("LAB_SPARK_IMAGE", "LAB_FLUID_RUNNER_IMAGE"))
        self.assertFalse(hasattr(spark, "RUNTIME_ENV"))

    def test_mirror_of_the_spark_tier_needs_no_cloud_credentials(self):
        tiers_doc = config.load_yaml(REPO_ROOT / config.TIERS)
        ids = {e["id"] for e in mirror.tier_images(LOCK, tiers_doc, "spark")}
        self.assertNotIn("emr-spark", ids)
        self.assertFalse([e for e in mirror.tier_images(LOCK, tiers_doc, "spark") if e.get("pull_auth")])

    def test_the_spark_tier_has_the_pod_security_namespace_and_the_footprint_estimate(self):
        namespaces = config.load_yaml(REPO_ROOT / "platform/namespaces.yaml")["namespaces"]
        self.assertEqual(set(namespaces["spark"]["tier"]), {"spark", "orchestration"})
        footprint = (REPO_ROOT / "tools/l0/footprint.py").read_text()
        self.assertNotIn("spark-emr", footprint)
        self.assertIn("one pod on the spark-iceberg image", footprint)

    def test_the_check_record_is_added_on_the_spark_tier_only(self):
        tiers_doc = config.load_yaml(REPO_ROOT / config.TIERS)
        self.assertIn("spark", check.records_for(config.resolve_tier(tiers_doc, "spark")))
        self.assertNotIn("spark", check.records_for(config.resolve_tier(tiers_doc, "core")))


class UpSparkEnvironment(unittest.TestCase):
    """What `lab up` hands the Helmfile render: the image of the Spark pods."""

    def helmfile_env(self, tier, images=None):
        from unittest import mock
        from labctl import up
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            runner = FakeRunner(table={("helmfile",): (0, "")})
            run = up.Up(root, tier, name="ffl-e", env={"LAB_STATE_DIR": str(Path(tmp) / "state")}, runner=runner,
                        log=lambda m: logs.append(m))
            logs = []
            tiers = config.load_yaml(REPO_ROOT / config.TIERS)
            run.tiers_doc, run.lock = tiers, LOCK
            run.tier = config.resolve_tier(tiers, tier)
            run.lab.state.mkdir(parents=True)
            if images is not None:
                state.write_json_private(run.lab.images_record, images)
            with mock.patch.object(bootstrap, "wait_keycloak"):
                run.helmfile()
            env = [kw["env"] for argv, kw in runner.calls if argv[0] == "helmfile"][0]
            return env, logs

    def test_tier_spark_passes_the_recorded_open_image(self):
        env, logs = self.helmfile_env("spark", {"fluid-runner": {"tag": "lab.local/fluid-runner:1"},
                                                 "spark-iceberg": {"tag": "lab.local/spark-iceberg:t"}})
        self.assertEqual(env["LAB_SPARK_IMAGE"], "lab.local/spark-iceberg:t")
        self.assertEqual(env["LAB_FLUID_RUNNER_IMAGE"], "lab.local/fluid-runner:1")
        self.assertNotIn("LAB_SPARK_RUNTIME", env)
        self.assertFalse([m for m in logs if "no spark-iceberg image" in m])

    def test_a_first_up_has_no_record_so_it_goes_on_without_fluid_host_and_says_what_to_do(self):
        # The record is written when `lab build` loads the image into this lab's node, so a first `lab up`
        # (no cluster yet) cannot have it: refusing here would break up, build, up again.
        env, logs = self.helmfile_env("spark")
        self.assertEqual((env["LAB_SPARK_IMAGE"], env["LAB_FLUID_RUNNER_IMAGE"]), ("", ""))
        (said,) = [m for m in logs if "no spark-iceberg image" in m]
        self.assertIn("./lab build spark-iceberg --name ffl-e", said)
        self.assertIn("fluid-host is not rendered", said)

    def test_a_tier_without_lab_spark_sets_none_of_it(self):
        import os
        env, _ = self.helmfile_env("core")
        for name in ("LAB_SPARK_IMAGE", "LAB_FLUID_RUNNER_IMAGE"):
            self.assertNotIn(name, {k for k in env if k not in os.environ})


class LicenceRecord(unittest.TestCase):
    def test_the_open_spark_image_carries_no_licence_exception(self):
        entry = next(b for b in LOCK["built_images"] if b["id"] == "spark-iceberg")
        self.assertEqual(entry["component"], "spark")
        self.assertNotIn("license", entry)
        self.assertFalse([i for i in LOCK["images"] if i.get("component") == "spark"])

    def test_the_licence_gate_has_no_cloud_exception_and_the_lock_no_such_licence(self):
        text = (REPO_ROOT / "tools/l0/licence.py").read_text()
        for gone in ("AWS_IP", "LOCALSTACK", "SCOPED"):
            self.assertNotIn(gone, text)
        self.assertFalse([i["id"] for i in LOCK["images"]
                          if str((i.get("license") or {}).get("spdx", "")).startswith("LicenseRef-")])
        self.assertFalse(list((REPO_ROOT / "tests/fixtures/licence").glob("runtime-aws*")))
        self.assertFalse(list((REPO_ROOT / "tests/fixtures/licence").glob("runtime-localstack*")))


if __name__ == "__main__":
    unittest.main()
