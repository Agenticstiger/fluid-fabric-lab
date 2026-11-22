# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab build spark-iceberg`: the lock entry, the Dockerfile and the properties file agree, the probe is
judged strictly, and only a checked image is loaded and recorded under its own name."""

import json
import re
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT, FakeRunner

from labctl import build, config, down, proc, spark_image, state
from labctl.output import FAIL, PASS, WARN

LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
ENTRY = next(b for b in LOCK["built_images"] if b["id"] == "spark-iceberg")
FILES = ENTRY["contents"]["files"]
DOCKERFILE = (REPO_ROOT / spark_image.DIR / "Dockerfile").read_text()
CONF = (REPO_ROOT / spark_image.CONF).read_text()
TAG = spark_image.tag_for(ENTRY)
LAYERS = ["sha256:" + "1" * 64, "sha256:" + "2" * 64]
IMAGE_ID = "sha256:" + "9" * 64


def good_probe() -> dict:
    return {"uid": 185,
            "files": {f["path"]: {"sha256": f["sha256"], "mode": "0o644"} for f in FILES},
            "version": {"rc": 0, "text": "Welcome to\n   version 3.5.8\nUsing Scala version 2.12.18"},
            "classes": {c: True for c in spark_image.CLASSES},
            "submit": {"rc": 0, "tail": "done"}}


class SparkIceberg(unittest.TestCase):
    """built_images[spark-iceberg]: the lock, the Dockerfile and the properties file agree, and the probe is strict."""

    def test_the_lock_entry_has_the_documented_shape(self):
        entry = spark_image.resolve(LOCK, ENTRY)
        contents = entry["contents"]
        self.assertEqual((entry["component"], entry["use"], entry["digest"]), ("spark", "used", None))
        self.assertEqual((contents["spark"], contents["iceberg"], contents["kafka-connector"]),
                         ("3.5.8", "1.12.0", "3.5.8"))
        self.assertEqual(entry["base"]["ref"], "docker.io/apache/spark:3.5.8-scala2.12-java17-python3-ubuntu")
        self.assertRegex(entry["base"]["digest"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(len(FILES), 11)
        self.assertEqual({f["path"].rsplit("/", 1)[0] for f in FILES if f["path"].startswith("/opt/lab/jars")},
                         {"/opt/lab/jars"})
        self.assertTrue(entry["note"])

    def test_every_file_is_a_maven_central_artifact_by_its_coordinate(self):
        for f in FILES:
            group, artifact, version, *kind = f["coordinate"].split(":")
            ext = kind[0] if kind else "jar"
            want = (f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/"
                    f"{artifact}-{version}.{ext}")
            self.assertEqual(f["url"], want, f["id"])
            self.assertRegex(f["sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(f["path"].endswith(f"{artifact}-{version}.{ext}"), f["id"])

    def test_nothing_of_the_emr_image_is_in_the_open_image(self):
        text = (DOCKERFILE + json.dumps(ENTRY) + CONF).lower()
        self.assertNotIn("public.ecr.aws", text)
        self.assertNotIn("emr-on-eks", text)

    def test_the_dockerfile_agrees_with_the_lock_and_the_properties_file(self):
        self.assertEqual(spark_image.input_problem(REPO_ROOT, ENTRY), "")

    def test_a_drift_between_the_lock_and_the_build_input_is_named(self):
        def problem(dockerfile=None, conf=None, entry=ENTRY):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / spark_image.DIR).mkdir(parents=True)
                (root / spark_image.CONF).parent.mkdir(parents=True)
                (root / spark_image.DIR / "Dockerfile").write_text(dockerfile or DOCKERFILE)
                (root / spark_image.CONF).write_text(conf or CONF)
                return spark_image.input_problem(root, entry)
        base = ENTRY["base"]["digest"]
        self.assertIn("not the lock's base", problem(DOCKERFILE.replace(base, "sha256:" + "0" * 64)))
        self.assertIn(FILES[0]["id"], problem(DOCKERFILE.replace(FILES[0]["path"], FILES[0]["path"] + "x")))
        self.assertIn("default", problem(DOCKERFILE.replace("ARG HADOOP_AWS_SHA256\n", "ARG HADOOP_AWS_SHA256=ab\n")))
        self.assertIn("spark.jars", problem(conf=CONF.replace("iceberg-aws-bundle-1.12.0.jar",
                                                              "iceberg-aws-bundle-1.11.0.jar")))
        moved = {**ENTRY, "contents": {**ENTRY["contents"], "files": [
            {**f, "path": f["path"].replace("/opt/spark/jars/", "/opt/spark/other/")} for f in FILES]}}
        self.assertTrue(problem(entry=moved))

    def test_planted_dockerfile_lines_are_refused(self):
        def problem(dockerfile):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / spark_image.DIR).mkdir(parents=True)
                (root / spark_image.CONF).parent.mkdir(parents=True)
                (root / spark_image.DIR / "Dockerfile").write_text(dockerfile)
                (root / spark_image.CONF).write_text(CONF)
                return spark_image.input_problem(root, ENTRY)
        real = next(ln for ln in DOCKERFILE.splitlines() if ln.startswith("ARG SPARK_IMAGE="))
        # The lock's value only in a comment, then a different real default.
        planted = DOCKERFILE.replace(real, "# " + real + "\nARG SPARK_IMAGE=docker.io/evil/spark:1@sha256:" + "0" * 64)
        self.assertIn("not the lock's base", problem(planted))
        self.assertIn("not the lock's base", problem(DOCKERFILE.replace(real, real + "\n" + real)))
        self.assertIn("FROM ${SPARK_IMAGE}", problem(DOCKERFILE.replace("FROM ${SPARK_IMAGE}", "FROM scratch")))
        self.assertIn("without a checked sha256", problem(
            DOCKERFILE + "ADD https://example.com/x.jar /opt/spark/jars/x.jar\n"))
        self.assertIn("default", problem(DOCKERFILE.replace("ARG HADOOP_AWS_URL\n", "ARG hadoop_aws_url=http://x\n"
                                                            "ARG HADOOP_AWS_URL\n")))
        self.assertIn("default", problem(DOCKERFILE + "arg extra=1\n"))
        self.assertEqual(problem(DOCKERFILE), "")

    def test_the_probe_outer_timeout_exceeds_its_inner_ones(self):
        self.assertGreaterEqual(build.SPARK_PROBE_TIMEOUT_S, 420)
        self.assertGreater(build.SPARK_PROBE_TIMEOUT_S, 120 + 240)     # spark-submit --version + the class probe
        self.assertIn("120, env", spark_image.PROBE)
        self.assertIn("240, env", spark_image.PROBE)
        runner = FakeRunner(table={("docker", "run"): (0, json.dumps(good_probe()) + "\n")})
        build.probe(runner, IMAGE_ID, "arm64", "spark-iceberg", ENTRY, CONF)
        self.assertEqual(runner.calls[0][1]["timeout"], build.SPARK_PROBE_TIMEOUT_S)

    def test_build_args_come_from_the_lock_and_name_every_dockerfile_arg(self):
        args = spark_image.build_args(ENTRY["contents"])
        given = dict(a.split("=", 1) for a in args[1::2])
        self.assertEqual(args[::2], ["--build-arg"] * (2 * len(FILES)))
        declared = re.findall(r"^ARG ([A-Z0-9_]+)$", DOCKERFILE, re.M)
        self.assertEqual(sorted(given), sorted(declared))      # every ARG is fed, and nothing else is passed
        for f in FILES:
            self.assertEqual(given[spark_image.arg_name(f["id"], "SHA256")], f["sha256"])
            self.assertEqual(given[spark_image.arg_name(f["id"], "URL")], f["url"])
        argv = build.build_argv(REPO_ROOT, "spark-iceberg", TAG, "arm64", ENTRY["contents"])
        self.assertEqual(argv[-1], str(REPO_ROOT / "platform/images/spark-iceberg"))

    def test_the_dockerfile_checks_every_download_and_runs_as_uid_185(self):
        adds = [ln for ln in DOCKERFILE.splitlines() if ln.startswith("ADD ")]
        self.assertEqual(len(adds), len(FILES))
        self.assertTrue(all("--checksum=sha256:${" in ln for ln in adds))
        self.assertEqual(DOCKERFILE.rstrip().splitlines()[-1], "USER 185")
        self.assertIn("ivysettings.xml", DOCKERFILE)
        self.assertTrue((REPO_ROOT / spark_image.DIR / "README.md").is_file())
        self.assertEqual(spark_image.UID, 185)

    def test_the_tag_follows_the_versions_the_base_and_every_file(self):
        self.assertRegex(TAG, r"^lab\.local/spark-iceberg:3\.5\.8-iceberg1\.12\.0-[0-9a-f]{8}$")
        other = {**ENTRY, "base": {**ENTRY["base"], "digest": "sha256:" + "1" * 64}}
        self.assertNotEqual(spark_image.tag_for(other), TAG)
        files = [dict(f) for f in FILES]
        files[3]["sha256"] = "2" * 64
        self.assertNotEqual(spark_image.tag_for({**ENTRY, "contents": {**ENTRY["contents"], "files": files}}), TAG)

    def test_the_probe_runs_with_no_network_by_image_id_and_loads_the_six_classes(self):
        argv = build.probe_argv("sha256:abc", "arm64", "p", "spark-iceberg", ENTRY, CONF)
        self.assertEqual(argv[argv.index("--network") + 1], "none")
        self.assertIn("sha256:abc", argv)
        self.assertEqual(argv[argv.index("--entrypoint") + 1], "python3")
        self.assertIn(CONF, argv)               # the shipped properties file is what the probe runs with
        self.assertIn("SPARK_LOCAL_HOSTNAME", spark_image.PROBE)
        self.assertEqual(spark_image.CLASSES, (
            "org.apache.spark.sql.kafka010.KafkaSourceProvider", "org.apache.kafka.clients.consumer.KafkaConsumer",
            "org.apache.iceberg.spark.SparkCatalog", "software.amazon.awssdk.services.s3.S3Client",
            "org.apache.hadoop.fs.s3a.S3AFileSystem", "org.apache.commons.pool2.impl.GenericKeyedObjectPool"))
        self.assertIn(",".join(spark_image.CLASSES), argv)
        self.assertIn(json.dumps([{"id": f["id"], "path": f["path"]} for f in FILES]), argv)

    def test_a_matching_image_has_no_problems(self):
        self.assertEqual(spark_image.compare(ENTRY["contents"], good_probe()), [])
        self.assertEqual(build.compare(ENTRY["contents"], good_probe(), "spark-iceberg"), [])

    def test_each_difference_is_reported(self):
        contents = ENTRY["contents"]
        broken = good_probe()
        broken["uid"] = 0
        broken["version"] = {"rc": 0, "text": "version 3.5.1"}
        broken["files"][FILES[0]["path"]]["sha256"] = "0" * 64
        broken["files"][FILES[1]["path"]] = {"error": "FileNotFoundError: No such file or directory"}
        broken["files"][FILES[2]["path"]]["mode"] = "0o600"
        broken["classes"]["org.apache.iceberg.spark.SparkCatalog"] = False
        broken["submit"] = {"rc": 1, "tail": "Exception in thread main"}
        text = "; ".join(spark_image.compare(contents, broken))
        for want in ("uid 0, not 185", "does not say 3.5.8", f"{FILES[0]['path']} has sha256 {'0' * 64}",
                     "No such file", "mode 0o600", "SparkCatalog", "Exception in thread main"):
            self.assertIn(want, text)
        for junk in ({}, {"uid": "x", "files": [], "version": 3, "classes": None, "submit": "x"}):
            self.assertTrue(spark_image.compare(contents, junk))          # a malformed probe is a problem, not a crash

    def test_a_malformed_lock_entry_is_refused(self):
        for entry in ({**ENTRY, "contents": None}, {**ENTRY, "contents": {"spark": "3.5.8"}},
                      {**ENTRY, "contents": {**ENTRY["contents"], "files": []}},
                      {**ENTRY, "contents": {**ENTRY["contents"], "files": [{**FILES[0], "sha256": "x"}]}},
                      {**ENTRY, "contents": {**ENTRY["contents"], "files": FILES + FILES[:1]}},
                      {**ENTRY, "base": {"ref": "x"}}):
            with self.assertRaises(config.ConfigError):
                spark_image.resolve(LOCK, entry)
        self.assertEqual(build.lock_entry(LOCK, "spark-iceberg"), ENTRY)


class RunBuildSparkIceberg(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        (self.root / spark_image.DIR).mkdir(parents=True)
        (self.root / spark_image.CONF).parent.mkdir(parents=True)
        (self.root / spark_image.DIR / "Dockerfile").write_text(DOCKERFILE)
        (self.root / spark_image.CONF).write_text(CONF)
        (self.root / "versions.lock.yaml").write_text("# fixture\n")
        self.env = {"LAB_STATE_DIR": str(Path(self.tmp.name) / "state")}
        self.lab = state.lab_for(self.root, "ffl-x", self.env)

    def tearDown(self):
        self.tmp.cleanup()

    def runner(self, probe=None, clusters=(0, "ffl-x\n")):
        facts = {"Id": IMAGE_ID, "RootFS": {"Layers": LAYERS}, "Os": "linux", "Architecture": "arm64"}
        node = {"info": {"imageSpec": {"rootfs": {"diff_ids": LAYERS}}}}
        return FakeRunner(docs={"versions.lock.yaml": LOCK}, table={
            ("docker", "version"): (0, "arm64\n"), ("docker", "build"): (0, ""),
            ("docker", "image", "inspect"): (0, json.dumps(facts)),
            ("docker", "run"): (0, json.dumps(probe or good_probe()) + "\n"),
            ("docker", "rm"): (0, ""), ("docker", "inspect"): (0, "node-container-id\n"),
            ("kind", "get", "clusters"): clusters, ("kind", "load", "docker-image"): (0, ""),
            ("docker", "exec"): (0, json.dumps(node))})

    def run_build(self, runner):
        return build.run_build(self.root, "spark-iceberg", name="ffl-x", runner=runner, env=self.env)

    @staticmethod
    def argv_of(runner, *prefix):
        return [a for a, _ in runner.calls if tuple(a[:len(prefix)]) == prefix]

    def test_a_checked_image_is_loaded_and_recorded_under_its_name(self):
        down.mark_state_dir(self.lab)
        runner = self.runner()
        doc = self.run_build(runner)
        self.assertTrue(doc["ok"], doc)
        self.assertEqual([c["status"] for c in doc["checks"]], [PASS, PASS, PASS])
        self.assertEqual(doc["tag"], TAG)
        (build_argv,) = self.argv_of(runner, "docker", "build")
        self.assertIn(f"{spark_image.arg_name('hadoop-aws', 'SHA256')}={FILES[2]['sha256']}", build_argv)
        self.assertEqual(build_argv[build_argv.index("--tag") + 1], TAG)
        (probe_argv,) = self.argv_of(runner, "docker", "run")
        self.assertIn(IMAGE_ID, probe_argv)
        self.assertEqual(probe_argv[probe_argv.index("--network") + 1], "none")
        record = state.read_json(self.lab.images_record)["spark-iceberg"]
        self.assertEqual((record["tag"], record["layers"], record["node_id"]), (TAG, LAYERS, "node-container-id"))
        self.assertEqual(record["versions"], {"spark": "3.5.8", "iceberg": "1.12.0", "kafka-connector": "3.5.8"})
        self.assertIn("6 classes", doc["checks"][1]["message"])
        self.assertFalse(self.argv_of(runner, "docker", "push"))

    def test_without_a_cluster_the_image_is_built_and_probed_but_not_loaded(self):
        runner = self.runner(clusters=(0, ""))
        doc = self.run_build(runner)
        self.assertTrue(doc["ok"], doc)
        self.assertEqual([c["status"] for c in doc["checks"]], [PASS, PASS, WARN])
        self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_a_probe_that_finds_a_wrong_file_or_a_missing_class_loads_nothing(self):
        for mutate in (lambda p: p["files"][FILES[3]["path"]].update(sha256="f" * 64),
                       lambda p: p["classes"].update({"org.apache.kafka.clients.consumer.KafkaConsumer": False}),
                       lambda p: p.update(uid=0)):
            probe = good_probe()
            mutate(probe)
            runner = self.runner(probe)
            doc = self.run_build(runner)
            self.assertFalse(doc["ok"])
            self.assertEqual(doc["checks"][-1]["id"], "build:probe")
            self.assertEqual(doc["checks"][-1]["status"], FAIL)
            self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_a_dockerfile_that_drifted_from_the_lock_is_refused_before_docker_build(self):
        (self.root / spark_image.DIR / "Dockerfile").write_text(DOCKERFILE.replace(
            ENTRY["base"]["digest"], "sha256:" + "0" * 64))
        runner = self.runner()
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "build:config")
        self.assertFalse(self.argv_of(runner, "docker", "build"))

    def test_the_cli_offers_it(self):
        from labctl import cli
        self.assertIn("spark-iceberg", cli.BUILDABLE)
        self.assertEqual(cli.BUILDABLE, tuple(build.IMAGES))
        args = cli._parser().parse_args(["build", "spark-iceberg", "--name", "ffl-x"])
        self.assertEqual((args.image, args.name), ("spark-iceberg", "ffl-x"))


if __name__ == "__main__":
    unittest.main()
