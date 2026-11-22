# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab render`: chart-cache verification, hermetic argv/env, per-release check."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT, TIERS_DOC, FakeRunner, make_root

from labctl import config, proc, render
from labctl.output import FAIL, PASS


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_cache(root: Path, files: dict, sums: dict = None) -> Path:
    charts = root / config.CHARTS_DIR
    charts.mkdir(parents=True)
    for rel, data in files.items():
        (charts / rel).parent.mkdir(parents=True, exist_ok=True)
        (charts / rel).write_bytes(data)
    sums = sums if sums is not None else {rel: sha(data) for rel, data in files.items()}
    (charts / "SHA256SUMS").write_text("".join(f"{d}  {r}\n" for r, d in sums.items()))
    (charts / "README.md").write_text("docs\n")
    return charts


class SumsFile(unittest.TestCase):
    def test_unsafe_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "SHA256SUMS"
            for bad in ("../evil.tgz", "/etc/passwd"):
                p.write_text(f"{'a' * 64}  {bad}\n")
                with self.subTest(bad=bad), self.assertRaisesRegex(render.CacheError, "unsafe"):
                    render.read_sums(p)

    def test_malformed_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "SHA256SUMS"
            p.write_text("nothex  x.tgz\n")
            with self.assertRaisesRegex(render.CacheError, "not a sha256sum line"):
                render.read_sums(p)


class ChartCache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_good_cache(self):
        build_cache(self.root, {"a-1.0.0.tgz": b"A", "dirchart/Chart.yaml": b"name: d\n"})
        count, problems = render.verify_chart_cache(self.root)
        self.assertEqual((count, problems), (2, []))

    def test_tampered_missing_and_unlisted_files(self):
        charts = build_cache(self.root, {"a.tgz": b"A", "b.tgz": b"B"})
        (charts / "a.tgz").write_bytes(b"tampered")
        (charts / "b.tgz").unlink()
        (charts / "sneaky.tgz").write_bytes(b"S")
        _, problems = render.verify_chart_cache(self.root)
        text = "\n".join(problems)
        self.assertIn("a.tgz: sha256 does not match", text)
        self.assertIn("b.tgz: listed in SHA256SUMS but missing", text)
        self.assertIn("sneaky.tgz: present but not listed", text)

    def test_lock_disagreeing_with_sums(self):
        build_cache(self.root, {"a.tgz": b"A"})
        lock = {"charts": [{"name": "a", "file": "platform/charts/a.tgz", "sha256": "0" * 64},
                           {"name": "d", "file": "platform/charts/dir", "sha256": "dir"}]}
        _, problems = render.verify_chart_cache(self.root, lock)
        self.assertEqual(len(problems), 1)
        self.assertIn("disagrees", problems[0])

    def test_missing_sums_file(self):
        (self.root / config.CHARTS_DIR).mkdir(parents=True)
        _, problems = render.verify_chart_cache(self.root)
        self.assertIn("not found", problems[0])

    def test_committed_cache_verifies(self):
        count, problems = render.verify_chart_cache(REPO_ROOT)
        self.assertEqual(problems, [])
        self.assertGreater(count, 0)


class Hermetic(unittest.TestCase):
    def test_argv_is_a_list_that_passes_the_tier_as_the_environment(self):
        tier = config.resolve_tier(TIERS_DOC, "streaming")
        argv = render.helmfile_argv(Path("/r/platform/helmfile.yaml.gotmpl"), tier, Path("/o"))
        self.assertIsInstance(argv, list)
        self.assertEqual(argv[0], "helmfile")
        self.assertNotIn("--selector", argv)  # glue releases must render too
        for flag in ("template", "--skip-deps", "--include-crds", "--output-dir"):
            self.assertIn(flag, argv)
        # The helmfile's environments ARE the tiers: `lab render --tier T` must pass -e T.
        self.assertEqual(argv[argv.index("--environment") + 1], "streaming")
        self.assertNotIn("--state-values-set", argv)

    def test_environment_override(self):
        tier = config.resolve_tier(TIERS_DOC, "core")
        argv = render.helmfile_argv(Path("/r/h.yaml"), tier, Path("/o"), "other")
        self.assertEqual(argv[argv.index("--environment") + 1], "other")

    def test_env_has_no_user_helm_state_and_no_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = render.hermetic_env(Path(tmp))
            self.assertEqual(env["HTTPS_PROXY"], proc.OFFLINE_PROXY)
            self.assertTrue(env["HELM_CACHE_HOME"].startswith(tmp))
            self.assertTrue(env["HOME"].startswith(tmp))
            self.assertEqual(Path(env["KUBECONFIG"]).read_text(), "")
            self.assertNotIn("AWS_ACCESS_KEY_ID", env)


class RunRender(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root, self.docs = make_root(Path(self.tmp.name) / "repo")
        build_cache(self.root, {"x.tgz": b"X"})
        self.out = Path(self.tmp.name) / "out"

    def tearDown(self):
        self.tmp.cleanup()

    def fake_helmfile(self, write_for, charts=None):
        def helmfile(argv, **kwargs):
            if "list" in argv:
                rels = [{"name": n, "installed": True, "chart": c}
                        for n, c in (charts or {"glue": "./components/lab-objects"}).items()]
                return proc.Result(argv, 0, json.dumps(rels), "")
            out = Path(argv[argv.index("--output-dir") + 1])
            self.seen_env = kwargs.get("env")
            for rel in write_for:
                d = out / rel / "chart" / "templates"
                d.mkdir(parents=True, exist_ok=True)
                (d / "x.yaml").write_text("apiVersion: v1\nkind: ConfigMap\n---\nkind: Service\n")
            return proc.Result(argv, 0, "", "")
        return FakeRunner(docs=self.docs, table={"helmfile": helmfile})

    def write_helmfile(self):
        hf = self.root / config.HELMFILE
        hf.parent.mkdir(parents=True, exist_ok=True)
        hf.write_text("releases: []\n")

    def test_missing_helmfile_fails(self):
        doc = render.run_render(self.root, "core", runner=FakeRunner(docs=self.docs), which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertIn("not present", doc["checks"][-1]["message"])

    def test_every_release_rendered_passes(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertTrue(doc["ok"], doc)
        self.assertEqual(doc["releases"]["trino"]["kinds"], {"ConfigMap": 1, "Service": 1})
        self.assertEqual(self.seen_env["HTTP_PROXY"], proc.OFFLINE_PROXY)

    def test_a_release_that_rendered_nothing_fails(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        status = {c["id"]: c["status"] for c in doc["checks"]}
        self.assertEqual(status["release:trino"], FAIL)
        self.assertEqual(status["release:cert-manager"], PASS)

    def test_release_of_another_tier_must_not_render(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino", "strimzi-kafka-operator"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        bad = next(c for c in doc["checks"] if c["id"] == "release:strimzi-kafka-operator")
        self.assertEqual(bad["status"], FAIL)
        self.assertIn("does not enable it", bad["message"])

    def test_off_by_default_addon_must_not_render(self):
        tiers = dict(TIERS_DOC, optional_addons={"kafbat-ui": {"release": "kafka-ui", "default": "off"}})
        self.root, self.docs = make_root(self.root, tiers=tiers)
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino", "kafka-ui"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertIn("off by default", next(c for c in doc["checks"] if c["id"] == "release:kafka-ui")["message"])

    def test_lab_glue_release_is_allowed(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino", "lab-postgres"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertTrue(doc["ok"], doc["checks"])
        self.assertIn("lab glue", next(c for c in doc["checks"] if c["id"] == "release:lab-postgres")["message"])

    def test_helmfile_failure_fails(self):
        self.write_helmfile()
        runner = FakeRunner(docs=self.docs, table={"helmfile": (1, "proxyconnect tcp: connection refused")})
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "helmfile-template")

    def test_non_empty_output_dir_is_refused(self):
        self.write_helmfile()
        self.out.mkdir()
        (self.out / "stale.yaml").write_text("kind: Old\n")
        doc = render.run_render(self.root, "core", out=str(self.out), runner=self.fake_helmfile([]),
                                which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "output")

    def test_tampered_cache_stops_before_helmfile(self):
        self.write_helmfile()
        (self.root / config.CHARTS_DIR / "x.tgz").write_bytes(b"evil")
        runner = self.fake_helmfile(["cert-manager", "trino"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertFalse(any(call[0][0] == "helmfile" for call in runner.calls))

    def test_chart_outside_the_cache_fails(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino"], charts={"trino": "../products/lk/lakekeeper"})
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        bad = next(c for c in doc["checks"] if c["id"] == "chart-source")
        self.assertEqual(bad["status"], FAIL)
        self.assertIn("products/lk/lakekeeper", bad["detail"]["problems"][0])

    def test_remote_chart_name_fails_the_source_check(self):
        self.write_helmfile()
        runner = self.fake_helmfile(["cert-manager", "trino"], charts={"trino": "oci://r.example.test/trino"})
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertEqual(next(c for c in doc["checks"] if c["id"] == "chart-source")["status"], FAIL)

    def test_lock_tier_disagreeing_with_tiers_yaml_fails(self):
        lock = {"charts": [{"name": "airflow", "use": "used", "tier": "core", "file": "platform/charts/x.tgz"}]}
        self.root, self.docs = make_root(self.root, lock=lock)
        self.write_helmfile()
        doc = render.run_render(self.root, "core", out=str(self.out), runner=self.fake_helmfile([]),
                                which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "tier-membership")

    def test_exec_in_the_helmfile_is_refused_before_anything_runs(self):
        hf = self.root / config.HELMFILE
        hf.parent.mkdir(parents=True, exist_ok=True)
        hf.write_text('{{ exec "curl" (list "http://127.0.0.1:8000") }}\nreleases: []\n')
        runner = self.fake_helmfile(["cert-manager", "trino"])
        doc = render.run_render(self.root, "core", out=str(self.out), runner=runner, which=lambda n: "/bin/" + n)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "helmfile-static")
        self.assertFalse(any(call[0][0] == "helmfile" for call in runner.calls))

    def test_dotfile_in_a_cached_chart_must_be_listed(self):
        (self.root / config.CHARTS_DIR / "dirchart").mkdir()
        (self.root / config.CHARTS_DIR / "dirchart" / ".helmignore").write_text("crds/\n")
        _, problems = render.verify_chart_cache(self.root)
        self.assertTrue(any(".helmignore" in p for p in problems), problems)

    def test_missing_helmfile_binary_fails(self):
        self.write_helmfile()
        doc = render.run_render(self.root, "core", runner=FakeRunner(docs=self.docs), which=lambda n: None)
        self.assertFalse(doc["ok"])
        self.assertIn("tool:helmfile", [c["id"] for c in doc["checks"]])


if __name__ == "__main__":
    unittest.main()
