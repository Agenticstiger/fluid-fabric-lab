# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""CLI dispatch, exit codes, subprocess wrapper and tier resolution."""

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401  (puts the repo root on sys.path)
from support import REPO_ROOT, TIERS_DOC, FakeRunner

from labctl import EXIT_USAGE, __version__, cli, config, proc


def run_cli(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            rc = cli.main(list(argv))
        except SystemExit as exc:  # argparse
            rc = exc.code
    return rc, out.getvalue(), err.getvalue()


class UsageErrors(unittest.TestCase):
    def assert_usage_errors(self, cases: dict):
        """Each argv exits 2 with the given text on stderr and no traceback."""
        for argv, said in cases.items():
            with self.subTest(argv=argv):
                rc, out, err = run_cli(*argv)
                self.assertEqual(rc, EXIT_USAGE)
                self.assertIn(said, err)
                self.assertNotIn("Traceback", err + out)

    def test_a_command_the_lab_does_not_have_is_a_usage_error(self):
        rc, _, err = run_cli("report")
        self.assertEqual(rc, EXIT_USAGE)
        self.assertNotIn("Traceback", err)

    def test_version_prints_the_version_only(self):
        with self.assertRaises(SystemExit) as raised, contextlib.redirect_stdout(io.StringIO()) as out:
            cli.main(["--version"])
        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(out.getvalue().strip(), f"lab {__version__}")

    def test_a_bad_lab_name_is_a_usage_error_not_a_traceback(self):
        self.assert_usage_errors({argv: "argument --name" for argv in (
            ("up", "--name", "Bad_Name"), ("down", "--name", "a--b"), ("down", "--name", "kind"))})

    def test_mirror_up_and_down_are_wired_with_their_flags(self):
        for argv in (("mirror", "--help"), ("up", "--help"), ("down", "--help"), ("check", "--help")):
            with self.subTest(argv=argv):
                rc, out, _ = run_cli(*argv)
                self.assertEqual(rc, 0)
                self.assertNotIn("not implemented", out)
        _, out, _ = run_cli("down", "--help")
        self.assertIn("irreversible", out)
        _, out, _ = run_cli("up", "--help")
        self.assertIn("--name", out)

    def test_an_existing_cluster_is_refused_by_doctor_and_up(self):
        self.assert_usage_errors({
            ("doctor", "--cluster", "existing"): "is not supported",
            ("up", "--tier", "core", "--cluster", "existing"): "invalid choice: 'existing'",
        })

    def test_no_command_is_a_usage_error(self):
        rc, _, _ = run_cli()
        self.assertEqual(rc, EXIT_USAGE)

    def test_entry_script_runs_under_python3(self):
        res = subprocess.run([sys.executable, str(REPO_ROOT / "lab"), "accept", "--runner", "dag"],
                             capture_output=True, text=True, check=False)
        self.assertEqual(res.returncode, 2)
        self.assertIn("the dag runner is not available yet", res.stderr)

    def test_entry_script_is_executable(self):
        self.assertTrue(os.access(REPO_ROOT / "lab", os.X_OK))


class L0Command(unittest.TestCase):
    def test_missing_tools_l0_fails_not_skips(self):
        with tempfile.TemporaryDirectory() as tmp:
            rc, out, _ = run_cli("--root", tmp, "l0")
        self.assertEqual(rc, 1)
        self.assertIn("not present", out)


class Proc(unittest.TestCase):
    def test_string_argv_is_refused(self):
        with self.assertRaises(TypeError):
            proc.run("echo hi")

    def test_missing_executable_is_a_result(self):
        res = proc.run(["definitely-not-a-real-binary-lab-test"])
        self.assertTrue(res.missing)
        self.assertFalse(res.ok)

    def test_timeout_is_a_result(self):
        res = proc.run([sys.executable, "-c", "import time; time.sleep(5)"], timeout=0.5)
        self.assertTrue(res.timed_out)
        self.assertFalse(res.ok)

    def test_offline_env_poisons_every_proxy(self):
        env = proc.offline_env({"PATH": "/bin", "NO_PROXY": "*"})
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
            self.assertEqual(env[var], proc.OFFLINE_PROXY)
        self.assertEqual(env["NO_PROXY"], "")
        self.assertEqual(env["PATH"], "/bin")


class TierResolution(unittest.TestCase):
    def test_requires_are_folded_in_without_duplicates(self):
        t = config.resolve_tier(TIERS_DOC, "streaming")
        self.assertEqual(t.releases, ["cert-manager", "trino", "strimzi-kafka-operator"])
        self.assertEqual(t.memory_target_gib, 13.0)
        full = config.resolve_tier(TIERS_DOC, "full")
        self.assertEqual(full.releases, ["cert-manager", "trino", "strimzi-kafka-operator"])

    def test_unknown_tier(self):
        with self.assertRaisesRegex(config.ConfigError, "unknown tier"):
            config.resolve_tier(TIERS_DOC, "huge")

    def test_cycle(self):
        doc = {"tiers": {"a": {"memory_target_gib": 1, "requires": ["b"], "releases": ["x"]},
                         "b": {"memory_target_gib": 1, "requires": ["a"], "releases": ["y"]}}}
        with self.assertRaisesRegex(config.ConfigError, "cycle"):
            config.resolve_tier(doc, "a")

    def test_bad_memory_target(self):
        for bad in (None, 0, -1, "8", True):
            doc = {"tiers": {"a": {"memory_target_gib": bad, "releases": ["x"]}}}
            with self.subTest(bad=bad), self.assertRaises(config.ConfigError):
                config.resolve_tier(doc, "a")

    def test_tier_without_releases(self):
        with self.assertRaisesRegex(config.ConfigError, "no releases"):
            config.resolve_tier({"tiers": {"a": {"memory_target_gib": 1}}}, "a")


class YamlLoading(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "tiers.yaml"
        self.path.write_text("x: 1\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_yq_is_a_config_error(self):
        with self.assertRaisesRegex(config.ConfigError, "yq is required"):
            config.load_yaml(self.path, FakeRunner(table={"yq": (None, "")}))

    def test_non_mapping(self):
        with self.assertRaisesRegex(config.ConfigError, "mapping"):
            config.load_yaml(self.path, FakeRunner(table={"yq": (0, "[1, 2]")}))

    def test_missing_file(self):
        with self.assertRaisesRegex(config.ConfigError, "not found"):
            config.load_yaml(Path(self.tmp.name) / "nope.yaml", FakeRunner())

    def test_success_uses_list_argv(self):
        runner = FakeRunner(docs={"tiers.yaml": {"a": 1}})
        self.assertEqual(config.load_yaml(self.path, runner), {"a": 1})
        self.assertEqual(runner.calls[0][0][:3], ["yq", "-o=json", "."])


if __name__ == "__main__":
    unittest.main()
