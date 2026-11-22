# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab l0`: gate discovery and execution (real subprocesses, tiny gates)."""

import os
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401

from labctl import config, l0, proc
from labctl.output import FAIL, PASS

GUARD = 'if __name__ == "__main__":\n'


def summary(name):
    """What a real gate prints last (tools/l0/_common.Report.finish)."""
    return f"    print('gate {name}: PASS (1 check(s))')\n"


class L0(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dir = self.root / config.L0_DIR

    def tearDown(self):
        self.tmp.cleanup()

    def gate(self, name, body, register=True):
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / name).write_text(body)
        stem = Path(name).stem
        looks_like_gate = name.endswith(".sh") or (name.endswith(".py") and GUARD in body)
        if register and looks_like_gate and not name.startswith(("_", "test_")) and name != "run_all.py":
            reg = self.dir / l0.REGISTRY
            names = reg.read_text().split() if reg.is_file() else []
            if stem not in names:
                reg.write_text("\n".join(names + [stem]) + "\n")

    def test_missing_dir_fails(self):
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        self.assertIn("not present", doc["checks"][0]["message"])

    def test_empty_dir_fails(self):
        self.dir.mkdir(parents=True)
        self.gate("helpers.py", "def x():\n    return 1\n")  # no __main__ block: not a gate
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        self.assertIn("no gates", doc["checks"][0]["message"])

    def test_discovery_rules(self):
        self.gate("licence_gate.py", "import sys\n" + GUARD + "    sys.exit(0)\n")
        self.gate("_common.py", GUARD + "    pass\n")
        self.gate("test_licence_gate.py", GUARD + "    pass\n")
        self.gate("run_all.py", GUARD + "    pass\n")
        self.gate("README.md", "docs")
        self.gate("brand.sh", "#!/bin/sh\nexit 0\n")
        os.chmod(self.dir / "brand.sh", 0o755)
        self.gate("notexec.sh", "#!/bin/sh\nexit 0\n")
        names = [g.name for g in l0.discover(self.dir)]
        self.assertEqual(names, ["brand", "licence_gate"])

    def test_pass_fail_and_offline_env(self):
        self.gate("ok_gate.py", "import os, sys\n" + GUARD + summary("ok_gate") +
                  "    sys.exit(0 if os.environ.get('HTTPS_PROXY') == %r and os.environ.get('LAB_OFFLINE') == '1' else 3)\n"
                  % proc.OFFLINE_PROXY)
        self.gate("bad_gate.py", "import sys\n" + GUARD + "    print('planted bitnamilegacy image found')\n    sys.exit(1)\n")
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        status = {c["id"]: c for c in doc["checks"]}
        self.assertEqual(status["gate:ok_gate"]["status"], PASS)
        self.assertEqual(status["gate:bad_gate"]["status"], FAIL)
        self.assertIn("bitnamilegacy", status["gate:bad_gate"]["message"])

    def test_gates_run_from_repo_root(self):
        self.gate("cwd_gate.py", "import os, sys\n" + GUARD + summary("cwd_gate") +
                  "    sys.exit(0 if os.path.isdir('tools/l0') else 4)\n")
        self.assertTrue(l0.run_l0(self.root)["ok"])

    def test_unknown_gate_name_fails(self):
        self.gate("a.py", GUARD + "    pass\n")
        doc = l0.run_l0(self.root, ["nope"])
        self.assertFalse(doc["ok"])
        self.assertIn("unknown gate", doc["checks"][0]["message"])

    def test_select_and_list(self):
        self.gate("a.py", GUARD + summary("a") + "    raise SystemExit(0)\n")
        self.gate("b.py", GUARD + "    raise SystemExit(1)\n")
        self.assertTrue(l0.run_l0(self.root, ["a"])["ok"])
        listed = l0.run_l0(self.root, list_only=True)
        self.assertTrue(listed["ok"])
        self.assertEqual(listed["gates"], ["a", "b"])

    def test_stub_gate_without_a_summary_fails(self):
        self.gate("kube_lint.py", "import sys\n" + GUARD + "    sys.exit(0)\n")
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        self.assertIn("checked nothing", next(c for c in doc["checks"] if c["id"] == "gate:kube_lint")["message"])

    def test_zero_check_summary_fails(self):
        self.gate("z.py", GUARD + "    print('gate z: PASS (0 check(s))')\n")
        self.assertFalse(l0.run_l0(self.root)["ok"])

    def test_unregistered_gate_fails(self):
        self.gate("a.py", GUARD + summary("a"))
        self.gate("extra.py", GUARD + summary("extra"), register=False)
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        inv = next(c for c in doc["checks"] if c["id"] == "l0-inventory")
        self.assertIn("gate extra is not registered", inv["detail"]["problems"][0])

    def test_registered_but_missing_gate_fails(self):
        self.gate("a.py", GUARD + summary("a"))
        (self.dir / l0.REGISTRY).write_text("a\ngone\n")
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        self.assertTrue(any("lists gone" in p for p in doc["checks"][0]["detail"]["problems"]))

    def test_gates_that_would_silently_never_run_fail(self):
        self.gate("a.py", GUARD + summary("a"))
        self.gate("odd.py", 'if "__main__" == __name__:\n    pass\n', register=False)
        self.gate("sh_gate.sh", "#!/bin/sh\nexit 0\n", register=False)
        self.gate("helpers.py", "def x():\n    return 1\n")
        _, problems = l0.inventory(self.dir)
        text = " | ".join(problems)
        self.assertIn("odd.py: has a non-standard __main__ guard", text)
        self.assertIn("sh_gate.sh: not executable", text)
        self.assertIn("helpers.py: neither a gate", text)
        self.assertFalse(l0.run_l0(self.root)["ok"])

    def test_missing_registry_fails(self):
        self.gate("a.py", GUARD + summary("a"), register=False)
        doc = l0.run_l0(self.root)
        self.assertFalse(doc["ok"])
        self.assertIn("gates.conf is missing", doc["checks"][0]["detail"]["problems"][-1])

    def test_real_registry_matches_the_gates(self):
        from support import REPO_ROOT
        gates, problems = l0.inventory(REPO_ROOT / config.L0_DIR)
        self.assertEqual(problems, [])
        self.assertGreaterEqual(len(gates), 10)

    def test_timeout_fails(self):
        self.gate("slow.py", "import time\n" + GUARD + "    time.sleep(5)\n")
        doc = l0.run_l0(self.root, timeout=0.5)
        self.assertFalse(doc["ok"])
        self.assertIn("timed out", doc["checks"][0]["message"])


if __name__ == "__main__":
    unittest.main()
