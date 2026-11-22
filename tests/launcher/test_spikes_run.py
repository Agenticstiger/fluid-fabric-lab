# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""spikes/run.py `runner_tag`: the runner image comes from the lab's own record, and a missing or stale one is refused."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import support  # noqa: F401
from support import REPO_ROOT

from labctl import build, state

spec = importlib.util.spec_from_file_location("spikes_run", REPO_ROOT / "spikes" / "run.py")
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)

TAG = "lab.local/fluid-runner:0.37.0-c826f3ae"
LAYERS = ["sha256:" + "1" * 64, "sha256:" + "2" * 64]


class RunnerTag(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name) / "repo"
        root.mkdir()
        self.lab = state.lab_for(root, "ffl-x", {"LAB_STATE_DIR": str(Path(self.tmp.name) / "state")})
        self.lab.state.mkdir(parents=True)

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, data):
        self.lab.images_record.write_text(json.dumps(data))

    def test_a_recorded_image_the_node_holds_is_used(self):
        self.record({"fluid-runner": {"tag": TAG, "layers": LAYERS}})
        with mock.patch.object(build, "node_layers", return_value=LAYERS):
            self.assertEqual(run.runner_tag(self.lab), TAG)

    def test_a_missing_or_unusable_record_is_refused_with_the_build_command(self):
        for data in (None, {}, {"fluid-runner": {}}, {"fluid-runner": {"layers": LAYERS}}, ["x"]):
            if data is not None:
                self.record(data)
            with mock.patch.object(build, "node_layers", return_value=LAYERS):
                with self.assertRaises(SystemExit) as ctx:
                    run.runner_tag(self.lab)
            self.assertIn("./lab build fluid-runner --name ffl-x", str(ctx.exception))

    def test_layers_that_differ_from_the_node_are_refused(self):
        self.record({"fluid-runner": {"tag": TAG, "layers": LAYERS}})
        for held in (None, LAYERS[:1]):
            with mock.patch.object(build, "node_layers", return_value=held):
                with self.assertRaises(SystemExit) as ctx:
                    run.runner_tag(self.lab)
            self.assertIn("does not hold", str(ctx.exception))
        self.record({"fluid-runner": {"tag": TAG, "layers": []}})
        with self.assertRaises(SystemExit):
            run.runner_tag(self.lab)


if __name__ == "__main__":
    unittest.main()
