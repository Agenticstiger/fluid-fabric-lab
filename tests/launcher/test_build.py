# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab build`: the image's inputs agree with versions.lock.yaml, the probe is
judged strictly, and only a checked image is loaded and recorded."""

import json
import re
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT, FakeRunner

from labctl import build, cli, config, down, proc, state
from labctl.output import FAIL, PASS, WARN

IMAGE_DIR = REPO_ROOT / build.IMAGES["fluid-runner"]
FORGE = {"version": "0.32.1.dev0", "commit": "e37cdda5513962cb5932370737d95527c33afd38", "sha256": "c" * 64}
LOCAL = "0.32.1.dev0+ge37cdda5"
CONTENTS = {"forge": FORGE, "dbt-core": "1.10.23", "dbt-trino": "1.10.6", "duckdb": "1.5.6",
            "opentofu": "1.13.1", "providers": {"apache/iceberg": "v0.1.0", "keycloak/keycloak": "v5.10.0"}}
LOCK = {"built_images": [{"id": "fluid-runner", "component": "runner", "use": "used", "digest": None,
                          "contents": CONTENTS}]}
LAYERS = ["sha256:" + "1" * 64, "sha256:" + "2" * 64]
IMAGE_ID = "sha256:" + "9" * 64


def good_probe() -> dict:
    return {
        "uid": 10001,
        "versions": {"data-product-forge": LOCAL, "dbt-core": "1.10.23", "dbt-trino": "1.10.6",
                     "duckdb": "1.5.6"},
        "env": dict(build.ENV),
        "tofu_init": {"rc": 0, "tail": "OpenTofu has been successfully initialized!"},
        "tofu": {"terraform_version": "1.13.1",
                 "provider_selections": {"registry.opentofu.org/apache/iceberg": "0.1.0",
                                         "registry.opentofu.org/keycloak/keycloak": "5.10.0"}},
        "forge_source": {"record": {"commit": FORGE["commit"], "archive_sha256": FORGE["sha256"],
                                    "wheel": "x.whl", "wheel_sha256": "w" * 64},
                         "wheel_sha256": "w" * 64, "checked": 3000, "mismatched": []},
        "spark_runner": True,
        "fluid_cli": {"rc": 0, "out": "FLUID Forge CLI v0.32.1.dev0"},
    }


def requirement_entries(text: str) -> list:
    """requirements.txt entries with their backslash continuations joined."""
    entries, current = [], ""
    for line in text.splitlines():
        if not current and (not line.strip() or line.lstrip().startswith("#")):
            continue
        current += line.rstrip("\\").strip() + " "
        if not line.rstrip().endswith("\\"):
            entries.append(current.strip())
            current = ""
    return entries


class LockAgreesWithBuildInputs(unittest.TestCase):
    """The lock's `contents` is what `lab build` holds the image to, so the
    files the image is built from must say the same."""

    @classmethod
    def setUpClass(cls):
        lock = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
        cls.entry = build.lock_entry(lock, "fluid-runner")
        cls.contents = cls.entry["contents"]
        cls.pin = build.forge_pin(cls.contents)
        cls.pin_in_lock = cls.contents["forge"]
        # forge is built from source and is not in requirements.txt; the rest is.
        cls.want = {dist: str(cls.contents[key]) for key, dist in build.DISTRIBUTIONS.items() if key != "forge"}

    def test_requirements_in_pins_the_locked_versions_and_the_forge_commit(self):
        pins, forge = {}, []
        for line in (IMAGE_DIR / "requirements.in").read_text().splitlines():
            m = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?==(\S+)", line)
            if m:
                pins[m.group(1)] = m.group(3)
            elif line.startswith("data-product-forge"):
                forge.append(line)
        self.assertEqual(pins, self.want)
        self.assertEqual(forge, [f"data-product-forge[local,aws] @ {self.pin['url']}"])

    def test_forge_is_not_installed_from_an_index_or_requirements_txt(self):
        entries = requirement_entries((IMAGE_DIR / "requirements.txt").read_text())
        self.assertFalse([e for e in entries if e.lower().startswith("data-product-forge")])
        self.assertRegex((IMAGE_DIR / "requirements.txt").read_text(),
                         r"(?m)^# The following packages were excluded from the output:\n# data-product-forge$")

    def test_requirements_txt_was_compiled_for_this_pin(self):
        # compile.sh writes the pin as the first line; a lock bump without compile.sh leaves the old one.
        first = (IMAGE_DIR / "requirements.txt").read_text().splitlines()[0]
        self.assertEqual(first, build.pin_header(self.contents))
        self.assertEqual(first, f"# forge commit {self.pin['commit']} archive-sha256 {self.pin['sha256']} "
                                f"version {self.pin['local_version']}")

    def test_a_lock_edit_without_compile_sh_changes_the_expected_header(self):
        header = build.pin_header(self.contents)
        for change in ({"sha256": "0" * 64}, {"version": "0.32.2.dev0", "release": False}, {"commit": "0" * 40},
                       {"release": True, "version": "0.99.0"}):
            edited = {**self.contents, "forge": {**self.pin_in_lock, **change}}
            self.assertNotEqual(build.pin_header(edited), header, change)

    def test_build_tools_are_pinned_by_hash_and_forge_builds_without_isolation(self):
        entries = requirement_entries((IMAGE_DIR / "requirements-build.txt").read_text())
        compiled = {e.split("==")[0] for e in entries}
        self.assertLessEqual({"setuptools", "setuptools-scm", "wheel"}, compiled)
        for entry in entries:
            self.assertRegex(entry, r"^[A-Za-z0-9][A-Za-z0-9._-]*==\S+ ")
            self.assertIn("--hash=sha256:", entry)
        text = (IMAGE_DIR / "Dockerfile").read_text()
        self.assertIn("--no-deps --no-build-isolation", text)
        self.assertIn("RUN --network=none", text)
        self.assertIn("--no-build-isolation --no-index", text)
        self.assertIn('test -n "$FORGE_VERSION" -a -n "$FORGE_COMMIT" -a -n "$FORGE_SHA256"', text)
        self.assertIn("record_forge.py", text)
        # forge.json comes from the bytes, not from the build args.
        # The RUN that writes /out/forge.json runs record_forge.py on the archive and nothing else writes it.
        (writer,) = [blk for blk in re.split(r"\n(?=[A-Z]+ )", text) if "/out/forge.json" in blk]
        self.assertIn("record_forge.py /src/forge.tar.gz", writer)
        self.assertNotRegex(writer, r"\b(printf|echo)\b")
        self.assertIn("--require-hashes --no-deps", text)

    def test_every_compiled_requirement_is_pinned_by_version_and_hash(self):
        entries = requirement_entries((IMAGE_DIR / "requirements.txt").read_text())
        self.assertGreater(len(entries), 50)
        compiled = {}
        for entry in entries:
            self.assertRegex(entry, r"^[A-Za-z0-9][A-Za-z0-9._-]*==\S+ ", entry[:80])
            self.assertIn("--hash=sha256:", entry, entry[:80])
            name, version = entry.split(" ")[0].split("==")
            compiled[name.lower()] = version
        # A changed requirements.in that was never recompiled fails here.
        self.assertEqual({d: compiled.get(d) for d in self.want}, self.want)

    def test_dockerfile_takes_forge_only_from_build_args_and_checks_the_archive(self):
        text = (IMAGE_DIR / "Dockerfile").read_text()
        for name in ("FORGE_VERSION", "FORGE_COMMIT", "FORGE_SHA256"):
            # An ARG with a value would be a second place to type the pin.
            self.assertRegex(text, rf"(?m)^ARG {name}$")
            self.assertNotRegex(text, rf"ARG {name}=")
        self.assertIn("ADD --checksum=sha256:${FORGE_SHA256} "
                      "https://github.com/Agenticstiger/forge-cli/archive/${FORGE_COMMIT}.tar.gz", text)
        self.assertNotIn(self.pin["commit"], text)
        self.assertNotIn(self.pin["sha256"], text)
        # The Dockerfile's URL template and requirements.in's URL are the same archive.
        self.assertEqual(self.pin["url"],
                         f"https://github.com/Agenticstiger/forge-cli/archive/{self.pin['commit']}.tar.gz")

    def test_the_build_passes_the_lock_pin_as_build_args(self):
        argv = build.build_argv(Path("/r"), "fluid-runner", "t", "arm64", self.contents)
        args = [argv[i + 1] for i, a in enumerate(argv) if a == "--build-arg"]
        self.assertEqual(args, [f"FORGE_VERSION={self.pin['local_version']}", f"FORGE_COMMIT={self.pin['commit']}",
                                f"FORGE_SHA256={self.pin['sha256']}"])
        if self.pin["release"]:   # a release pin is the release: no dev part, no local part
            self.assertRegex(self.pin["local_version"], r"^\d+\.\d+\.\d+$")
            self.assertEqual(self.pin["local_version"], self.pin["version"])
        else:
            self.assertRegex(self.pin["local_version"], r"^\d+\.\d+\.\d+(\.dev\d+)?\+g[0-9a-f]{8}$")
            self.assertTrue(self.pin["local_version"].endswith("+g" + self.pin["commit"][:8]))

    def test_dockerfile_base_and_opentofu_match_the_lock(self):
        text = (IMAGE_DIR / "Dockerfile").read_text()
        base = self.entry["base"]
        self.assertIn(f"ARG PYTHON_IMAGE={base['ref']}@{base['digest']}\n", text)
        self.assertIn(f"ARG TOFU_VERSION={self.contents['opentofu']}\n", text)
        self.assertIn("--require-hashes --no-deps", text)
        self.assertIn("--only-binary=:all:", text)

    def test_providers_and_their_lock_match_the_lock(self):
        want = {src: build._norm(v) for src, v in self.contents["providers"].items()}
        tf = (IMAGE_DIR / "tofu" / "providers.tf").read_text()
        declared = dict(re.findall(r'source\s*=\s*"([^"]+)",\s*version\s*=\s*"([^"]+)"', tf))
        self.assertEqual(declared, want)
        hcl = (IMAGE_DIR / "tofu" / ".terraform.lock.hcl").read_text()
        locked = dict(re.findall(r'provider "registry\.opentofu\.org/([^"]+)" \{\s*version\s*=\s*"([^"]+)"', hcl))
        self.assertEqual(locked, want)
        tofurc = (IMAGE_DIR / "tofu" / "tofurc").read_text()
        self.assertIn("filesystem_mirror", tofurc)
        self.assertNotIn("direct", re.sub(r"#.*", "", tofurc))

    def test_the_build_context_holds_only_the_build_inputs(self):
        lines = [ln for ln in (IMAGE_DIR / ".dockerignore").read_text().splitlines()
                 if ln and not ln.startswith("#")]
        self.assertEqual(lines[0], "*")
        for kept in lines[1:]:
            self.assertTrue((IMAGE_DIR / kept[1:]).is_file(), kept)


class Compare(unittest.TestCase):
    def test_a_matching_image_has_no_problems(self):
        self.assertEqual(build.compare(CONTENTS, good_probe()), [])

    def test_each_difference_is_reported(self):
        found = good_probe()
        found["versions"]["dbt-trino"] = "1.10.7"
        found["uid"] = 0
        found["env"]["FLUID_COMMAND_CENTER_ENABLED"] = "true"
        found["tofu_init"] = {"rc": 1, "tail": "the cached package does not match any of the checksums"}
        found["tofu"]["provider_selections"].pop("registry.opentofu.org/keycloak/keycloak")
        found["spark_runner"] = False
        found["forge_source"]["record"]["commit"] = "a" * 40
        problems = build.compare(CONTENTS, found)
        self.assertEqual(len(problems), 7, problems)
        joined = "\n".join(problems)
        for text in ("dbt-trino 1.10.7", "uid 0", "FLUID_COMMAND_CENTER_ENABLED", "checksums",
                     "keycloak", "Spark runner", "forge was built from commit " + "a" * 40):
            self.assertIn(text, joined)

    def test_forge_at_another_version_or_commit_or_archive_is_refused(self):
        for mutate, text in (
                (lambda f: f["versions"].update({"data-product-forge": "0.32.0"}), "data-product-forge 0.32.0"),
                (lambda f: f["versions"].update({"data-product-forge": "0.32.1.dev0+gdeadbeef"}), "gdeadbeef"),
                (lambda f: f["forge_source"]["record"].update(commit="b" * 40), "commit " + "b" * 40),
                (lambda f: f["forge_source"]["record"].update(archive_sha256="d" * 64), "archive sha256 " + "d" * 64),
                (lambda f: f["forge_source"].update(wheel_sha256="e" * 64), "wheel kept in the image has sha256 " + "e" * 64),
                (lambda f: f["forge_source"].update(mismatched=["fluid_build/cli.py"]), "differ from the recorded wheel"),
                (lambda f: f["forge_source"].update(checked=0), "no installed forge file was compared"),
                (lambda f: f.pop("forge_source"), "does not record which forge source"),
                (lambda f: f.update(forge_source={"error": "FileNotFoundError"}), "FileNotFoundError")):
            found = good_probe()
            mutate(found)
            problems = build.compare(CONTENTS, found)
            self.assertEqual(len(problems), 1, problems)
            self.assertIn(text, problems[0])

    def test_a_release_pin_is_the_release_with_no_local_part(self):
        release = {**CONTENTS, "forge": {**FORGE, "version": "0.33.0", "release": True}}
        self.assertEqual(build.forge_pin(release)["local_version"], "0.33.0")
        self.assertEqual(build.tag_for("fluid-runner", release), "lab.local/fluid-runner:0.33.0-e37cdda5")
        self.assertEqual(build.compare(release, {**good_probe(), "versions": {**good_probe()["versions"],
                                                                              "data-product-forge": "0.33.0"}}), [])
        # An image of the same release built from a dev commit reports a local version and is refused.
        self.assertIn("data-product-forge 0.33.0+ge37cdda5", "\n".join(build.compare(
            release, {**good_probe(), "versions": {**good_probe()["versions"],
                                                   "data-product-forge": "0.33.0+ge37cdda5"}})))
        args = build.build_args(release)
        self.assertIn("FORGE_VERSION=0.33.0", args)

    def test_release_and_version_forms(self):
        ok = {**CONTENTS, "forge": {**FORGE, "version": "0.33.0rc1", "release": True}}
        self.assertEqual(build.forge_pin(ok)["local_version"], "0.33.0rc1")   # what installed metadata reports
        for bad in ({"version": "0.33.0.rc1"}, {"version": "0.33.0.dev0", "release": True}, {"release": "yes"}):
            with self.assertRaises(config.ConfigError, msg=bad):
                build.forge_pin({**CONTENTS, "forge": {**FORGE, **bad}})

    def test_the_tag_carries_the_commit(self):
        self.assertEqual(build.tag_for("fluid-runner", CONTENTS), "lab.local/fluid-runner:0.32.1.dev0-e37cdda5")
        other = {**CONTENTS, "forge": {**FORGE, "commit": "f" * 40}}
        self.assertNotEqual(build.tag_for("fluid-runner", other), build.tag_for("fluid-runner", CONTENTS))

    def test_an_unknown_key_in_the_forge_pin_is_refused(self):
        for key in ("relase", "Release", "commit8"):
            with self.assertRaises(config.ConfigError, msg=key) as ctx:
                build.forge_pin({**CONTENTS, "forge": {**FORGE, key: True}})
            self.assertIn(key, str(ctx.exception))

    def test_a_malformed_forge_pin_is_refused(self):
        for bad in ("0.32.0", None, {**FORGE, "commit": "e37cdda5"}, {**FORGE, "commit": "E" * 40},
                    {**FORGE, "sha256": "abc"}, {**FORGE, "version": "0.32.1+local"}, {**FORGE, "version": ""},
                    {k: v for k, v in FORGE.items() if k != "sha256"}):
            with self.assertRaises(config.ConfigError, msg=bad):
                build.lock_entry({"built_images": [{"id": "fluid-runner", "contents": {**CONTENTS, "forge": bad}}]},
                                 "fluid-runner")

    def test_fields_of_the_wrong_type_are_problems_not_crashes(self):
        found = {"versions": [], "tofu": [], "tofu_init": "x", "env": None, "fluid_cli": 3}
        self.assertGreater(len(build.compare(CONTENTS, found)), 5)

    def test_a_lock_entry_without_usable_contents_is_refused(self):
        for entry in ({"id": "fluid-runner"}, {"id": "fluid-runner", "contents": ["x"]},
                      {"id": "fluid-runner", "contents": {**CONTENTS, "providers": "x"}}):
            with self.assertRaises(config.ConfigError):
                build.lock_entry({"built_images": [entry]}, "fluid-runner")
        with self.assertRaises(config.ConfigError):
            build.lock_entry({"built_images": []}, "fluid-runner")


class RunBuild(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        (self.root / "versions.lock.yaml").write_text("# fixture\n")
        self.env = {"LAB_STATE_DIR": str(Path(self.tmp.name) / "state")}
        self.lab = state.lab_for(self.root, "ffl-x", self.env)
        self.image_dir = self.root / build.IMAGES["fluid-runner"]
        self.image_dir.mkdir(parents=True)
        self.write_inputs()

    def write_inputs(self, header=None, forge_lines=None):
        pin = build.forge_pin(CONTENTS)
        (self.image_dir / "requirements.txt").write_text((header or build.pin_header(CONTENTS)) + "\nagate==1.9.1\n")
        lines = forge_lines if forge_lines is not None else [f"data-product-forge[local,aws] @ {pin['url']}"]
        (self.image_dir / "requirements.in").write_text("dbt-core==1.10.23\n" + "\n".join(lines) + "\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_lock_bump_without_compile_sh_is_refused_before_docker_build(self):
        pin = build.forge_pin(CONTENTS)
        good = f"data-product-forge[local,aws] @ {pin['url']}"
        stale = build.pin_header({**CONTENTS, "forge": {**FORGE, "sha256": "d" * 64}})
        for header, lines, text in ((stale, None, "was not compiled"),
                                    (None, [good.replace(pin["commit"], "a" * 40)], "exactly one line"),
                                    (None, [good, good], "exactly one line"),
                                    (None, [], "exactly one line")):
            self.write_inputs(header, lines)
            runner = self.runner()
            doc = self.run_build(runner)
            self.assertFalse(doc["ok"])
            self.assertEqual(doc["checks"][-1]["id"], "build:config")
            self.assertIn(text, doc["checks"][-1]["message"])
            self.assertFalse(self.argv_of(runner, "docker", "build"))
        self.write_inputs()
        self.assertIsNone(build.input_problem(self.image_dir, CONTENTS))
        self.assertTrue(self.run_build(self.runner())["checks"][0]["status"] == PASS)

    def runner(self, *, probe=None, clusters=(0, "ffl-x\n"), node_layers=None, build_rc=0, ids=None) -> FakeRunner:
        ids = list(ids or [IMAGE_ID, IMAGE_ID])

        def inspect(argv, **_kw):
            facts = {"Id": ids.pop(0) if len(ids) > 1 else ids[0], "RootFS": {"Layers": LAYERS},
                     "Os": "linux", "Architecture": "arm64"}
            return proc.Result(argv, 0, json.dumps(facts), "")

        node = {"info": {"imageSpec": {"rootfs": {"diff_ids": LAYERS if node_layers is None else node_layers}}}}
        return FakeRunner(docs={"versions.lock.yaml": LOCK}, table={
            ("docker", "version"): (0, "arm64\n"),
            ("docker", "build"): (build_rc, "" if build_rc == 0 else "ERROR: THESE PACKAGES DO NOT MATCH THE HASHES"),
            ("docker", "image", "inspect"): inspect,
            ("docker", "run"): probe if callable(probe) else (0, "noise\n" + json.dumps(probe or good_probe()) + "\n"),
            ("docker", "rm"): (0, ""),
            ("docker", "inspect"): (0, "node-container-id\n"),
            ("kind", "get", "clusters"): clusters,
            ("kind", "load", "docker-image"): (0, ""),
            ("docker", "exec"): (0, json.dumps(node)),
        })

    def run_build(self, runner):
        return build.run_build(self.root, "fluid-runner", name="ffl-x", runner=runner, env=self.env)

    def argv_of(self, runner, *prefix):
        return [argv for argv, _ in runner.calls if tuple(argv[:len(prefix)]) == prefix]

    def test_a_checked_image_is_loaded_and_recorded_with_its_node(self):
        down.mark_state_dir(self.lab)
        runner = self.runner()
        doc = self.run_build(runner)
        self.assertTrue(doc["ok"], doc)
        self.assertEqual([c["status"] for c in doc["checks"]], [PASS, PASS, PASS])
        self.assertEqual(doc["tag"], "lab.local/fluid-runner:0.32.1.dev0-e37cdda5")
        self.assertIn("--build-arg", self.argv_of(runner, "docker", "build")[0])
        self.assertEqual(state.read_json(self.lab.images_record)["fluid-runner"]["forge_commit"], FORGE["commit"])
        (build_argv,) = self.argv_of(runner, "docker", "build")
        self.assertEqual(build_argv[2:4], ["--platform", "linux/arm64"])
        (probe_argv,) = self.argv_of(runner, "docker", "run")
        self.assertEqual(probe_argv[probe_argv.index("--network") + 1], "none")
        self.assertIn(IMAGE_ID, probe_argv)  # the probe runs the image by id, not by tag
        (load_argv,) = self.argv_of(runner, "kind", "load")
        self.assertEqual(load_argv[-2:], ["--name", "ffl-x"])
        record = state.read_json(self.lab.images_record)["fluid-runner"]
        self.assertEqual((record["layers"], record["node_id"]), (LAYERS, "node-container-id"))
        self.assertEqual(record["versions"]["data-product-forge"], LOCAL)
        self.assertEqual(oct(self.lab.images_record.stat().st_mode & 0o777), "0o600")
        self.assertFalse(self.argv_of(runner, "docker", "push"))

    def test_without_a_cluster_the_image_is_built_but_not_loaded(self):
        runner = self.runner(clusters=(0, ""))
        doc = self.run_build(runner)
        self.assertTrue(doc["ok"], doc)
        self.assertEqual(doc["checks"][-1]["status"], WARN)
        self.assertIn("./lab up --name ffl-x", doc["checks"][-1]["advice"])
        self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_kind_that_cannot_answer_fails_instead_of_warning(self):
        for clusters in ((None, ""), (1, "ERROR: failed to list clusters")):
            doc = self.run_build(self.runner(clusters=clusters))
            self.assertFalse(doc["ok"], clusters)
            self.assertEqual(doc["checks"][-1]["status"], FAIL)
            self.assertIn("kind get clusters failed", doc["checks"][-1]["message"])

    def test_a_cluster_without_lab_state_gets_nothing(self):
        runner = self.runner()
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["status"], FAIL)
        self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_an_image_that_differs_from_the_lock_is_never_loaded(self):
        down.mark_state_dir(self.lab)
        bad = good_probe()
        bad["versions"]["duckdb"] = "1.5.5"
        runner = self.runner(probe=bad)
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        self.assertEqual(doc["checks"][-1]["id"], "build:probe")
        self.assertIn("duckdb 1.5.5", doc["checks"][-1]["message"])
        self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_a_tag_that_moves_during_the_probe_is_not_loaded(self):
        down.mark_state_dir(self.lab)
        runner = self.runner(ids=[IMAGE_ID, "sha256:" + "8" * 64])
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        self.assertIn("changed while it was being probed", doc["checks"][-1]["message"])
        self.assertFalse(self.argv_of(runner, "kind", "load"))

    def test_a_probe_that_hangs_is_removed(self):
        def hang(argv, **_kw):
            return proc.Result(argv, 124, "", "timed out", timed_out=True)

        runner = self.runner(probe=hang)
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        (probe_argv,) = self.argv_of(runner, "docker", "run")
        (rm_argv,) = self.argv_of(runner, "docker", "rm")
        self.assertEqual(rm_argv[-1], probe_argv[probe_argv.index("--name") + 1])

    def test_different_layers_in_the_node_fail_and_drop_the_old_record(self):
        down.mark_state_dir(self.lab)
        state.write_json_private(self.lab.images_record, {"fluid-runner": {"layers": LAYERS}, "other": {}})
        doc = self.run_build(self.runner(node_layers=LAYERS[:1]))
        self.assertFalse(doc["ok"])
        self.assertIn("does not hold the layers", doc["checks"][-1]["message"])
        self.assertEqual(state.read_json(self.lab.images_record), {"other": {}})

    def test_an_unreadable_record_is_replaced(self):
        down.mark_state_dir(self.lab)
        for text in ("{trunc", "[1]"):
            self.lab.images_record.write_text(text)
            doc = self.run_build(self.runner())
            self.assertTrue(doc["ok"], (text, doc))
            self.assertEqual(list(state.read_json(self.lab.images_record)), ["fluid-runner"])

    def test_a_failed_build_stops_before_the_probe(self):
        runner = self.runner(build_rc=1)
        doc = self.run_build(runner)
        self.assertFalse(doc["ok"])
        self.assertIn("DO NOT MATCH THE HASHES", doc["checks"][-1]["message"])
        self.assertFalse(self.argv_of(runner, "docker", "run"))
        (build_call,) = [kw for argv, kw in runner.calls if argv[:2] == ["docker", "build"]]
        self.assertEqual(build_call["env"]["DOCKER_BUILDKIT"], "1")

    def test_only_the_docker_runtime_builds(self):
        runner = self.runner()
        doc = build.run_build(self.root, "fluid-runner", name="ffl-x", runner=runner,
                              env={**self.env, "KIND_EXPERIMENTAL_PROVIDER": "podman"})
        self.assertFalse(doc["ok"])
        self.assertIn("docker runtime only", doc["checks"][0]["message"])
        self.assertEqual(runner.calls, [])

    def test_lab_down_drops_the_record_with_the_cluster(self):
        down.mark_state_dir(self.lab)
        state.write_json_private(self.lab.images_record, {"fluid-runner": {}})
        gone = "Error response from daemon: No such object: ffl-x-openbao"
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (0, ""),
            ("docker", "inspect"): (1, gone),
            ("docker", "network", "inspect"): (1, "Error response from daemon: network ffl-x-net not found"),
        })
        doc = down.run_down(self.root, name="ffl-x", runner=runner, env=self.env)
        self.assertTrue(doc["ok"], doc)
        self.assertFalse(self.lab.images_record.exists())

    def test_lab_down_warns_when_it_cannot_drop_the_record(self):
        down.mark_state_dir(self.lab)
        self.lab.images_record.mkdir()  # unlink() of a directory raises
        runner = FakeRunner(table={
            ("kind", "get", "clusters"): (0, ""),
            ("docker", "inspect"): (1, "Error response from daemon: No such object: ffl-x-openbao"),
            ("docker", "network", "inspect"): (1, "Error response from daemon: network ffl-x-net not found"),
        })
        doc = down.run_down(self.root, name="ffl-x", runner=runner, env=self.env)
        self.assertTrue(doc["ok"], doc)
        self.assertIn(("down:images", WARN), [(c["id"], c["status"]) for c in doc["checks"]])
        self.assertIn("down:network", [c["id"] for c in doc["checks"]])  # the rest of `down` still ran

    def test_a_probe_container_that_cannot_be_removed_is_named(self):
        def hang(argv, **_kw):
            return proc.Result(argv, 124, "", "timed out", timed_out=True)

        runner = self.runner(probe=hang)
        runner.table[("docker", "rm")] = (1, "Error response from daemon: cannot remove")
        doc = self.run_build(runner)
        self.assertIn("could not be removed", doc["checks"][-1]["message"])
        self.assertIn("docker rm -f fluid-fabric-lab-probe-", doc["checks"][-1]["message"])

    def test_a_new_cluster_drops_the_record_of_the_old_node(self):
        from unittest import mock

        from labctl import cluster, up

        down.mark_state_dir(self.lab)
        state.write_json_private(self.lab.images_record, {"fluid-runner": {"node_id": "dead"}})
        step = up.Up(self.root, "core", name="ffl-x", runner=FakeRunner(), env=self.env)
        step.kube = mock.Mock()
        step.kube.get.return_value = {"data": {"Corefile": "x"}}
        # `create` is also patch.multiple's own keyword, so it is patched on its own.
        with mock.patch.multiple(cluster, runtime_config=mock.DEFAULT, ensure_network=mock.DEFAULT,
                                 gateway_host_port=mock.DEFAULT, pin_pulls_to_mirror=mock.DEFAULT,
                                 corefile_with_rewrites=mock.DEFAULT, exists=mock.DEFAULT) as fakes, \
                mock.patch.object(cluster, "create") as create, \
                mock.patch.object(up.mirror, "tier_images", return_value=[]), \
                mock.patch.object(up.mirror, "upstream_hosts", return_value=[]):
            fakes["create"] = create
            fakes["exists"].return_value = True
            fakes["create"].return_value = "kind cluster ffl-x already exists (reused)"
            fakes["gateway_host_port"].return_value = None
            fakes["pin_pulls_to_mirror"].return_value = []
            fakes["corefile_with_rewrites"].return_value = "x"
            step.cluster()
            self.assertTrue(self.lab.images_record.exists())  # same node: the record stays
            fakes["exists"].return_value = False
            fakes["create"].return_value = "created kind cluster ffl-x"
            step.cluster()
        self.assertFalse(self.lab.images_record.exists())

    def test_the_cli_offers_exactly_the_buildable_images(self):
        self.assertEqual(cli.BUILDABLE, tuple(build.IMAGES))
        args = cli._parser().parse_args(["build", "fluid-runner", "--name", "ffl-x"])
        self.assertEqual((args.image, args.name), ("fluid-runner", "ffl-x"))


if __name__ == "__main__":
    unittest.main()
