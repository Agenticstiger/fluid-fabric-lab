# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab doctor`: runtime, tool pins, memory fit, disk. No real runtime needed."""

import json
import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import GIB, LOCK_DOC, TIERS_DOC, DiskUsage, FakeRunner, docker_info, make_root, tools_ok_table

from labctl import config, doctor
from labctl.output import FAIL, PASS, WARN


def by_id(doc, cid):
    return next(c for c in doc["checks"] if c["id"] == cid)


class VersionParsing(unittest.TestCase):
    def test_real_tool_outputs(self):
        samples = {
            "v4.3.0+gbec5b06": "4.3.0",
            "v1.8.1": "1.8.1",
            "kind v0.33.0 go1.27.0 linux/arm64": "0.33.0",
            "8.30.1": "8.30.1",
            "reuse, version 6.2.0\n\nThis program is free software": "6.2.0",
            "Version: 0.71.0\nVulnerability DB:\n  Version: 2": "0.71.0",
            "yq (https://github.com/mikefarah/yq/) version v4.54.1": "4.54.1",
            "no version here": None,
        }
        for text, want in samples.items():
            with self.subTest(text=text):
                self.assertEqual(doctor.parse_version(text), want)

    def test_helm_plugin_listing(self):
        listing = "NAME\tVERSION\tTYPE\ndiff\t3.15.15\tcli/v1\nsecrets\t4.6.0\tcli/v1\n"
        self.assertEqual(doctor.parse_helm_plugin_version(listing, "diff"), "3.15.15")
        self.assertIsNone(doctor.parse_helm_plugin_version(listing, "unittest"))


class RuntimeSelection(unittest.TestCase):
    def test_kind_provider_env_wins(self):
        self.assertEqual(doctor.select_runtime(None, {"KIND_EXPERIMENTAL_PROVIDER": "podman"}, lambda n: True), "podman")

    def test_explicit_wins(self):
        self.assertEqual(doctor.select_runtime("docker", {"KIND_EXPERIMENTAL_PROVIDER": "podman"}, lambda n: True), "docker")

    def test_falls_back_to_podman(self):
        self.assertEqual(doctor.select_runtime(None, {}, lambda n: n == "podman"), "podman")

    def test_docker_down_is_a_failure(self):
        runner = FakeRunner(table={"docker": (1, json.dumps({"ServerErrors": ["Cannot connect"]}))})
        chk, mem, _ = doctor.probe_runtime("docker", runner)
        self.assertEqual(chk["status"], FAIL)
        self.assertIsNone(mem)

    def test_podman_memory(self):
        info = {"host": {"memTotal": 12 * GIB, "cpus": 4}, "version": {"Version": "5.6.0"}}
        runner = FakeRunner(table={"podman": (0, json.dumps(info))})
        chk, mem, _ = doctor.probe_runtime("podman", runner)
        self.assertEqual(chk["status"], PASS)
        self.assertEqual(mem, 12 * GIB)

    def test_unsupported_runtime(self):
        chk, mem, _ = doctor.probe_runtime("nerdctl", FakeRunner())
        self.assertEqual(chk["status"], FAIL)


class MemoryFit(unittest.TestCase):
    def setUp(self):
        self.all = {n: config.resolve_tier(TIERS_DOC, n) for n in TIERS_DOC["tiers"]}

    def test_tier_that_does_not_fit_fails_and_names_tiers_that_do(self):
        out = doctor.memory_checks(self.all["full"], self.all, int(8 * GIB), int(16 * GIB), "docker", None)
        mem = out[0]
        self.assertEqual(mem["status"], FAIL)
        self.assertIn("does not fit", mem["message"])
        self.assertIn("core", mem["advice"])
        self.assertFalse(mem["detail"]["fits"]["full"])

    def test_tier_that_fits_passes(self):
        out = doctor.memory_checks(self.all["core"], self.all, int(8 * GIB), int(16 * GIB), "docker", None)
        self.assertEqual(out[0]["status"], PASS)

    def test_unknown_vm_memory_fails_never_skips(self):
        out = doctor.memory_checks(self.all["core"], self.all, None, None, "docker", None)
        self.assertEqual(out[0]["status"], FAIL)
        self.assertEqual(out[1]["status"], WARN)  # host memory unreadable

    def test_host_too_small_for_tier(self):
        out = doctor.memory_checks(self.all["full"], self.all, int(7 * GIB), int(8 * GIB), "docker", None)
        self.assertIn("larger machine", out[0]["advice"])
        self.assertEqual(out[1]["status"], FAIL)

    def test_linux_meminfo(self):
        with tempfile.NamedTemporaryFile("w", suffix="meminfo", delete=False) as fh:
            fh.write("MemTotal:       16384000 kB\nMemFree: 1 kB\n")
        try:
            self.assertEqual(doctor.host_memory_bytes(FakeRunner(), "Linux", fh.name), 16384000 * 1024)
        finally:
            Path(fh.name).unlink()


class ToolPins(unittest.TestCase):
    def test_exact_pins_pass(self):
        out = doctor.tool_checks(LOCK_DOC, FakeRunner(table=tools_ok_table()), strict=False)
        self.assertEqual([c["status"] for c in out], [PASS, PASS, PASS])

    def test_mismatch_warns_and_strict_fails(self):
        table = tools_ok_table({("helm", "version", "--short"): (0, "v4.3.0+gbec5b06\n")})
        self.assertEqual(doctor.tool_checks(LOCK_DOC, FakeRunner(table=table), strict=False)[0]["status"], WARN)
        self.assertEqual(doctor.tool_checks(LOCK_DOC, FakeRunner(table=table), strict=True)[0]["status"], FAIL)

    def test_missing_tool_and_missing_plugin_fail(self):
        table = tools_ok_table({("helm", "plugin", "list"): (0, "NAME\tVERSION\n"), ("yq", "--version"): (None, "")})
        out = {c["id"]: c["status"] for c in doctor.tool_checks(LOCK_DOC, FakeRunner(table=table), strict=False)}
        self.assertEqual(out["tool:helm-diff"], FAIL)
        self.assertEqual(out["tool:yq"], FAIL)

    def test_tool_without_probe_fails(self):
        lock = {"tools": [{"name": "brand-new-tool", "version": "1.0.0"}]}
        out = doctor.tool_checks(lock, FakeRunner(), strict=False)
        self.assertEqual(out[0]["status"], FAIL)
        self.assertIn("no version probe", out[0]["message"])

    def test_every_pinned_tool_in_the_real_lock_has_a_probe(self):
        # Read the tool names straight from the committed lock without yq.
        text = (support.REPO_ROOT / "versions.lock.yaml").read_text()
        section = text.split("\ntools:\n", 1)[1].split("\n\n", 1)[0] if "\ntools:\n" in text else ""
        names = [ln.split("name:", 1)[1].strip() for ln in section.splitlines() if ln.startswith("  - name:")]
        self.assertTrue(names, "tools section not found in versions.lock.yaml")
        self.assertEqual(sorted(set(names) - set(doctor.TOOL_PROBES)), [])


class Disk(unittest.TestCase):
    def test_below_floor_fails(self):
        self.assertEqual(doctor.disk_check(Path("."), 15, lambda p: DiskUsage(4)).get("status"), FAIL)

    def test_above_floor_passes(self):
        self.assertEqual(doctor.disk_check(Path("."), 15, lambda p: DiskUsage(40)).get("status"), PASS)

    def test_the_disk_floor_is_stated_plainly(self):
        self.assertEqual(doctor.DEFAULT_MIN_FREE_DISK_GIB, 15.0)
        ok = doctor.disk_check(Path("."), doctor.DEFAULT_MIN_FREE_DISK_GIB, lambda p: DiskUsage(40))
        low = doctor.disk_check(Path("."), doctor.DEFAULT_MIN_FREE_DISK_GIB, lambda p: DiskUsage(4))
        self.assertEqual(ok["message"], "40.0 GiB free (minimum 15 GiB)")
        self.assertIn("the lab needs at least 15 GiB", low["message"])
        for rec in (ok, low):
            self.assertNotIn("provisional", (rec["message"] + str(rec.get("detail"))).lower())
        self.assertEqual(ok["detail"]["default_min_free_gib"], 15.0)


class DoctorEndToEnd(unittest.TestCase):
    def run_doctor(self, tier="core", vm_gib=8.0, free_gib=50.0, strict=False, extra=None, docs=None):
        with tempfile.TemporaryDirectory() as tmp:
            root, fixture_docs = make_root(Path(tmp))
            table = tools_ok_table(extra)
            table.setdefault("docker", (0, docker_info(vm_gib)))
            runner = FakeRunner(docs=docs if docs is not None else fixture_docs, table=table)
            return doctor.run_doctor(root, tier, runtime="docker", strict=strict, runner=runner,
                                     env={}, host_memory=lambda: int(32 * GIB),
                                     disk_usage=lambda p: DiskUsage(free_gib))

    def test_core_on_an_8gib_vm_passes(self):
        doc = self.run_doctor()
        self.assertTrue(doc["ok"], json.dumps(doc, indent=1))

    def test_full_on_an_8gib_vm_fails(self):
        doc = self.run_doctor(tier="full")
        self.assertFalse(doc["ok"])
        self.assertEqual(by_id(doc, "memory")["status"], FAIL)

    def test_runtime_down_fails_memory_too(self):
        doc = self.run_doctor(extra={"docker": (1, "")})
        self.assertFalse(doc["ok"])
        self.assertEqual(by_id(doc, "runtime")["status"], FAIL)
        self.assertEqual(by_id(doc, "memory")["status"], FAIL)

    def test_unreadable_config_fails(self):
        doc = self.run_doctor(docs={})
        self.assertFalse(doc["ok"])
        self.assertEqual(by_id(doc, "config")["status"], FAIL)
        self.assertEqual(by_id(doc, "memory")["status"], FAIL)

    def test_unknown_tier_fails(self):
        doc = self.run_doctor(tier="galaxy")
        self.assertFalse(doc["ok"])
        self.assertEqual(by_id(doc, "tier")["status"], FAIL)

    def test_warning_only_passes_unless_strict(self):
        extra = {("helm", "version", "--short"): (0, "v4.3.0\n")}
        self.assertTrue(self.run_doctor(extra=extra)["ok"])
        self.assertFalse(self.run_doctor(extra=extra, strict=True)["ok"])

    def test_no_skip_status_exists(self):
        doc = self.run_doctor(extra={"docker": (1, "")}, free_gib=1)
        self.assertTrue(all(c["status"] in (PASS, WARN, FAIL) for c in doc["checks"]))

    def test_json_shape(self):
        doc = self.run_doctor()
        json.dumps(doc)
        for key in ("command", "tier", "cluster", "runtime", "ok", "checks"):
            self.assertIn(key, doc)


if __name__ == "__main__":
    unittest.main()
