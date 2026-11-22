# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The e2e tier: products/stream-batch runs through forge in fluid-host, and the record passes only on
exactly this run's rows (labctl/e2e.py); `lab e2e` chains mirror, up, the two builds, up and the check,
and stops at the first failure (labctl/e2e_command.py); `lab check --only` judges just the named records."""

import hashlib
import json
import re
import unittest
from datetime import datetime
from unittest import mock

import support  # noqa: F401
from support import REPO_ROOT

from labctl import accept, check, config, e2e, e2e_command, proc, streaming
from labctl.output import FAIL, PASS, check as record

TIERS = config.load_yaml(REPO_ROOT / config.TIERS)
LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
RUN = "e2e-1795350000-abc123"


def load_yaml(path):
    return config.load_yaml(path)


def good(**over):
    produced = {"rc": 0, "n": e2e.EVENTS}
    result = {"run": RUN, "spark": "3.5.8",
              "raw": {"rows": e2e.EVENTS, "event_ids": e2e.EVENTS, "first": "2026-11-20 09:00:00", "snapshots": 3},
              "daily": {"rows": dict(e2e.EXPECTED), "snapshots": 2}}
    args = {"produced": produced, "result": result, "run_id": RUN,
            "daily_before": {"snapshot": 1, "total": 2, "snapshots": 1},
            "daily_after": {"snapshot": 2, "total": 2, "snapshots": 2},
            "stop": {"stopped": ["20261122T120000Z-0a1b2c3d"], "active": [], "states": {}}}
    args.update(over)
    return args


def judge(**over):
    a = good(**over)
    return e2e.judge(a["produced"], a["result"], a["run_id"], daily_before=a["daily_before"],
                     daily_after=a["daily_after"], stop=a["stop"])


class Events(unittest.TestCase):
    def test_each_run_sends_its_own_events_spread_over_the_expected_dates(self):
        lines = e2e.records(RUN)
        self.assertEqual(len(lines), e2e.EVENTS)
        events = [json.loads(line) for line in lines]
        self.assertEqual(len({e["event_id"] for e in events}), e2e.EVENTS)
        self.assertTrue(all(e["run_id"] == RUN and e["event_id"].startswith(RUN + "-") for e in events))
        days = {}
        for e in events:
            when = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.000Z")
            days[when.date().isoformat()] = days.get(when.date().isoformat(), 0) + 1
        self.assertEqual(days, e2e.EXPECTED)

    def test_run_ids_are_unique_and_shaped_for_forge_and_the_read_back(self):
        ids = {e2e.new_run_id(1795350000) for _ in range(20)}
        self.assertEqual(len(ids), 20)
        verify = (REPO_ROOT / "platform/e2e/verify.py").read_text()
        pattern = re.search(r're\.fullmatch\(r"([^"]+)"', verify).group(1)
        for run_id in ids:
            self.assertRegex(run_id, e2e._RUN_ID)
            self.assertRegex(run_id, "^" + pattern + "$")
            self.assertRegex(run_id, r"^[a-z0-9][a-z0-9-]{2,62}$")   # FLUID_RUN_ID, as accept_runner checks it


class Judge(unittest.TestCase):
    def test_exactly_this_runs_rows_pass(self):
        out = judge()
        self.assertEqual(out["raw_rows"], e2e.EVENTS)
        self.assertEqual(out["daily"], e2e.EXPECTED)

    def test_anything_else_fails(self):
        cases = {
            "producer failed": {"produced": {"rc": 1, "n": e2e.EVENTS}},
            "producer short": {"produced": {"rc": 0, "n": e2e.EVENTS - 1}},
            "producer rc not an int": {"produced": {"rc": False, "n": e2e.EVENTS}},
            "another run": {"run_id": "e2e-1795350000-ffffff"},
            "lost events": {"result": {**good()["result"], "raw": {"rows": e2e.EVENTS - 1, "event_ids": e2e.EVENTS - 1}}},
            "duplicated events": {"result": {**good()["result"], "raw": {"rows": e2e.EVENTS + 1, "event_ids": e2e.EVENTS}}},
            "wrong daily": {"result": {**good()["result"], "daily": {"rows": {"2026-11-20": 7}, "snapshots": 2}}},
            "daily as floats": {"result": {**good()["result"],
                                           "daily": {"rows": {k: float(v) for k, v in e2e.EXPECTED.items()}}}},
            "no new snapshot": {"daily_after": {"snapshot": 1, "total": 2, "snapshots": 1}},
            "no daily table": {"daily_after": None},
            "stream never stopped": {"stop": {"stopped": [], "active": []}},
            "stream still running": {"stop": {"stopped": ["r1"], "active": ["r1"]}},
            "stop failed": {"stop": {"error": "Traceback"}},
        }
        for what, over in cases.items():
            with self.subTest(what), self.assertRaises(e2e.E2EError):
                judge(**over)

    def test_a_float_count_is_not_a_count(self):
        raw = {"rows": float(e2e.EVENTS), "event_ids": e2e.EVENTS}
        with self.assertRaises(e2e.E2EError):
            judge(result={**good()["result"], "raw": raw})


class Pods(unittest.TestCase):
    def _restricted(self, job):
        spec = job["spec"]["template"]["spec"]
        self.assertTrue(spec["securityContext"]["runAsNonRoot"])
        self.assertEqual(spec["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertFalse(spec["automountServiceAccountToken"])
        for c in spec["containers"]:
            sc = c["securityContext"]
            self.assertFalse(sc["allowPrivilegeEscalation"])
            self.assertEqual(sc["capabilities"], {"drop": ["ALL"]})
            self.assertIn("memory", c["resources"]["limits"])
        self.assertEqual(job["spec"]["template"]["metadata"]["labels"][e2e.JOB_LABEL], "e2e")
        return spec

    def test_the_producer_is_restricted_sends_the_runs_events_and_holds_no_credential(self):
        job = e2e.produce_job("lab-check-e2e-produce-1", "quay.io/strimzi/kafka@sha256:" + "0" * 64, RUN)
        spec = self._restricted(job)
        self.assertEqual(job["metadata"]["namespace"], streaming.NAMESPACE)
        env = {e["name"]: e for e in spec["containers"][0]["env"]}
        self.assertEqual(env["TOPIC"]["value"], e2e.TOPIC)
        self.assertEqual(env["PLAIN"]["value"], streaming.BOOTSTRAP_PLAIN)
        self.assertEqual(env["RECORDS"]["value"].splitlines(), e2e.records(RUN))
        self.assertNotIn("secretKeyRef", json.dumps(spec))
        self.assertIn("enable.idempotence=false", e2e.PRODUCE_SCRIPT)
        self.assertIn("LAB-E2E-PRODUCED", e2e.PRODUCE_SCRIPT)

    def test_the_read_back_runs_the_lab_built_image_as_spark_through_the_spark_tiers_entry(self):
        job = e2e.verify_job("lab-check-e2e-verify-1", "lab.local/spark-iceberg:3.5.8-x", RUN)
        spec = self._restricted(job)
        c = spec["containers"][0]
        self.assertEqual(c["imagePullPolicy"], "Never")
        self.assertEqual((c["securityContext"]["runAsUser"], c["securityContext"]["runAsGroup"]),
                         (e2e.SPARK_UID, e2e.SPARK_GID))
        self.assertEqual(c["command"], ["bash", "/spark/lab-spark-entry.sh", "/e2e/verify.py", RUN])
        refs = [e for e in c["env"] if "valueFrom" in e]
        self.assertEqual({e["valueFrom"]["secretKeyRef"]["name"] for e in refs}, {"spark-lakekeeper-client"})
        self.assertTrue(all("value" not in e for e in refs))
        self.assertIn({"name": "e2e", "configMap": {"name": "lab-e2e"}}, spec["volumes"])


class Forge(unittest.TestCase):
    def setUp(self):
        self.allow = accept.parse_operator_env(REPO_ROOT / e2e.PRODUCT_DIR / "operator.env")

    def test_each_step_runs_forge_strict_with_the_operators_allowlists_only(self):
        for step, tail in (("validate", ["fluid", "validate", "contract.fluid.yaml", "--env", "local", "--strict"]),
                           ("plan", ["fluid", "plan", "contract.fluid.yaml", "--env", "local", "--out", e2e.PLAN]),
                           ("apply", ["fluid", "apply", e2e.PLAN, "--env", "local", "--yes"]),
                           ("batch", ["fluid", "apply", "contract.fluid.yaml", "--env", "local", "--mode",
                                      "amend-and-build", "--build-id", e2e.BATCH_BUILD, "--yes"])):
            with self.subTest(step):
                argv = e2e.forge_argv(step, RUN, self.allow)
                self.assertEqual(argv[-len(tail):], tail)
                self.assertEqual(argv[0], "env")
                self.assertIn("FLUID_ENV=local", argv)
                self.assertIn(f"FLUID_RUN_ID={RUN}", argv)
                self.assertIn("VALIDATE_STRICT=true", argv)
                for k, v in self.allow.items():
                    self.assertIn(f"{k}={v}", argv)

    def test_the_operator_lists_exactly_the_products_kafka_host_sql_and_namespaces(self):
        contract = load_yaml(REPO_ROOT / e2e.PRODUCT_DIR / e2e.CONTRACT)
        builds = {b["id"]: b for b in contract["builds"]}
        sql = builds[e2e.BATCH_BUILD]["properties"]["sql"].strip()
        digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        self.assertEqual(self.allow["FLUID_SPARK_CONTRACT_SQL_ALLOWLIST"],
                         f"{e2e.PRODUCT_ID}/{e2e.BATCH_BUILD}@sha256:{digest}",
                         "the batch's SQL changed: review it and list its new digest in operator.env")
        self.assertEqual(self.allow["FLUID_ACQUISITION_DEPLOYMENT_HOST_ALLOWLIST"],
                         streaming.BOOTSTRAP_PLAIN.rsplit(":", 1)[0])
        self.assertEqual(set(self.allow["FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST"].split(",")),
                         {f"{e2e.WAREHOUSE}/{e2e.RAW[0]}", f"{e2e.WAREHOUSE}/{e2e.DAILY[0]}"})


class Product(unittest.TestCase):
    def setUp(self):
        self.contract = load_yaml(REPO_ROOT / e2e.PRODUCT_DIR / e2e.CONTRACT)
        self.builds = {b["id"]: b for b in self.contract["builds"]}
        self.exposes = {x["exposeId"]: x["binding"]["location"] for x in self.contract["exposes"]}

    def test_the_contract_is_what_the_record_reads(self):
        self.assertEqual(self.contract["id"], e2e.PRODUCT_ID)
        stream = self.builds[e2e.STREAM_BUILD]["properties"]
        self.assertEqual(stream["stream"]["topic"], e2e.TOPIC)
        self.assertEqual(stream["parameters"]["spark"]["source"]["bootstrapServers"], streaming.BOOTSTRAP_PLAIN)
        self.assertEqual(stream["parameters"]["spark"]["runtime"], "local")
        self.assertEqual(self.builds[e2e.BATCH_BUILD]["properties"]["parameters"]["spark"]["mode"], "batch")
        raw = self.exposes[self.builds[e2e.STREAM_BUILD]["outputs"][0]]
        daily = self.exposes[self.builds[e2e.BATCH_BUILD]["outputs"][0]]
        self.assertEqual((raw["database"], raw["table"]), e2e.RAW)
        self.assertEqual((daily["database"], daily["table"]), e2e.DAILY)
        self.assertEqual({raw["warehouse"], daily["warehouse"]}, {e2e.WAREHOUSE})
        self.assertIn(f"FROM {e2e.RAW[0]}.{e2e.RAW[1]}", self.builds[e2e.BATCH_BUILD]["properties"]["sql"])

    def test_the_checkpoint_is_where_forge_and_the_build_identity_allow_it(self):
        checkpoint = self.builds[e2e.STREAM_BUILD]["properties"]["stream"]["checkpoint"]
        self.assertTrue(checkpoint.startswith(f"s3://lab-spark-artifacts/_checkpoints/{e2e.PRODUCT_ID}/"), checkpoint)

    def test_env_local_names_a_real_overlay(self):
        overlay = load_yaml(REPO_ROOT / e2e.PRODUCT_DIR / "overlays" / "local.yaml")
        self.assertEqual({b["id"] for b in overlay["builds"]}, set(self.builds))


class Tier(unittest.TestCase):
    def test_the_e2e_tier_stacks_streaming_and_spark_on_core_and_adds_its_objects(self):
        tier = config.resolve_tier(TIERS, "e2e")
        self.assertEqual(TIERS["tiers"]["e2e"]["requires"], ["core", "streaming", "spark"])
        for release in ("lab-e2e", "lab-spark", "strimzi-kafka-operator", "lakekeeper", "seaweedfs"):
            self.assertIn(release, tier.releases)
        self.assertEqual(check.ADDON_RECORDS["e2e"], "lab-e2e")
        self.assertIn("e2e", check.select_records(tier))
        self.assertNotIn("e2e", check.select_records(config.resolve_tier(TIERS, "spark")))

    def test_the_objects_name_the_topic_in_kafka_and_ship_the_read_back_script(self):
        text = (REPO_ROOT / "platform/components/lab-e2e/values.yaml.gotmpl").read_text()
        self.assertIn(f"name: {e2e.TOPIC}", text)
        self.assertIn("namespace: kafka", text)
        self.assertIn('readFile "../../e2e/verify.py"', text)
        self.assertNotIn("{{", (REPO_ROOT / "platform/e2e/verify.py").read_text())   # lab-objects runs tpl on it
        self.assertIn('hasKey $on "lab-e2e"', (REPO_ROOT / "platform/helmfile.yaml.gotmpl").read_text())


class Only(unittest.TestCase):
    def test_only_keeps_the_tiers_order_and_refuses_a_record_the_tier_has_not(self):
        tier = config.resolve_tier(TIERS, "e2e")
        self.assertEqual(check.select_records(tier, ["e2e", "ready"]), ["ready", "e2e"])
        self.assertEqual(check.select_records(tier), check.records_for(tier))
        with self.assertRaisesRegex(ValueError, "not a record of tier e2e"):
            check.select_records(tier, ["orchestration"])
        with self.assertRaisesRegex(ValueError, "not a record of tier core"):
            check.select_records(config.resolve_tier(TIERS, "core"), ["e2e"])


class FakeSteps:
    def __init__(self, fail=None):
        self.fail, self.calls = fail, []

    def _doc(self, what):
        self.calls.append(what)
        ok = what != self.fail
        return {"ok": ok, "checks": [record(f"{what}:x", PASS if ok else FAIL, what)]}

    def mirror(self):
        return self._doc("mirror")

    def up(self):
        return self._doc("up")

    def build(self, image):
        return self._doc(f"build {image}")

    def check(self):
        return self._doc("check")

    def down(self):
        return self._doc("down")


class Command(unittest.TestCase):
    def test_lab_e2e_runs_every_step_in_order(self):
        steps = FakeSteps()
        doc = e2e_command.run(REPO_ROOT, name="ffl-x", steps=steps)
        self.assertTrue(doc["ok"])
        self.assertEqual(steps.calls, ["mirror", "up", "build fluid-runner", "build spark-iceberg", "up", "check"])

    def test_it_stops_at_the_first_failure_and_still_runs_down_when_asked(self):
        steps = FakeSteps(fail="build spark-iceberg")
        doc = e2e_command.run(REPO_ROOT, name="ffl-x", steps=steps, down_after=True)
        self.assertFalse(doc["ok"])
        self.assertEqual(steps.calls, ["mirror", "up", "build fluid-runner", "build spark-iceberg", "down"])

    def test_skip_mirror_and_a_failed_check_fail_the_run(self):
        steps = FakeSteps(fail="check")
        doc = e2e_command.run(REPO_ROOT, name="ffl-x", steps=steps, mirror=False)
        self.assertFalse(doc["ok"])
        self.assertEqual(steps.calls[0], "up")
        self.assertEqual(steps.calls[-1], "check")

    def test_the_check_judges_the_health_records_and_the_run_only(self):
        self.assertEqual(e2e_command.RECORDS, ("ready", "capacity", "runner", "e2e"))
        tier = config.resolve_tier(TIERS, "e2e")
        self.assertEqual(check.select_records(tier, list(e2e_command.RECORDS)), list(e2e_command.RECORDS))


class Stop(unittest.TestCase):
    def test_the_stream_is_stopped_with_forges_own_cancel(self):
        self.assertIn("LocalRuns(LocalHost(Path(work)), product, build)", e2e.STOP)
        self.assertIn("runs.cancel(run_id)", e2e.STOP)
        compile(e2e.STOP, "STOP", "exec")
        compile(e2e.LOG, "LOG", "exec")

    def test_the_stream_is_stopped_even_when_landing_fails(self):
        host = mock.MagicMock()
        host.forge.return_value = proc.Result(["fluid"], 0, "ok", "")
        host.python.return_value = proc.Result(["python3"], 0, 'LAB-E2E-STOP {"active": [], "stopped": ["r1"]}\n', "")
        with mock.patch.object(e2e, "Host", return_value=host), \
             mock.patch.object(e2e, "spark_image", return_value="lab.local/spark-iceberg:x"), \
             mock.patch.object(e2e, "table_state", return_value=None), \
             mock.patch.object(e2e.jobs, "run", side_effect=e2e.E2EError("producer Job could not start")):
            with self.assertRaisesRegex(e2e.E2EError, "producer Job could not start"):
                e2e.run(mock.MagicMock(), REPO_ROOT, LOCK, mock.MagicMock(), REPO_ROOT / "images.json",
                        sleep=lambda _s: None)
        self.assertEqual(host.python.call_args_list[-1].args[0], e2e.STOP)


if __name__ == "__main__":
    unittest.main()
