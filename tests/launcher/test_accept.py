# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab accept`: the status mapping, the comparison in both directions, strict, the argv of every stage,
accept.yaml, secrets, the report, --update-ledger and the cli runner against a fake kube.

Fakes only: no cluster, no kubectl, no container runtime. yq is the real one, as everywhere in the launcher tests.
"""

import base64
import contextlib
import datetime
import io
import json
import shutil
import os
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT

from labctl import EXIT_FAIL, EXIT_USAGE, accept, accept_runner, cli, proc
from labctl.accept import AcceptError, AcceptUsageError, Observed, Product, Step
from labctl.accept_runner import CliRunner

NOW = datetime.datetime(2026, 11, 20, 9, 30, 0, tzinfo=datetime.timezone.utc)
SECRET = "s3cr3t-VALUE-0123456789"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
NAMES = accept.STAGE_NAMES

LEDGER_HEAD = """# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# A comment that must survive --update-ledger.

schemaVersion: 1
profile: lab-kind
tier: orchestration
stagesRun: true
runners: [cli, dag]

products:
  events-pipeline:
    summary: "A raw Kafka stream, a daily Spark SQL batch and an hourly dbt model, in one contract."
    domains: [lab]
    runners:
      cli:
"""


def cell(status, code="test_reason", reason="A reason that is long enough.", extra=""):
    text = f'{{status: {status}, reasonCode: {code}, reason: "{reason}"{extra}}}'
    return text


def evidence(run="acc-earlier-1"):
    return (f', evidence: {{run: {run}, date: "2026-11-01", '
            f"report: conformance/reports/2026-11-01-lab-kind-orchestration-{run}.json}}")


def ledger_text(cli_cells: dict, default=("pending", "not_run_on_lab")) -> str:
    """A whole lab-kind (orchestration) ledger: the given cli cells, the rest pending; dag pending.

    A cell is a status, (status, code) or (status, code, forge reason): the last is written the way
    --update-ledger writes it, with the fingerprint of the reason forge gave.
    """
    out = [LEDGER_HEAD]
    for name in NAMES:
        given = cli_cells.get(name, default)
        status, code, forge = ((given, "test_reason", None) if isinstance(given, str)
                               else (tuple(given) + (None,))[:3])
        extra = evidence() if status == "passed" else ""
        reason = "A reason that is long enough."
        if forge is not None:
            reason = f"Observed earlier: {forge} [fp:{accept.fingerprint(forge)}]"
        out.append(f"        {name}: {cell(status, code, reason=reason, extra=extra)}  # {name}\n")
    out.append("      dag:\n")
    for name in NAMES:
        out.append(f"        {name}: {cell('pending', 'not_run_on_lab')}\n")
    return "".join(out)


def make_root(tmp: Path, cli_cells=None, site_edit=None) -> Path:
    """A repository root with the real profile, product and schemas, and a ledger written for the test."""
    root = tmp / "repo"
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    for sub in ("profiles", "products", "tools/l0"):
        shutil.copytree(REPO_ROOT / sub, root / sub, ignore=ignore)
    for f in ("versions.lock.yaml", "tiers.yaml", "REUSE.toml"):
        shutil.copy(REPO_ROOT / f, root / f)
    (root / "profiles/lab-kind/expected/orchestration.yaml").write_text(
        ledger_text(cli_cells if cli_cells is not None else {}), encoding="utf-8")
    path = root / "profiles/lab-kind/site.yaml"
    text = path.read_text(encoding="utf-8").replace("  default: core\n", "  default: orchestration\n")   # the tier written here
    path.write_text(site_edit(text) if site_edit else text, encoding="utf-8")
    return root


def proc_runner(argv, **kw):
    """The real yq (and anything else), but git answers a fixed commit."""
    if argv[0] == "git":
        return proc.Result(list(argv), 0, COMMIT + "\n" if "rev-parse" in argv else "", "")
    return proc.run(argv, **kw)


class FakeKube:
    """Stands in for labctl.kube.Kube as the cli runner uses it: get, first_pod and exec.

    It models what `fluid ship` leaves behind: a report that merges the records of one run (`run_id` of
    FLUID_RUN_ID), written by each stage. Faults: `no_write` stages exit 0 and write nothing, `wrong_run_id` makes
    every report another run's, `copy_fail` fails the copy into the pod, `live_exit` is the stale-process check.
    """

    def __init__(self, scenario=None, apply_exit=0, no_write=(), wrong_run_id=None, copy_fail=False, live_exit=0):
        self.scenario = scenario or {}        # stage number -> (ship status, reason, exit code)
        self.apply_exit, self.no_write, self.wrong_run_id = apply_exit, set(no_write), wrong_run_id
        self.copy_fail, self.live_exit = copy_fail, live_exit
        self.records: dict = {}
        self.run_id = "?"
        self.calls: list = []                 # (namespace, pod, command, input)
        self.envs: list = []                  # the env assignments of each forge exec
        self.ship_order: list = []
        self.applied: list = []
        self.removed: list = []
        self.copied: bytes = b""
        self.selector = ""

    def get(self, kind, name=None, namespace=None, selector=None):
        self.deployment = (kind, name, namespace)
        return {"spec": {"selector": {"matchLabels": {"app.kubernetes.io/name": "fluid-host", "tier": "spark"}}}}

    def first_pod(self, namespace, selector, timeout_s=300):
        self.selector = selector
        return "fluid-host-7d9"

    def exec(self, namespace, pod, command, *, container=None, input=None, timeout=120):  # noqa: A002
        self.calls.append((namespace, pod, list(command), input))
        if command == ["fluid", "--version"]:
            return proc.Result(command, 0, "fluid 0.7.7 (cc9044c2)\n", "")
        if command[:3] == ["rm", "-rf", "--"]:
            self.removed.append(command[3])
            return proc.Result(command, 0, "", "")
        script = command[2]
        if script == accept_runner.LIVE_CHECK:
            return proc.Result(command, self.live_exit, "4242\n" if self.live_exit else "", "")
        if script.startswith("mkdir"):
            if self.copy_fail:
                return proc.Result(command, 1, "", "tar: no space left on device")
            self.copied = base64.b64decode(input)
            return proc.Result(command, 0, "", "")
        if script == accept_runner.FORGE_WRAPPER:
            argv = command[6:]
            assert command[5].isdigit(), "the forge wrapper takes a timeout in seconds"
            i = 1
            env = {}
            while "=" in argv[i]:
                key, _, value = argv[i].partition("=")
                env[key], i = value, i + 1
            self.envs.append(env)
            self.run_id = env.get("FLUID_RUN_ID", "?")
            argv = argv[i:]
        else:
            assert script == accept_runner.CD_WRAPPER, script
            argv = command[5:]
        if argv[:2] == ["fluid", "ship"]:
            n = int(argv[argv.index("--stage") + 1])
            self.ship_order.append(n)
            status, reason, code = self.scenario.get(n, ("passed", None, 0))
            if n not in self.no_write:
                self.records[n] = {"number": n, "name": NAMES[n - 1], "status": status,
                                   "skip_reason": reason if status in ("skipped", "not-run") else None,
                                   "failure_reason": reason if status == "failed" else None, "argv": [],
                                   "exit_code": code, "duration_seconds": 0.1}
            return proc.Result(command, code, "stage output\n", "")
        if argv[:2] == ["fluid", "apply"]:
            self.applied.append(argv[argv.index("--build-id") + 1])
            return proc.Result(command, self.apply_exit, "", "apply failed: boom" if self.apply_exit else "")
        if argv == ["cat", "runtime/ship-report.json"]:
            stages = [self.records[k] for k in sorted(self.records)]
            return proc.Result(command, 0, json.dumps({"run_id": self.wrong_run_id or self.run_id, "stages": stages}), "")
        return proc.Result(command, 127, "", f"unexpected {argv}")


class FakeCatalog:
    def __init__(self, appears_after=0):
        self.polls: list = []
        self.appears_after = appears_after
        self.closed = False

    def table_exists(self, warehouse, namespace, table):
        self.polls.append((warehouse, namespace, table))
        return len(self.polls) > self.appears_after

    def close(self):
        self.closed = True


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


def factory_for(kube, catalog=None, allow=None, keep=False):
    clock = Clock()

    def factory(run):
        return CliRunner(kube, run.site, env=run.env, run_id=run.run_id,
                         allowlists=accept.parse_operator_env(run.site.directory / "operator" / f"{run.env}.env")
                         if allow is None else allow,
                         catalog=catalog or FakeCatalog(), clock=clock, sleep=clock.sleep, keep=keep)

    return factory


def run_accept(root, kube, *, catalog=None, allow=None, env_vars=None, update=False, run_id="acc-test-1", **kw):
    return accept.run_accept(root, runner_factory=factory_for(kube, catalog, allow, kw.get("keep", False)),
                             env_vars=env_vars if env_vars is not None else {}, proc_runner=proc_runner,
                             now=lambda: NOW, run_id=run_id, do_update_ledger=update, **kw)


# --------------------------------------------------------------------------- mapping


class StatusMapping(unittest.TestCase):
    def test_every_ship_status_has_one_explicit_ledger_status(self):
        table = [
            (("passed", None), "passed"),
            (("failed", None), "failed"),
            (("failed", "exit code 2"), "failed"),
            (("skipped", "not supported on this platform"), "unsupported"),
            (("skipped", "the target is unsupported"), "unsupported"),
            (("skipped", "not available on this platform"), "unsupported"),
            (("skipped", "SCHEDULER is blank: no scheduler to sync to"), "not_checked"),
            (("skipped", "dist/artifacts/policy/bindings.json does not exist (no policy bindings to apply)"), "not_checked"),
            (("skipped", None), "not_checked"),
            (("not-run", "not run"), "pending"),
            (("not_run", None), "pending"),
        ]
        for (ship, reason), want in table:
            with self.subTest(ship=ship, reason=reason):
                self.assertEqual(accept.map_status(ship, reason), want)

    def test_a_status_forge_does_not_write_is_refused_not_guessed(self):
        for bad in ("", "ok", "PASSED", "waived", "error"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                accept.map_status(bad)

    def test_the_mapping_only_produces_ledger_words(self):
        for ship in ("passed", "failed", "skipped", "not-run"):
            self.assertIn(accept.map_status(ship, "x"), accept.LEDGER_STATUSES)


# --------------------------------------------------------------------------- comparison


def exp(**kw):
    return {name: {"status": s, "reasonCode": "r_code", "reason": "A ledger reason that quotes boom and schema error."}
            for name, s in kw.items()}


class Comparison(unittest.TestCase):
    def test_a_run_that_equals_the_ledger_has_no_mismatch(self):
        cells, bad = accept.compare("p", "cli", exp(bundle="passed", validate="failed"),
                                    {"bundle": Observed("passed"), "validate": Observed("failed", "boom")})
        self.assertEqual(bad, [])
        self.assertTrue(all(c["match"] for c in cells.values()))

    def test_an_unexpected_pass_fails_the_run_like_an_unexpected_failure(self):
        _, bad = accept.compare("events-pipeline", "cli", exp(validate="failed"),
                                {"validate": Observed("passed", "validation passed")})
        self.assertEqual(bad, ["events-pipeline/cli/validate: expected failed, observed passed (validation passed)"])
        _, bad = accept.compare("events-pipeline", "cli", exp(validate="passed"),
                                {"validate": Observed("failed", "schema error")})
        self.assertEqual(bad, ["events-pipeline/cli/validate: expected passed, observed failed (schema error)"])

    def test_every_other_status_difference_is_a_mismatch_too(self):
        for want, got in (("pending", "passed"), ("unsupported", "not_checked"), ("not_checked", "unsupported"),
                          ("passed", "pending")):
            with self.subTest(want=want, got=got):
                self.assertEqual(len(accept.compare("p", "cli", exp(plan=want), {"plan": Observed(got)})[1]), 1)

    def test_a_cell_on_one_side_only_is_a_mismatch(self):
        _, bad = accept.compare("p", "cli", exp(bundle="passed", plan="pending"), {"bundle": Observed("passed")})
        self.assertEqual(bad, ["p/cli/plan: expected pending, observed (nothing) (no result)"])
        _, bad = accept.compare("p", "cli", exp(bundle="passed"),
                                {"bundle": Observed("passed"), "extra-stage": Observed("passed", "ran")})
        self.assertEqual(bad, ["p/cli/extra-stage: expected (no cell), observed passed (ran)"])

    def test_a_stage_the_harness_could_not_observe_never_matches(self):
        _, bad = accept.compare("p", "cli", exp(validate="failed"), {"validate": accept.error_cell("no ship report")})
        self.assertEqual(len(bad), 1)
        self.assertIn("not observed", bad[0])


# --------------------------------------------------------------------------- strict


class Strict(unittest.TestCase):
    def test_a_false_strict_variable_stops_the_run_before_anything_is_read(self):
        for env in ({"VALIDATE_STRICT": "false"}, {"VERIFY_STRICT": "0"}, {"VALIDATE_STRICT": "FALSE"},
                    {"VERIFY_STRICT": "off"}):
            with self.subTest(env=env), tempfile.TemporaryDirectory() as tmp:
                kube = FakeKube()
                with self.assertRaises(AcceptError) as ctx:
                    run_accept(make_root(Path(tmp)), kube, env_vars=env)
                self.assertIn("always runs strict", str(ctx.exception))
                self.assertEqual(kube.calls, [])

    def test_true_or_unset_strict_variables_are_fine(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_accept(make_root(Path(tmp)), FakeKube(), env_vars={"VALIDATE_STRICT": "true", "VERIFY_STRICT": ""})

    def test_the_operator_file_cannot_weaken_strict_either(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "local.env"
            path.write_text("VALIDATE_STRICT=false\n")
            with self.assertRaises(AcceptError):
                accept.parse_operator_env(path)

    def test_no_strict_flag_can_reach_an_argv(self):
        for flag in accept.NO_STRICT_FLAGS:
            with self.assertRaises(AcceptError):
                accept.strict_guard(["fluid", "ship", flag])
        for n in range(1, 12):
            argv = accept_runner.ship_argv(n, SITE_WITH_ALL, "local")
            self.assertIn("--strict", argv)
            self.assertIn("--verify-strict", argv)
            self.assertFalse([a for a in argv if a.startswith("--no-")])

    def test_every_exec_pins_strict_on_in_the_environment_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube()
            run_accept(make_root(Path(tmp)), kube)
        seen = 0
        for _, _, command, _ in kube.calls:
            if command[2:3] == [accept_runner.FORGE_WRAPPER]:
                self.assertIn("VALIDATE_STRICT=true", command)
                self.assertIn("VERIFY_STRICT=true", command)
                seen += 1
        self.assertEqual(seen, 12)      # stages 1-9 and 11, and the two apply_build steps


# --------------------------------------------------------------------------- argv


def site(publish="fluid-command-center", scheduler=("airflow", "s3://lab-airflow/dags")):
    return accept.Site("p", "t", Path("."), Path("."), "spark", "fluid-host", publish, scheduler[0] if scheduler else None,
                       scheduler[1] if scheduler else None)


SITE_WITH_ALL = site()


class StageArgv(unittest.TestCase):
    def test_each_stage_is_one_ship_stage(self):
        for n in range(1, 12):
            argv = accept_runner.ship_argv(n, SITE_WITH_ALL, "local")
            self.assertEqual(argv[:6], ["fluid", "ship", "contract.fluid.yaml", "--env", "local", "--strict"])
            self.assertEqual(argv[argv.index("--stage") + 1], str(n))

    def test_flags_on_stage_7_10_and_11_only(self):
        plain = accept_runner.ship_argv(8, SITE_WITH_ALL, "local")
        self.assertEqual(plain[-2:], ["--stage", "8"])
        self.assertEqual(accept_runner.ship_argv(7, SITE_WITH_ALL, "local")[-1], "--yes")
        self.assertEqual(accept_runner.ship_argv(10, SITE_WITH_ALL, "local")[-3:],
                         ["--publish", "--target", "fluid-command-center"])
        self.assertEqual(accept_runner.ship_argv(11, SITE_WITH_ALL, "local")[-5:],
                         ["--schedule-sync", "--scheduler", "airflow", "--scheduler-destination", "s3://lab-airflow/dags"])
        for n in (1, 2, 3, 4, 5, 6, 8, 9):
            argv = accept_runner.ship_argv(n, SITE_WITH_ALL, "local")
            self.assertFalse({"--yes", "--publish", "--schedule-sync"} & set(argv), n)

    def test_the_apply_build_command_is_the_dags_task(self):
        self.assertEqual(accept_runner.apply_build_argv("daily-curated", "local"),
                         ["fluid", "apply", "contract.fluid.yaml", "--env", "local", "--mode", "amend-and-build",
                          "--build-id", "daily-curated", "--yes"])

    def test_the_environment_is_the_run_the_env_and_the_operators_allowlists_only(self):
        prefix = accept_runner.env_prefix("local", "acc-1", {"FLUID_B_ALLOWLIST": "b", "FLUID_A_ALLOWLIST": "a"})
        self.assertEqual(prefix, ["env", "FLUID_ENV=local", "FLUID_RUN_ID=acc-1", "VALIDATE_STRICT=true",
                                  "VERIFY_STRICT=true", "FLUID_A_ALLOWLIST=a", "FLUID_B_ALLOWLIST=b"])

    def test_a_site_without_a_publish_target_or_scheduler_skips_stages_10_and_11(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube()
            root = make_root(Path(tmp), site_edit=lambda t: t.replace(
                "  scheduler:\n    kind: airflow\n    dagsDestination: s3://lab-airflow/dags\n", ""))
            doc = run_accept(root, kube)
            report = json.loads((root / doc["report"]).read_text())
        self.assertEqual(kube.ship_order, list(range(1, 10)))
        for stage in ("publish", "schedule-sync"):
            obs = report["products"]["events-pipeline"]["cli"]["cells"][stage]["observed"]
            self.assertEqual((obs["status"], obs["reasonCode"]), ("pending", "addon_absent"))
        notes = " ".join(report["notes"])
        self.assertIn("stage 10 publish: the site has no publish target", notes)
        self.assertIn("stage 11 schedule-sync: the site has no scheduler", notes)

    def test_the_real_lab_kind_site_has_no_publish_target_but_a_scheduler(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube()
            run_accept(make_root(Path(tmp)), kube)
        self.assertNotIn(10, kube.ship_order)
        self.assertIn(11, kube.ship_order)
        sync = [c[2] for c in kube.calls if "--stage" in c[2] and c[2][c[2].index("--stage") + 1] == "11"][0]
        self.assertIn("--scheduler-destination", sync)
        self.assertIn("s3://lab-airflow/dags", sync)


# --------------------------------------------------------------------------- accept.yaml

BUILDS = ["stream-raw", "daily-curated", "hourly-marts"]
GOOD_WAIT = {"warehouse": "sales-wh", "namespace": "events_curated", "table": "events_daily", "timeout_s": 600}


class AcceptYaml(unittest.TestCase):
    def bad(self, doc, said):
        with self.assertRaises(AcceptError) as ctx:
            accept.parse_accept_yaml(doc, BUILDS)
        self.assertIn(said, str(ctx.exception))

    def test_the_products_own_file_is_valid_and_ordered(self):
        steps = accept.load_products(REPO_ROOT, ["events-pipeline"])[0].steps
        self.assertEqual([s.label() for s in steps], [
            "apply_build daily-curated", "wait_table sales-wh/events_curated.events_daily",
            "apply_build hourly-marts", "wait_table sales-wh/events_marts.events_hourly"])

    def test_every_table_the_file_waits_for_is_an_expose_of_the_contract(self):
        from labctl import config

        contract = config.load_yaml(REPO_ROOT / "products/events-pipeline/contract.fluid.yaml")
        tables = {(e["binding"]["location"]["warehouse"], e["binding"]["location"]["database"],
                   e["binding"]["location"]["table"]) for e in contract["exposes"]}
        for s in accept.load_products(REPO_ROOT, ["events-pipeline"])[0].steps:
            if s.kind == "wait_table":
                self.assertIn((s.args["warehouse"], s.args["namespace"], s.args["table"]), tables)

    def test_the_shape_is_checked_strictly(self):
        step = {"apply_build": "daily-curated"}
        self.bad([], "must be a mapping")
        self.bad({"steps": [step]}, "schemaVersion must be 1")
        self.bad({"schemaVersion": 2, "steps": [step]}, "schemaVersion must be 1")
        self.bad({"schemaVersion": True, "steps": [step]}, "schemaVersion must be 1")
        self.bad({"schemaVersion": 1}, "non-empty list")
        self.bad({"schemaVersion": 1, "steps": []}, "non-empty list")
        self.bad({"schemaVersion": 1, "steps": [step], "after": []}, "unknown key 'after'")
        self.bad({"schemaVersion": 1, "steps": ["apply_build"]}, "exactly one key")
        self.bad({"schemaVersion": 1, "steps": [{"apply_build": "daily-curated", "wait_table": {}}]}, "exactly one key")
        self.bad({"schemaVersion": 1, "steps": [{"sleep": 5}]}, "unknown step 'sleep'")

    def test_apply_build_names_a_build_of_the_contract(self):
        self.bad({"schemaVersion": 1, "steps": [{"apply_build": "nope"}]}, "is not a build of the contract")
        self.bad({"schemaVersion": 1, "steps": [{"apply_build": 5}]}, "takes a build id")
        self.bad({"schemaVersion": 1, "steps": [{"apply_build": "../x"}]}, "takes a build id")

    def test_wait_table_needs_exactly_its_four_keys_with_sane_values(self):
        def wait(**kw):
            return {"schemaVersion": 1, "steps": [{"wait_table": {**GOOD_WAIT, **kw}}]}

        self.assertEqual(accept.parse_accept_yaml(wait(), BUILDS)[0].args, GOOD_WAIT)
        self.bad({"schemaVersion": 1, "steps": [{"wait_table": "t"}]}, "takes a mapping")
        self.bad({"schemaVersion": 1, "steps": [{"wait_table": {k: v for k, v in GOOD_WAIT.items() if k != "table"}}]},
                 "is missing table")
        self.bad(wait(extra=1), "unknown key 'extra'")
        self.bad(wait(timeout_s=0), "timeout_s")
        self.bad(wait(timeout_s=3601), "timeout_s")
        self.bad(wait(timeout_s=True), "timeout_s")
        self.bad(wait(timeout_s="600"), "timeout_s")
        self.bad(wait(table="a b"), "is not a name")
        self.bad(wait(namespace="x/y"), "is not a name")

    def test_kafka_produce_waits_for_the_streaming_tier(self):
        self.bad({"schemaVersion": 1, "steps": [{"kafka_produce": {"topic": "events.source"}}]}, "not available yet")


# --------------------------------------------------------------------------- secrets and redaction


class Secrets(unittest.TestCase):
    def test_redact_removes_what_looks_like_a_secret(self):
        jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl"
        for text in (f"password={SECRET}", f"client_secret: {SECRET}", f"Authorization: Bearer {SECRET}",
                     f"token={SECRET} done", jwt, "AK" + "IAABCDEFGHIJKLMNOP", "LABABCDEFGHIJKLMNOPQ",
                     f"https://user:{SECRET}@host/path", "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
                     f"LAKEKEEPER_CLIENT_SECRET='{SECRET}'"):
            with self.subTest(text=text[:30]):
                out = accept.redact(text)
                self.assertNotIn(SECRET, out)
                self.assertNotIn("eyJzdWIi", out)
                self.assertNotIn("AK" + "IAABCDEFGHIJKLMNOP", out)
                self.assertNotIn("LABABCDEFGHIJKLMNOPQ", out)
                self.assertNotIn("abc\n", out)
                self.assertIn("***", out)

    def test_ordinary_text_is_left_alone(self):
        text = "stage 7 apply failed: exit code 1 at lab.events-pipeline/daily-curated@sha256:66e296"
        self.assertEqual(accept.redact(text), text)

    def test_the_operator_file_holds_allowlists_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "local.env"
            for body, said in ((f"AWS_SECRET_ACCESS_KEY={SECRET}\n", "only FLUID_*_ALLOWLIST"),
                               (f"FLUID_TOKEN={SECRET}\n", "only FLUID_*_ALLOWLIST"),
                               ("FLUID_X_ALLOWLIST=a b\n", "one value"), ("FLUID_X_ALLOWLIST=\n", "one value"),
                               ("just text\n", "only FLUID_*_ALLOWLIST")):
                path.write_text(body)
                with self.subTest(body=body), self.assertRaises(AcceptError) as ctx:
                    accept.parse_operator_env(path)
                self.assertIn(said, str(ctx.exception))
                self.assertNotIn(SECRET, str(ctx.exception))

    def test_the_real_operator_file_parses_to_its_allowlists(self):
        env = accept.parse_operator_env(REPO_ROOT / "profiles/lab-kind/operator/local.env")
        self.assertEqual(sorted(env), ["FLUID_ACQUISITION_DEPLOYMENT_HOST_ALLOWLIST",
                                       "FLUID_LAKEKEEPER_NAMESPACE_ALLOWLIST", "FLUID_SPARK_CONTRACT_SQL_ALLOWLIST"])
        self.assertEqual(env["FLUID_LAKEKEEPER_NAMESPACE_ALLOWLIST"],
                         "sales-wh/events_raw,sales-wh/events_curated,sales-wh/events_marts")

    def test_no_secret_reaches_an_argv_or_a_report(self):
        launcher_env = {"SOME_API_TOKEN": SECRET, "AWS_SECRET_ACCESS_KEY": SECRET, "VALIDATE_STRICT": "true"}
        scenario = {2: ("failed", f"validation failed with token={SECRET} and Bearer {SECRET}", 1)}
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube(scenario)
            root = make_root(Path(tmp))
            doc = run_accept(root, kube, env_vars=launcher_env)
            written = [p.read_text() for p in (root / "conformance/reports").iterdir()]
        self.assertEqual(len(written), 2)
        self.assertNotIn(SECRET, json.dumps(kube.calls))
        self.assertNotIn(SECRET, json.dumps(doc))
        for text in written:
            self.assertNotIn(SECRET, text)
        self.assertIn("token=***", written[0])


# --------------------------------------------------------------------------- the run, the cli runner, the report


class Run(unittest.TestCase):
    def run_in(self, scenario=None, cli_cells=None, **kw):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        kube = FakeKube(scenario)
        self.kube = kube
        self.root = make_root(Path(tmp.name), cli_cells)
        return run_accept(self.root, kube, **kw)

    def test_a_run_equal_to_the_ledger_exits_clean_and_runs_the_stages_in_order_with_the_steps_after_apply(self):
        cells = {n: ("passed", "stage_passed") for n in NAMES if n != "publish"}
        cells["publish"] = ("pending", "addon_absent")
        catalog = FakeCatalog(appears_after=2)
        doc = self.run_in(cli_cells=cells, catalog=catalog)
        self.assertTrue(doc["ok"], doc["mismatches"])
        self.assertEqual(self.kube.ship_order, [1, 2, 3, 4, 5, 6, 7, 8, 9, 11])
        self.assertEqual(self.kube.applied, ["daily-curated", "hourly-marts"])
        # the order of everything that touched forge: 1-7, the first apply_build, 8..., exact interleaving
        seq = []
        for _, _, command, _ in self.kube.calls:
            if command[2:3] != [accept_runner.FORGE_WRAPPER]:
                continue
            argv = command[6:]
            argv = argv[next(i for i, a in enumerate(argv) if "=" not in a and a != "env"):]
            if "ship" in argv:
                seq.append(int(argv[argv.index("--stage") + 1]))
            elif "apply" in argv:
                seq.append("apply:" + argv[argv.index("--build-id") + 1])
        self.assertEqual(seq, [1, 2, 3, 4, 5, 6, 7, "apply:daily-curated", "apply:hourly-marts", 8, 9, 11])
        self.assertEqual(catalog.polls[:3], [("sales-wh", "events_curated", "events_daily")] * 3)
        self.assertIn(("sales-wh", "events_marts", "events_hourly"), catalog.polls)
        self.assertTrue(catalog.closed)

    def test_the_first_failed_stage_stops_the_run_and_the_rest_is_pending(self):
        cells = {"bundle": ("passed", "stage_ok"),
                 "validate": ("failed", "forge_runtime_local_unreleased", "schema: runtime local is not allowed")}
        doc = self.run_in({2: ("failed", "schema: runtime local is not allowed", 1)}, cells)
        self.assertTrue(doc["ok"], doc["mismatches"])
        self.assertEqual(self.kube.ship_order, [1, 2])
        report = json.loads((self.root / doc["report"]).read_text())
        later = report["products"]["events-pipeline"]["cli"]["cells"]["generate-artifacts"]["observed"]
        self.assertEqual((later["status"], later["reasonCode"]), ("pending", "earlier_stage_failed"))
        self.assertIn("stage 2 validate failed", later["reason"])

    def test_an_unexpected_pass_and_an_unexpected_failure_both_fail_the_run(self):
        doc = self.run_in(cli_cells={"bundle": "passed", "validate": "failed"})
        self.assertFalse(doc["ok"])
        self.assertIn("events-pipeline/cli/validate: expected failed, observed passed (", " ".join(doc["mismatches"]))
        doc = self.run_in({1: ("failed", "no contract", 2)}, {"bundle": "passed"})
        self.assertFalse(doc["ok"])
        self.assertIn("events-pipeline/cli/bundle: expected passed, observed failed (no contract)", doc["mismatches"])

    def test_a_skip_is_not_a_pass_and_is_mapped_with_forges_reason(self):
        scenario = {8: ("skipped", "dist/artifacts/policy/bindings.json does not exist (no policy bindings to apply)", 0)}
        doc = self.run_in(scenario, {"bundle": "passed", "policy-apply": "passed"})
        self.assertIn("events-pipeline/cli/policy-apply: expected passed, observed not_checked (dist/artifacts/policy",
                      " ".join(doc["mismatches"]))

    def test_a_missing_ship_record_is_an_error_not_a_matching_failure(self):
        doc = self.run_in(cli_cells={"bundle": "failed"})
        self.assertFalse(doc["ok"])
        # now make ship leave no record while the ledger expects failed
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        kube = FakeKube()
        kube.records = {}
        original = kube.exec

        def no_record(namespace, pod, command, **kw):
            res = original(namespace, pod, command, **kw)
            if command[-2:] == ["cat", "runtime/ship-report.json"]:
                return proc.Result(command, 1, "", "cat: no such file")
            return res

        kube.exec = no_record
        root = make_root(Path(tmp.name), {"bundle": "failed"})
        doc = run_accept(root, kube)
        self.assertFalse(doc["ok"])
        self.assertIn("error, not observed", " ".join(doc["mismatches"]))

    def test_a_failed_step_fails_the_run_even_when_the_ledger_says_pending(self):
        cells = {n: "passed" for n in NAMES[:7]}
        doc = self.run_in(cli_cells=cells, catalog=FakeCatalog(appears_after=10 ** 9))
        self.assertFalse(doc["ok"])
        self.assertEqual(self.kube.ship_order, [1, 2, 3, 4, 5, 6, 7])
        report = json.loads((self.root / doc["report"]).read_text())
        self.assertTrue(any("wait_table sales-wh/events_curated.events_daily" in e for e in report["errors"]))
        self.assertEqual(self.kube.applied, ["daily-curated"])

    def test_a_failed_apply_build_stops_the_run(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        kube = FakeKube(apply_exit=1)
        root = make_root(Path(tmp.name), {n: "passed" for n in NAMES[:7]})
        doc = run_accept(root, kube)
        self.assertFalse(doc["ok"])
        self.assertEqual(kube.ship_order, [1, 2, 3, 4, 5, 6, 7])
        self.assertIn("apply_build daily-curated", " ".join(json.loads((root / doc["report"]).read_text())["errors"]))

    def test_the_dag_runner_is_not_compared_and_the_report_says_so(self):
        doc = self.run_in()
        report = json.loads((self.root / doc["report"]).read_text())
        self.assertEqual(report["runners"], ["cli"])
        self.assertEqual(list(report["products"]["events-pipeline"]), ["cli"])
        self.assertIn("runner dag was not run", " ".join(report["notes"]))

    def test_the_report_names_the_run_forge_the_commit_the_argv_and_the_comparison(self):
        doc = self.run_in({2: ("failed", "boom", 1)}, {"bundle": "passed", "validate": ("failed", "test_reason", "boom")})
        self.assertEqual(doc["report"], "conformance/reports/2026-11-20-lab-kind-orchestration-acc-test-1.json")
        self.assertEqual(doc["report_md"], "conformance/reports/2026-11-20-lab-kind-orchestration-acc-test-1.md")
        report = json.loads((self.root / doc["report"]).read_text())
        self.assertEqual((report["runId"], report["date"], report["strict"]), ("acc-test-1", "2026-11-20", True))
        self.assertEqual(report["forge"]["fluidVersion"]["cli"], "fluid 0.7.7 (cc9044c2)")
        self.assertEqual(report["lab"], {"commit": COMMIT, "dirty": False})
        cell = report["products"]["events-pipeline"]["cli"]["cells"]["validate"]
        self.assertEqual(cell["expected"]["status"], "failed")
        self.assertEqual(cell["observed"]["reason"], "boom")
        self.assertTrue(cell["match"], report["mismatches"])
        argv = cell["observed"]["argv"]
        self.assertEqual(argv[:4], ["env", "FLUID_ENV=local", "FLUID_RUN_ID=acc-test-1", "VALIDATE_STRICT=true"])
        self.assertIn("--stage", argv)
        md = (self.root / doc["report_md"]).read_text()
        for needle in ("acc-test-1", "fluid 0.7.7 (cc9044c2)", COMMIT, "| validate | failed | failed | yes | boom |",
                       "Exact argv per stage"):
            self.assertIn(needle, md)

    def test_a_report_is_never_overwritten(self):
        self.run_in(run_id="acc-same-1")
        with self.assertRaises(AcceptError) as ctx:
            run_accept(self.root, FakeKube(), run_id="acc-same-1")
        self.assertIn("never overwritten", str(ctx.exception))

    def test_a_request_accept_does_not_offer_is_refused_before_anything_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, kube = make_root(Path(tmp)), FakeKube()
            for runner, said in (("dag", "orchestration tier"), ("ci", "unknown runner"), ("bogus", "unknown runner")):
                with self.subTest(runner), self.assertRaises(AcceptUsageError) as ctx:
                    run_accept(root, kube, runner=runner)
                self.assertIn(said, str(ctx.exception))
            with self.assertRaises(AcceptUsageError):
                run_accept(root, kube, products=["nope"])
            with self.assertRaises(AcceptUsageError):
                run_accept(root, kube, profile="nope")
            with self.assertRaises(AcceptUsageError):
                run_accept(root, kube, tier="spark")
            self.assertEqual(kube.calls, [])


class CliRunnerBehaviour(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = make_root(Path(tmp.name))
        self.kube = FakeKube()
        self.site = accept.load_site(self.root, "lab-kind", None, proc_runner)
        self.product = accept.load_products(self.root, ["events-pipeline"], proc_runner)[0]
        self.runner = CliRunner(self.kube, self.site, env="local", run_id="acc-r-1", allowlists={}, catalog=FakeCatalog())

    def test_the_pod_comes_from_the_site_hosts_deployment_selector(self):
        self.runner.prepare(self.product)
        self.assertEqual(self.kube.deployment, ("deployment", "fluid-host", "spark"))
        self.assertEqual(self.kube.selector, "app.kubernetes.io/name=fluid-host,tier=spark")
        self.assertEqual({c[:2] for c in self.kube.calls}, {("spark", "fluid-host-7d9")})

    def test_the_product_is_copied_as_a_tar_stream_on_stdin_under_the_run_id(self):
        self.runner.prepare(self.product)
        copy = self.kube.calls[1]                                # [0] is the check for a live run of the product
        self.assertEqual(copy[2][-2:], ["sh", "/work/accept/acc-r-1/events-pipeline"])
        self.assertIn("base64 -d | tar -xf -", copy[2][2])
        names = tarfile.open(fileobj=io.BytesIO(self.kube.copied)).getnames()
        for want in ("contract.fluid.yaml", "accept.yaml", "overlays/local.yaml", "dbt/models/marts/events_hourly.sql"):
            self.assertIn(want, names)

    def test_forges_own_output_and_vcs_files_are_not_copied(self):
        src = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, src)
        for rel in ("contract.fluid.yaml", "runtime/ship-report.json", "dist/artifacts/x.json", ".git/HEAD",
                    ".fluid/run-id.txt", "dbt/__pycache__/m.pyc", ".DS_Store", "dbt/model.sql"):
            (src / rel).parent.mkdir(parents=True, exist_ok=True)
            (src / rel).write_text("x")
        names = tarfile.open(fileobj=io.BytesIO(accept_runner.product_tar(src))).getnames()
        self.assertEqual(sorted(n for n in names if "." in n.split("/")[-1]), ["contract.fluid.yaml", "dbt/model.sql"])
        self.assertEqual(accept_runner.product_tar(src), accept_runner.product_tar(src))

    def test_a_symbolic_link_in_a_product_is_refused(self):
        src = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, src)
        (src / "link").symlink_to("/etc/passwd")
        with self.assertRaises(AcceptError):
            accept_runner.product_tar(src)

    def test_each_stage_is_its_own_exec_in_the_workdir_with_the_report_read_back(self):
        self.runner.stage(self.product, 4)
        ship, cat = self.kube.calls[-2][2], self.kube.calls[-1][2]
        self.assertEqual(ship[:6], ["sh", "-c", accept_runner.FORGE_WRAPPER, "sh",
                                    "/work/accept/acc-r-1/events-pipeline", "1770"])        # 1800 s minus the margin
        self.assertEqual(ship[6:13], ["env", "FLUID_ENV=local", "FLUID_RUN_ID=acc-r-1", "VALIDATE_STRICT=true",
                                     "VERIFY_STRICT=true", "fluid", "ship"])
        self.assertEqual(cat[:5], ["sh", "-c", accept_runner.CD_WRAPPER, "sh", "/work/accept/acc-r-1/events-pipeline"])
        self.assertEqual(cat[5:], ["cat", "runtime/ship-report.json"])

    def test_the_version_is_what_fluid_in_the_pod_says(self):
        self.assertEqual(self.runner.version(), "fluid 0.7.7 (cc9044c2)")

    def test_wait_table_polls_with_the_injected_clock_and_gives_up_at_its_timeout(self):
        clock = Clock()
        catalog = FakeCatalog(appears_after=10 ** 9)
        runner = CliRunner(self.kube, self.site, env="local", run_id="acc-r-1", allowlists={}, catalog=catalog,
                           clock=clock, sleep=clock.sleep)
        res = runner.step(self.product, Step("wait_table", dict(GOOD_WAIT, timeout_s=20)))
        self.assertFalse(res.ok)
        self.assertEqual(len(catalog.polls), 5)
        self.assertIn("not there after 20 s", res.summary)
        ok = CliRunner(self.kube, self.site, env="local", run_id="acc-r-2", allowlists={}, catalog=FakeCatalog(),
                       clock=clock, sleep=clock.sleep).step(self.product, Step("wait_table", GOOD_WAIT))
        self.assertTrue(ok.ok)

    def test_the_lab_catalog_asks_lakekeeper_for_the_table_through_the_lab_client(self):
        class Resp:
            def __init__(self, status, body=None):
                self.status, self._body = status, body or {}

            def json(self):
                return self._body

            def text(self, n=0):
                return "x"

        class Client:
            def __init__(self):
                self.paths = []

            def token(self, name):
                return "tok-" + name

            def https(self, method, host, path, **kw):
                self.paths.append((method, host, path, kw.get("headers")))
                if "/config" in path:
                    return Resp(200, {"defaults": {"prefix": "wh-uuid"}})
                return Resp(200 if path.endswith("events_daily") else 404)

        catalog = accept_runner.LabCatalog(None, None, None)
        catalog.client = Client()
        self.assertTrue(catalog.table_exists("sales-wh", "events_curated", "events_daily"))
        self.assertFalse(catalog.table_exists("sales-wh", "events_marts", "events_hourly"))
        self.assertEqual(catalog.client.paths[0][2], "/catalog/v1/config?warehouse=sales-wh")
        self.assertEqual(catalog.client.paths[1][2], "/catalog/v1/wh-uuid/namespaces/events_curated/tables/events_daily")
        self.assertEqual(catalog.client.paths[1][3], {"Authorization": "Bearer tok-lab-admin"})


class ObserveStage(unittest.TestCase):
    def report(self, **rec):
        base = {"number": 2, "name": "validate", "status": "passed", "skip_reason": None, "failure_reason": None,
                "exit_code": 0}
        return json.dumps({"stages": [{**base, **rec}]})

    def observe(self, report, exit_code=0, **kw):
        return accept_runner.observe_stage(2, exit_code, "tail", report, ["fluid"], **kw)

    def with_run(self, run_id="acc-1", **rec):
        doc = json.loads(self.report(**rec))
        doc["run_id"] = run_id
        return json.dumps(doc)

    def test_a_record_becomes_its_ledger_status_with_forges_reason(self):
        self.assertEqual(self.observe(self.report()).status, "passed")
        failed = self.observe(self.report(status="failed", failure_reason="schema invalid", exit_code=1), 1)
        self.assertEqual((failed.status, failed.reason, failed.error), ("failed", "schema invalid", False))
        skipped = self.observe(self.report(status="skipped", skip_reason="not supported here"))
        self.assertEqual((skipped.status, skipped.reason), ("unsupported", "not supported here"))
        self.assertEqual(self.observe(self.report(), run_id=None).status, "passed")

    def test_this_runs_report_is_read(self):
        self.assertEqual(self.observe(self.with_run("acc-1"), run_id="acc-1").status, "passed")

    def test_what_cannot_be_judged_is_an_error_cell(self):
        cases = {
            "no report": self.observe(None, 1),
            "garbled report": self.observe("{not json", 1),
            "no record": accept_runner.observe_stage(3, 0, "", self.report(), []),
            "wrong name": self.observe(self.report(name="plan")),
            "unknown status": self.observe(self.report(status="weird")),
            "passed but exit 1": self.observe(self.report(), 1),
            "not-run with exit 0": self.observe(self.report(status="not-run", skip_reason="RUN_STAGE_2_VALIDATE=false")),
            "not_run with exit 0": self.observe(self.report(status="not_run")),
            "another run's report": self.observe(self.with_run("acc-other-9"), run_id="acc-1"),
            "a report with no run id": self.observe(self.report(), run_id="acc-1"),
            "timeout exit code": self.observe(self.report(), 124),
            "timeout": self.observe(self.report(), 124, timed_out=True),
        }
        for what, obs in cases.items():
            with self.subTest(what):
                self.assertTrue(obs.error)


# --------------------------------------------------------------------------- --update-ledger


class UpdateLedger(unittest.TestCase):
    def make(self, cli_cells, scenario, **kw):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = make_root(Path(tmp.name), cli_cells)
        self.path = self.root / "profiles/lab-kind/expected/orchestration.yaml"
        self.before = self.path.read_text()
        return run_accept(self.root, FakeKube(scenario), update=True, **kw)

    def l0_gate(self):
        res = subprocess.run([sys.executable, str(self.root / "tools/l0/profile_schema.py"), "--root", str(self.root)],
                             capture_output=True, text=True, check=False)
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)

    def test_the_observed_status_is_written_with_evidence_and_nothing_else_moves(self):
        doc = self.make({"bundle": "pending", "validate": "failed"}, {})
        after = self.path.read_text()
        led = doc["ledger"]
        # every stage but 10 (addon_absent pending, as the ledger already says) changed from pending/failed to passed
        self.assertEqual(led["cells_updated"], 10)
        self.assertIn("-        bundle:", led["diff"])
        self.assertIn("+        bundle: {status: passed, reasonCode: stage_passed", led["diff"])
        lines_before, lines_after = self.before.splitlines(), after.splitlines()
        self.assertEqual(len(lines_before), len(lines_after))
        self.assertEqual(lines_before[:12], lines_after[:12])                         # header comments, top-level keys
        self.assertEqual(lines_before[lines_before.index("      dag:"):], lines_after[lines_after.index("      dag:"):])
        bundle = next(l for l in lines_after if l.strip().startswith("bundle: {status: passed"))
        self.assertIn('evidence: {run: acc-test-1, date: "2026-11-20", '
                      "report: conformance/reports/2026-11-20-lab-kind-orchestration-acc-test-1.json}", bundle)
        self.assertTrue(bundle.endswith("# bundle"))                                  # a trailing comment stays
        publish = next(l for l in lines_after if l.strip().startswith("publish: {"))
        self.assertIn("status: pending", publish)                                     # unchanged cells are untouched
        self.assertEqual(publish, next(l for l in lines_before if l.strip().startswith("publish: {")))
        self.l0_gate()

    def test_round_trip_on_a_copy_of_the_real_ledger(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = make_root(Path(tmp.name))
        self.path = self.root / "profiles/lab-kind/expected/orchestration.yaml"
        shutil.copy(REPO_ROOT / "profiles/lab-kind/expected/orchestration.yaml", self.path)
        # The real ledger says stagesRun false (every cell pending, reasonCode not_run, no evidence): a run that
        # observes results needs it true, so the copy says so and nothing else changes.
        self.path.write_text(self.path.read_text().replace("stagesRun: false\n", "stagesRun: true\n", 1))
        before = self.path.read_text().splitlines()
        scenario = {2: ("failed", "schema error in the contract", 1)}
        first = run_accept(self.root, FakeKube(scenario), update=True, products=["events-pipeline"])
        after = self.path.read_text().splitlines()
        # events-pipeline expects every cell pending: bundle was observed passed and validate failed, the rest
        # stayed pending, so exactly these two lines move
        self.assertEqual(first["ledger"]["cells_updated"], 2)
        start = before.index("  events-pipeline:")
        cli = before.index("      cli:", start)
        want = [cli + 1 + NAMES.index(n) for n in ("bundle", "validate")]
        self.assertEqual([i for i, (a, b) in enumerate(zip(before, after)) if a != b], want)
        validate = next(l for l in after[start:] if l.strip().startswith("validate: {"))
        self.assertIn("status: failed", validate)
        self.assertIn(f"[fp:{accept.fingerprint('schema error in the contract')}]", validate)
        self.assertEqual(len(before), len(after))
        self.assertEqual(before[:12], after[:12])                                  # the file's header comments
        self.assertIn("status: passed", next(l for l in after[start:] if l.strip().startswith("bundle: {")))
        self.l0_gate()
        again = run_accept(self.root, FakeKube(scenario), run_id="acc-test-2", products=["events-pipeline"])
        self.assertTrue(again["ok"], again["mismatches"])

    def test_a_second_run_against_the_updated_ledger_matches(self):
        self.make({}, {2: ("failed", "schema error", 1)})
        again = run_accept(self.root, FakeKube({2: ("failed", "schema error", 1)}), run_id="acc-test-2")
        self.assertTrue(again["ok"], again["mismatches"])
        self.l0_gate()

    def test_a_status_that_is_not_passed_gets_the_observed_reason_and_no_evidence(self):
        doc = self.make({n: "passed" for n in NAMES[:2]}, {2: ("failed", "schema invalid", 1)})
        after = self.path.read_text()
        validate = next(l for l in after.splitlines() if l.strip().startswith("validate: {"))
        self.assertIn("status: failed, reasonCode: stage_failed", validate)
        self.assertIn("schema invalid", validate)
        self.assertNotIn("evidence", validate)
        self.assertEqual(doc["ledger"]["cells_updated"], 1)
        self.l0_gate()

    def test_the_capability_of_a_changed_cell_is_kept(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = make_root(Path(tmp.name), {})
        path = root / "profiles/lab-kind/expected/orchestration.yaml"
        path.write_text(path.read_text().replace("        bundle: {status: pending, reasonCode: not_run_on_lab, ",
                                                 "        bundle: {status: pending, capability: bundle.contract, "
                                                 "reasonCode: not_run_on_lab, ", 1))
        run_accept(root, FakeKube(), update=True)
        bundle = next(l for l in path.read_text().splitlines() if l.strip().startswith("bundle: {"))
        self.assertIn("capability: bundle.contract", bundle)
        self.assertIn("status: passed", bundle)

    def refused(self, kube, **kw):
        """A run that started from an all-`passed` ledger and was told to update it: the ledger must not move."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = make_root(Path(tmp.name), {n: "passed" for n in NAMES if n != "publish"}
                         | {"publish": ("pending", "addon_absent")})
        path = root / "profiles/lab-kind/expected/orchestration.yaml"
        before = path.read_text()
        doc = run_accept(root, kube, update=True, **kw)
        self.assertFalse(doc["ok"])
        self.assertIn("ledger was not touched", doc["ledger"]["refused"])
        self.assertEqual(doc["ledger"]["cells_updated"], 0)
        self.assertEqual(path.read_text(), before)
        return root, doc

    def test_an_error_in_the_run_refuses_the_update_and_every_cell_after_it_is_an_error_too(self):
        # stage 3: ship exits 1 but its own report says passed (a contradiction the harness cannot settle)
        root, doc = self.refused(FakeKube({3: ("passed", None, 1)}))
        cells = json.loads((root / doc["report"]).read_text())["products"]["events-pipeline"]["cli"]["cells"]
        self.assertEqual([n for n in NAMES if cells[n]["observed"]["error"]],
                         [n for n in NAMES if n != "publish" and NAMES.index(n) >= 2])
        self.assertEqual(cells["generate-artifacts"]["observed"]["reasonCode"], "harness_error")
        self.assertEqual(cells["plan"]["observed"]["reasonCode"], "earlier_stage_failed")
        self.assertFalse(cells["plan"]["match"])
        self.assertTrue(cells["bundle"]["match"])

    def test_a_failed_copy_into_the_pod_refuses_the_update_with_all_eleven_cells_unobserved(self):
        root, doc = self.refused(FakeKube(copy_fail=True))
        cells = json.loads((root / doc["report"]).read_text())["products"]["events-pipeline"]["cli"]["cells"]
        self.assertEqual([n for n in NAMES if cells[n]["observed"]["error"]], [n for n in NAMES if n != "publish"])
        self.assertEqual(sum(1 for c in cells.values() if c["match"]), 1)                # only publish, which never ran

    def test_a_failed_step_refuses_the_update_too(self):
        self.refused(FakeKube(apply_exit=1))

    def test_an_exit_0_not_run_and_another_runs_report_are_errors_that_refuse_the_update(self):
        self.refused(FakeKube({8: ("not-run", "RUN_STAGE_8_POLICY_APPLY=false", 0)}))
        self.refused(FakeKube(wrong_run_id="acc-someone-else-1"))

    def test_the_cli_says_the_ledger_was_not_updated(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = make_root(Path(tmp.name), {})
        real = accept.run_accept
        accept.run_accept = lambda r, **kw: real(r, runner_factory=factory_for(FakeKube(copy_fail=True)),
                                                 proc_runner=proc_runner, now=lambda: NOW, run_id="acc-cli-2",
                                                 env_vars={}, **kw)
        try:
            rc, out, _ = run_cli("--root", str(root), "accept", "--update-ledger")
        finally:
            accept.run_accept = real
        self.assertEqual(rc, EXIT_FAIL)
        self.assertIn("NOT updated", out)

    def test_a_ledger_cell_it_cannot_edit_safely_stops_the_update_and_writes_nothing(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = make_root(Path(tmp.name), {})
        path = root / "profiles/lab-kind/expected/orchestration.yaml"
        text = path.read_text().replace(
            '        bundle: {status: pending, reasonCode: not_run_on_lab, reason: "A reason that is long enough."}  # bundle\n',
            '        bundle:\n          status: pending\n          reasonCode: not_run_on_lab\n'
            '          reason: "A reason that is long enough."\n', 1)
        path.write_text(text)
        with self.assertRaises(AcceptError) as ctx:
            run_accept(root, FakeKube(), update=True)
        self.assertIn("not a one-line flow mapping", str(ctx.exception))
        self.assertEqual(path.read_text(), text)

    def test_a_run_with_no_difference_changes_no_file(self):
        doc = self.make({n: "passed" for n in NAMES if n != "publish"} | {"publish": ("pending", "addon_absent")}, {})
        self.assertEqual(doc["ledger"]["cells_updated"], 0)
        self.assertEqual(doc["ledger"]["diff"], "")
        self.assertEqual(self.path.read_text(), self.before)

    def test_the_text_edit_alone_moves_only_the_named_cells(self):
        text = ledger_text({})
        new = accept.edit_ledger_text(text, {("events-pipeline", "cli", "plan"): "{status: failed, reasonCode: abc, reason: \"x y z w v u t\"}"})
        diff = [l for l in new.splitlines() if l not in text.splitlines()]
        self.assertEqual(len(diff), 1)
        self.assertTrue(diff[0].lstrip().startswith("plan: {status: failed"))
        with self.assertRaises(AcceptError):
            accept.edit_ledger_text(text, {("events-pipeline", "cli", "nope"): "{}"})


# --------------------------------------------------------------------------- redaction


class JsonRedaction(unittest.TestCase):
    def test_json_form_secrets_are_redacted(self):
        # built from parts: a literal "key": "value" pair in this file would be a finding of the secret scan
        value = "".join(["hunter", "2xyz", "-", "0123456"])
        q = '"'
        cases = [
            "{" + q + "password" + q + ": " + q + value + q + "}",
            "{" + q + "client_secret" + q + ":" + q + value + q + "}",
            "{" + q + "password" + q + ":" + q + "two words " + value + q + "}",
            "secret_access_key" + q + ": " + q + value + "/" + value + q,
            "{'api_key': '" + value + "'}",
            "{" + q + "a" + q + ": 1, " + q + "token" + q + " : " + q + value + q + ", " + q + "b" + q + ": 2}",
            q + "private_key" + q + ": " + q + value + q,
            "token = '" + value + " abc def'",
        ]
        for text in cases:
            with self.subTest(text=text.replace(value, "<v>")):
                out = accept.redact(text)
                for leaked in (value, "two words", "abc def"):
                    self.assertNotIn(leaked, out)
                self.assertIn("***", out)
        self.assertEqual(accept.redact("{" + q + "a" + q + ": 1, " + q + "token" + q + ": " + q + value + q + ", "
                                       + q + "b" + q + ": 2}"),
                         "{" + q + "a" + q + ": 1, " + q + "token" + q + ": ***, " + q + "b" + q + ": 2}")

    def test_the_cut_never_comes_before_the_redaction(self):
        res = proc.Result(["x"], 1, "start " + "x" * 10 + f" token={SECRET} end", "")
        cut = len(" end") + len(SECRET) - 4
        self.assertIn(SECRET[4:], res.output_tail(cut))                # cutting first would leave most of the secret
        self.assertNotIn(SECRET[4:], accept_runner.safe_tail(res, cut))
        self.assertNotIn(SECRET[8:], accept_runner.safe_tail(res, 200))

    def test_a_secret_in_a_failed_stages_output_does_not_reach_the_report(self):
        scenario = {2: ("failed", None, 1)}                              # no failure_reason: the tail is used
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube(scenario)
            original = kube.exec

            def noisy(namespace, pod, command, **kw):
                res = original(namespace, pod, command, **kw)
                if res.returncode == 1 and "fluid" in command:
                    return proc.Result(command, 1, f'boom {{"password": "{SECRET}"}}', "")
                return res

            kube.exec = noisy
            root = make_root(Path(tmp))
            run_accept(root, kube)
            written = "".join(p.read_text() for p in (root / "conformance/reports").iterdir())
        self.assertNotIn(SECRET, written)
        self.assertIn("boom", written)


class ReasonRule(unittest.TestCase):
    def cmp(self, ledger_reason, forge, status="failed"):
        return accept.compare("p", "cli", {"validate": {"status": status, "reasonCode": "abc", "reason": ledger_reason}},
                              {"validate": Observed(status, forge)})[1]

    def test_a_later_failure_with_another_reason_no_longer_matches_a_failed_cell(self):
        ledger = "forge has no runtime local yet, so stage 2 fails schema validation"
        self.assertEqual(len(self.cmp(ledger, "schema validation: runtime local is not allowed")), 1)   # paraphrase: no
        mism = self.cmp("forge has no runtime local yet.", "dbt compile error in model events_hourly")
        self.assertEqual(len(mism), 1)
        self.assertIn("expected failed, observed failed (reason changed: forge said \"dbt compile error", mism[0])
        self.assertIn(f"[fp:{accept.fingerprint('dbt compile error in model events_hourly')}]", mism[0])

    def test_a_ledger_that_quotes_forges_first_words_matches(self):
        ledger = "stage 2 fails: Schema validation failed: runtime local is not allowed (https://github.com/Agenticstiger/forge-cli/pull/975)"
        self.assertEqual(self.cmp(ledger, "schema validation failed: runtime 'local' is not allowed"), [])

    def test_a_fingerprint_marker_must_equal_forges_reason(self):
        forge = "exit code 2: bundle found no file"
        marked = f"Observed on acc-1: {forge} [fp:{accept.fingerprint(forge)}]"
        self.assertEqual(self.cmp(marked, forge), [])
        self.assertEqual(self.cmp(marked, "exit code 2: bundle found no file"), [])                     # same words
        self.assertEqual(len(self.cmp(marked, "exit code 1: validation failed")), 1)
        self.assertEqual(len(self.cmp("Observed on acc-1: x [fp:0123456789]", forge)), 1)             # wrong marker

    def test_digits_are_not_part_of_the_fingerprint(self):
        self.assertEqual(accept.fingerprint("failed after 31 s on build 20261120"),
                         accept.fingerprint("failed after 92 s on build 20260101"))
        self.assertNotEqual(accept.fingerprint("validation failed"), accept.fingerprint("bundle failed"))

    def test_statuses_without_a_forge_reason_are_not_compared_by_reason(self):
        for status in ("passed", "pending"):
            self.assertEqual(accept.compare("p", "cli", {"x": {"status": status, "reasonCode": "abc", "reason": "anything at all"}},
                                            {"x": Observed(status, "something else entirely")})[1], [])
        self.assertEqual(len(self.cmp("anything here at all", "something else", "unsupported")), 1)
        self.assertEqual(len(self.cmp("anything here at all", "something else", "not_checked")), 1)

    def test_a_changed_reason_fails_the_run_and_update_ledger_rewrites_the_cell_with_a_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_root(Path(tmp), {"bundle": "passed", "validate": ("failed", "abc_def", "old forge words here")})
            scenario = {2: ("failed", "a different failure entirely", 1)}
            doc = run_accept(root, FakeKube(scenario), update=True)
            self.assertFalse(doc["ok"])
            self.assertIn("reason changed", " ".join(doc["mismatches"]))
            line = next(l for l in (root / "profiles/lab-kind/expected/orchestration.yaml").read_text().splitlines()
                        if l.strip().startswith("validate: {"))
            self.assertIn(f"[fp:{accept.fingerprint('a different failure entirely')}]", line)
            self.assertEqual(doc["ledger"]["cells_updated"], 1)
            again = run_accept(root, FakeKube(scenario), run_id="acc-test-2")
            self.assertTrue(again["ok"], again["mismatches"])


class PoisonedPod(unittest.TestCase):
    """The pod's own environment must not weaken a stage: run the real wrapper against a poisoned one."""

    POISON = {"POLICY_APPLY_MODE": "check", "APPLY_MODE": "dry-run", "ALLOW_DATA_LOSS": "true",
              "NO_VERIFY_DIGEST": "true", "DIFF_EXIT_ON_DRIFT": "false", "SCHEDULE_SYNC_DRY_RUN": "true",
              "RUN_STAGE_8_POLICY_APPLY": "false", "RUN_STAGE_2_VALIDATE": "false", "VALIDATE_STRICT": "false",
              "VERIFY_STRICT": "false", "FLUID_SPARK_TARGET_ALLOWLIST": "other-target:everything", "SOME_OTHER": "x"}
    KEPT = {"HOME": "/home/fluid", "AWS_REGION": "us-east-1", "FLUID_COMMAND_CENTER_ENABLED": "false",
            "LAKEKEEPER_CLIENT_ID": "spark", "KUBERNETES_SERVICE_HOST": "10.0.0.1", "LC_ALL": "C.UTF-8"}

    def run_wrapper(self, argv, env, limit="30", path=None):
        env = {"PATH": path or os.environ["PATH"], **env}
        return subprocess.run([shutil.which("sh"), "-c", accept_runner.FORGE_WRAPPER, "sh", tempfile.gettempdir(), limit,
                               *argv],
                              env=env, capture_output=True, text=True, check=False, timeout=60)

    def seen(self, env, operator=None):
        prefix = accept_runner.env_prefix("local", "acc-1", operator or {})
        res = self.run_wrapper(prefix + ["env"], env)
        self.assertEqual(res.returncode, 0, res.stderr)
        return dict(line.split("=", 1) for line in res.stdout.splitlines() if "=" in line)

    def test_no_weakening_variable_of_the_pod_reaches_the_stage(self):
        got = self.seen({**self.POISON, **self.KEPT})
        for name in self.POISON:
            if name in ("VALIDATE_STRICT", "VERIFY_STRICT"):
                self.assertEqual(got[name], "true", name)                  # re-set by the runner, never the pod's value
            else:
                self.assertNotIn(name, got)
        self.assertEqual(got["FLUID_ENV"], "local")
        self.assertEqual(got["FLUID_RUN_ID"], "acc-1")

    def test_what_the_lab_needs_is_kept(self):
        got = self.seen({**self.POISON, **self.KEPT})
        for name, value in self.KEPT.items():
            self.assertEqual(got[name], value)
        self.assertIn("PATH", got)

    def test_the_operators_allowlists_replace_the_pods(self):
        got = self.seen({**self.POISON}, {"FLUID_SPARK_TARGET_ALLOWLIST": "target-one:x"})
        self.assertEqual(got["FLUID_SPARK_TARGET_ALLOWLIST"], "target-one:x")
        self.assertNotIn("FLUID_SPARK_TARGET_ALLOWLIST", self.seen({**self.POISON}))

    def test_the_pods_variables_never_appear_in_an_argv(self):
        run_id = "acc-secret-1"
        argv = accept_runner.env_prefix("local", run_id, {}) + accept_runner.ship_argv(2, SITE_WITH_ALL, "local")
        self.assertNotIn("LAKEKEEPER_CLIENT_SECRET", " ".join(argv))

    def test_a_stage_that_outlives_its_limit_is_killed_in_the_pod_with_exit_124(self):
        # with a `timeout` binary if the host has one, and with the watchdog fallback on a PATH that has none
        started = time.monotonic()
        res = self.run_wrapper(["sleep", "30"], {}, limit="1")
        self.assertEqual(res.returncode, 124, res.stderr)
        self.assertLess(time.monotonic() - started, 20)
        with tempfile.TemporaryDirectory() as bin_dir:
            for tool in ("env", "sed", "sleep"):
                os.symlink(shutil.which(tool), Path(bin_dir) / tool)
            started = time.monotonic()
            res = self.run_wrapper(["sleep", "30"], {}, limit="1", path=bin_dir)
            self.assertEqual(res.returncode, 124, res.stderr)
            self.assertLess(time.monotonic() - started, 20)

    def test_a_stage_that_finishes_keeps_its_own_exit_code(self):
        res = self.run_wrapper(["sh", "-c", "exit 7"], {}, limit="30")
        self.assertEqual(res.returncode, 7)


class StrictCannotBeWeakenedOnTheWay(unittest.TestCase):
    """The guard sits on the exec path, not only in the functions that build argv: these calls could fail."""

    def runner(self, **kw):
        site_ = accept.load_site(make_root(self.tmp()), "lab-kind", None, proc_runner)
        return CliRunner(FakeKube(), site_, env="local", run_id="acc-s-1", allowlists=kw.pop("allowlists", {}), **kw)

    def tmp(self):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        return Path(d.name)

    def test_an_argv_with_a_no_strict_flag_never_reaches_the_pod(self):
        runner = self.runner()
        for flag in ("--no-strict", "--no-verify-strict"):
            with self.assertRaises(AcceptError):
                runner.forge(["env", "fluid", "ship", flag], cwd="/work/accept/x", timeout=60)
        self.assertEqual(runner.kube.calls, [])

    def test_an_allowlist_that_sets_strict_false_is_refused_by_the_runner_and_by_the_prefix(self):
        for bad in ({"VALIDATE_STRICT": "false"}, {"VERIFY_STRICT": "0"}):
            with self.assertRaises(AcceptError):
                self.runner(allowlists=bad)
            with self.assertRaises(AcceptError):
                accept_runner.env_prefix("local", "acc-1", bad)

    def test_a_poisoned_stage_builder_is_caught_when_the_stage_runs(self):
        runner = self.runner()
        original = accept_runner.ship_argv
        accept_runner.ship_argv = lambda *a, **k: ["fluid", "ship", "--no-strict", "--stage", "2"]
        try:
            with self.assertRaises(AcceptError):
                runner.stage(Product("events-pipeline", Path(".")), 2)
        finally:
            accept_runner.ship_argv = original
        self.assertEqual([c for c in runner.kube.calls if c[2][2:3] == [accept_runner.FORGE_WRAPPER]], [])


class PodHygiene(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = make_root(Path(tmp.name))
        self.site = accept.load_site(self.root, "lab-kind", None, proc_runner)
        self.product = accept.load_products(self.root, ["events-pipeline"], proc_runner)[0]

    def runner(self, kube, **kw):
        return CliRunner(kube, self.site, env="local", run_id="acc-h-1", allowlists={}, **kw)

    def test_the_run_directory_is_removed_when_the_runner_closes(self):
        kube = FakeKube()
        runner = self.runner(kube)
        runner.prepare(self.product)
        runner.close()
        self.assertEqual(kube.removed, ["/work/accept/acc-h-1"])
        self.assertEqual(kube.calls[-1][2], ["rm", "-rf", "--", "/work/accept/acc-h-1"])
        self.assertEqual(runner.notes, [])

    def test_keep_leaves_it_and_says_so(self):
        kube = FakeKube()
        runner = self.runner(kube, keep=True)
        runner.prepare(self.product)
        runner.close()
        self.assertEqual(kube.removed, [])
        self.assertIn("/work/accept/acc-h-1 (--keep)", runner.notes[0])

    def test_nothing_is_removed_from_a_pod_that_was_never_reached(self):
        kube = FakeKube()
        self.runner(kube).close()
        self.assertEqual(kube.calls, [])

    def test_the_whole_run_cleans_up_and_a_kept_run_says_so_in_the_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube()
            run_accept(make_root(Path(tmp)), kube)
            self.assertEqual(kube.removed, ["/work/accept/acc-test-1"])
            kube2 = FakeKube()
            root = make_root(Path(tmp) / "again")
            doc = run_accept(root, kube2, keep=True)
            self.assertEqual(kube2.removed, [])
            self.assertIn("(--keep)", " ".join(json.loads((root / doc["report"]).read_text())["notes"]))

    def test_a_live_process_of_an_earlier_run_of_the_product_refuses_the_start(self):
        kube = FakeKube(live_exit=3)
        with self.assertRaises(AcceptError) as ctx:
            self.runner(kube).prepare(self.product)
        self.assertIn("still running in the pod", str(ctx.exception))
        self.assertEqual(kube.copied, b"")

    def test_the_live_check_script_finds_nothing_on_a_host_without_such_a_process(self):
        res = subprocess.run(["sh", "-c", accept_runner.LIVE_CHECK, "sh", "/work/accept", "events-pipeline"],
                             capture_output=True, text=True, check=False, timeout=60)
        self.assertEqual(res.returncode, 0, res.stderr)

    def test_the_exec_bit_of_a_product_file_is_kept(self):
        src = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, src)
        (src / "run.sh").write_text("#!/bin/sh\n")
        (src / "run.sh").chmod(0o755)
        (src / "data.yaml").write_text("a: 1\n")
        (src / "data.yaml").chmod(0o600)
        tar = tarfile.open(fileobj=io.BytesIO(accept_runner.product_tar(src)))
        self.assertEqual(tar.getmember("run.sh").mode, 0o755)
        self.assertEqual(tar.getmember("data.yaml").mode, 0o644)


class CatalogForward(unittest.TestCase):
    class Resp:
        def __init__(self, status, body=None):
            self.status, self._body = status, body or {}

        def json(self):
            return self._body

        def text(self, n=0):
            return "x"

    class Client:
        def __init__(self, dead=False):
            self.dead, self.calls = dead, 0

        def token(self, name):
            return "t"

        def https(self, method, host, path, **kw):
            self.calls += 1
            if self.dead:
                raise accept_runner.http.HTTPError("connection reset")
            if "/config" in path:
                return CatalogForward.Resp(200, {"defaults": {"prefix": "p"}})
            return CatalogForward.Resp(200)

    class Forward:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def test_a_dead_port_forward_is_replaced_once(self):
        opened = []

        def opener():
            fwd = self.Forward()
            opened.append(fwd)
            return self.Client(dead=len(opened) == 1), fwd

        catalog = accept_runner.LabCatalog(None, None, None, opener=opener)
        self.assertTrue(catalog.table_exists("sales-wh", "events_curated", "events_daily"))
        self.assertEqual(len(opened), 2)
        self.assertTrue(opened[0].closed)
        catalog.close()
        self.assertTrue(opened[1].closed)

    def test_a_forward_that_stays_dead_is_an_error_not_a_loop(self):
        catalog = accept_runner.LabCatalog(None, None, None, opener=lambda: (self.Client(dead=True), self.Forward()))
        with self.assertRaises(accept_runner.http.HTTPError):
            catalog.table_exists("sales-wh", "a", "b")

    def test_a_forward_that_does_not_start_leaves_nothing_to_close(self):
        def opener():
            raise accept_runner.kube.KubeError("port-forward did not start")

        catalog = accept_runner.LabCatalog(None, None, None, opener=opener)
        with self.assertRaises(accept_runner.kube.KubeError):
            catalog.table_exists("sales-wh", "a", "b")
        self.assertIsNone(catalog.forward)
        catalog.close()


class SmallFixes(unittest.TestCase):
    def test_the_flow_cell_parser_is_brace_and_quote_aware(self):
        text = ('products:\n  p:\n    runners:\n      cli:\n'
                '        bundle: {status: pending, reasonCode: abc, reason: "a } brace \\" inside"}  # note {braces} }\n'
                '        plan: {status: pending, reasonCode: abc, reason: "x"}\n')
        new = accept.edit_ledger_text(text, {("p", "cli", "bundle"): "{NEW}"})
        self.assertIn("        bundle: {NEW}  # note {braces} }\n", new)
        self.assertIn('plan: {status: pending, reasonCode: abc, reason: "x"}', new)
        for line in ("bundle: {a: 1} trailing", "bundle: {a: 1", "bundle: x", "bundle:"):
            self.assertIsNone(accept.split_flow_cell("        " + line))
        self.assertEqual(accept.split_flow_cell("  plan: {a: {b: 1}}  # c"), ("  ", "plan", "  # c"))

    def test_a_repeated_product_runs_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube()
            doc = run_accept(make_root(Path(tmp)), kube, products=["events-pipeline", "events-pipeline"])
            report = json.loads((Path(tmp) / "repo" / doc["report"]).read_text())
        self.assertEqual(list(report["products"]), ["events-pipeline"])
        self.assertEqual(kube.ship_order, [1, 2, 3, 4, 5, 6, 7, 8, 9, 11])

    def test_a_ship_stage_that_was_not_run_and_a_foreign_report_make_error_cells_in_a_real_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = run_accept(make_root(Path(tmp)), FakeKube({2: ("not-run", "RUN_STAGE_2_VALIDATE=false", 0)}))
            self.assertFalse(doc["ok"])
            self.assertIn("did not run it", " ".join(doc["mismatches"]))
        with tempfile.TemporaryDirectory() as tmp:
            doc = run_accept(make_root(Path(tmp)), FakeKube(wrong_run_id="acc-old-1"))
            self.assertIn("belongs to run 'acc-old-1'", " ".join(doc["mismatches"]))
        with tempfile.TemporaryDirectory() as tmp:
            doc = run_accept(make_root(Path(tmp)), FakeKube(no_write={3}))
            self.assertIn("has no record of stage 3", " ".join(doc["mismatches"]))


# --------------------------------------------------------------------------- the command line


def run_cli(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            rc = cli.main(list(argv))
        except SystemExit as exc:
            rc = exc.code
    return rc, out.getvalue(), err.getvalue()


class CommandLine(unittest.TestCase):
    def test_accept_is_wired_with_its_flags_and_help(self):
        rc, out, _ = run_cli("accept", "--help")
        self.assertEqual(rc, 0)
        for flag in ("--name", "--profile", "--tier", "--product", "--runner", "--env", "--update-ledger", "--json",
                     "both directions", "committing it"):
            self.assertIn(flag, out)

    def test_the_dag_runner_is_refused_with_a_clear_message_and_exit_2(self):
        rc, out, err = run_cli("accept", "--runner", "dag")
        self.assertEqual(rc, EXIT_USAGE)
        self.assertIn("the dag runner is not available yet", err)
        self.assertIn("orchestration tier", err)

    def test_an_unknown_product_is_a_usage_error_and_a_missing_lab_is_a_failure(self):
        rc, _, err = run_cli("accept", "--product", "no-such-product")
        self.assertEqual((rc, "no product 'no-such-product'" in err), (EXIT_USAGE, True))
        with tempfile.TemporaryDirectory() as tmp:
            import os

            old = os.environ.get("LAB_STATE_DIR")
            os.environ["LAB_STATE_DIR"] = str(Path(tmp) / "none")
            try:
                rc, out, _ = run_cli("accept", "--product", "events-pipeline")
            finally:
                if old is None:
                    del os.environ["LAB_STATE_DIR"]
                else:
                    os.environ["LAB_STATE_DIR"] = old
        self.assertEqual(rc, EXIT_FAIL)
        self.assertIn("has no kubeconfig", out)

    def test_the_real_ledgers_list_products_not_authored_yet_so_a_bare_accept_names_them(self):
        # profiles/lab-kind keeps its rows for products that are not authored: `lab accept` without
        # --product refuses them by name and says how to run the products that exist.
        rc, out, _ = run_cli("accept")
        self.assertEqual(rc, EXIT_FAIL)
        self.assertIn("batch-dbt-trino", out)
        self.assertIn("--product", out)

    def test_the_command_prints_the_comparison_the_report_and_the_ledger_diff(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_root(Path(tmp), {})
            kube = FakeKube()
            accept_real = accept.run_accept

            def fake_run_accept(r, **kw):
                return accept_real(r, runner_factory=factory_for(kube), proc_runner=proc_runner, now=lambda: NOW,
                                   run_id="acc-cli-1", env_vars={}, **kw)

            accept.run_accept = fake_run_accept
            try:
                rc, out, _ = run_cli("--root", str(root), "accept", "--update-ledger")
                rc_json, out_json, _ = run_cli("--root", str(root), "accept", "--json")
            finally:
                accept.run_accept = accept_real
        self.assertEqual(rc, EXIT_FAIL)
        self.assertRegex(out, r"FAIL  events-pipeline/cli/bundle +expected pending, observed passed \(the stage passed\)")
        self.assertIn("Report: conformance/reports/", out)
        self.assertIn("cell(s) updated", out)
        self.assertIn("lab accept never commits", out)
        self.assertEqual(rc_json, EXIT_FAIL)
        self.assertFalse(json.loads(out_json)["ok"])


if __name__ == "__main__":
    unittest.main()
