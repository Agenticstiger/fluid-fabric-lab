# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The streaming add-on tier: the check Job is restricted and carries no password,
its three claims are judged exactly (the stream intact, the positive control, the
refusal that is an authorization error and not a failed login), a stuck or
competing Job fails fast and an unfinished one is deleted, and the Kafka CR keeps
what the check relies on (digests, both listeners, ACL enforcement, no KafkaConnect)."""

import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import support  # noqa: F401
from support import REPO_ROOT

from labctl import check, config, jobs, kube, mirror, proc, streaming, up
from test_spark import FakeKube, waiting

LOCK = config.load_yaml(REPO_ROOT / config.VERSIONS_LOCK)
TIERS = config.load_yaml(REPO_ROOT / config.TIERS)
MARKER = "lab-check-abc123"
PASSWORDS = {"lab-check-writer": "w-s3cr3t", "lab-check-nobody": "n-s3cr3t"}


def good_logs(marker=MARKER, **over):
    """What the check prints when everything held; `over` replaces a mark's payload (None drops it)."""
    marks = {
        "PRODUCED": {"marker": marker, "n": 20, "start": 7, "rc": 0},
        "CONSUMED": {"rc": 0},
        "WRITER": {"user": "lab-check-writer", "rc": 0, "denied": 0, "authn": 0},
        "NOBODY": {"user": "lab-check-nobody", "rc": 1, "denied": 2, "authn": 0},
        "ACLDENY": {"rc": 1, "denied": 1},
        # The live shape (2026-11-19): the console consumer logs the refusal and exits 0.
        "ANONREAD": {"rc": 0, "denied": 1, "leaked": 0},
    }
    marks.update(over)
    lines = ["=== check", "  producer: noise"]
    for name in ("PRODUCED", "CONSUMED"):
        if marks[name] is not None:
            lines.append(f"LAB-STREAM-{name} {json.dumps(marks[name])}")
    lines += [f"LAB-STREAM-RECORD {json.dumps({'run': marker, 'seq': i})}" for i in range(1, 21)]
    lines += [f"LAB-STREAM-{n} {json.dumps(marks[n])}" for n in ("WRITER", "NOBODY", "ACLDENY", "ANONREAD") if marks[n] is not None]
    return "\n".join(lines) + "\n"


def judge(logs, marker=MARKER):
    return streaming.judge(streaming.marks(logs), streaming.record_lines(logs), marker)


class CheckJob(unittest.TestCase):
    def setUp(self):
        self.job = streaming.job("lab-check-streaming-1", streaming.kafka_ref(LOCK), MARKER)
        self.pod = self.job["spec"]["template"]["spec"]
        self.container = self.pod["containers"][0]

    def test_the_container_meets_pod_security_restricted(self):
        self.assertTrue(self.pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(self.pod["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertFalse(self.pod["automountServiceAccountToken"])
        sc = self.container["securityContext"]
        self.assertFalse(sc["allowPrivilegeEscalation"])
        self.assertEqual(sc["capabilities"], {"drop": ["ALL"]})
        self.assertGreater(sc["runAsUser"], 0)
        self.assertIn("memory", self.container["resources"]["limits"])
        self.assertEqual(self.job["metadata"]["namespace"], "kafka")

    def test_the_image_is_the_lock_digest_of_the_strimzi_kafka_image(self):
        entry = mirror.lock_image(LOCK, "strimzi-kafka")
        self.assertEqual(self.container["image"], f"{entry['registry']}/{entry['repository']}@{entry['digest']}")

    def test_the_passwords_reach_the_pod_only_through_strimzis_secrets(self):
        env = {e["name"]: e for e in self.container["env"]}
        for var, secret in (("WRITER_PASSWORD", "lab-check-writer"), ("NOBODY_PASSWORD", "lab-check-nobody")):
            self.assertEqual(env[var]["valueFrom"]["secretKeyRef"], {"name": secret, "key": "password"})
            self.assertNotIn("value", env[var])
        self.assertTrue(all("PASSWORD" not in e["name"] or "valueFrom" in e for e in self.container["env"]))

    def test_the_job_trusts_the_lab_ca_and_uses_the_plain_and_tls_listeners(self):
        env = {e["name"]: e.get("value") for e in self.container["env"]}
        self.assertTrue(env["PLAIN"].endswith(":9092"))
        self.assertTrue(env["TLS"].endswith(":9093"))
        self.assertEqual(env["N"], "20")
        self.assertIn({"name": "lab-ca", "configMap": {"name": "lab-ca-bundle"}}, self.pod["volumes"])
        self.assertIn("/etc/lab-ca/ca.crt", streaming.SCRIPT)
        self.assertIn("SCRAM-SHA-512", streaming.SCRIPT)
        self.assertIn("--sync", streaming.SCRIPT)


class Judge(unittest.TestCase):
    def test_all_three_claims_holding_pass(self):
        self.assertEqual(judge(good_logs()), {"records": 20, "marker": MARKER, "start_offset": 7})

    def test_the_stream_must_come_back_exactly(self):
        base = good_logs()
        lines = base.splitlines()
        records = [i for i, line in enumerate(lines) if line.startswith("LAB-STREAM-RECORD")]
        cases = {
            "a record missing": [x for i, x in enumerate(lines) if i != records[3]],
            "a record twice": lines[:records[3]] + [lines[records[2]]] + lines[records[3] + 1:],
            "another run's record": [x.replace(MARKER, "lab-check-old") if i == records[0] else x
                                     for i, x in enumerate(lines)],
            "an extra record": lines + ["LAB-STREAM-RECORD " + json.dumps({"run": MARKER, "seq": 21})],
            "not JSON": [("LAB-STREAM-RECORD hello" if i == records[0] else x) for i, x in enumerate(lines)],
        }
        for why, kept in cases.items():
            with self.subTest(why), self.assertRaises(streaming.StreamingError):
                judge("\n".join(kept) + "\n")

    def test_a_failed_producer_or_consumer_fails(self):
        for over in ({"PRODUCED": {"marker": MARKER, "n": 20, "start": 7, "rc": 1}},
                     {"PRODUCED": {"marker": "other", "n": 20, "start": 7, "rc": 0}},
                     {"CONSUMED": {"rc": 1}}):
            with self.subTest(over), self.assertRaises(streaming.StreamingError):
                judge(good_logs(**over))

    def test_without_the_positive_control_nothing_is_proven(self):
        for writer in ({"rc": 1, "denied": 1, "authn": 0}, {"rc": 1, "denied": 0, "authn": 1}, {"rc": 0, "denied": 1, "authn": 0}):
            with self.subTest(writer), self.assertRaises(streaming.StreamingError) as caught:
                judge(good_logs(WRITER={"user": "lab-check-writer", **writer}))
            self.assertIn("positive control", str(caught.exception))

    def test_the_refusal_must_be_an_authorization_error_not_a_failed_login(self):
        for nobody in ({"rc": 0, "denied": 0, "authn": 0},       # it wrote: the ACLs do not hold
                       {"rc": 1, "denied": 0, "authn": 0},       # failed for some other reason
                       {"rc": 1, "denied": 1, "authn": 1}):      # bad credentials: proves nothing
            with self.subTest(nobody), self.assertRaises(streaming.StreamingError):
                judge(good_logs(NOBODY={"user": "lab-check-nobody", **nobody}))

    def test_an_anonymous_client_on_plain_must_be_refused_the_cluster_and_the_acl_topic(self):
        for over in ({"ACLDENY": {"rc": 0, "denied": 0}},                    # it added an ACL
                     {"ACLDENY": {"rc": 1, "denied": 0}},                    # failed, but not as an authorization error
                     {"ANONREAD": {"rc": 0, "denied": 0, "leaked": 1}},      # it read the restricted topic
                     {"ANONREAD": {"rc": 1, "denied": 0, "leaked": 0}},      # failed for another reason
                     {"ANONREAD": {"rc": 1, "denied": 1, "leaked": 1}},      # refused, yet a record came out
                     {"ANONREAD": {"rc": 0, "denied": 1, "leaked": 1}}):     # exit 0 and a record came out
            with self.subTest(over), self.assertRaises(streaming.StreamingError):
                judge(good_logs(**over))

    def test_the_anonymous_read_is_judged_by_the_refusal_not_the_consumer_exit_code(self):
        # The console consumer exits 0 after logging TopicAuthorizationException (live run, 2026-11-19),
        # and a timeout gives 124: either way the refusal and no leaked record decide.
        for rc in (0, 1, 124):
            with self.subTest(rc=rc):
                self.assertEqual(judge(good_logs(ANONREAD={"rc": rc, "denied": 1, "leaked": 0}))["records"], 20)

    def test_a_mark_is_only_trusted_when_its_counts_are_real_ints(self):
        for over in ({"CONSUMED": {"rc": False}},                            # False == 0 in Python
                     {"CONSUMED": {"rc": 0.0}},
                     {"CONSUMED": {"rc": "0"}},
                     {"NOBODY": {"user": "lab-check-nobody", "rc": 1, "denied": True, "authn": 0}},
                     {"WRITER": {"user": "lab-check-writer", "rc": 0, "denied": 0, "authn": False}},
                     {"ANONREAD": {"rc": 1, "denied": 1, "leaked": 0.0}},
                     {"PRODUCED": {"marker": MARKER, "n": 20.0, "start": 7, "rc": 0}}):
            with self.subTest(over), self.assertRaises(streaming.StreamingError):
                judge(good_logs(**over))
        for seq in ("3.0", "true", '"3"'):
            with self.subTest(seq), self.assertRaises(streaming.StreamingError):
                judge(good_logs().replace('"seq": 3}', f'"seq": {seq}}}'))

    def test_the_script_runs_the_anonymous_probes_and_the_plain_producer_is_not_idempotent(self):
        self.assertIn("kafka-acls.sh", streaming.SCRIPT)
        self.assertIn("LAB-STREAM-ACLDENY", streaming.SCRIPT)
        self.assertIn("LAB-STREAM-ANONREAD", streaming.SCRIPT)
        self.assertIn("enable.idempotence=false", streaming.SCRIPT)
        # the anonymous read comes after the writer has put a record there
        self.assertGreater(streaming.SCRIPT.index("LAB-STREAM-ANONREAD"), streaming.SCRIPT.index('attempt "$WRITER"'))

    def test_a_run_that_stopped_early_fails(self):
        for name in ("PRODUCED", "CONSUMED", "WRITER", "NOBODY", "ACLDENY", "ANONREAD"):
            with self.subTest(name), self.assertRaises(streaming.StreamingError):
                judge(good_logs(**{name: None}))

    def test_the_passwords_are_redacted_raw_and_in_base64(self):
        known = PASSWORDS
        text = ("a w-s3cr3t b " + base64.b64encode(b"lab-check-writer:w-s3cr3t").decode() + " c "
                + base64.b64encode(b"n-s3cr3t").decode())
        self.assertEqual(streaming._redact(text, known), "a <redacted> b <redacted> c <redacted>")


class StreamKube(FakeKube):
    """FakeKube plus what run_check reads: Strimzi's Secrets and `kubectl wait`. `logs` may name the marker."""

    def __init__(self, *args, not_ready=None, **kw):
        super().__init__(*args, **kw)
        self.not_ready, self.waited = not_ready, []

    def wait(self, condition, target, namespace=None, timeout_s=300):
        self.waited.append(target)
        if target == self.not_ready:
            raise kube.KubeError(f"timed out waiting for the condition on {target}")

    def get(self, kind, name=None, namespace=None, selector=None):
        if kind == "secret":
            return {"data": {"password": base64.b64encode(PASSWORDS[name].encode()).decode()}}
        return super().get(kind, name, namespace, selector)

    def run(self, *args, **kw):
        if args[0] == "logs" and callable(self.logs):
            env = {e["name"]: e.get("value") for e in self.applied[0]["spec"]["template"]["spec"]["containers"][0]["env"]}
            return proc.Result(list(args), 0, self.logs(env["MARKER"]), "")
        return super().run(*args, **kw)


def run_check(k, sleep=lambda _s: None):
    return streaming.run_check(k, LOCK, sleep=sleep, clock=iter(range(0, 10 ** 6, 10)).__next__)


class RunCheck(unittest.TestCase):
    def test_a_completed_job_is_judged_and_kept_for_its_logs(self):
        k = StreamKube(statuses=[{"active": 1}, {"succeeded": 1}], logs=good_logs)
        result = run_check(k)
        self.assertEqual(result["records"], 20)
        self.assertEqual(k.deleted, [])
        self.assertEqual(k.waited, list(streaming.READY_TARGETS))
        self.assertTrue(result["job"].startswith("lab-check-streaming-"))

    def test_the_marker_is_unique_per_run_and_not_the_job_name(self):
        markers = []
        for _ in range(2):
            k = StreamKube(statuses=[{"succeeded": 1}], logs=lambda m: markers.append(m) or good_logs(m))
            run_check(k)
        self.assertEqual(len(set(markers)), 2)

    def test_a_stale_stream_from_an_earlier_run_does_not_pass(self):
        k = StreamKube(statuses=[{"succeeded": 1}], logs=lambda _m: good_logs("lab-check-previous-run"))
        with self.assertRaises(streaming.StreamingError) as caught:
            run_check(k)
        self.assertIn("marker", str(caught.exception))
        self.assertIn(k.name, str(caught.exception))
        self.assertEqual(k.deleted, [])   # a finished Job is left to its TTL

    def test_a_failed_job_ends_the_wait_at_once_and_is_kept_for_its_logs(self):
        k = StreamKube(statuses=[{"conditions": [{"type": "Failed", "status": "True"}]}], logs="boom\n")
        with self.assertRaises(streaming.StreamingError) as caught:
            run_check(k)
        self.assertIn("failed", str(caught.exception))
        self.assertIn("boom", str(caught.exception))
        self.assertIn("stays an hour", caught.exception.advice)
        self.assertEqual(k.deleted, [])

    def test_a_password_in_a_log_is_redacted_from_the_error(self):
        k = StreamKube(statuses=[{"conditions": [{"type": "Failed", "status": "True"}]}],
                       logs="login lab-check-writer w-s3cr3t then n-s3cr3t\n")
        with self.assertRaises(streaming.StreamingError) as caught:
            run_check(k)
        self.assertNotIn("s3cr3t", str(caught.exception))
        self.assertIn("<redacted>", str(caught.exception))

    def test_a_job_that_cannot_start_or_keeps_failing_to_pull_fails_and_is_deleted(self):
        for pod, says in ((waiting("check", "CreateContainerConfigError", "secret not found", init=False),
                           "cannot start: container check: CreateContainerConfigError"),
                          (waiting("check", "ErrImagePull", "refused", init=False),
                           f"still failing after {jobs.PULL_GRACE_S} s")):
            with self.subTest(says):
                k = StreamKube(statuses=[{"active": 1}], pods=[[pod]])
                with self.assertRaises(streaming.StreamingError) as caught:
                    run_check(k)
                self.assertIn(says, str(caught.exception))
                self.assertIn("deleted", caught.exception.advice)
                self.assertEqual(k.deleted, [k.name])

    def test_an_interrupted_run_deletes_its_job(self):
        def interrupt(_s):
            raise KeyboardInterrupt

        class InterruptedApply(StreamKube):
            def apply(self, objects):
                super().apply(objects)
                raise KeyboardInterrupt
        for k, sleep in ((StreamKube(statuses=[{"active": 1}]), interrupt), (InterruptedApply(), lambda _s: None)):
            with self.subTest(type(k).__name__), self.assertRaises(KeyboardInterrupt):
                run_check(k, sleep=sleep)
            self.assertEqual(k.deleted, [k.name])

    def test_a_cluster_that_is_not_ready_never_gets_a_job(self):
        for target in streaming.READY_TARGETS:
            with self.subTest(target):
                k = StreamKube(not_ready=target)
                with self.assertRaises(streaming.StreamingError) as caught:
                    run_check(k)
                self.assertIn(target, str(caught.exception))
                self.assertEqual((k.applied, k.deleted), ([], []))

    def test_a_competing_job_is_left_alone_and_a_stuck_one_is_deleted(self):
        k = StreamKube(jobs=[{"metadata": {"name": "lab-check-streaming-1"}, "status": {"active": 1}}])
        with self.assertRaises(streaming.StreamingError) as caught:
            run_check(k)
        self.assertIn("another streaming check Job is unfinished (lab-check-streaming-1)", str(caught.exception))
        self.assertEqual((k.applied, k.deleted), ([], []))
        old = {"metadata": {"name": "lab-check-streaming-old"}, "status": {"active": 1}}
        k = StreamKube(statuses=[{"succeeded": 1}], jobs=[old], logs=good_logs,
                       old_pods={"lab-check-streaming-old": [waiting("check", "ErrImageNeverPull", init=False)]})
        run_check(k)
        self.assertEqual(k.deleted, ["lab-check-streaming-old"])

    def test_a_vanished_job_is_reported(self):
        k = StreamKube(gone=True)
        with self.assertRaises(streaming.StreamingError) as caught:
            run_check(k)
        self.assertIn("disappeared", str(caught.exception))


class StreamingRecord(unittest.TestCase):
    def test_the_record_belongs_to_tiers_with_the_streaming_releases_only(self):
        core = check.records_for(config.resolve_tier(TIERS, "core"))
        streaming_tier = check.records_for(config.resolve_tier(TIERS, "streaming"))
        self.assertNotIn("streaming", core)
        self.assertEqual(streaming_tier, core + ["streaming"])
        self.assertNotIn("streaming", check.records_for(config.resolve_tier(TIERS, "spark")))

    def test_every_expected_error_the_check_raises_is_reported_as_a_fail(self):
        self.assertTrue(issubclass(streaming.StreamingError, check.EXPECTED_ERRORS))
        self.assertIs(check.CHECKS["streaming"], check.streaming_check)

    def test_a_finished_check_job_in_the_standing_namespace_is_not_a_not_ready_tier(self):
        job = {"metadata": {"namespace": "kafka", "name": "lab-check-streaming-1",
                            "labels": {"fluid-fabric-lab/check": "streaming"}}, "status": {"failed": 1}}
        pod = {"metadata": {"namespace": "kafka", "name": "kafka-0"},
               "status": {"phase": "Running", "containerStatuses": [{"ready": True}]}}
        self.assertEqual(check.pod_problems([pod], [job], {"kafka"}), [])
        job["metadata"]["labels"] = {}
        self.assertEqual(check.pod_problems([pod], [job], {"kafka"}), ["job kafka/lab-check-streaming-1 failed"])

    def test_the_mirror_carries_the_images_the_tier_runs(self):
        ids = {e["id"] for e in mirror.tier_images(LOCK, TIERS, "streaming")}
        self.assertLessEqual({"strimzi-operator", "strimzi-kafka"}, ids)


class KafkaReadyWait(unittest.TestCase):
    """`lab up`'s bootstrap step waits for Kafka, because Helm cannot wait for the CR."""

    def step(self, tier="streaming"):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name) / "repo"
        root.mkdir()
        step = up.Up(root, tier, name="ffl-x", runner=mock.Mock(),
                     env={"LAB_STATE_DIR": tmp.name + "/state"})
        step.tier = config.resolve_tier(TIERS, tier)
        step.kube = mock.Mock()
        # The core half of the step (bucket, port-forward, Lakekeeper) is not under test here.
        for name, value in (("warehouse_bucket", "bucket"), ("gateway", mock.MagicMock()),
                            ("LabClient", mock.Mock()), ("lakekeeper", "catalog"),
                            ("keycloak_reconcile", "realm reconciled"), ("trino_grant", "trino granted"),
                            ("spark_grant", "spark granted")):   # the core grants, the realm step included
            patch = mock.patch.object(up.bootstrap, name, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)
        return step

    def test_the_wait_names_the_cluster_and_the_step_reports_it(self):
        step = self.step()
        self.assertTrue(step.bootstrap().endswith("; Kafka lab-kafka is Ready"))
        step.kube.wait.assert_called_once_with("condition=Ready", "kafka/lab-kafka", "kafka", timeout_s=900)

    def test_a_cluster_that_does_not_become_ready_fails_the_step_with_advice(self):
        step = self.step()
        step.kube.wait.side_effect = kube.KubeError("timed out waiting for the condition on kafkas/lab-kafka")
        with self.assertRaises(up.StepError) as caught:
            step.bootstrap()
        self.assertIn("did not become ready", str(caught.exception))
        self.assertIn("timed out", str(caught.exception))
        self.assertIn("kubectl -n kafka describe kafka lab-kafka", caught.exception.advice)

    def test_a_tier_without_the_streaming_releases_never_waits_for_kafka(self):
        step = self.step("core")
        step.bootstrap()
        step.kube.wait.assert_not_called()


class KafkaObjects(unittest.TestCase):
    def setUp(self):
        values = config.load_yaml(REPO_ROOT / "platform/components/kafka/values.yaml")
        self.objects = values["objects"]
        self.kafka = next(o for o in self.objects.values() if o["kind"] == "Kafka")["spec"]

    def kinds(self):
        return sorted(o["kind"] for o in self.objects.values())

    def test_no_kafka_connect_until_the_connect_iceberg_image_exists(self):
        self.assertNotIn("KafkaConnect", self.kinds())
        built = {b["id"]: b for b in LOCK["built_images"]}
        self.assertIsNone(built["connect-iceberg"]["digest"])

    def test_every_image_is_a_template_over_the_lock_digest_never_a_literal_tag(self):
        text = (REPO_ROOT / "platform/components/kafka/values.yaml").read_text()
        images = [line.split("image:", 1)[1].strip() for line in text.splitlines()
                  if line.strip().startswith("image:")]
        self.assertEqual(len(images), 3)   # the broker, both entity-operator containers
        self.assertTrue(all(i.startswith("'{{ .Values.images.") for i in images), images)

    def test_the_listeners_and_authorization_are_what_the_check_relies_on(self):
        listeners = {li["name"]: li for li in self.kafka["kafka"]["listeners"]}
        self.assertEqual((listeners["plain"]["port"], listeners["plain"]["tls"]), (9092, False))
        self.assertNotIn("authentication", listeners["plain"])
        self.assertEqual((listeners["tls"]["port"], listeners["tls"]["tls"]), (9093, True))
        self.assertEqual(listeners["tls"]["authentication"], {"type": "scram-sha-512"})
        self.assertEqual(listeners["tls"]["configuration"]["brokerCertChainAndKey"]["secretName"],
                         "lab-kafka-listener-tls")
        self.assertEqual(self.kafka["kafka"]["authorization"]["type"], "simple")
        self.assertEqual(self.kafka["kafka"]["config"]["allow.everyone.if.no.acl.found"], "true")

    def test_the_cluster_resource_has_an_acl_so_anonymous_does_not_hold_every_cluster_operation(self):
        users = [o["spec"] for o in self.objects.values() if o["kind"] == "KafkaUser"]
        on_cluster = [acl for u in users for acl in (u.get("authorization") or {}).get("acls") or []
                      if acl["resource"]["type"] == "cluster"]
        self.assertTrue(on_cluster)
        self.assertTrue(all("All" not in acl["operations"] and "Alter" not in acl["operations"]
                            for acl in on_cluster))

    def test_no_acl_matches_the_open_topic_or_every_group_so_anonymous_plain_keeps_it(self):
        # An ACL that matches a resource switches off allow.everyone.if.no.acl.found for it ("*" matches all).
        for o in self.objects.values():
            if o["kind"] != "KafkaUser":
                continue
            for acl in (o["spec"].get("authorization") or {}).get("acls") or []:
                res = acl["resource"]
                if res["type"] not in ("topic", "group"):
                    continue
                name, pattern = res["name"], res.get("patternType", "literal")
                hit = name == "*" or (pattern == "prefix" and "lab-check.events".startswith(name)) \
                    or name == "lab-check.events"
                self.assertFalse(hit, f"{o['metadata']['name']}: {res} closes lab-check.events to anonymous clients")

    def test_the_optional_kafbat_ui_matches_the_listener_and_user_the_cluster_has(self):
        ui = config.load_yaml(REPO_ROOT / "platform/components/kafka-ui/values.yaml")
        cluster = ui["yamlApplicationConfig"]["kafka"]["clusters"][0]
        self.assertTrue(cluster["bootstrapServers"].endswith(":9093"))
        self.assertEqual(cluster["properties"]["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertEqual(cluster["properties"]["ssl.truststore.location"], "/etc/lab-ca/ca.crt")
        mapping = ui["envs"]["secretMappings"]["KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG"]
        self.assertEqual(mapping, {"name": "kafka-ui", "keyName": "sasl.jaas.config"})
        user = next(o for o in self.objects.values()
                    if o["kind"] == "KafkaUser" and o["metadata"]["name"] == "kafka-ui")
        self.assertEqual(user["spec"]["authentication"], {"type": "scram-sha-512"})
        acls = user["spec"]["authorization"]["acls"]
        operations = {op for acl in acls for op in acl["operations"]}
        self.assertFalse(operations & {"Write", "Create", "Delete", "Alter", "AlterConfigs", "All"})
        # An ACL that matches a topic or group closes it to anonymous clients on plain (seen live), so none here.
        self.assertEqual({acl["resource"]["type"] for acl in acls}, {"cluster"})
        self.assertNotIn(":9094", (REPO_ROOT / "platform/components/kafka-ui/values.yaml").read_text())

    def test_the_certificate_comes_from_the_lab_ca_and_covers_the_bootstrap_and_brokers(self):
        cert = next(o for o in self.objects.values() if o["kind"] == "Certificate")["spec"]
        self.assertEqual(cert["issuerRef"]["name"], "lab-ca")
        self.assertEqual(cert["secretName"], "lab-kafka-listener-tls")
        self.assertEqual(cert["privateKey"]["encoding"], "PKCS8")
        self.assertIn("lab-kafka-kafka-bootstrap.kafka.svc", cert["dnsNames"])
        self.assertIn("*.lab-kafka-kafka-brokers.kafka.svc", cert["dnsNames"])

    def test_the_topics_and_users_are_the_checks(self):
        topics = {o["metadata"]["name"] for o in self.objects.values() if o["kind"] == "KafkaTopic"}
        self.assertEqual(topics, {"lab-check.events", "lab-check.restricted"})
        users = {o["metadata"]["name"]: o["spec"] for o in self.objects.values() if o["kind"] == "KafkaUser"}
        self.assertEqual(set(users), {"lab-check-writer", "lab-check-nobody", "kafka-ui"})
        for spec in users.values():
            self.assertEqual(spec["authentication"], {"type": "scram-sha-512"})
        topic_acl, cluster_acl = users["lab-check-writer"]["authorization"]["acls"]
        self.assertEqual((topic_acl["resource"]["name"], sorted(topic_acl["operations"])),
                         ("lab-check.restricted", ["Describe", "Write"]))
        self.assertEqual((cluster_acl["resource"]["type"], cluster_acl["operations"]), ("cluster", ["Describe"]))
        self.assertNotIn("authorization", users["lab-check-nobody"])

    def test_the_node_is_one_combined_kraft_node_with_a_small_heap(self):
        pool = next(o for o in self.objects.values() if o["kind"] == "KafkaNodePool")["spec"]
        self.assertEqual((pool["replicas"], sorted(pool["roles"])), (1, ["broker", "controller"]))
        self.assertLessEqual(int(pool["jvmOptions"]["-Xmx"].rstrip("m")), 512)

    def test_the_namespace_is_recorded_and_the_up_tier_is_supported(self):
        from labctl import up
        namespaces = config.load_yaml(REPO_ROOT / "platform/namespaces.yaml")["namespaces"]
        self.assertEqual(namespaces["kafka"]["tier"], "streaming")
        self.assertIn("streaming", up.SUPPORTED_TIERS)


if __name__ == "__main__":
    unittest.main()
