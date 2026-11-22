# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Example products stay consistent with their site profile: the one declared
environment (`local`) has one overlay and one operator file, and the operator
allowlists pin each Spark build's SQL by the digest forge checks (sha256 of the
stripped SQL, forge-cli fluid_build/streaming/policy.py) and hold allowlists only."""

import hashlib
import unittest

import support  # noqa: F401
from support import REPO_ROOT

from labctl import config

PRODUCT = REPO_ROOT / "products" / "events-pipeline"
PROFILE = REPO_ROOT / "profiles" / "lab-kind"
ALLOWLISTS = {"FLUID_ACQUISITION_DEPLOYMENT_HOST_ALLOWLIST", "FLUID_SPARK_CONTRACT_SQL_ALLOWLIST",
              "FLUID_LAKEKEEPER_NAMESPACE_ALLOWLIST"}


def env_file(env: str) -> dict:
    out = {}
    for line in (PROFILE / "operator" / f"{env}.env").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            key, _, value = line.partition("=")
            out[key] = value
    return out


class EventsPipeline(unittest.TestCase):
    def setUp(self):
        self.contract = config.load_yaml(PRODUCT / "contract.fluid.yaml")
        self.envs = sorted(self.contract["environments"])

    def test_the_one_declared_environment_has_one_overlay_and_operator_file(self):
        self.assertEqual(self.envs, ["local"])
        self.assertEqual(sorted(p.stem for p in (PRODUCT / "overlays").glob("*.yaml")), self.envs)
        self.assertEqual(sorted(p.stem for p in (PROFILE / "operator").glob("*.env")), self.envs)

    def test_the_operator_pins_each_spark_sql_by_forges_digest(self):
        wanted = sorted(f"{self.contract['id']}/{b['id']}@sha256:"
                        + hashlib.sha256(str(b["properties"]["sql"]).strip().encode()).hexdigest()
                        for b in self.contract["builds"]
                        if b.get("engine") == "spark" and (b.get("properties") or {}).get("sql") is not None)
        self.assertTrue(wanted)
        for env in self.envs:
            with self.subTest(env):
                self.assertEqual(sorted(env_file(env)["FLUID_SPARK_CONTRACT_SQL_ALLOWLIST"].split(",")), wanted)

    def test_operator_files_hold_allowlists_only(self):
        for env in self.envs:
            with self.subTest(env):
                self.assertLessEqual(set(env_file(env)), ALLOWLISTS)

    def test_stage8_may_change_exactly_the_local_exposes_namespaces(self):
        # https://github.com/Agenticstiger/forge-cli/pull/993: stage 8 grants on Lakekeeper only inside the operator's namespace allowlist.
        wanted = sorted({f"{e['binding']['location']['warehouse']}/{e['binding']['location']['database']}"
                         for e in self.contract["exposes"]
                         if (e["binding"]["location"].get("catalog") == "lakekeeper")})
        self.assertEqual(wanted, ["sales-wh/events_curated", "sales-wh/events_marts", "sales-wh/events_raw"])
        self.assertEqual(sorted(env_file("local")["FLUID_LAKEKEEPER_NAMESPACE_ALLOWLIST"].split(",")), wanted)

    def stream_source(self, env=None):
        """The stream's Kafka source in the base, or as overlays/<env>.yaml sets it."""
        doc = self.contract if env is None else config.load_yaml(PRODUCT / "overlays" / f"{env}.yaml")
        stream = next(b for b in doc["builds"] if b["id"] == "stream-raw")
        return stream["properties"]["parameters"]["spark"]["source"]

    def test_the_base_and_the_lab_overlay_read_the_profiles_kafka(self):
        kafka = config.load_yaml(PROFILE / "site.yaml")["endpoints"]["kafka"]
        for env in (None, "local"):
            with self.subTest(env or "base"):
                source = self.stream_source(env)
                self.assertEqual((source["bootstrapServers"], source["auth"]),
                                 (kafka["bootstrapServers"], kafka["auth"]))
        self.assertEqual(env_file("local")["FLUID_ACQUISITION_DEPLOYMENT_HOST_ALLOWLIST"],
                         kafka["bootstrapServers"].rsplit(":", 1)[0])


if __name__ == "__main__":
    unittest.main()
