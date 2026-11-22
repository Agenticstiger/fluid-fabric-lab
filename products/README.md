<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Example data products

`stream-batch/` is the lab's end-to-end product: a Kafka topic landed in Iceberg
by a forge Spark stream, and one forge Spark SQL batch over it. `lab check --tier e2e`
runs it through `fluid validate`, `plan` and `apply --env local`, and reads the rows
back (docs/e2e.md); `lab e2e` is the one-command entry point.

`events-pipeline/` is the lab's other example product: a raw Kafka stream, a daily
Spark SQL batch and an hourly dbt model, run with `--env local` only. The ledgers in
`profiles/lab-kind/expected/` also name four products that are not authored,
each to be a canonical `contract.fluid.yaml` plus an on-prem overlay. Their ids
are the row keys below, and every cell is `pending`.

| Product id | What it would prove | Expected status |
|---|---|---|
| `batch-dbt-trino` | A batch dbt-trino model over Iceberg (unrestricted) | Not set |
| `streaming-kafka-iceberg` | Kafka topic to Iceberg through a `v1` KafkaConnector, in two domains (`sales`, `hr`) | `unsupported` in tiers without the streaming add-on |
| `restricted-rows-masks` | `rowFilters` and `columnRestrictions` enforced by Trino + OPA (Mode B) | `unsupported` under `--strict` |
| `duckdb-vended-read` | A DuckDB read of an unrestricted table through a vended-credential attach | `unsupported` |

## How they are exercised

`lab accept` runs all 11 stages for every product with `--strict`, from the CLI
and (in the orchestration and full tiers) from the generated Airflow DAG:

bundle, validate, generate artifacts, validate-artifacts, diff, plan, apply,
policy-apply, verify, publish, schedule-sync.

It passes only when every observed status equals the ledger in both directions.
A skip is never a pass, and changing a cell needs a commit that names the
capability change behind it.

forge is built into the runner image from its release source archive, checked
by sha256, and its dependencies are installed with `--require-hashes`; nothing
from forge is vendored here.

A product may carry an `accept.yaml`: steps that `lab accept` runs between stage 7 and stage 8
(docs/lab-accept.md). `lab accept` exits non-zero until every cell it compares equals its ledger.
