<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# stream-batch: the lab's end-to-end product

`lab.stream-batch` is the smallest FLUID product that needs every part of a
streaming and batch platform:

| Build | What it does | How forge runs it in the lab |
|---|---|---|
| `events-stream` | Spark Structured Streaming: Kafka topic `lab-e2e.events` (JSON) to the Iceberg table `e2e_raw.events`, a micro-batch every 10 seconds, with a checkpoint on SeaweedFS | `fluid apply --env local` starts it and forge's supervisor keeps it running in fluid-host |
| `daily-counts` | Spark SQL batch: events per run and event date into `e2e_curated.events_daily` | `fluid apply --env local --mode amend-and-build --build-id daily-counts`, the command a scheduler runs |

Both tables live in Lakekeeper's Iceberg REST catalog (warehouse `sales-wh`).
Spark reads and writes their data files with credentials that Lakekeeper vends
per table. Spark runs on the lab's open runtime: Apache Spark 3.5.8 with
Iceberg 1.12.0 (the `spark` tier).

`lab check --tier e2e` runs the product and reads the rows back
(docs/e2e.md). `lab e2e` brings the tier up first.

| File | Holds |
|---|---|
| `contract.fluid.yaml` | the contract (fluidVersion 0.7.7) |
| `overlays/local.yaml` | the `--env local` overlay: the base contract's lab values, restated |
| `operator.env` | the operator's allowlists for this product. forge reads them only from the operator's environment, so `lab check` passes them to each forge command it runs |

Changing the batch's SQL changes its reviewed digest. Then update
`FLUID_SPARK_CONTRACT_SQL_ALLOWLIST` in `operator.env`;
`tests/launcher/test_e2e.py` fails until you do.
