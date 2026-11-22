<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# events-pipeline

The lab's example product: one contract, three builds, one place (`--env local`).

| Build | Engine | What it does | Trigger |
|---|---|---|---|
| `stream-raw` | Spark Structured Streaming (forge's runner) | Kafka topic `events.source` (JSON) to `events_raw.events` | continuous; `fluid apply` keeps it running |
| `daily-curated` | Spark SQL batch (forge's runner) | events per event date, to `events_curated.events_daily` | `0 2 * * *` |
| `hourly-marts` | dbt, model `events_hourly` | events and distinct users per hour from the raw table, with that day's total from `events_daily`, to `events_marts.events_hourly` | `0 * * * *` |

| | |
|---|---|
| Spark | the lab's open image (Apache Spark with Iceberg), run by `spark-submit` in a pod (`runtime: local`) |
| Tables | Lakekeeper (Iceberg REST) on SeaweedFS |
| Kafka | Strimzi, `auth: none` |
| dbt | dbt-trino |

`environments` declares `local` alone, so any other `--env` is refused. `overlays/local.yaml` restates the base
contract's values so `--env local` names a real overlay.

The operator's allowlists are in [profiles/lab-kind/operator/local.env](../../profiles/lab-kind/operator/local.env).
A contract cannot list itself:
- the Kafka hosts;
- the reviewed Spark SQL, pinned by its sha256;
- the namespaces stage 8 may change.

## Compile

```sh
set -a; . profiles/lab-kind/operator/local.env; set +a
fluid validate products/events-pipeline/contract.fluid.yaml --env local
fluid ship products/events-pipeline/contract.fluid.yaml --env local --stage 1-4
```

`fluid ship` runs stage 2 strict. The ledgers in [profiles/lab-kind/expected/](../../profiles/lab-kind/expected/)
list the product with every cell `pending`: nothing of it has run through `lab accept` yet.
