<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# The end-to-end run: a streaming and batch product through forge

`lab e2e` brings up a lab, runs one FLUID data product on it through forge,
and checks the rows that the product wrote. The product,
[`products/stream-batch`](../products/stream-batch/README.md), has two builds:

- a Spark Structured Streaming build that lands a Kafka topic in an Iceberg
  table;
- a Spark SQL batch that runs over the landed rows.

Every engine and service involved runs on the lab, from pinned upstream
images:

- Kafka from Strimzi;
- Apache Spark 3.5.8 with Iceberg 1.12.0;
- Lakekeeper's Iceberg REST catalog, with credentials vended per table;
- SeaweedFS as the object store;
- Keycloak for identity.

No cloud account and no external service is used.

## Entry point (for other repositories)

A repository that wants a local end-to-end run calls one command from a
checkout of this repository:

```sh
./lab e2e --name <lab>
```

The command exits 0 only when the run passed. It can be wrapped in a make
target, for example:

```make
# FFL is a checkout of this repository; LAB names the lab.
FFL ?= ../fluid-fabric-lab
LAB ?= e2e

lab-e2e:
	cd $(FFL) && ./lab e2e --name $(LAB)
```

| Option | Effect |
|---|---|
| `--name NAME` | The lab's name. It prefixes the cluster, containers and state directory. Reusing a name reuses that lab |
| `--skip-mirror` | Skip `lab mirror --tier e2e`, when the local mirror already holds the tier |
| `--down` | Run `lab down` at the end. The mirror and the lab's state directory are kept, so the next run starts faster |
| `--https-port PORT` | The Gateway's host loopback port, used only when the cluster is created (default 443) |
| `--json` | Machine-readable output: every step's records |

**Needs:**
- Docker (Docker Desktop, Colima, Rancher Desktop or Docker Engine on Linux)
  with a VM of at least 14 GiB;
- about 30 GiB of free disk;
- the pinned CLI tools (`./lab doctor --tier e2e` lists them);
- the network for the first `lab mirror` and the two image builds. Later runs
  reuse what those fetched.

## What `lab e2e` does

| Step | Command | Notes |
|---|---|---|
| mirror | `lab mirror --tier e2e` | Copies every pinned image of the tier into the local registry by digest, once. The cluster pulls only from there |
| up | `lab up --tier e2e` | Creates or reuses the kind cluster, and installs the core tier, the streaming tier (Kafka), the spark tier and the e2e objects (the topic `lab-e2e.events` and the read-back script) |
| build | `lab build fluid-runner`, then `lab build spark-iceberg` | Builds forge's runner image (forge 0.37.0) and the open Spark runtime on this machine, then loads them into the node. Both are cached after the first build and never pushed |
| up | `lab up --tier e2e` | Runs again now that both images are recorded, so `fluid-host` starts on them |
| check | `lab check --tier e2e --only ready,capacity,runner,e2e` | The lab is healthy, and the product ran (next section) |
| down | `lab down` | Only with `--down` |

The command stops at the first step that fails, and prints that step's records.

## What the `e2e` record proves

`lab check --tier e2e` (and so `lab e2e`) runs the product. The forge steps
run inside the lab's `fluid-host` pod, the stand-in for the machine where
`fluid apply --env local` runs, with forge's own CLI:

1. It copies `products/stream-batch` into `fluid-host`, under
   `/state/e2e/lab.stream-batch`. forge keeps its local runs' state beside the
   contract, so the directory survives between runs.
2. `fluid validate contract.fluid.yaml --env local --strict`.
3. `fluid plan contract.fluid.yaml --env local --out runtime/plan.json`.
4. `fluid apply runtime/plan.json --env local --yes`. This provisions the
   datasets and starts the always-on stream. forge's supervisor keeps it
   running in `fluid-host`, and each Spark run becomes one pod on the open
   runtime (`lab-spark-submit`).
5. It sends 12 JSON events carrying this run's id to the topic. They come from
   a Job on the Strimzi Kafka image, over the listener the stream reads.
6. It waits until Lakekeeper reports 12 more records in `e2e_raw.events`.
7. It stops the stream with forge's own cancel, the protocol that `fluid apply`
   uses. The batch and the read-back then run with no other Spark pod beside
   them.
8. `fluid apply contract.fluid.yaml --env local --mode amend-and-build
   --build-id daily-counts --yes`, the command a scheduler runs.
9. A Job on the open runtime reads both tables back for this run's id.

The record passes only when all of these hold:
- `e2e_raw.events` holds exactly this run's 12 events, with 12 distinct ids;
- `e2e_curated.events_daily` holds exactly `{2026-11-20: 7, 2026-11-21: 5}`
  for this run, and has a new snapshot;
- the stream was stopped cleanly.

Every forge command sees only the lab's own environment variables and the
operator's allowlists in `products/stream-batch/operator.env`. A contract
cannot list itself.

## Evidence

**Live run, 2026-11-22.** It ran forge 0.37.0
and the open Spark runtime. The lab already ran core, the streaming and spark tiers, and the orchestration tier
(Airflow).

| Step | Result |
|---|---|
| `./lab up --tier e2e` | PASS 10/10. KafkaTopic `lab-e2e.events` became Ready; ConfigMap `lab-e2e` carries the read-back script |
| `./lab check --tier e2e --only ready --only capacity --only runner --only e2e` | PASS 4/4 in 95 s |
| `e2e`: forge in fluid-host | `fluid validate --strict`, `fluid plan` and `fluid apply --env local` exited 0. The apply started `events-stream` as one pod on the open image (`lab-spark-submit`) |
| `e2e`: stream | The producer Job sent 12 events for run `e2e-1795350664-aae145`. The stream landed them in `e2e_raw.events` (1 snapshot), and forge's cancel stopped its run (`20261122T123107Z-903de5e1`) |
| `e2e`: batch | `fluid apply --mode amend-and-build --build-id daily-counts` wrote `e2e_curated.events_daily` |
| `e2e`: read-back | A Job on `lab.local/spark-iceberg:3.5.8-iceberg1.12.0-c15dd816` (Spark 3.5.8) read 12 raw rows with 12 distinct ids and `{2026-11-20: 7, 2026-11-21: 5}` for the run |
| After the run | No forge Spark pod was left in namespace `spark`. The node held 8.1 GiB, Airflow included |

**From a clean start.** On a new lab, `./lab e2e` passed 51 of 51 checks in 507 s in the latest run (476 s in the one
before), every step included. That covers the mirror, `lab up` (a new cluster, OpenBao and the tier: 5.6 GiB on the
node after it), the two builds, the second `lab up`
(fluid-host on the recorded images) and the check. The `e2e` record landed this run's 12 events and wrote
`{2026-11-20: 7, 2026-11-21: 5}`. The kind node peaked at about 7.6 GiB (7.28 GiB in the run before). No cloud credentials
were present, and no `aws` command was called.
The time is with warm caches: the mirror already held every image, and both image builds were cache hits. A
first run on a new machine also pulls the images and runs the builds' downloads.

## What it does not prove

- **Kafka authentication.** forge's Kafka source has no SCRAM or mTLS
  (docs/streaming-tier.md), so the stream reads the unauthenticated plain
  listener inside the cluster.
- **Scheduling by Airflow.** The batch runs through the same `fluid apply`
  command that forge's generated DAG task runs, but `lab e2e` does not start
  it from Airflow. The orchestration tier proves the DAG path separately
  (docs/orchestration-tier.md).
- **Trino.** The rows are read back with Spark. Trino's read of the same
  tables is criterion (a) of `lab check`.
