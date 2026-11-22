<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# fluid-fabric-lab

> **For local development only.** Every endpoint binds to 127.0.0.1. Do not
> expose the lab on a network, do not use it with real data, and do not use it
> in production. It is NO-GO for any real use; see
> [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md).

A test lab for [data-product-forge](https://github.com/Agenticstiger/forge-cli)
(the `fluid` CLI, Apache-2.0). It assembles a sovereign, on-prem-shaped data
platform on one [kind](https://kind.sigs.k8s.io/) cluster from pinned
upstream Helm charts (unmodified; the Keycloak Operator's raw manifests sit in
a thin local wrapper chart), so that forge's eleven pipeline stages can be
run against it and their results checked against an expected matrix.

The lab is a thin umbrella: one Helmfile release per upstream component, a
committed chart cache and a lock file. It forks no upstream project and adds
nothing to forge core.

## What it runs

Every version below is pinned in [`versions.lock.yaml`](versions.lock.yaml)
by version and digest. Airflow runs its chart's own 3.2.2. Newer patches are
recorded there as candidates and are not used until they have soaked and
passed a green run.

| Plane | Component |
| --- | --- |
| Cluster | kind v0.33.0, Kubernetes v1.36.4 |
| TLS and ingress | cert-manager v1.21.2, trust-manager v0.25.0, Envoy Gateway v1.9.2 (Gateway API) |
| Metadata database | CloudNativePG 1.30.1 with PostgreSQL 17.11, one cluster shared by all services |
| Object storage | SeaweedFS 4.48 (S3 with STS) |
| Iceberg catalog | Lakekeeper v0.14.0 with OpenFGA v1.21.0 |
| Identity | Keycloak 26.8.0 (operator) |
| Query and policy | Trino 483 with an OPA 1.21.1 sidecar |
| Streaming | Strimzi 1.2.0, Kafka 4.3.1, Kafka Connect with an Apache Iceberg sink |
| Orchestration | Apache Airflow chart 1.22.0 (Airflow 3.2.2) |
| Keys | OpenBao 2.7.1, beside the cluster, not in it |
| Runner | OpenTofu 1.13.1, dbt-trino, DuckDB and forge, in one runner image |

Licences and sources are in [THIRD-PARTY.md](THIRD-PARTY.md).

## Quick start

```sh
./lab doctor --tier core      # does this machine fit the tier?
./lab mirror --tier core      # once, with the network: copy every pinned image by digest
./lab up --tier core          # kind + OpenBao + the tier, pulling only from the mirror
./lab build fluid-runner      # build forge's runner image here and load it into the lab (never pushed)
./lab check                   # each criterion on real evidence; exit 1 until all pass
./lab down                    # remove the cluster and OpenBao; their state is kept
```

[docs/lab-up.md](docs/lab-up.md) explains each step, the names, ports and
state directory, and how to reach the `*.lab.test` endpoints.

## Tiers

The lab installs in tiers so that the base fits a 16 GB laptop. [`tiers.yaml`](tiers.yaml)
holds the memory budgets the footprint check enforces.

| Tier | Adds | Memory target (container VM) |
| --- | --- | --- |
| `core` | SeaweedFS, Postgres, Lakekeeper and OpenFGA, Keycloak, single-node Trino with OPA, cert-manager, gateway, runner Job; OpenBao beside the cluster | at most 8 GiB (tight) |
| `streaming` (add-on) | Strimzi and single-node KRaft Kafka (plain and TLS+SCRAM listeners); Kafka Connect is not installed ([docs/streaming-tier.md](docs/streaming-tier.md)) | core + about 1.1 GiB measured for Kafka alone (the 5 GiB budget leaves room for Connect) |
| `orchestration` (add-on) | Airflow with the LocalExecutor on the lab-built `airflow-fluid` image, reading forge's generated DAGs from S3 on SeaweedFS ([docs/orchestration-tier.md](docs/orchestration-tier.md)) | core + about 3 GiB |
| `spark` (add-on) | forge's Spark runner on the lab-built `spark-iceberg` image (Apache Spark 3.5.8 with Iceberg, S3A and the Kafka connector; `spark-submit`, local master), against Lakekeeper and SeaweedFS; open software only ([docs/spark-tier.md](docs/spark-tier.md)) | core + about 3 GiB |
| `full` | core, streaming and orchestration; not the `spark` add-on, which stays opt-in (no room for a Spark pod) | about 16 GiB: needs a 32 GB machine |

On a 16 GB machine, run `core` plus one add-on at a time. A tier that does not
fit fails the run; it never skips.

## How forge is used

forge is not vendored here. The runner image
([platform/images/fluid-runner](platform/images/fluid-runner/README.md)),
built on the operator's machine by `lab build fluid-runner`, builds forge
from a release pinned by commit (a checksum-verified source archive;
dependencies with `--require-hashes`). `lab accept` runs every example data
product through all eleven `fluid` stages with `--strict` (no override), from
the CLI ([docs/lab-accept.md](docs/lab-accept.md); no live run has been made
yet):

bundle, validate, generate artifacts, validate-artifacts, diff, plan, apply,
policy-apply, verify, publish, schedule-sync.

A run passes only if every stage's status equals the expected status, in both
directions: an unexpected `passed` fails just as an unexpected `failed` does.
Capabilities the stack does not have are expected to report
`unsupported`.

## Repository layout

| Path | What it is |
| --- | --- |
| `versions.lock.yaml` | Every chart, image, tool and schema: version, digest, licence, signature mode, SBOM status |
| `tiers.yaml` | Releases and memory budget per tier |
| `lab`, `labctl/` | Standard-library Python launcher: `doctor`, `render`, `l0`, `mirror`, `build`, `up`, `down`, `check`, `e2e`, `accept` |
| `conformance/` | The reports `lab accept` writes: evidence, committed on purpose ([conformance/README.md](conformance/README.md)) |
| `platform/helmfile.yaml.gotmpl` | One Helmfile release per component |
| `platform/images/fluid-runner/` | forge's runner image: Dockerfile, hash-pinned requirements, the OpenTofu provider lock |
| `platform/namespaces.yaml` | The namespaces `lab up` creates, with their Pod Security levels and exemptions |
| `platform/identity/realm-import.yaml` | The Keycloak realm `fluid` and its clients (secrets are placeholders filled from OpenBao) |
| `platform/charts/` | Digest-checked chart cache, so rendering needs no network |
| `platform/schemas/` | Offline kubeconform schemas for Kubernetes v1.36.4 and the vendored CRDs |
| `tools/l0/` | The L0 gates: render, schemas, licence gate, image denylist, brand fence, secret scan, REUSE, version lint, footprint floor, exposure (nothing listens beyond this machine), third-party notices |
| `spikes/` | The PG proof gates, run as throwaway pods against a lab ([spikes/README.md](spikes/README.md)) |
| `docs/proven.md` | What has been run, with the commands and results, and what is not proven yet |
| `docs/pins.md` | Why each pin was chosen |

## What is proven

**What runs.** The `core` tier and the `streaming`, `orchestration` and `spark`
add-ons install on kind from the local mirror (see Tiers above). The `e2e`
tier adds one streaming and batch product on top of `core`, `streaming` and
`spark`. Three images are built here: `fluid-runner`, `airflow-fluid` and
`spark-iceberg`.

**What `lab check` verifies.** It writes one record per criterion, each from
evidence read off the running lab: every workload is ready; the machine has
the memory and disk; the catalog vends temporary storage credentials; Trino
accepts Keycloak JWTs and OPA decides per principal; OpenBao survives a
restart with its keys; Lakekeeper denies an unauthorized request; the
installation pulled only from the local mirror; every endpoint is HTTPS and
rejects a wrong CA; and each add-on's own record. A criterion the lab cannot
prove fails and says what it needs. It never passes by default. On `core`,
9 of 16 records pass and 7 fail this way; the list is in
[docs/proven.md](docs/proven.md#not-proven-yet).

**What `lab e2e` verifies.** It runs a streaming and batch product through
forge and checks the rows it wrote ([docs/e2e.md](docs/e2e.md)). From a
clean start, with no cloud credentials present, it passes 51 of 51 checks.

**Where it runs.** macOS or Linux with a Docker engine (Docker Desktop,
Colima, Rancher Desktop or Docker Engine) and kind. The images build for arm64
and amd64. The end-to-end run is proven on arm64. The offline checks run on
Linux in CI. Memory per tier comes from [`tiers.yaml`](tiers.yaml).

The commands and results behind these claims are in
[docs/proven.md](docs/proven.md).

## Requirements

A Docker engine with a Linux VM of at least 8 GiB for `core`: Colima, Rancher
Desktop or Docker Engine on Linux. Docker Desktop also works, but check its
licence terms for your organisation.
The L0 checks need only the pinned CLI tools and Python 3.10 or newer; see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Naming

This project is not related to the CNCF **Fluid** project
(`fluid-cloudnative/fluid`) and uses none of its `fluid.io` API groups or
labels. Any API group, label or annotation prefix the lab introduces is owned
by Agentics. "FLUID" here refers to the data-product contract format used by
data-product-forge.

All other product names are used only to identify the upstream projects the
lab runs. Their use implies no affiliation or endorsement.

## Licence

Apache-2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE). Contributions are
accepted under the same licence with a DCO sign-off; see
[CONTRIBUTING.md](CONTRIBUTING.md). Security reports: [SECURITY.md](SECURITY.md).
