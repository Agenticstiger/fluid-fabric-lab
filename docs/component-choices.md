<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Component choices for a streaming and batch product

What a FLUID product with a Kafka stream, a Spark job and a batch transform
needs from the lab: a Spark 3.5 runtime with Iceberg, a workflow scheduler, a
Kafka-compatible broker and an S3-compatible object store. This page compares
the options for each, against three tests:

- **Licence.** The lab is an Apache-2.0 umbrella. A component on the default
  path must be Apache-2.0, MIT or another licence the L0 licence gate accepts.
  AGPL, SSPL, BUSL and unlicensed code are denied (`tools/l0/licence.py`).
- **Footprint.** `core` must fit an 8 GiB container VM; each add-on has its own
  budget in `tiers.yaml`.
- **Fit on kind.** One node, pulls only from the lab's mirror or `kind load`,
  Pod Security `restricted` where it can be enforced.

Checked on 2026-11-22. Licence fields come from the GitHub API
(`gh api repos/<owner>/<repo>`); the L0 gate reads the licence text itself for
anything the lab vendors. Release dates come from the projects' GitHub
releases or registry tags.

## Summary

| Need | Pick | Licence | Status in the lab |
|---|---|---|---|
| Spark 3.5 with Iceberg | Apache Spark 3.5.8 (`apache/spark` image) plus Iceberg 1.12.0, built by `lab build spark-iceberg` | Apache-2.0 (image and every added jar) | The spike below passed, and the `spark` tier runs it |
| Workflow scheduler | Airflow 3.2.2 on chart 1.22.0 (already pinned) | Apache-2.0 | Built and checked on a live lab (docs/orchestration-tier.md) |
| Kafka-compatible broker | Strimzi 1.2.0 with Kafka 4.3.1 (already pinned) | Apache-2.0 | Live: `lab check --tier streaming` PASS on 2026-11-22 (docs/streaming-tier.md) |
| S3-compatible object store | SeaweedFS 4.48 with STS (already pinned) | Apache-2.0 | Live: STS credentials vended through Lakekeeper (`lab check` a-catalog). Open upstream advisories: see the object-store section |

## Spark 3.5 runtime with Iceberg

| Option | Licence | Latest (2026-11-22) | Fit | Verdict |
|---|---|---|---|---|
| `apache/spark` image, published by the Apache Spark project | Apache-2.0 | 3.5.8 (image tag `3.5.8-scala2.12-java17-python3-ubuntu`; Spark 3.5.9 is on Maven Central, but its image is not published yet) | Ubuntu 22.04, Python 3.10, JDK 17, non-root uid 185, amd64 and arm64. It holds Spark and Hadoop 3.3.4 only, so the lab adds Iceberg, the Kafka connector and S3A | **Pick**, as a lab-built image |
| Kubeflow Spark Operator | Apache-2.0 | v2.6.0, 2026-11-11 | Adds an operator and a webhook. forge's local runtime runs `spark-submit` in client mode and needs no operator | Not needed now |
| Apache Spark Kubernetes Operator | Apache-2.0 | 1.1.0, 2026-11-12 | Young (about 330 GitHub stars); same reason as above | Not needed now |
| Bitnami Spark images | Bitnami terms; the lab denies every `bitnami*` image | n/a | n/a | Denied by the L0 denylist |

**Iceberg version.** Iceberg 1.12.0 (2026-09-30) still publishes
`iceberg-spark-runtime-3.5_2.12` and `iceberg-aws-bundle`, checked in Maven
Central's metadata. The lab's Kafka Connect sink pin is the same 1.12.0.
Iceberg 1.11.0 deprecated Spark 3.4, and Spark 3.5 is still maintained.

**What the lab image adds**, each file fetched from Maven Central and pinned by
sha256 (Maven's `.sha1` matched as well):

| Where | Files | Why |
|---|---|---|
| `/opt/lab/jars` (named by `spark.jars`) | `iceberg-spark-runtime-3.5_2.12-1.12.0`, `iceberg-aws-bundle-1.12.0` | Iceberg catalog and S3FileIO with vended credentials |
| `/opt/spark/jars` (always on the class path) | `hadoop-aws-3.3.4`, `aws-java-sdk-bundle-1.12.262`, `kafka-clients-3.4.1`, `commons-pool2-2.11.1`, `spark-token-provider-kafka-0-10_2.12-3.5.8` | S3A for `s3://` artifacts and stream checkpoints. spark-submit needs hadoop-aws itself, to fetch an `s3://` application before `spark.jars` load. Also the Kafka connector's dependencies |
| `/opt/lab/maven`, an offline Maven repository, with `/opt/lab/ivysettings.xml` | `spark-sql-kafka-0-10_2.12-3.5.8` (jar and pom), `spark-parent_2.12-3.5.8.pom`, `apache-18.pom` | forge adds `spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` to every Kafka-source build. Its `merge_conf` lets the operator's properties file go first, so the file names 3.5.8, and Ivy resolves it from the image with no network |

The versions match Spark 3.5.8's own build (`kafka.version` 3.4.1,
`hadoop.version` 3.3.4 and `commons-pool2.version` 2.11.1 in
`spark-parent_2.12-3.5.8.pom`). `aws-java-sdk-bundle` is the AWS SDK for Java
v1, which Hadoop 3.3.4's S3A requires. It is 280 MB and has reached end of
support upstream. The lab uses it only for its own artifacts bucket. Table data
goes through Iceberg's S3FileIO, on the SDK v2 inside `iceberg-aws-bundle`.

**Spike, 2026-11-22.** All of it ran
in namespace `spark` under Pod Security `restricted`, as uid 185.

| Step | Result |
|---|---|
| `spark-submit` with `--network none`, `spark.jars.ivySettings` set to the offline repository, and the Kafka coordinate in `spark.jars.packages` | Ivy reported `found org.apache.spark#spark-sql-kafka-0-10_2.12;3.5.8 in lab-offline`, 1 artifact copied. These classes all loaded: `KafkaSourceProvider`, `KafkaConsumer`, `SparkCatalog`, `S3Client` (SDK v2), `S3AFileSystem` and commons-pool2's `GenericKeyedObjectPool` |
| A pod with the `spark` client's OAuth2 credentials and `X-Iceberg-Access-Delegation: vended-credentials` | Created namespace `spike_oss` and an Iceberg table through Lakekeeper, appended 5 rows and read back 5, on Spark 3.5.8. No S3 key was configured for table data |
| The same pod reading Kafka over the `plain` listener | Connected to `lab-kafka-kafka-bootstrap.kafka.svc:9092` and read `lab-check.events` as a batch, with no error |
| S3A with the `spark-build-s3` key | Read 3 lines from `s3://lab-spark-artifacts/runs/spike/hello.txt` and wrote and read back a Parquet file under `checkpoints/` |

## Workflow scheduler: Airflow

| Option | Licence | Latest (2026-11-22) | Fit | Verdict |
|---|---|---|---|---|
| Airflow 3.2.2 on the official chart 1.22.0 | Apache-2.0 | Chart 1.22.0 (2026-06) is the newest chart; Airflow 3.4.0 came out on 2026-11-06 | The chart's own appVersion. LocalExecutor, with the database on the lab's CloudNativePG. No Redis, StatsD or Bitnami Postgres | **Keep** (already pinned) |
| Airflow 3.4.0 on chart 1.22.0 | Apache-2.0 | 3.4.0 | Newer than the chart's appVersion; running it on this chart is unverified | Candidate after the 3.2.2 live run and a soak |
| Airflow 2.10 | Apache-2.0 | 2.11.2 is the last 2.x release | Airflow 2 reached end of life on 2026-04-22 (Airflow's supported-versions page). Chart 1.22.0 needs Airflow 2.11.0 or later, so 2.10 would need chart 1.19.0 | **Not offered.** A DAG written for 2.10 can still be parse-tested without a running Airflow |

## Kafka-compatible broker

| Option | Licence | Latest (2026-11-22) | Fit | Verdict |
|---|---|---|---|---|
| Strimzi operator with Apache Kafka (KRaft) | Apache-2.0 (CNCF) | Strimzi 1.3.1, 2026-11-04. The lab pins 1.2.0, and 1.3.0 is a recorded candidate | One KRaft node in about 1.1 GiB at rest (measured). Its `kafka.strimzi.io/v1` CRDs are the connector delivery that the on-prem spec chose (7.5). Topics, users and ACLs are declarative | **Keep** (already pinned) |
| `apache/kafka` image alone | Apache-2.0 | Kafka 4.x | No operator, so topics, users and ACLs would be scripts, and there is no `KafkaConnector` resource | Not chosen: it gives up the operator's delivery API |
| Redpanda | BUSL-1.1 (source-available). The GitHub API reports no SPDX licence and, on 2026-11-22, an archived repository | n/a | Denied on the default path by the licence gate (BUSL) | Denied |
| AutoMQ | Apache-2.0 | 1.7.5, 2026-11-19 | A Kafka fork that stores data on S3. It needs an object-store write path and more memory than one KRaft node | Not needed for a lab |
| Tansu | Apache-2.0 | `tansu-io/tansu` now redirects to `nisshi-io/solace-nisshi` | Not a Kafka fork; its storage is pluggable (S3, Postgres). Young | Not chosen |

## S3-compatible object store

| Option | Licence | Latest (2026-11-22) | Fit | Verdict |
|---|---|---|---|---|
| SeaweedFS | Apache-2.0 | 4.54, 2026-11-16. The lab pins 4.48 | STS `AssumeRole` is what Lakekeeper uses to vend per-table credentials, proven live in the lab. Single node, small, path-style S3, prefix-scoped identities | **Keep**, with the advisories below recorded |
| MinIO | AGPL-3.0 | The repository was archived in 2026, and community binaries stopped in October 2025 | AGPL is denied on the default path | Denied |
| Garage | AGPL-3.0 | active | AGPL is denied on the default path | Denied |
| RustFS | Apache-2.0 | 1.0.6, 2026-11-15 | Documents STS `AssumeRole` and `AssumeRoleWithWebIdentity`. The 1.0 line is new, and the GitHub API lists 42 advisories for 2026, 4 of them critical | Watch; a possible second object store |
| Ceph RGW (Rook) | LGPL-2.1 and others (the GitHub API reports NOASSERTION) | active | Mature STS, but heavy: mon, mgr and OSDs on loop devices | The `lab-ceph` profile, CI only, never on laptops |
| Apache Ozone | Apache-2.0 | active | Has an S3 gateway, but a full cluster is heavier than SeaweedFS. STS was not checked | Not chosen |

**SeaweedFS advisories (recorded, not fixed here).** SeaweedFS published a batch
of security advisories on 2026-11-09. Several affect 4.48 and list no patched
version, among them:

- GHSA-p983-2j86-34vj (critical): STS temporary credentials can be recovered from the session token;
- GHSA-25hp-4mw7-9c75 (critical): unauthenticated admin access to the S3 Tables, Iceberg and Lance APIs;
- GHSA-m3m8-mrgq-hf9h (critical): the `weed admin` HTTP server is unauthenticated by default;
- GHSA-5vhw-52xr-8x4j (medium): `AssumeRoleWithWebIdentity` ignores trust policies when `policy.defaultEffect` is Allow.

The lab binds every host port to 127.0.0.1, runs one user's workloads, and does
not run `weed admin`. It is not for production (PRODUCTION-READINESS.md). The
L0 gate `exposure` checks, on every render, that the admin server is not run
and that SeaweedFS is reachable only inside the cluster or through the Gateway
on 127.0.0.1. A version bump lands only once an upstream release names these fixes, and after
the soak and a green run. Ceph RGW (`lab-ceph`) and RustFS are the
alternatives if the advisories stay open.

## Receipts

```
Checked locally:
- docs/proven.md, docs/spark-tier.md, docs/streaming-tier.md, docs/orchestration-tier.md,
  docs/pins.md, tiers.yaml, versions.lock.yaml (Strimzi 1.2.0 / Kafka 4.3.1, SeaweedFS 4.48,
  Airflow chart 1.22.0 / 3.2.2 already pinned)
- forge 0.36.0 wheel: fluid_build/build_runners/spark/_desired.py (_submit_conf: spark.jars,
  spark.jars.packages), _local.py (merge_conf, FLUID_SPARK_LOCAL_ROOT and its S3A note)
- apache/spark:3.5.8-scala2.12-java17-python3-ubuntu (crane config; /opt/spark/jars listing)

Searched (2026-11-22):
- "Apache Spark 3.5.8 release notes 2026" -> 3.5.8 released January 2026
- "Apache Iceberg 1.10 1.11 release Spark 3.5 support" -> Spark 3.5 maintained; 1.11 deprecates 3.4
- "open source S3-compatible object storage Apache-2.0 STS ..." -> SeaweedFS, RustFS Apache-2.0; Garage, MinIO AGPL
- "Kafka-compatible broker open source license comparison ..." -> Kafka, AutoMQ, Tansu Apache-2.0; Redpanda BSL
- "apache airflow helm chart 1.23 1.24 release" -> chart 1.22.0 is the newest; its minimum is Airflow 2.11.0
- "MinIO community edition maintenance mode archived" -> maintenance mode Dec 2025, archived Feb 2026
- "SeaweedFS STS AssumeRole ... Lakekeeper", "RustFS STS AssumeRole ...", "kubeflow spark-operator ..."
- "Apache Airflow 2.x end of life" -> 2026-04-22
- gh api repos/<r> (licence, archived, pushed) for seaweedfs, rustfs, garage, minio, redpanda, automq,
  tansu, strimzi, kafka, ozone, ceph, kubeflow/spark-operator, apache/spark-kubernetes-operator,
  apache/spark, apache/iceberg, apache/airflow; releases/latest for the same
- gh api repos/<r>/security-advisories for seaweedfs, rustfs, strimzi, kafka, lakekeeper
- crane ls docker.io/apache/spark; crane ls quay.io/strimzi/operator
- repo1.maven.org maven-metadata.xml for iceberg-spark-runtime-3.5_2.12, iceberg-aws-bundle,
  spark-sql-kafka-0-10_2.12, hadoop-aws; each pinned jar and pom downloaded, sha256 computed, .sha1 checked
- Fetched: spark-sql-kafka-0-10_2.12-3.5.8.pom and spark-parent_2.12-3.5.8.pom (dependencies, versions)

Prior art found:
- apache/spark (Apache-2.0) and its official image: the runtime, unchanged
- apache/iceberg 1.12.0 (Apache-2.0): Spark runtime and AWS bundle jars
- Spark's own SparkSubmitUtils (spark.jars.ivySettings, spark.jars.excludes): offline package resolution
- strimzi/strimzi-kafka-operator, seaweedfs/seaweedfs, apache/airflow chart: already in the umbrella

Reuse strategy: depend on upstream images and Maven artifacts, pinned by digest and sha256; build
one lab image FROM apache/spark (never pushed); write no Spark code.
Rationale: the open image plus four jar groups gives forge's local runtime what it needs,
under Apache-2.0, with no cloud account and no network at run time.
```
