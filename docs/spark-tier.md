<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# The `spark` add-on tier: forge's Spark runner on an open Spark image

The `spark` tier runs forge's Spark runner on Apache Spark 3.5.8 with Iceberg 1.12.0, in an image the lab
builds (`spark-iceberg`). It runs it with `spark-submit` and a local master, against the lab's Iceberg REST
catalog (Lakekeeper) and object store (SeaweedFS). The lab and its end-to-end check run on open software only: no
cloud credentials, no licensed image.

```sh
./lab up --tier spark --name <lab>            # core, plus namespace `spark` and the spark client's grant in Lakekeeper
./lab build fluid-runner --name <lab>         # forge, which runs `fluid apply` in fluid-host
./lab build spark-iceberg --name <lab>        # the Spark image the pods run (network once: Docker Hub, Maven Central)
./lab up --tier spark --name <lab>            # again: fluid-host renders once both builds are recorded
./lab check --tier spark --name <lab>         # every core record, plus `spark`
```

`lab mirror --tier spark` copies no Spark image: nothing of this tier comes from a registry but what core needs.

## The image

`platform/images/spark-iceberg` (its README has the layout and the bump steps). It is built and probed by
`lab build spark-iceberg`, loaded into the lab's kind node and never pushed: the lab publishes no image.

| | |
|---|---|
| Base | `docker.io/apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`, pinned by index digest (amd64 and arm64). Ubuntu 22.04, Python 3.10, JDK 17, user `spark` uid 185 |
| Added | Iceberg 1.12.0 (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`); `hadoop-aws` 3.3.4 and the AWS SDK for Java v1 bundle 1.12.262 (S3A); `kafka-clients` 3.4.1, `commons-pool2` 2.11.1 and the Spark 3.5.8 Kafka token provider; an offline Maven repository with only the Spark 3.5.8 Kafka connector and its poms |
| How it is pinned | Every added file is fetched from Maven Central with `ADD --checksum`. Its coordinate, URL, path and sha256 are in `versions.lock.yaml` (`built_images[spark-iceberg].contents.files`), the one place they are written; `lab build` passes them to the Dockerfile as build args |
| Licence | Apache-2.0 throughout (`THIRD-PARTY.md`). No licence exception, no cloud credentials |
| Tag | `lab.local/spark-iceberg:<Spark>-iceberg<Iceberg>-<8 characters of a hash of the base digest and every file's sha256>`, run with `imagePullPolicy: Never` |

**The probe.** `lab build spark-iceberg` runs the image by id with `--network none` and fails unless:
- the uid is 185 and `spark-submit --version` says 3.5.8;
- every added file has the sha256 the lock records;
- `spark-submit` with the lab's properties file loads `KafkaSourceProvider`, `KafkaConsumer`, Iceberg's
  `SparkCatalog`, the AWS SDK v2 `S3Client`, `S3AFileSystem` and commons-pool2's `GenericKeyedObjectPool`.

It is a check that the build produced what the lock says, run by the image's own tools. It is not an attestation.

**Why an offline Maven repository.** forge 0.36.0 adds `spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
to any Kafka-source build, and `spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar` (the path of an Iceberg jar in another vendor's image) to
every non-Glue build. forge's `merge_conf` lets the operator's properties file replace `spark.jars` and union
`spark.jars.packages` with the file's value first, one coordinate per group and artifact. So the lab's file
(`platform/spark/spark-defaults.apache.conf`) sets `spark.jars` to the two Iceberg jars and `spark.jars.packages` to
the connector at 3.5.8, which wins over forge's 3.5.1. Ivy resolves it from `/opt/lab/maven` through
`/opt/lab/ivysettings.xml`, with no network. The connector's dependencies are already in `/opt/spark/jars` and are
excluded (`spark.jars.excludes`). A Kafka build therefore needs the image alone.

## What `lab check --tier spark` runs

Three steps in namespace `spark`, which enforces Pod Security `restricted`:

1. **Seed.** A Job on the recorded `spark-iceberg` image (uid and gid 185) seeds `raw.events` with three rows and
   empties `curated.events_daily` (`platform/spark/seed.py`).
2. **Apply.** In the `fluid-host` pod, forge runs `fluid apply contract.fluid.yaml --env local --mode amend-and-build
   --build-id daily-curated --yes` on `platform/spark/probe.fluid.yaml`, the command a scheduler runs. The contract is a
   local batch contract: `runtime: local`, Lakekeeper tables in warehouse `sales-wh`, and one Spark SQL
   (`raw.events` to `curated.events_daily`). forge starts one Spark pod on the same image through `lab-spark-submit`.
   forge refuses the contract until the operator allows two things, and `lab check` sets each one, behind
   `VALIDATE_STRICT=true` (`labctl.spark.OPERATOR_ALLOWLISTS`):
   - its SQL, pinned to the reviewed hash (`FLUID_SPARK_CONTRACT_SQL_ALLOWLIST`);
   - the namespaces its local run may write (`FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST`: `sales-wh/raw` and
     `sales-wh/curated`).
3. **Read back.** A second Job on the same image reads `curated.events_daily` (`platform/spark/verify.py`).

The check passes only on exactly `{2026-11-17: 2, 2026-11-18: 1}`, with a snapshot newer than the seed's, so the rows
are this run's write. The contract is copied from ConfigMap `lab-spark` into `/state/spark/lab.spark-probe` in fluid-host,
with its `--env local` overlay, because forge keeps its run state beside the contract.

The `spark` record names the image it ran. `lab check` needs the `spark-iceberg` and `fluid-runner` records in
`<state>/images.json` and the node holding exactly their layers; without them it fails with `./lab build ... --name <lab>`
as the advice. The h-mirror check accepts the recorded image as it accepts the recorded fluid-runner image.

A Job that does not finish is deleted: one that cannot start (an image missing from the node, say) fails
the check at once, a pull error fails it after 300 s of kubelet retries, and an interrupted or timed-out
run deletes its Job too. So no stuck Job holds the node's memory or blocks the next check, and the next
check also deletes a leftover that can never start. A finished Job stays an hour for its logs.

Catalog `fluid` is the lab's properties file (`platform/spark/spark-defaults.apache.conf`) on the image's own, empty
base conf (`/opt/spark/conf` is not in the image):
- Lakekeeper's REST catalog at `https://lakekeeper.lab.test/catalog`;
- OAuth2 client credentials for the Keycloak client `spark`. Its secret reaches the pod from Secret
  `spark-lakekeeper-client` by environment variable, and is appended to the file at run time;
- `X-Iceberg-Access-Delegation: vended-credentials`, so S3FileIO reads and writes SeaweedFS with temporary
  credentials Lakekeeper vends per table. No S3 key is configured for tables;
- the lab CA, as every JVM's only trust store (`JAVA_TOOL_OPTIONS` names a PKCS12 trust store).

`lab up --tier spark` provisions the `spark` client in Lakekeeper and grants it `create` on warehouse `sales-wh`,
nothing more. On OpenFGA a namespace's creator owns it, so Spark writes only its own namespaces.

## The ConfigMap and the image

`lab up` passes the Spark image to the Helmfile render as `LAB_SPARK_IMAGE` (and the fluid-runner image as
`LAB_FLUID_RUNNER_IMAGE`). ConfigMap `lab-spark` then carries:

| Key | Holds |
|---|---|
| `spark-defaults.conf` | the tier's properties file (`spark-defaults.apache.conf`). forge's `FLUID_SPARK_SUBMIT_PROPERTIES_FILE` and the pods read this key |
| `spark-image` | the recorded `lab.local/spark-iceberg:<tag>` (empty until `lab build spark-iceberg` has loaded one) |
| the scripts | `lab-spark-submit`, `lab-spark-entry.sh`, `seed.py`, `verify.py` |
| the probe | `probe.fluid.yaml` and `probe.overlay.local.yaml` |

The image is `lab.local/spark-iceberg:<tag>` with `imagePullPolicy: Never`, run as uid 185, gid 185. Its `spark-submit` is
`/opt/spark/bin/spark-submit`, its base conf directory `/opt/spark/conf` is absent, and the properties file is the whole
conf: catalog, S3A and the image's classpath.

A first `lab up` cannot know the `spark-iceberg` tag: the record is written when `lab build` loads the image into
this lab's node, which needs the cluster. So `lab up --tier spark` does not fail without the record. It renders the
files with an empty `spark-image`, does not render `fluid-host`, and says so. The order on a fresh lab is `lab up`,
`lab build fluid-runner`, `lab build spark-iceberg`, `lab up` again.

## How it runs

| Choice | Why |
|---|---|
| forge's runner, byte for byte what forge uploads | the lab runs forge's own local Spark runtime, not a copy |
| The image is Apache Spark 3.5.8 with Iceberg 1.12.0 | open software only |
| The master is `local[2]` | one pod per build, no executors |
| Catalog `fluid` is Lakekeeper over REST, declared whole | the runner uses it as declared |
| Storage is SeaweedFS with vended STS credentials, through S3A for artifacts | the object store is open and local |
| The smoke run applies the batch build only | the stream is in the e2e product (docs/e2e.md) |
| One pod's settings: event log off, `spark.authenticate` off, region pinned to `us-east-1` | one JVM |

The lab's properties file is shaped as the file forge's local Spark runtime takes
(`FLUID_SPARK_SUBMIT_PROPERTIES_FILE`, the catalog declared whole); `fluid apply --env local` uses it as described next.

## `fluid apply --env local`

forge's `parameters.spark.runtime: local` runs a Spark build on the machine where `fluid apply --env local` runs. It
starts the operator's `FLUID_SPARK_SUBMIT_EXECUTABLE` with a spark-submit argv (a list, never a shell) in a new
session, captures its stdout and stderr as the driver log, and cancels with SIGTERM to the process group, then
SIGKILL after a grace period. forge uploads the runner and `job.json` with boto3 to `FLUID_SPARK_LOCAL_ROOT`
(an `s3://` URL, through `AWS_ENDPOINT_URL_S3`); spark-submit fetches them itself.

In the lab, forge runs in the fluid-runner image, and the build runs in its own pod on the Spark image:

```
fluid-host pod (fluid-runner image)         Pod lab-spark-submit-<id> (Spark image, restricted)
  fluid apply --env local                     lab-spark-entry.sh
    -> lab-spark-submit <spark-submit argv>      -> <image's spark-submit> --master local[2] <argv>
         create Pod, stream its log, exit with its code, delete it
```

**`lab-spark-submit`** (`platform/spark/lab-spark-submit`, stdlib-only Python, shipped in ConfigMap `lab-spark`):
- takes forge's argv and creates one Pod in namespace `spark` from the Spark image, through the in-cluster Kubernetes
  API (ServiceAccount token and CA, `urllib`, no kubectl). The image is read from the read-only file
  `/opt/lab-spark/spark-image`, and it must be `lab.local/spark-iceberg:<tag>`; `LAB_SPARK_IMAGE` may only repeat what
  the file says, or the wrapper refuses, so a shell in fluid-host cannot point it at another image.
  Args reach the Pod with every `$` doubled, because Kubernetes expands `$(VAR)` and `$$` in container args;
- it refuses a `--properties-file` other than `/opt/lab-spark/spark-defaults.conf`, so forge's `merge_conf` and the Pod
  read the same file;
- the Pod is restricted like the smoke Jobs: uid and gid 185, `runAsNonRoot`, no privilege escalation, all
  capabilities dropped, seccomp `RuntimeDefault`, no ServiceAccount token, the same memory limits. It gets the build
  identity's S3 key (`spark-build-s3`), not fluid-host's;
- its entry, `lab-spark-entry.sh`, uses the image's `spark-submit` (`/opt/spark/bin`), drops any `--master`, runs
  `spark-submit --master local[2]`, and turns `--properties-file`
  into the pod's: the image's base `spark-defaults.conf` (none in the image), then the copy of the lab's file mounted
  from the ConfigMap, then the OAuth2 credential from Secret `spark-lakekeeper-client` as a `0600` file in that pod. The
  secret is never in argv, the ConfigMap or a log; Spark's redaction covers the key. It also refuses `--conf` of
  `spark.sql.catalog.fluid*` and `spark.redaction.regex` (exit 65). forge already denies both (`spark.sql.catalog`,
  `spark.redaction.` in `_desired.py` of forge 0.33.0 and later), so this is defence in depth;
- streams the Pod's log to stdout and exits with the container's exit code (125: the wrapper failed, or the Pod ended
  without one). The stream is resumed by time (`timestamps=true`, then `sinceTime` and the boundary line skipped), not
  by byte offset, so kubelet rotating the log file does not corrupt it;
- on SIGTERM, SIGINT or SIGHUP it deletes the Pod and exits 143, 130 or 129. A second signal during the cleanup is
  ignored, and the delete is retried if a signal lands inside it. It deletes the Pod on every other way out too.

A SIGKILL of the wrapper, or a fluid-host restart, cannot run that cleanup, so three more things bound what is left:
- the Pod has an `ownerReference` to the fluid-host Pod (read through `$HOSTNAME`), so Kubernetes' garbage collector
  deletes it with fluid-host. The wrapper refuses to start without it;
- a batch run gets `activeDeadlineSeconds` of 6 h (`LAB_SPARK_POD_DEADLINE_S`, `0` for none). A stream has none:
  forge passes `--fluid-checkpoint` only for a stream;
- at its start the wrapper deletes every other Pod of the same build, found by label `fluid-fabric-lab/spark-build`
  (the 8 hex characters that end forge's `--name`, `fluid_build.spark.names.build_key`), and waits until they are gone,
  so two drivers never share a checkpoint.

Every Pod carries the label `fluid-fabric-lab/spark-submit`: `kubectl -n spark get pods -l fluid-fabric-lab/spark-submit`
lists them, and `lab check`'s `ready` does not count them as problems.

Pod creation, log following and deletion in a `finally` are adapted from Apache Spark's Kubernetes backend and Apache
Airflow's `KubernetesPodOperator`, and the in-cluster client from kubernetes-client/python; the file header names each.

**The artifacts bucket.** `lab up --tier spark` creates SeaweedFS bucket `lab-spark-artifacts` and two S3 identities,
both limited to it. Their keys are generated like the other credentials, kept in OpenBao (`kv/lab/credentials`) and reach
pods as Secrets in `spark`:

| Identity | Secret | Reaches | For |
|---|---|---|---|
| `spark-artifacts` | `spark-artifacts-s3` | the whole bucket: read, list, write, tag | fluid-host: forge uploads the runner and `job.json` under `runs/` |
| `spark-build` | `spark-build-s3` | read and list `runs*`, read, list and write `_checkpoints*` | the build pods: they read what forge uploaded and cannot overwrite it |

SeaweedFS scopes an identity to an object prefix with a wildcard action (`Write:bucket/_checkpoints*`, `CanDo` in
`weed/s3api/auth_credentials.go`); a plain `Write:bucket/prefix` would match the bucket only. The `*` also matches
`runs-other`, so the scoping is by prefix, not by directory. A stream's checkpoint must be declared under
`s3://lab-spark-artifacts/_checkpoints/<product id>/`: forge 0.36.0 refuses a local stream's checkpoint anywhere else.

The properties file maps `s3://` to S3A (`fs.s3.impl`) and sets S3A's endpoint (`https://s3.lab.test`), path-style
access and credentials-from-environment for bucket `lab-spark-artifacts` only (`fs.s3a.bucket.lab-spark-artifacts.*`).
S3A is the image's only S3 client (it speaks the S3 protocol to SeaweedFS), and `hadoop-aws` sits in
`/opt/spark/jars` because spark-submit reads an `s3://` application resource before `spark.jars` load. Table data keeps
using the credentials Lakekeeper vends; neither key has warehouse access, and the Lakekeeper and STS keys have no
artifacts access.

*STS trust.* The role `LakekeeperVendedRole` (`s3:*` on `lake`) trusts only the `lakekeeper` identity:
`"Principal": {"AWS": "arn:aws:iam::000000000000:user/lakekeeper"}`, the form SeaweedFS matches against a static
identity's principal (`callerPrincipalArn` in `weed/s3api/s3api_sts.go`). It was `"*"`, which let any SeaweedFS key assume the
role and reach the warehouse without Lakekeeper's authorisation. **The live proof must show an `AssumeRole` signed with the
`spark-artifacts` key, and one with the `spark-build` key, both refused, and Lakekeeper still vending credentials.**

*An existing lab loads the new identities by itself.* `lab up` writes Secret `seaweedfs-s3-config` before it runs Helmfile,
and the SeaweedFS chart stamps the S3 pod with a checksum of that live Secret (`checksum/s3config`, from `lookup`), so a
changed Secret rolls the S3 pod during `helmfile sync`. The live run should confirm that annotation changed.

**`fluid-host`.** A Deployment of one replica in namespace `spark`, the host where
`fluid apply --env local` runs (a stream's supervisor lives there too). It runs the fluid-runner image idle
(`sleep infinity`), restricted, as ServiceAccount `fluid-host`, whose Role allows `create`, `get`, `list` and `delete`
on pods and `get` on `pods/log` in `spark`, and nothing else (`list` is for the sweep, `get` for its own pod; `watch` is
not needed). It sets `shareProcessNamespace: true`, so the pause container is PID 1 and reaps the orphans that forge's
supervisors and spark-submit wrappers leave, which `sleep` as PID 1 would not. A 1 GiB volume holds forge's run state
(`.fluid/`). ConfigMap `lab-spark` is mounted at `/opt/lab-spark`, and the environment is:

| Variable | Value |
|---|---|
| `FLUID_SPARK_SUBMIT_EXECUTABLE` | `/opt/lab-spark/lab-spark-submit` |
| `FLUID_SPARK_SUBMIT_PROPERTIES_FILE` | `/opt/lab-spark/spark-defaults.conf` |
| `FLUID_SPARK_LOCAL_ROOT` | `s3://lab-spark-artifacts/runs` |
| `AWS_ENDPOINT_URL_S3`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `https://s3.lab.test`; the `spark-artifacts` key from Secret `spark-artifacts-s3` |
| `AWS_CA_BUNDLE`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` | the lab CA (`/etc/lab-ca/ca.crt`) |
| `LAB_SPARK_IMAGE` | the Spark image (must equal `/opt/lab-spark/spark-image`) |
| `FLUID_SPARK_CONTRACT_SQL_ALLOWLIST`, `FLUID_SPARK_LOCAL_NAMESPACE_ALLOWLIST` | the operator allowlists for the probe contract, the ones `lab check` sets |

fluid-host runs the image tag recorded in `<state>/images.json` for `fluid-runner`, as `lab check` reads it. On the
`spark` tier it renders only when `spark-iceberg` is recorded too (the pods it starts need that image), so the order on
a fresh lab is `lab up`, both `lab build`s, then `lab up` again. With no record the Deployment is not rendered. `lab check` fails `ready`
while a rendered fluid-host is not Available. Open a shell in it with `kubectl -n spark exec -it deploy/fluid-host -- bash`.

How the submitter differs from a cluster scheduler:
- The submitter is a separate process from the driver, and the driver is a Pod on the Spark image, with `local[2]`, so
  there are no executor Pods.
- forge uploads the runner and `job.json` to S3 (SeaweedFS) and spark-submit fetches them through S3A with
  prefix-scoped keys.
- The Pod is owned by fluid-host's Pod and has a deadline (batch) and a per-build sweep. A cancel ends the driver Pod,
  and a lost submitter leaves no driver running. The Pod's log, read through the Kubernetes API, is the driver log forge
  captures.

Follow-ups, not done: a `ResourceQuota` on namespace `spark` (pods, memory), so a runaway submitter cannot fill the node,
and a `ValidatingAdmissionPolicy` that admits only pods in `spark` from the Spark image with the service-account
token off. Today the Role lets fluid-host create any pod in `spark`, and namespace Pod Security `restricted` is what bounds
them.

## Evidence

**The open runtime, 2026-11-22.** A spike on a live lab proved the recipe this tier is built on, before the image existed
as a lab build:

| Run | Result |
|---|---|
| Apache Spark 3.5.8 with Iceberg 1.12.0 | wrote and read rows through Lakekeeper's Iceberg REST catalog with OAuth2 client credentials and vended credentials |
| S3A on SeaweedFS | read and wrote `s3://lab-spark-artifacts/runs/...` and `.../checkpoints/...` with the `spark-build-s3` key |
| The Kafka connector | `--packages` resolved it offline with `--network none`, from the image's Maven repository |
| `lab build spark-iceberg` | built and probed with no cluster (the load step was then a warning): 11 files matched the lock, and the six classes loaded with no network |

**Live, 2026-11-22 (forge 0.37.0).** These rows ran an earlier smoke: an init container compiled the probe contract with
`fluid generate iac` and forge's runner ran on its `job.json`. The tier now runs the seed, `fluid apply` and read-back
described above; the next table is its live run.

| Run | Result |
|---|---|
| `./lab build spark-iceberg` | PASS 3/3: the probe ran with no network, and the node holds the image's layers |
| `./lab up --tier e2e` | PASS 10/10. ConfigMap `lab-spark` names `lab.local/spark-iceberg:3.5.8-iceberg1.12.0-c15dd816` |
| `./lab check --tier spark --only spark` (the image ships no `/opt/spark/conf`) | Both scripts copy a base conf only when it exists, and a test pins the missing directory |
| `./lab check --tier spark --only spark` | PASS. forge 0.37.0 compiled the probe contract, and its runner, on Apache Spark 3.5.8 in the open image, wrote `curated.events_daily {'2026-11-17': 2, '2026-11-18': 1}` through Lakekeeper with vended credentials (snapshot 20, after the seed's 19) |
| `./lab check --tier e2e` | `e2e` PASS (docs/e2e.md). Through `lab-spark-submit`, forge's stream read its runner and `job.json` from `s3://lab-spark-artifacts/runs` with S3A and kept its checkpoint under `_checkpoints/`. It resolved the Kafka connector offline and landed 12 events in Iceberg. The batch then wrote its table. Each Spark run was one pod on the open image |

**Live, 2026-11-22 (forge 0.37.0): the local smoke.** The lab was brought up from scratch with `./lab e2e`.

| Run | Result |
|---|---|
| `./lab up --tier e2e`, on the local smoke's code | PASS 10/10 |
| `./lab check --tier e2e --only ready --only capacity --only runner --only spark --only e2e` | PASS 5/5 in about 3.5 minutes. `spark`: a seed Job on the open image wrote 3 rows to `raw.events`. forge 0.37.0 in fluid-host then ran `fluid apply --env local --mode amend-and-build --build-id daily-curated` on the probe contract. Its Spark pod wrote `curated.events_daily {'2026-11-17': 2, '2026-11-18': 1}` through Lakekeeper with vended credentials (snapshot 2, after the seed's 1), and a second Job read it back. `e2e` passed in the same run (docs/e2e.md). No Spark pod was left behind |

**Not proven yet on the open image:** the refusal of an `AssumeRole` signed with the `spark-artifacts` or `spark-build`
key (the STS trust change), and the tier's own memory (the node held core, Kafka, Airflow and fluid-host at 8.1 to
8.4 GiB around the runs; one Spark pod is limited to 2.5 GiB). Offline, the wrapper runs against a fake Kubernetes API
(`tests/launcher/test_spark_submit.py`), and the entry script runs on a fake image layout with a fake `spark-submit`.
