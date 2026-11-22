<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# The `orchestration` add-on tier: Airflow reading DAGs from SeaweedFS

The `orchestration` tier runs Apache Airflow 3.2.2 the way forge's generated DAGs expect: the DAGs are objects in an S3
bucket, and each task is `fluid apply` on a product checkout. Here the bucket is `lab-airflow` on the lab's SeaweedFS,
and Airflow reads it with the Amazon provider's S3 bundle, which is an S3 client and talks the S3 protocol to
SeaweedFS.

```sh
./lab mirror --tier orchestration            # once, with the network: the pinned images of the tier
./lab up --tier orchestration --name <lab>   # core, plus Airflow; stops and says so if the image below is missing
./lab build airflow-fluid --name <lab>       # the Airflow image with forge (needs the network once); then `lab up` again
./lab check --tier orchestration --name <lab>   # every core record, plus `orchestration`
```

The first `lab up` on a new lab creates the cluster and then stops at the Helmfile step with
"no lab.local/airflow-fluid", because the Airflow pods run a lab-built image that is never
pulled: build it, and run `lab up` again (finished steps are reused). `lab build fluid-runner`
is also needed by the core `runner` record, as on every tier.

## What runs

| Piece | How |
|---|---|
| Image `lab.local/airflow-fluid:<forge version>-<commit>` | `./lab build airflow-fluid` ([platform/images/airflow-fluid](../platform/images/airflow-fluid/README.md)): the official `apache/airflow:3.2.2` by digest, with forge, dbt-core, dbt-trino and DuckDB in their own venv `/opt/fluid/venv`, so Airflow's package pins are untouched. forge is the same commit and archive checksum as fluid-runner's (one pin, `contents.pins_from` in `versions.lock.yaml`), from the same hashed requirements. `fluid` and `dbt` are on PATH for the airflow user. Never pushed; loaded into kind; the pods use `imagePullPolicy: Never` |
| Airflow chart 1.22.0 (`platform/charts/airflow-1.22.0.tgz`, values in `platform/components/airflow/values.yaml`) | LocalExecutor; api-server, scheduler and dag-processor. No triggerer, redis, statsd, pgbouncer, or the chart's Bitnami PostgreSQL. Tasks run in the scheduler pod |
| Metadata DB | The lab's CNPG Postgres, role and database `airflow`. The chart reads Secret `airflow-metadata` (key `connection`), which `lab up` writes from OpenBao with the role's password |
| Pod Security | namespace `airflow` enforces `restricted`: uid 50000, no privilege escalation, no capabilities, `RuntimeDefault` seccomp. Checked on the offline render (every pod of the release, init containers included); not yet measured on a running lab |
| Login | No default user. Airflow 3's Simple auth manager has one user, `lab-admin` (role admin), whose generated password is `airflow.admin_password` in OpenBao and Secret `airflow-simple-auth`. `POST /auth/token` with it returns the token for the REST API. The auth manager is documented as "for development and testing": acceptable for a lab on loopback, not for anything exposed |
| Gateway | There is no Airflow route on `*.lab.test` yet; reach the UI and API with `kubectl -n airflow port-forward svc/airflow-api-server 8080` |

The committed Helmfile renders the pinned `apache/airflow` image by digest, because the L0 gates require every
rendered image to be a lock digest and a never-pushed image has none. `lab up` runs the lab-built one by passing
`--state-values-set labImages.airflow=lab.local/airflow-fluid:<tag>` (the tag `lab build` recorded) with
`imagePullPolicy: Never`. So `lab render` and the gates show the base image; a running lab runs `airflow-fluid`.

## Where the DAGs come from

DAGs are read from `s3://lab-airflow/dags/`, not from a bind mount, by an Airflow DAG bundle
(`airflow.providers.amazon.aws.bundles.s3.S3DagBundle`, from the Amazon provider in the official
image). The bundle's connection is `lab_s3`, an environment connection
(`AIRFLOW_CONN_LAB_S3`, from Secret `airflow-s3-connection`): endpoint `https://s3.lab.test`,
path-style addressing, verification against the lab CA (`/etc/lab-ca/ca.crt`, trust-manager's
ConfigMap). S3DagBundle reaches a non-AWS endpoint through that connection's `endpoint_url`
extra, so no sync sidecar is needed for DAGs. The bundle refreshes every 30 s.

- The bucket is created by `lab up` (`weed shell s3.bucket.create`, like the warehouse bucket),
  together with a marker object `.lab-keep` under `dags/` and `projects/`: the bundle refuses a
  prefix that does not exist (provider 9.29.0, `check_for_prefix`), and without a marker the
  dag-processor would crash before the first DAG. The prefix is configured as `dags/`, with the
  slash, so `dags-old/` is not read as well.
- Airflow reads with SeaweedFS identity `airflow`: **read and list on bucket `lab-airflow`,
  nothing else**. Its key is generated into OpenBao and reaches pods only through the connection
  Secret. Whoever uploads (CI, `lab check`) uses identity `airflow-publish`: the same bucket plus
  write and delete. Its key is in OpenBao (`kv/lab/credentials`, `s3.airflow_publish.*`) and in no pod.
- forge's stage 11 is `fluid schedule-sync --scheduler airflow --dags-dir dist/artifacts/schedule/
  --destination s3://lab-airflow/dags`; point it at the lab with `AWS_ENDPOINT_URL_S3=https://s3.lab.test`,
  the lab CA, and the `airflow-publish` key.

## Where `fluid apply` finds its project

forge's DAG task runs `cd "$FLUID_PROJECT_DIR"` and `fluid apply "$FLUID_PROJECT_DIR/<contract path>" --env <env>
--mode amend-and-build --build-id <id> --yes`. `FLUID_PROJECT_DIR` is `/opt/fluid/projects` in the
scheduler pod, filled from `s3://lab-airflow/projects/` by a sidecar (`platform/airflow/sync_projects.py`: boto3,
the same image and connection, read-only, one full listing every 30 s, last good copy kept on a failed pass).
**CI uploads the product checkout under `projects/`, next to the DAGs, keeping the relative paths**, so a DAG
that names `contract.fluid.yaml` finds `projects/contract.fluid.yaml`.

This is a sidecar and not a second S3DagBundle on purpose: a bundle is materialised only for the DAGs
that belong to it, so the scheduler would never have the files, and the dag-processor would parse the project's
Python files as DAGs.

## The scheduler's environment

What a fluid host would have set, as operator settings (forge's generated DAG starts `fluid` with `env -i` and
passes only `PASSTHROUGH_PREFIXES` and `PASSTHROUGH_NAMES` of `fluid_build/schedulers/airflow/fluid_apply.py`
at forge `e37cdda5`; every variable below starts with `FLUID_`, `AWS_` or `DBT_`, or is a listed name, so it
gets through. `AIRFLOW_CONN_LAB_S3` starts with `AIRFLOW`, which never passes, so the S3 key does not reach fluid):

| Variable | Value |
|---|---|
| `FLUID_PROJECT_DIR` | `/opt/fluid/projects` |
| `FLUID_SPARK_SUBMIT_EXECUTABLE`, `FLUID_SPARK_SUBMIT_PROPERTIES_FILE` | `/spark/lab-spark-submit`, `/spark/spark-defaults.conf` (below) |
| `FLUID_SPARK_LOCAL_ROOT` | `/opt/fluid/spark` (an emptyDir) |
| `AWS_ENDPOINT_URL_S3`, `AWS_REGION`, `AWS_CA_BUNDLE`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` | `https://s3.lab.test`, `us-east-1`, the lab CA |
| `DBT_PROFILES_DIR` | `/opt/lab/dbt`: profile `fluid`, target `local`, dbt-trino to `trino.trino.svc:8080`, catalog `lakehouse`, user `fluid-pipeline`, method none (`platform/airflow/profiles.yml`) |
| `FLUID_COMMAND_CENTER_ENABLED`, `DBT_SEND_ANONYMOUS_USAGE_STATS`, `DBT_VERSION_CHECK` | `false`, baked into the image |

The three `FLUID_SPARK_*` names are the ones forge's local Spark runtime reads (forge 0.33.0 and later); they point at
the lab's `lab-spark-submit` script and its properties file.

**The Spark submit script.** `/spark/lab-spark-submit` is a file of ConfigMap `lab-spark`, which the spark tier owns in
`platform/spark/` (with `spark-defaults.conf`). A ConfigMap cannot be mounted across namespaces,
so the scheduler cannot mount `spark/lab-spark`; `lab up` copies `platform/spark/lab-spark-submit` and
`spark-defaults.conf` into a ConfigMap `lab-spark` in namespace `airflow`, which the scheduler mounts at `/spark`
(optional, mode 0555). Without `platform/spark/lab-spark-submit` in the checkout the copy is skipped, with a note,
and a Spark build fails in `fluid apply`; every other build is unaffected. The orchestration tier does not run the
spark tiers (they need a Spark image); this is the scheduler's side of the local Spark runtime. The properties file it copies is the spark tier's (`platform/spark/spark-defaults.apache.conf`).

A Role in namespace `spark` lets the scheduler's ServiceAccount (`airflow-scheduler`) `create`, `get`, `list`, `watch`
and `delete` pods and `get` `pods/log` there, which is what `lab-spark-submit` needs, and nothing else (the chart's
own pod-launcher Role is off). Namespace `spark` is therefore also part of the orchestration tier (it runs nothing
between checks).

## What the Airflow identities can and cannot reach

- `airflow` (read, list on `lab-airflow`) is in every Airflow pod as `AIRFLOW_CONN_LAB_S3`; `airflow-publish` uploads.
  SeaweedFS lets any signed identity assume a role whose trust policy matches it. The vended role
  `LakekeeperVendedRole` (`s3:*` on the warehouse bucket `lake`) therefore trusts only Lakekeeper's own principal
  (`credentials.LAKEKEEPER_PRINCIPAL`), not `*`, or either Airflow key could read, write and delete the whole
  warehouse past Lakekeeper and OpenFGA. `lab check` proves it: AssumeRole of the role with each Airflow key must be
  refused, and with Lakekeeper's key accepted (a positive control, so the refusal means something). No explicit
  `Deny` statement is added: the narrowed trust already refuses these keys, and whether SeaweedFS honours an explicit
  deny on the AssumeRole path was not verified here.
- The Role in namespace `spark` lets the scheduler create pods there, and those pods can mount
  `spark-lakekeeper-client` (the `spark` client's OAuth secret) and read it back through `pods/log`. That is by design:
  `lab-spark-submit` runs Spark pods that authenticate to Lakekeeper with that client. Anyone who can run a DAG on this
  scheduler can therefore act as the `spark` client (create namespaces in `sales-wh`, own what it creates). The lab
  trusts its DAG authors; do not give DAG upload to anyone you would not give that client.

## What `lab check --tier orchestration` runs

Record `orchestration`, only on a tier that includes the `airflow` release:

1. confirms the node holds the `airflow-fluid` layers `lab build` recorded;
2. refuses to go on if an Airflow key can assume the warehouse role (above);
3. uploads a one-task probe DAG (`platform/airflow/probe_dag.py`, with this run's random nonce) to
   `s3://lab-airflow/dags/lab-check/` as `airflow-publish`;
4. waits up to 7 minutes until Airflow's REST API (the api-server, through a port-forward, with a token from
   `POST /auth/token`) lists THIS upload (the DAG carries a `nonce-<nonce>` tag; an earlier copy does not count);
5. unpauses and triggers a run and waits for it;
6. requires `success` and the marker the task printed: the nonce, `fluid --version`, and the version of the forge
   distribution in the image's venv (`0.36.0` for the release pin; a dev pin would carry the commit, for example `0.32.1.dev0+gb9c675e1`). The marker is read
   from the task's log through the API, or, when the api-server cannot serve the log, from the task's XCom, which
   is the same last line (a 200 that says "Could not read served logs" counts as no log; a failed run then shows the
   scheduler pod's own output); the record says which;
7. deletes the DAG file from S3 and, best effort, its records in Airflow.

It changes nothing else. A record without all of 1 to 6 fails; nothing is skipped.

Criterion (j), forge's *generated* apply DAG running a real contract through the env allowlist, stays failing in
every tier: the probe proves the S3 bundle, the image and the env, not a contract run.

## What this runs, and what it leaves out

| Runs | Left out, or different on purpose |
|---|---|
| Airflow 3 with DAGs as objects in S3, read by the Amazon provider's S3 bundle | LocalExecutor: tasks run in the scheduler pod, one at a time up to `parallelism` 4. No Celery workers that scale |
| Tasks are the generated `BashOperator`s running `fluid apply` with forge in the image | forge and dbt are baked into the image |
| The bucket is `s3://<bucket>/dags` | The S3 key is a static, bucket-limited SeaweedFS identity |
| The metadata DB is Postgres | CNPG on kind, one instance |
| The API is Airflow's REST API | Login is Simple auth manager |
| | Task logs stay in the scheduler's emptyDir (lost with the pod) |
| | The DAG bundle refreshes every 30 s and the project sync every 30 s |
| | The Airflow version is the chart's appVersion (3.2.2) |
| | One scheduler and one dag-processor; no triggerer, so no deferrable operators |
| | No NetworkPolicy yet (criterion h); no private networking |

## Decisions that differ from the first design, and why

| Decision | Reason |
|---|---|
| The scheduler mounts a copy of the spark files from namespace `airflow` | A ConfigMap cannot be mounted from another namespace |
| Simple auth manager, with the password file copied by an init container | The chart's `createUserJob` takes the user's password as a plain value in values; FAB needs a user created in the database. Simple auth manager opens its file read-write (`a+`), and a Secret volume is read-only |
| Python is 3.13, not 3.12 | `apache/airflow:3.2.2` runs 3.13.13 (read from the image config). The venv is built for that minor, and uses fluid-runner's hashed requirements byte for byte (a test enforces it) |
| `prefix: dags/` | See above: exact directory; the bundle's existence check needs a real object, hence the marker |
| Two SeaweedFS identities | Airflow needs read and list only; whoever uploads needs write. A single identity would give every Airflow pod write access to the DAGs it runs |
| Airflow is its own Helmfile wave, after Lakekeeper | `lab up` must create the bucket, the markers and the identity check before the dag-processor starts (the chart waits for readiness), and the image check must fail before the long first wave |
| Scheduler as a Deployment | With worker persistence on (the chart default) LocalExecutor turns the scheduler into a StatefulSet with a 100 Gi log volume; the lab keeps no history |
| Role in `spark` is rendered in release `lab-airflow` | One object release per tier, next to the chart; `spark` joins the orchestration tier's namespaces |

## Borrowed, not built

- Airflow chart: external metadata DB (`data.metadataSecretName`) and secrets by name, and `dagProcessor.dagBundleConfigList` for bundles:
  <https://airflow.apache.org/docs/helm-chart/stable/production-guide.html> and the chart's own `values.yaml` (1.22.0).
- DAG bundles: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html>;
  `S3DagBundle(aws_conn_id, bucket_name, prefix)` and its connection:
  <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/bundles/s3/index.html>
  (source read at provider 9.29.0, the version in the 3.2.2 constraints); AWS connection extras (`endpoint_url`, `verify`,
  `config_kwargs`): <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html>.
- Simple auth manager: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html>.
- AWS Signature Version 4 for the host's few S3 calls (`labctl/s3.py`, standard library only, tested against AWS's worked examples):
  <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html>.

## Not measured

See [proven.md](proven.md#not-proven-yet). Not measured: the `restricted` pods starting, the migration
Job's time, the S3 bundle against SeaweedFS (path-style listing, the prefix check), the API's log endpoint for a LocalExecutor
task (the XCom fallback exists for it), the scheduler's memory under `fluid apply` plus dbt, and SeaweedFS picking up the new
identities (`lab up` restarts its S3 gateway once if the `airflow` key is refused).

## Evidence: a live run (2026-11-22, forge 0.36.0)

The lab already had core, the spark tier and the streaming tier up.

| Run | Result |
|---|---|
| `./lab build airflow-fluid` | PASS 3/3. The probe ran with no network and found forge 0.36.0 (commit `c0db145d`; 1065 installed files match the wheel), dbt-core 1.10.23, dbt-trino 1.10.6 and DuckDB 1.5.6. Airflow 3.2.2 imports `S3DagBundle` (Amazon provider 9.29.0), and `fluid` and `dbt` run. The node holds the same 24 layers |
| `./lab up --tier orchestration` (first try) | FAIL at `up:mirror`: the mirror lacked `docker.io/apache/airflow:3.2.2`. Run `./lab mirror --tier orchestration` first. The failure was correct |
| `./lab mirror --tier orchestration`, then `./lab up --tier orchestration` | PASS 20/20, then PASS 10/10 in 54 s: 3 Helmfile waves, 26 Secrets from OpenBao, the node at 7.82 GiB |
| `./lab check --tier orchestration` | `orchestration` PASS. A one-task DAG uploaded to `s3://lab-airflow/dags/lab-check/` as `airflow-publish` was listed by Airflow's REST API (bundle `lab-s3-dags`, read from SeaweedFS by the S3DagBundle). Its run succeeded, and the task log shows this run's nonce and `fluid --version` (FLUID Forge CLI v0.36.0) from the airflow-fluid image. `AssumeRole` of `LakekeeperVendedRole` was refused for `airflow` and `airflow-publish` (HTTP 403) and accepted for Lakekeeper's own key (the positive control). The DAG file and its Airflow records were deleted afterwards. 10 PASS, plus the 7 criteria that fail by design |

The pods in namespace `airflow` (api-server, scheduler with its sync sidecar, dag-processor, and the migration Job) ran under Pod Security `restricted`, the level the namespace enforces.
Still not proven: criterion (j), a real contract's generated `fluid apply` DAG through the env allowlist, and the
scheduler's memory under a real `fluid apply`.

