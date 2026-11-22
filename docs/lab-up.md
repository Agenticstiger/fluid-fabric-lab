<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Running the lab: `lab mirror`, `lab up`, `lab build`, `lab check`, `lab down`

`lab up` brings a tier up on one [kind](https://kind.sigs.k8s.io/) cluster,
from a local image mirror only. `lab up` proves that the platform installs
and starts. `lab check` proves, criterion by criterion, that its protocols
work.

```sh
./lab doctor --tier core                 # can this machine run the tier?
./lab mirror --tier core                 # once, with the network: copy every pinned image
./lab up --tier core                     # no upstream registry is contacted from here on
./lab build fluid-runner                 # forge's runner image, built here and loaded into the lab
./lab check                              # each criterion on real evidence
./lab down                               # remove the cluster and OpenBao, keep their state
./lab down --purge                       # also delete OpenBao's data and the state directory
```

`lab up` brings up the `core` tier and the `spark`, `streaming` and `orchestration` add-ons
([docs/spark-tier.md](spark-tier.md),
[docs/streaming-tier.md](streaming-tier.md), [docs/orchestration-tier.md](orchestration-tier.md))
(see [docs/proven.md](proven.md) for what each has run). The spark tier needs no
credential and no mirror image: it runs the lab-built `spark-iceberg` image (`./lab build spark-iceberg`, after the
first `lab up` has created the cluster; docs/spark-tier.md). The streaming tier needs only
`lab mirror --tier streaming` (the Strimzi operator and Kafka images) and a
runtime VM of 13 GiB (`tiers.yaml`); it brings up Kafka without KafkaConnect. The
orchestration tier needs the lab-built `airflow-fluid` image: `./lab build airflow-fluid`
(network once: Docker Hub, PyPI, github.com), after the first `lab up` has created the
cluster; `lab up --tier orchestration` stops before any Helm work with that command
as the advice when the node lacks the image.
Add-ons stack: `lab up --tier streaming` on a lab that runs the spark add-on
leaves spark alone. It
runs with the docker runtime (Docker Desktop, Colima, Rancher Desktop with
moby, or Docker Engine).

Every command prints one line per step (`PASS`, `WARN` or `FAIL`; `lab check`
also has `SKIP`, for a record that cannot run; no record skips today) and exits 0
only when nothing failed and nothing was skipped. `--json` gives the same result as JSON. A failure
names the step, the command output's tail, and what to do next. Every step
is idempotent: re-running `lab up` resumes where the last run stopped.

## What runs where

| Piece | Where | Name (default lab name `fluid-fabric-lab`) |
|---|---|---|
| Mirror registry (`registry:3.1.2`) | sibling container, `127.0.0.1:5001`, attached to every lab's network | `fluid-fabric-lab-registry`, volume `fluid-fabric-lab-registry-data` |
| Lab network | Docker bridge network, IPv4 only (kind's own `kind` network is dual-stack, and an IPv4 cluster's NodePorts then get IPv6 forwards) | `<lab>-net` |
| OpenBao 2.7.1 | sibling container, an ephemeral `127.0.0.1` port, on `<lab>-net` | `<lab>-openbao`, volume `<lab>-openbao-data` |
| kind cluster (Kubernetes v1.36.4) | one node container | `<lab>-control-plane`, context `kind-<lab>` |
| Lab Gateway (Envoy) | in the cluster, node port 30443 | host `127.0.0.1:443` (`--https-port`) |

`--name` selects the lab, so two labs can sit side by side. `kind` is
reserved (it is kind's own default cluster name). The mirror is one per
machine and shared: it is a cache of pinned digests. `lab mirror` creates it;
`lab up` refuses to run without it rather than pull its image from Docker
Hub.

The state directory is `$LAB_STATE_DIR`, else
`$XDG_STATE_HOME/fluid-fabric-lab/<lab>`, else
`~/.local/state/fluid-fabric-lab/<lab>`. It is mode 0700 and holds the lab CA
(`ca/`), OpenBao's TLS files and unseal record (`openbao/init.json`, 0600),
the cluster's kubeconfig and the Helmfile log. It is never inside the
repository, and `lab up` refuses a `LAB_STATE_DIR` that already holds files
but is not this lab's (it marks the directory as the lab's, and `lab down
--purge` deletes what is marked). Your own `~/.kube/config` is not read or
changed:

```sh
export KUBECONFIG=~/.local/state/fluid-fabric-lab/fluid-fabric-lab/kubeconfig
kubectl --context kind-fluid-fabric-lab get pods -A
```

## What `lab up` does

1. **preflight**: the container runtime answers, the tier fits the runtime
   VM's memory (`tiers.yaml`), the disk has room, and the tools are on PATH.
   The tier must be one `lab up` supports (core, spark, streaming, orchestration, e2e).
2. **mirror**: the registry runs and holds every image of the tier at its
   pinned digest. A missing image fails the run with `./lab mirror --tier T`
   as the advice; `lab up` never pulls from upstream.
3. **pki**: the lab CA (ECDSA P-256, `openssl`) and OpenBao's certificate.
4. **cluster**: the lab's own Docker network `<lab>-net` (IPv4 only), then
   kind on it from the mirror's node image. containerd in the node gets a `hosts.toml` per
   upstream registry pointing at the mirror, with no upstream fallback, plus
   a `_default` that points at a closed port, so an unpinned image fails to
   pull instead of being fetched. CoreDNS rewrites `*.lab.test` to the lab
   Gateway and `openbao.lab.test` to OpenBao.
5. **openbao**: start it on `<lab>-net`, initialise once (3 shares,
   threshold 2), unseal, configure, revoke the root token. The shares are
   saved the moment `init` returns, and the root token stays in the same
   0600 record only until the `lab-admin` login exists, so a run that dies in
   between is finished by the next one. Every credential of the lab is
   generated once and kept in OpenBao at `kv/lab/credentials`.
6. **secrets**: the tier's namespaces with Pod Security labels
   (`platform/namespaces.yaml`) and every Secret the charts read, rendered
   from OpenBao and sent on kubectl's stdin.
7. **helmfile**: `helmfile sync` of the tier from the committed chart cache, in
   waves: everything but Lakekeeper (and Airflow), then Lakekeeper once Keycloak
   and its realm import are ready, and, on the orchestration tier, Airflow last.
   Before the first wave the orchestration tier checks that the node holds the
   `airflow-fluid` image; before Airflow's wave it creates bucket `lab-airflow`
   with a marker object under `dags/` and `projects/`, checks that the read-only
   identity `airflow` can list them (restarting SeaweedFS's S3 gateway once if it
   has not loaded the new identities) and copies the spark tier's spark-submit
   files into namespace `airflow` (docs/orchestration-tier.md).
8. **bootstrap**: the warehouse bucket; Lakekeeper bootstrap (the `lab-admin`
   client becomes its server admin) and warehouse `sales-wh`, which vends
   per-table credentials through SeaweedFS STS. The launcher reaches the
   Gateway through `kubectl port-forward` to its Service, not through the
   host port, so a broken host forward cannot stop the bootstrap.
9. **measure**: memory of the node and the sibling containers, written to
   `<state>/measured-<tier>.json`.

Last, `lab up` checks that the host port (`--https-port`, default 443) reaches
the Gateway and reports a WARN if it does not: the lab still works, but a
browser cannot reach `*.lab.test`. On Docker Desktop the forward for a
privileged port can go stale after a cluster with the same mapping is
recreated, and it then resets every connection (seen 2026-11-18). Restarting
Docker Desktop clears it, or a new lab can use a high port such as
`--https-port 18443`.

## Endpoints

Every endpoint is HTTPS with a certificate from the lab CA,
`<state>/ca/ca.crt`. Pods resolve the names through CoreDNS. The host does
not: either pass `--connect-to` (curl) or add the names to your hosts file
yourself:

```text
127.0.0.1 keycloak.lab.test lakekeeper.lab.test s3.lab.test trino.lab.test
```

```sh
curl --cacert ~/.local/state/fluid-fabric-lab/fluid-fabric-lab/ca/ca.crt \
  --connect-to keycloak.lab.test:443:127.0.0.1:443 \
  https://keycloak.lab.test/realms/fluid/.well-known/openid-configuration
```

If the host port does not answer (see the WARN above), go through the API
server instead, as the launcher does:

```sh
kubectl --kubeconfig ~/.local/state/fluid-fabric-lab/fluid-fabric-lab/kubeconfig \
  -n envoy-gateway-system port-forward svc/lab-gateway 8443:443
```

and use `--connect-to keycloak.lab.test:443:127.0.0.1:8443`.

| Name | Serves |
|---|---|
| `keycloak.lab.test` | Keycloak 26.8.0, realm `fluid` |
| `lakekeeper.lab.test` | Lakekeeper v0.14.0 (Iceberg REST at `/catalog`, management at `/management`) |
| `s3.lab.test` | SeaweedFS S3 and STS |
| `trino.lab.test` | Trino 483 (authentication and policies are still to come: `lab check` (b) and (c) fail until they land) |
| `openbao.lab.test:8200` | OpenBao, pods only (the host uses its loopback port) |

## The runner image

`lab build fluid-runner --name <lab>` builds forge's runner image from
`platform/images/fluid-runner` on this machine. It needs the network for the
build: PyPI, forge's source archive on github.com, the OpenTofu release and the provider registry. It then probes the
image with no network against `versions.lock.yaml` and loads it into the lab's
kind node with `kind load docker-image`. The image is never pushed anywhere.
Pods run it as `lab.local/fluid-runner:<forge version>-<commit, 8 characters>` (for example `0.37.0-c826f3ae`) with
`imagePullPolicy: Never`. A new cluster (after `lab down` and `lab up`) needs
`lab build` again, which reuses Docker's build cache.
[platform/images/fluid-runner/README.md](../platform/images/fluid-runner/README.md)
lists what is in it and how each piece is pinned.

## `lab check`

`lab check --name <lab>` writes one record per criterion (a) to (j).
Criteria (a) and (h) are split into two halves that are
proven separately. Three more records make the evidence meaningful:
- every workload of the tier is ready;
- the machine has the memory and free disk;
- the runner image `lab build` checked is in the node.

A criterion the lab cannot prove is a FAIL that says what is missing, so
`lab check` exits 1 until every criterion passes on evidence.

| Record | Evidence, or why it fails today |
|---|---|
| `ready` | each namespace of the tier runs workloads; every pod is running and ready, operator-made ones (CNPG's Postgres) included; no Job failed |
| `capacity` | the runtime VM fits the tier's memory target and the volume holding the repository has the free-disk floor |
| `runner` | the node holds the layers `lab build` recorded for `fluid-runner`, and the record names this node |
| `a-catalog` | `loadTable` with `X-Iceberg-Access-Delegation: vended-credentials` issues an access key, a secret and a session token, none of them the warehouse's static key; Trino's `lakehouse` catalog asks for vended credentials; its ConfigMaps, pods and the Secrets they load hold no static S3 key. Values are never printed |
| `a-trino` | fails: no Trino read of an Iceberg table through vended credentials has run yet |
| `b-jwt` | a `fluid-pipeline` token (client credentials, scope `trino`) runs `SELECT 1` through the Gateway to a FINISHED result; a request without a token, a token from another client (`lab-probe`) and a `fluid-pipeline` token without `aud: trino` each get Trino's 401 (with its Bearer challenge); OPA allows the granted `SHOW SCHEMAS FROM lakehouse`, `SHOW TABLES FROM lakehouse.events_raw` and dbt-trino's list-relations query (its join with `system.metadata.materialized_views`; SCHEMA_NOT_FOUND is accepted, Access Denied is not), and refuses `SHOW TABLES FROM lakehouse.lab_check` with Access Denied (docs/trino-auth.md). Tokens are never printed |
| `b-oauth2` | fails: Trino's browser login is not configured yet; it needs a realm client for the web UI and a browser |
| `c` | fails: no forge-shaped rows/masks package yet; the lab policy returns no row filters and no column masks |
| `d` | fails: needs KafkaConnect on the connect-iceberg image, which is not built yet (the streaming tier's Kafka is up without it) |
| `e` | OpenBao keeps its data on its volume; `docker restart` brings it back sealed; it unseals from the stored shares and reads `kv/lab/credentials` back unchanged over TLS; the KV2 failure is recorded in [docs/proven.md](proven.md#recorded-failures) |
| `f` | fails: no signed OPA bundle from SeaweedFS yet |
| `g` | Lakekeeper reports `authz-backend: openfga`; lab-admin creates and deletes a namespace on the same route; `lab-probe`, signed in with no grant, is refused (403, or 404 `NoSuchWarehouseException` for a warehouse it may not see) and nothing is created |
| `h-mirror` | the node's `hosts.toml` files are exactly what `lab up` writes; every running image is pinned by the lock digest of its own repository, except kind's own preloaded system images |
| `h-egress` | fails: no default-deny egress NetworkPolicy and no canary yet |
| `i` | the `*.lab.test` endpoints answer from their backends (on core the four: keycloak 200, lakekeeper 200, s3 403, trino 200, `/v1/info` stays public; an add-on tier adds its own) over TLS checked against the lab CA; a client that trusts another CA is refused by each; the Gateway has only HTTPS listeners; every namespace enforces the level `platform/namespaces.yaml` records |
| `j` | fails: forge's generated apply DAG running a real contract is not run yet (the `orchestration` record proves Airflow, on the orchestration tier) |
| `orchestration` | only with `--tier orchestration` (or a tier that includes Airflow): the image `lab build` loaded is in the node; a one-task DAG uploaded to `s3://lab-airflow/dags/lab-check/` as the bucket-limited identity `airflow-publish` is listed by Airflow's REST API (token from the lab's one login), triggered, and succeeds; the task's log (or XCom) carries this run's nonce, `fluid --version` and the pinned forge version; the DAG is deleted afterwards (docs/orchestration-tier.md) |
| `spark` | only with `--tier spark`: a Job on the `spark-iceberg` image seeds `raw.events`; forge's `fluid apply --env local --mode amend-and-build --build-id daily-curated` runs the probe contract in fluid-host, with its Spark pod on the same image; a second Job reads the expected rows of `curated.events_daily` back, written through Lakekeeper with vended credentials (docs/spark-tier.md) |
| `streaming` | only with `--tier streaming`: a Job in namespace `kafka`, with the Strimzi Kafka image's console clients, passes only if 20 marked JSON records cross the `plain` listener and come back exactly, `lab-check-writer` writes to an ACL-protected topic over TLS with SCRAM, `lab-check-nobody` is refused there with an authorization error, and an anonymous client on `plain` is refused adding an ACL and reading that topic (docs/streaming-tier.md) |

`lab check` changes these things, and nothing else:
- it creates namespace `lab_check` and table `lab_check.probe` in `sales-wh`, once;
- for (g), lab-admin creates and deletes namespace `lab_check_admin`;
- for (b-jwt), `fluid-pipeline` runs read-only Trino statements (`SELECT 1`, `SHOW SCHEMAS`, `SHOW TABLES` of `events_raw`, dbt-trino's list-relations query, and a `SHOW TABLES` OPA refuses);
- on the spark tier, it seeds `raw.events` and `curated.events_daily` in `sales-wh` (namespaces `raw` and `curated`) and
  runs forge's batch in fluid-host (state under `/state/spark/lab.spark-probe`);
- on the streaming tier, it writes 20 records to topic `lab-check.events` and one from `lab-check-writer` to
  `lab-check.restricted`;
- it restarts the lab's OpenBao container for (e), then unseals it again.

A sealed OpenBao, after a host restart for example, is unsealed from the stored
shares before anything is read. `lab check` never initialises OpenBao or
changes its configuration.

## `lab accept`

`lab accept --name <lab>` runs forge's eleven stages on each example
product in the lab's `fluid-host` pod and compares every result with the expectations ledger, in both
directions. It needs a lab that `lab up` and `lab check` have brought up with a spark tier, and it
has not run on one yet. What it runs, the order, the report and `--update-ledger`:
[docs/lab-accept.md](lab-accept.md).

## Removing a lab

`lab down` deletes the kind cluster `<lab>`, the container `<lab>-openbao`
and the network `<lab>-net` (once only the shared mirror is left on it), and
nothing else. It acts only on a lab whose state directory carries this
lab's marker, and only on a container that carries the label
`com.agentics.fluid-fabric-lab.name=<lab>`; anything else of the same name is
left alone and reported. If Docker or kind cannot answer, `lab down` stops and
keeps everything: "could not check" never counts as "already gone". The OpenBao volume and the state directory stay, so the next
`lab up` unseals the same OpenBao and keeps every credential. `lab down
--purge` also deletes them; it refuses a state directory that does not carry
the marker `lab up` wrote. The mirror keeps running for the next lab. Stop it
with `docker stop fluid-fabric-lab-registry`. Nothing in the lab uses
`docker compose` or prunes anything.

## Measured footprint

`lab up` writes `<state>/measured-<tier>.json` (memory at rest).
[docs/proven.md](proven.md#measured-memory) records the measurements; the targets in
`tiers.yaml` stay estimates until a peak under load is measured.
