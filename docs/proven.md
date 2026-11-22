<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# What is proven

Every claim here comes from a command run against a lab, and the commands are
listed so anyone can run them again. A lab is named by `--name <lab>`; the
examples write `<lab>` for it. Nothing here is a claim about production: the
lab is for local development only (see
[PRODUCTION-READINESS.md](../PRODUCTION-READINESS.md)).

| Section | What it covers |
|---|---|
| [Offline checks (L0)](#offline-checks-l0) | Gates that need no network and no cluster |
| [Bringing a tier up](#bringing-a-tier-up) | `lab mirror`, `lab up`, `lab down` |
| [`lab check` results](#lab-check-results) | One record per criterion, with the reason for each failure |
| [Lab-built images](#lab-built-images) | Build and probe results |
| [Measured memory](#measured-memory) | Figures from running labs |
| [The end-to-end run](#the-end-to-end-run) | `lab e2e` from a clean start |
| [PG spikes](#pg-spikes) | Protocol spikes run as throwaway pods |
| [Recorded failures](#recorded-failures) | What was tried and does not work |
| [Not proven yet](#not-proven-yet) | Criteria that fail today, and what each needs |

## Offline checks (L0)

The L0 gates run with the network blocked. Locally, run them inside any
sandbox that denies network access. CI (`.github/workflows/l0.yml`) runs the
same commands in a network namespace with only loopback.

| Command | What it shows |
|---|---|
| `./lab l0` | All twelve gates pass on the repository: render, schemas, licence, image denylist, chart and app versions, brand fence, secret scan, REUSE, footprint floor, profile schema, exposure (nothing listens beyond this machine) and third-party notices |
| `python3 tools/l0/selftest` | Every gate fails on its planted cases and passes on its good ones (149 cases) |
| `./lab render --tier T` for T = core, streaming, orchestration, spark, e2e, full | `helmfile template` renders every release from the committed chart cache with no network. The `chart-source` check proves every release's chart is one the lock pins in `platform/charts/` |
| `python3 -m unittest discover -s tests/launcher` | The launcher tests pass |
| `reuse lint` | REUSE 3.3 compliant |
| `python3 tools/l0/secret_scan.py` | No findings in the tree |

Every rendered image is pinned by digest and matches `versions.lock.yaml`: a
walk over every non-CRD string in the core, streaming, orchestration, full and
kafbat-ui renders found 26 distinct image references, each with a digest that
matches a lock entry by repository, digest and tag. Bitnami appears 0 times in
the renders. No personal email address and no generated-by attribution text is
in the first-party files.

Footprint floor, counted by `python3 tools/l0/footprint.py` from limits, heaps
and estimates: core 7.41 of 8 GiB, streaming 9.28 of 13 GiB, orchestration 10.03 of 11 GiB, spark 9.91 of 11 GiB, e2e 13.28 of 14 GiB, full 11.91 of 16 GiB. The footprint gate fails when this line differs from
what it counts.

### Planted cases

A gate that passes everything proves nothing, so each gate has planted cases
that it must reject. Each is a fixture in `tools/l0/selftest` or a test in
`tests/launcher/test_l0.py`.

| Gate | Planted case | What the gate does |
|---|---|---|
| Licence | AGPL text in a renamed file of a chart (`licence/renamed-text`) | Reads every file of the cache in full |
| Licence | An SPDX header that lies about the body (`licence/header-lies`) | Reads the body, whatever the header says |
| Licence | AGPL in a subchart archive (`licence/nested-archive`, `chart-app/nested-archive-unrecorded`) | Unpacks archives recursively; subchart archives must be recorded |
| Licence | An AGPL rider appended to an Apache LICENSE (`licence/rider`) | Reads licence-named files for short ids too |
| Licence | A chart outside `platform/` (`licence/chart-outside-platform`) | Any Chart.yaml or archive outside the cache fails |
| Licence | No chart cache, or only `LICENSES/` (`licence/no-cache`) | A missing cache or lock fails |
| Licence | A placeholder OSL record (`licence/osl-trivial`) | Obligations need 40 or more characters of text and a real digest |
| Licence | BUSL, SSPL, AGPL, unlicensed and EULA fixtures, and vendored OSL | Fails each by reading the licence text; the fixture `Chart.yaml` files declare no licence. A runtime-referenced OSL artefact with its obligations recorded passes |
| Render | An uncached, modified chart (`render/uncached-chart`) | `chart-source`: each release's chart must be pinned in the cache |
| Render | A dotfile in a cached chart (`render/dotfile-in-cache`) | Dotfiles must be listed in SHA256SUMS |
| Render | `exec` in the Helmfile that reaches the network (`render/exec-in-helmfile`) | Refused statically before anything runs, as are secret fetchers, remote composition and hook commands |
| Render | A tier that silently loses a release (`render/tier-drops-release`) | Each lock chart records its `tier`, and tiers.yaml must agree |
| Denylist | `bitnamilegacy/*`, `pgsty/silo` and `lakekeeper-plus` images | Caught, behind any registry, mirror or port (`denylist/lakekeeper-plus-mirror`) |
| Denylist | A ghcr Lakekeeper PostgreSQL, a Percona MongoDB (`denylist/lakekeeper-postgresql-ghcr`, `denylist/percona-mongodb`) | Registry-agnostic rule; SSPL MongoDB images are denied |
| Denylist | Split, base64 and suffixed references (`denylist/split-org`, `denylist/base64-secret`, `denylist/pgsty-silo-arm64`) | Denied org names match on their own; base64 is decoded |
| Denylist | Floating images in custom resources or a ReplicationController (`denylist/cr-latest`, `denylist/cr-untagged`, `denylist/rc-untagged`) | Image fields of custom resources are checked; ReplicationController counts as a workload |
| Brand fence | A planted vendor name, with punctuation, zero-width characters, Cyrillic lookalikes or base64 (`brand/lookalike`, `brand/base64`) | NFKC folding, invisible characters dropped, lookalikes folded, separators allowed, base64 decoded |
| Brand fence | Joined forms (`brand/joined`) | Substring match |
| Brand fence | A NUL byte or UTF-16 text (`brand/nul-byte`, `brand/utf16`) | Such files are read as text; only known binary formats are skipped |
| Brand fence | An unscanned `__pycache__` (`brand/pycache-hidden`) | `__pycache__` is walked and only `.pyc` is allowed there. Fixture allowlists are per case, and the selftest fails an orphan fixture |
| Brand fence | Allowlisted lines (`brand/allowlisted`) | Passes the lines that forge core legitimately carries |
| Footprint | A two-worker 8G Trino (`footprint/trino-two-workers-8g`) | Fails at "OVER TARGET"; the "workers counted" case shows `trino-worker x2` |
| Footprint | A heap hidden in `-XX:MaxHeapSize`, `-Xms`, envFrom or key refs; a missing limit (`footprint/trino-heap-hidden`, `no-limit`, `heap-via-configmapkeyref`, `heap-via-envfrom`) | Every container needs a memory limit; the heap is read from every source; a heap above its limit fails |
| Footprint | An unmodelled workload (`footprint/replicationcontroller`, `unmodelled-flink`, `cnpg-no-limit`, `cnpg-shared-buffers`) | Every built-in workload kind is counted; any unknown kind fails; CNPG `shared_buffers` must fit its limit |
| Footprint | An autoscaler (`footprint/hpa-max`); an empty input (`footprint/empty`) | Counted at `maxReplicas`; a tier with no workload fails |
| Exposure | A NodePort Service, a hostPort, a SeaweedFS admin server, a route to the SeaweedFS master, an Envoy LoadBalancer, a Gateway with no EnvoyProxy, a Strimzi `nodeport` listener, a CloudNativePG LoadBalancer template, a NodePort Service inside a `List`, an Ingress (`exposure/*`) | Fails on each: nothing in any render may listen beyond the cluster except the Gateway on node port 30443 |
| Exposure | kind mapped or its API server on 0.0.0.0; a Docker port on every interface (separate, attached or f-string `-p`), `--network host`, a port-forward on 0.0.0.0 (`exposure/kind-*`, `exposure/launcher-*`) | Fails on each: kind and the launcher bind 127.0.0.1 only |
| Third-party notices | A lock image with no THIRD-PARTY row, a row with no lock entry, a chart NOTICE missing from `NOTICE` (`third_party/*`) | Fails on each: THIRD-PARTY.md is regenerated from the lock and compared row by row |
| Gate runner | A stub gate, a zero-check gate, an unregistered gate | Exit 0 counts only with a `gate <name>: PASS (N check(s))` summary with N of 1 or more; gates not in `tools/l0/gates.conf` fail |
| Gate runner | A gate that is never run | A non-standard guard, a non-executable `.sh` or any unexpected file fails |
| Secret scan | An empty tree (`secrets/empty-tree`) | An empty tree fails; a missing root exits 2 |

Known limits of L0, stated as they are:
- A local `./lab l0` or `./lab render` has no network namespace of its own.
  Poisoned proxies stop only tools that honour proxy variables, and the static
  Helmfile check is a pattern list, not a sandbox. CI has real isolation.
- The licence sweep exempts `tests/`, because the planted fixtures are the
  point. A bad chart added there cannot render: `chart-source` accepts only
  charts the lock pins in the cache.
- `tests/fixtures/brand/**` allows every term, because planted cases must name
  vendors. The orphan check ensures that every fixture directory belongs to a
  selftest case.

## Bringing a tier up

Run on kind, with the network available only to `lab mirror`.

| Command | Result |
|---|---|
| `./lab mirror --tier core` | PASS, 18 images. Every copy verified with `crane digest` against the lock. 5.1 GiB in the registry volume (all platforms of every index). The registry limit is 1 GiB with 2 copies at a time, and a resumed run verifies the finished copies instead of trusting them |
| `./lab down --name <lab> --purge`, then `./lab up --tier core --name <lab>` | PASS, 9 of 9 steps in 323.6 s on a clean start: new CA, new OpenBao (initialised with 3 shares and threshold 2, root token revoked), generated credentials, a new kind cluster, 2 Helmfile waves, the `lake` bucket, Lakekeeper bootstrapped and warehouse `sales-wh` created with STS vending |
| The same with `--https-port 18443` | PASS, 10 of 10 checks in 528.5 s, on the lab's own IPv4-only network `<lab>-net` |
| `./lab up --tier core --name <lab>` again | PASS, 9 of 9 steps in 28.6 s. Idempotent: credentials are read back from OpenBao and the warehouse is found |
| `./lab down --name <lab>` twice, then `./lab up` | PASS. `down` without `--purge` keeps the OpenBao volume and the state directory. The second `down` reports each object as already gone. `up` then unseals OpenBao from the stored shares and reuses the credentials in `kv/lab/credentials` |
| `./lab down --name <lab> --purge` | PASS, 5 of 5. Removes the cluster, the OpenBao container, the network, the OpenBao volume and the state directory (after checking its marker). The shared mirror keeps running |
| `./lab up --tier streaming`, then `./lab check --tier streaming` | PASS 10/10 in 268 s. Kafka (KRaft, one node) became Ready under Pod Security `restricted`. Details in [streaming-tier.md](streaming-tier.md) |
| `./lab up --tier orchestration`, then `./lab check --tier orchestration` | PASS 10/10. The DAG from SeaweedFS ran, and its task log shows the forge CLI of the airflow-fluid image ([orchestration-tier.md](orchestration-tier.md)) |
| `./lab up --tier spark`, then `./lab check --tier spark` | PASS 10/10. forge's runner on the open Spark image wrote `curated.events_daily {'2026-11-17': 2, '2026-11-18': 1}` through Lakekeeper with vended credentials ([spark-tier.md](spark-tier.md)) |

What these runs showed:

| Claim | Evidence |
|---|---|
| The cluster pulls only from the mirror | Each node `hosts.toml` sets the mirror as `server` with `override_path`, and `_default` points at a closed port. A pinned image pulled by digest is served by the mirror. An image that is not mirrored (`docker.io/library/busybox:1.37`) fails with `not found` and is never fetched upstream |
| The kind node image comes from the mirror | The runtime kind config names the mirror's `kindest/node` reference with the lock's digest |
| Pods use the lab's HTTPS names | From the Trino pod, `https://keycloak.lab.test/realms/fluid/.well-known/openid-configuration` returns 200, verified against the lab CA. `openbao.lab.test` resolves to the OpenBao container on the kind network |
| Lakekeeper accepts Keycloak tokens | The `lab-admin` client-credentials token (scope `lakekeeper`) bootstrapped Lakekeeper and created the warehouse; Lakekeeper validated it against `https://keycloak.lab.test/realms/fluid` from inside the cluster |
| Pod Security `restricted` holds where it is enforced | `cert-manager`, `envoy-gateway-system`, `cnpg-system` and `lab-db` enforce `restricted`; a server-side dry run found no violating pod, and every workload in them was restarted under enforcement and became Ready. The other namespaces enforce `baseline` and record the measured violation in `platform/namespaces.yaml` |

Problems found and fixed while bringing `core` up, each pinned in the code or
the values:

| Symptom | Cause | Fix |
|---|---|---|
| Trino stuck in `ContainerCreating` | It mounts ConfigMap `lab-ca-bundle`, which exists only after trust-manager and `lab-pki`, a later Helmfile level | `needs: [cert-manager/lab-pki]` for Trino and Lakekeeper |
| Lakekeeper's init container waits for migrations forever | The chart runs migrations as a post-install hook, after Helm's `--wait` | `helmWait: true` |
| Keycloak: "The '--optimized' flag was used for first ever server start" | The operator starts a custom image reference with `--optimized` | `startOptimized: false` in the Keycloak CR |
| Lakekeeper exits: "Failed to parse openid configuration" | It reads the OIDC discovery document before Keycloak answers | `lab up` syncs in two waves and waits for the Keycloak CR and the realm import between them |
| `lab-pki` failed once on the trust-manager webhook | The Deployment was Ready a few seconds before its webhook answered | `lab up` retries a wave, at most 3 times, only on that exact webhook error |
| Warehouse credentials were not in OpenBao | Chart 0.12.0 sets `LAKEKEEPER__KV2__*` for `secretBackend.type: kv2` but never `LAKEKEEPER__SECRET_BACKEND` | Lakekeeper uses its Postgres store on purpose, with a per-lab key (see [Recorded failures](#recorded-failures)) |
| Secret values in an annotation | Client-side `kubectl apply` copies the object into `last-applied-configuration` | `lab up` applies with `--server-side` |

## `lab check` results

`lab check` writes one record per criterion (a) to (j), plus the records of
each add-on in the tier. A criterion the lab cannot prove fails and names what
it needs.

On `core`, 9 of 16 records pass, and the other 7 fail on purpose:

| Record | Result | What it proved, or why it fails |
|---|---|---|
| `ready` | PASS | Every namespace runs workloads; every pod is running and ready; a Job that never succeeded fails |
| `capacity` | PASS | The machine has the tier's memory and free disk |
| `runner` | PASS | The `fluid-runner` image that `lab build` checked is in the node |
| `a-catalog` | PASS | STS credentials were vended for `lab_check.probe`; no static keys in Trino |
| `b-jwt` | PASS | Trino accepts Keycloak JWTs (client `fluid-pipeline`, scope `trino`): `SELECT 1` ran to FINISHED through the Gateway; no token, another client's token and a token without `aud: trino` each got Trino's 401; OPA allowed `SHOW SCHEMAS FROM lakehouse` and refused `SHOW TABLES FROM lakehouse.lab_check` ([trino-auth.md](trino-auth.md)) |
| `e` | PASS | OpenBao restarted, came back sealed, unsealed, and its credentials were unchanged. Lakekeeper's KV2 backend is recorded as failed (below) |
| `g` | PASS | Lakekeeper reports `authz-backend: openfga`; `lab-admin` creates and deletes on the same route; `lab-probe` is refused with Lakekeeper's own error body |
| `h-mirror` | PASS | 3 upstream registries to the mirror only; every image pinned by a lock digest of its own repository; the kind system containers preloaded |
| `i` | PASS | 4 endpoints answer from their backends over TLS, each refuses the wrong CA, and Pod Security is as recorded |
| `a-trino` | FAIL | No Trino read of an Iceberg table through vended credentials has run. The lab seeds no `events_raw` table for it |
| `b-oauth2` | FAIL | Trino's browser login is not configured. It needs a confidential realm client for the web UI and a browser to prove it |
| `c` | FAIL | No forge-shaped Rego package: the lab policy returns no row filters and no column masks |
| `d` | FAIL | A KafkaConnector needs KafkaConnect on the `connect-iceberg` image, which is not built |
| `f` | FAIL | OPA reads its policy from a ConfigMap; no signed bundle is served from SeaweedFS |
| `h-egress` | FAIL | Default-deny egress per namespace and its canary do not exist |
| `j` | FAIL | forge's generated apply DAG running a real contract through the env allowlist has not run |

Add-on records:

| Record | Result | What it proved |
|---|---|---|
| `streaming` | PASS | Records cross Kafka's plain listener intact (20 sent, the same 20 back); `lab-check-writer` writes over TLS with SCRAM; `lab-check-nobody` is refused by ACL; anonymous clients on plain cannot reach an ACL topic |
| `spark` | PASS | forge's `fluid apply` ran a Spark batch on the open image and wrote `curated.events_daily` through Lakekeeper with vended credentials |
| `orchestration` | PASS | Airflow listed a DAG uploaded to SeaweedFS and ran it, with forge, in the airflow-fluid image |
| `e2e` | PASS | See [the end-to-end run](#the-end-to-end-run) |

How each record is kept honest:
- `ready` fails when a namespace runs nothing, and it judges pods a controller replaced through that controller.
- `g` requires Lakekeeper's own error body, so the Gateway's bare 403 cannot pass it.
- `i` requires each endpoint's backend answer, so the Gateway's own 404 cannot pass it.
- `a-catalog` requires that the catalog asks for vended credentials, and checks the key names of Secrets loaded with `envFrom` without fetching any value.
- `h-mirror` requires a digest to belong to the image's own repository; kind's namespaces may run only kind's own images.
- `lab check` opens the existing OpenBao only: it unseals and logs in, and never initialises or reconfigures it.
- A stale sink cannot pass the `spark` record. The seed empties the sink and records its snapshot count, and the check needs exactly the expected rows and a newer snapshot.

## Lab-built images

`./lab build <image>` builds the image on this machine, probes it with no
network, and loads it into the node. The image is never pushed to a registry.

| Command | Result |
|---|---|
| `./lab build fluid-runner` | PASS, 3 of 3 checks. The probe ran the image by id with no network and found forge 0.37.0 (commit `c826f3ae`), dbt-core 1.10.23, dbt-trino 1.10.6, DuckDB 1.5.6 and OpenTofu 1.13.1. Every installed forge file matches the wheel's `RECORD`, `tofu init` installs the 4 providers from the baked mirror, and the process runs as uid 10001. `kind load` put the image into the node, and the node holds the same layers |
| `./lab build airflow-fluid` | PASS, 3 of 3 checks. The same forge 0.37.0, dbt and DuckDB versions; Airflow 3.2.2 imports `S3DagBundle` |
| `./lab build spark-iceberg` | PASS, 3 of 3 checks. Apache Spark 3.5.8 with Iceberg 1.12.0, S3A and the Kafka connector |
| `docker build --no-cache --platform linux/amd64` of the runner, then the same probe (emulated) | PASS. The amd64 wheel hashes, OpenTofu's amd64 sha256 and the `linux_amd64` providers all came from a build with no cache |

The pip layer installs wheels only (`--only-binary=:all:`), so no build tool is
fetched that no hash covers. The probe runs the image by id, and the tag must
still name that id before the load.

## Measured memory

`docker stats` after `lab up`, before any workload ran. These are resting
figures; the targets in `tiers.yaml` are budgets.

| Container | Memory, `core` at rest |
|---|---|
| kind node (every pod of the tier, plus the control plane) | 3.95 to 4.70 GiB over eight runs |
| OpenBao | 31 to 63 MiB |
| Mirror registry | 58 to 66 MiB |
| Total | 4.0 to 4.8 GiB, against the 8 GiB `core` target |

Kafka adds about 1.1 GiB at rest (kind node 5.9 to 7.03 GiB). With Airflow
running too, the node held 8.1 GiB. During the latest clean-start end-to-end
run, the kind node peaked at about 7.6 GiB (about 7.8 GiB with OpenBao and the
mirror).

## The end-to-end run

`./lab e2e` from a clean start passed 51 of 51 checks in 507 s in the latest
run (476 s in the one before it). It covers the mirror, `lab up` (a new
cluster, OpenBao and the tier), the image builds, the second `lab up` and the
check. The kind node peaked at about 7.6 GiB (7.28 GiB in the run before), about
7.8 GiB with OpenBao and the mirror. No cloud credentials were present, and no
`aws` command was called.

The time is with warm caches: the mirror already held every image, and both
image builds were cache hits. A first run on a new machine also pulls the
images and runs the builds' downloads.

The `e2e` record passes only when all of these hold:
- `e2e_raw.events` holds exactly this run's 12 events, with 12 distinct ids;
- `e2e_curated.events_daily` holds exactly `{2026-11-20: 7, 2026-11-21: 5}` for
  this run, and has a new snapshot;
- the stream was stopped cleanly.

| Step | Result |
|---|---|
| `./lab up --tier e2e --name <lab>` | PASS 10/10. KafkaTopic `lab-e2e.events` became Ready; ConfigMap `lab-e2e` carries the read-back script |
| `./lab check --tier e2e --name <lab> --only ready --only capacity --only runner --only e2e` | PASS 4/4 in 95 s |
| forge in `fluid-host` | `fluid validate --strict`, `fluid plan` and `fluid apply --env local` exited 0. The apply started the stream as one pod on the open Spark image |
| Stream | The producer Job sent 12 events. The stream landed them in `e2e_raw.events`, and forge's cancel stopped it |
| Batch | `fluid apply --mode amend-and-build --build-id daily-counts` wrote `e2e_curated.events_daily` |
| Read-back | A Job on the `spark-iceberg` image read 12 raw rows with 12 distinct ids and `{2026-11-20: 7, 2026-11-21: 5}` for the run |
| After the run | No forge Spark pod was left in namespace `spark` |

Details of the steps are in [e2e.md](e2e.md).

## PG spikes

The proof gates PG-1 to PG-8, run against a lab (core plus the spark tier)
with `python3 spikes/run.py --name <lab> pgN` ([spikes/README.md](../spikes/README.md)).
Each runs as a throwaway pod in the fluid-runner image (DuckDB 1.5.6, OpenTofu
1.13.1) under Pod Security `restricted`; PG-1, PG-2 and PG-5 sign in as the
Keycloak client `spark`. Each ran twice, then again with stricter checks; the
verdicts held.

| Gate | Verdict | Evidence |
|---|---|---|
| PG-1 | PASS | A plain `duckdb.connect()` ran `CREATE SECRET (TYPE iceberg, CLIENT_ID, CLIENT_SECRET, OAUTH2_SERVER_URI, OAUTH2_SCOPE)` and `ATTACH 'sales-wh' (TYPE iceberg, ENDPOINT 'https://lakekeeper.lab.test/catalog')`. It read the 3 rows Spark had seeded into `raw.events`, and inserted a 4th. Storage access used only vended credentials (per-table `s3` secrets from provider `iceberg`); no `AWS_*` variable was set. The lab CA came from `SET ca_cert_file` |
| PG-2 | PASS, with a forge finding | The same ATTACH ran inside forge's `secure_duckdb_connect`, in `before_lock`. After the lock, `lock_configuration` is true, `enable_external_access` false, and `allowed_directories` holds the spill directory plus the 3 declared prefixes. The REST and S3 reads work. The sandbox refused a local file and another bucket (`PermissionException`) and unlocking (`InvalidInputException`). **Finding:** the sandbox pins `ca_cert_file` only after `before_lock` returns. The same ATTACH without its own `SET ca_cert_file` cannot get its OAuth2 token ("SSL peer certificate or SSH remote key was not OK"), so a `before_lock` that signs in must set the CA itself |
| PG-5 | PARTIAL | `apache/iceberg` 0.1.0 with a bearer token: create works, the second plan is a no-op, and a nullable column add plans as an in-place update, applies, and leaves a no-op plan. **Every schema change plans as `update`, never as a replace:** an incompatible change (`long` to `string`), which the apply refuses ("string is not a valid type promotion from long"), and a column drop, which applies. A gate that keys on plan actions would let both through, so a data-loss gate must classify the schema change itself. **Provider finding:** a drop back to the table's first schema fails at apply ("Cannot set current schema to unknown schema with id: '2'"); the provider likely expects a new schema id, and Iceberg reuses the id of the identical earlier schema. Destroy works; afterwards the namespace and table answer 404. The warehouse's delete profile is hard; object-store deletion was not checked |
| PG-6 | PARTIAL | The `pg` backend on the lab Postgres (TLS verified against CNPG's CA) works with schema `"Lab Spike-PG6"`, so schema-name quoting works. State and plan are encrypted with pbkdf2 and aes_gcm, both enforced: the saved plan is not a plain plan zip and holds no cleartext canary, and the state row Postgres stores is an encrypted envelope with no canary. But **`tofu show -json` prints the sensitive canary in cleartext**; `state pull` decrypts, as designed. `show -json` is byte-stable across two runs. Not run: the OpenBao transit key provider and the `TF_ENCRYPTION` merge behaviour |
| PG-3 | not run | It needs Trino OAuth2 and the OPA bridge (`lab check` records `b-oauth2` and `c` fail) |
| PG-4 | not run | Rook-Ceph RGW is a profile that is not part of kind's `core` |
| PG-7 | not run | It needs Kafka Connect and a Strimzi custom resource applied through `kubernetes_manifest` |
| PG-8 | not run | The Key Service pilot is a platform drill outside the lab |

Spark and DuckDB read and wrote the same Iceberg table through Lakekeeper,
each with credentials vended for that table.

Planted cases that keep the spikes honest:
- PG-2 counts only DuckDB's typed refusal as the sandbox refusing; a failed connect or read ends the spike with exit 1.
- PG-5 checks that the table answers 200 after create, deletes what a killed run left, and never reads a stale saved plan. It says only that a column drop applies as an update, because a spike that writes no rows cannot show data loss.
- PG-6 also checks that the plan is not a plain zip and that the stored state row is an encrypted envelope; an unreadable row is "unknown", never clean.
- PG-6 connects with `sslmode=verify-full`, and the password lives only in `PGPASSWORD`.
- PG-1 and PG-2 redact the client secret from DuckDB's error text.
- A pod that cannot start fails at once, and SIGTERM runs the cleanup.

## Recorded failures

| What | Result | Evidence | What the lab does instead |
|---|---|---|---|
| Lakekeeper's KV2 secret backend against OpenBao | FAILED, Lakekeeper v0.14.0 with OpenBao 2.7.1 | With `LAKEKEEPER__SECRET_BACKEND=kv2`, Lakekeeper logs `Vault is unhealthy ... missing field performance_standby` for every `GET /v1/sys/health`: its Vault client requires a field OpenBao does not return. `/health` answers 503 and the liveness probe restarts the pod (7 restarts in 9 minutes) | Lakekeeper's Postgres secret store, encrypted with a per-lab key that `lab up` generates into OpenBao and hands over as Secret `lakekeeper-pg-encryption` |

## Not proven yet

These criteria fail today. Each line says what it needs.

- `a-trino`: a Trino read of an Iceberg table through vended credentials. It needs a seeded `events_raw` table and the read.
- `b-oauth2`: Trino's browser login. It needs a confidential realm client for the web UI, the OAUTH2 authentication type beside JWT, and a browser.
- `c`: a forge-shaped Rego package that filters rows and masks columns, and denies when a group lookup fails.
- `d`: a KafkaConnector that writes Iceberg through OAuth2 and vended credentials. It needs KafkaConnect on the `connect-iceberg` image, which is not built.
- `f`: a signed OPA bundle polled from SeaweedFS with a static read-only key.
- `h-egress`: default-deny egress per namespace, and an egress canary.
- `j`: forge's generated apply DAG running a real contract through the env allowlist. The `orchestration` record proves a stand-in DAG.
- `lab accept`: no live run. It needs `fluid-host`, a Kafka fixture and a Trino JWT for dbt-trino ([lab-accept.md](lab-accept.md)).
- PG-3, PG-4, PG-7 and PG-8: each needs the part named in the PG spikes table.
- Kafka authentication from forge: its Kafka source supports only `auth: none` and `msk-iam`, so the stream uses the unauthenticated `plain` listener ([streaming-tier.md](streaming-tier.md)).
- `restricted` Pod Security in the namespaces that enforce `baseline` today.
- Memory under a real `fluid apply` in the orchestration tier.
- The `h-mirror` record counts a finished smoke Job's pod, kept for an hour for its logs, as a running image. The record clears once the Job's TTL has passed.
