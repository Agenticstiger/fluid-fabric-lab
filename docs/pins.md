<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Pins, chart cache and offline schemas

Resolved 2026-10-14. `versions.lock.yaml` is the source of truth; this page
explains the choices behind it. Nothing here was run in a cluster: every
runtime claim below is either an upstream fact (cited) or marked unverified.

## 1. Gateway API implementation: Envoy Gateway v1.9.2

The lab needs one Gateway API implementation for kind. Candidates were compared on 2026-10-14 from primary sources: the GitHub
API (licence, release dates), the Gateway API project's own conformance
reports (`kubernetes-sigs/gateway-api` `conformance/reports/v1.6/`, each
implementation's self-report against Gateway API v1.6.1), and each Helm chart
rendered locally with its defaults.

| Implementation | Licence | Latest release | Conformance (v1.6 reports, core) | Defaults rendered | Fit for the lab |
|---|---|---|---|---|---|
| **Envoy Gateway** (Envoy project, CNCF) | Apache-2.0 | v1.9.2, 2026-09-28 | HTTP, TLS, GRPC: success (v1.9.0) | controller 100m / 256Mi req, 1Gi limit; per-Gateway Envoy 100m / 512Mi req + shutdown manager 10m / 32Mi | Chart ships Gateway API CRDs (v1.6.1) and its own CRDs; BackendTLSPolicy (re-encrypt to HTTPS backends with a CA bundle) in the extended report; SecurityPolicy has OIDC/JWT for later edge auth; its own e2e and conformance run on kind |
| NGINX Gateway Fabric (F5/NGINX) | Apache-2.0 | v2.8.0, 2026-10-14 (today) | HTTP, TLS, GRPC, TCP, UDP: success (2.7.0) | no resource requests set | Lighter, but the chart does **not** ship Gateway API CRDs (README installs them with `kubectl kustomize` from GitHub, a second artefact to vendor); single-vendor; latest release too new to pin |
| kgateway (CNCF Sandbox) | Apache-2.0 | v2.4.6, 2026-10-06 | HTTP, GRPC, TCP, TLS: success (v2.4.5) | not rendered | Envoy-based like Envoy Gateway with a heavier control plane; younger in CNCF |
| Traefik | MIT | v3.7.14, 2026-10-06 | HTTP **partial** (one skipped core test), TLS, GRPC success (v3.7.10) | not rendered | Lightest (one pod) but partial core HTTP |
| Contour (CNCF) | Apache-2.0 | v1.33.7, 2026-09-07 | absent from the v1.5 and v1.6 report folders | not rendered | Conformance self-reports have lapsed |
| Cilium Gateway | Apache-2.0 | v1.20.2, 2026-09-16 | HTTP, TLS, GRPC (v1.20.0) | not rendered | Needs Cilium as the CNI (and kube-proxy replacement): replaces kind's default networking and its kube-network-policies NetworkPolicy support; a per-node agent; too invasive for a laptop lab |

**Choice: Envoy Gateway v1.9.2** (chart `oci://docker.io/envoyproxy/gateway-helm`
v1.9.2). Reasons: full core conformance on every profile we need, Apache-2.0
under a CNCF project, one archive renders everything offline (Gateway API CRDs
included), BackendTLSPolicy for "every endpoint HTTPS with a caBundle", and it
is tested on kind upstream. The cost is footprint: about 0.8 GiB of requests
for one Gateway at defaults. The lab's `EnvoyProxy` resource should lower the
proxy request and set a limit; the footprint floor counts the proxy
Deployment (`tiers.yaml`).

Pinned images: `docker.io/envoyproxy/gateway:v1.9.2` (control plane, certgen
Job and shutdown manager; the chart's generated config sets the shutdown
manager to this image) and `docker.io/envoyproxy/envoy:distroless-v1.39.1`,
whose digest the gateway binary itself pins
(`api/v1alpha1/shared_types.go` at v1.9.2) and which `crane` resolved to the
same digest. Neither carries a cosign signature (checked 2026-10-14).
Unverified: Envoy Gateway on Podman or Colima-backed kind; the Keycloak issuer
URL reachable through the Gateway from both browsers and pods.

## 2. Chart cache (`platform/charts/`)

Every chart is stored as the exact archive `helm pull` returned, and its
sha256 matches the repository index digest (or, for cert-manager,
CloudNativePG and Airflow, the signed `.prov` file, which is vendored too).
`platform/charts/SHA256SUMS` lists every file; verify with
`sha256sum -c platform/charts/SHA256SUMS` (or `shasum -a 256 -c`).

| Release | Chart | Version | Runs | Chart LICENSE in archive | Upstream repo licence (read at pin) |
|---|---|---|---|---|---|
| cert-manager | jetstack cert-manager | v1.21.2 | v1.21.2 | absent | Apache-2.0 |
| trust-manager | jetstack trust-manager | v0.25.0 | v0.25.0 | absent | Apache-2.0 |
| envoy-gateway | envoyproxy gateway-helm (OCI) | v1.9.2 | v1.9.2 | absent | Apache-2.0 |
| cloudnative-pg | cloudnative-pg | 0.29.1 | 1.30.1 | Apache-2.0 | Apache-2.0 |
| seaweedfs | seaweedfs | 4.48.0 | 4.48 | absent | Apache-2.0 |
| lakekeeper | lakekeeper (git main@cffd5b74, packaged) | 0.12.0+main.cffd5b74 | **v0.14.0** (chart ships 0.13.3) | absent | Apache-2.0 |
| openfga | openfga | 0.3.15 | v1.21.0 | absent | Apache-2.0 |
| keycloak-operator | local wrapper of raw manifests | 26.8.0 | 26.8.0 | Apache-2.0 (upstream LICENSE.txt) | Apache-2.0 |
| trino | trinodb trino | 1.44.0 | 483 | absent | Apache-2.0 |
| strimzi-kafka-operator | strimzi | 1.2.0 | 1.2.0 (Kafka 4.3.1) | absent | Apache-2.0 |
| airflow | apache-airflow | 1.22.0 | 3.2.2 | Apache-2.0 + NOTICE | Apache-2.0 |
| kafka-ui (optional) | kafbat kafka-ui | 1.6.5 | v1.5.0 | absent | Apache-2.0 |
| superset (optional) | apache superset | 0.22.8 | 6.1.0 | absent | Apache-2.0 |

**"absent" is recorded as `chart_spdx: unlicensed`** in the lock, exactly as the
archive is. Ten of thirteen archives ship no LICENSE file. For each, the source
repository's LICENSE was read at the pinned commit and copied to
`platform/charts/upstream-licenses/`, so the licence gate has text to read.
The licence gate therefore needs a rule for "chart archive unlicensed, upstream
repository licence text vendored and read": that is a policy for the
licence gate, not something to settle by editing upstream archives.

**Every chart renders with no network.** All dependencies are inside the
archives. Checked by rendering each one with every proxy variable pointed at a
dead port: 13 of 13 rendered. Vendored Bitnami subcharts (all Apache-2.0 chart
code) and what the lab must do with them:

- openfga 0.3.15: `common` 2.13.3 is a library chart and is always rendered
  (templates only, no image). `postgresql` and `mysql` default to
  `bitnamilegacy/*` images and must stay disabled.
- airflow 1.22.0: `postgresql` 13.2.24 (`bitnamilegacy/postgresql`,
  `postgresPassword: postgres`) must stay disabled.
- superset 0.22.8: `postgresql` and `redis` (Bitnami) must stay disabled.
- lakekeeper: its `openfga` 0.3.10 subchart (with Bitnami subcharts inside) is
  behind `internalOpenFGA: false`; its groundhog2k `postgres` subchart (MIT,
  `docker.io/library/postgres:18.4`) is behind `postgresql.enabled: false`.

No chart needs a Bitnami or OCI dependency **at render time**, so none was
refused. The image denylist must still run on the rendered output, because
flipping any of those switches renders a denied image.

### Lakekeeper: packaged from `main@cffd5b74`

The lab pins chart 0.12.0 at `main@cffd5b74`. That commit is the
current head of `lakekeeper/lakekeeper-charts` main and differs from the
released `lakekeeper-0.12.0.tgz` by exactly one template line
(`templates/config/secret-config-envs.yaml`: `secretBackend.kv2.secretMount`
was read as the undefined `secret_mount`) plus its Changelog entry (checked by
`diff -r`). It was built with `git archive cffd5b74 charts/lakekeeper`,
`helm dependency update` (both dependency archives match their repository
index digests) and `helm package --version 0.12.0+main.cffd5b74`. The build
metadata suffix keeps it distinguishable from the release. The archive is
committed and pinned by sha256; rebuilding gives a different sha256
(timestamps), so the committed file, not a rebuild, is authoritative.

The chart's `appVersion` is 0.13.3 and the lab runs v0.14.0: recorded as an
allowed `app_version_mismatch` with its reason. The chart's helm-test hook uses
`curlimages/curl:latest`; the lab never runs `helm test`.

### Keycloak Operator: a minimal local wrapper chart

Upstream ships the operator as raw manifests per tag in
`keycloak/keycloak-k8s-resources` (no Helm chart). `platform/charts/keycloak-operator/`
holds:

- `crds/` and `upstream/`: byte-identical copies of the 26.8.0 files (commit
  `44077f21`), with per-file sha256 in the lock;
- `templates/operator.yaml` (ours): renders `upstream/kubernetes.yml` into the
  release namespace the way upstream's own `kustomization.yml`
  NamespaceTransformer does (namespace on namespaced objects;
  `setRoleBindingSubjects: allServiceAccounts`), then sets the operator and
  Keycloak images to digest pins and the pull policy to `IfNotPresent`
  (upstream says `Always`). Nothing else changes. This avoids a kustomize
  dependency while staying a pure Helm release under Helmfile.

### Charts we did not vendor

The candidate patch releases (section 5) are recorded, not stored. Rook-Ceph
(`lab-ceph`) and the OTel Collector are outside this chart set.

## 3. Images

`versions.lock.yaml` `images:` lists 50 references (49 resolved on 2026-10-14, plus the mirror registry on 2026-11-18): every image the charts
would run with defaults **and** every override the lab applies.
Each has its multi-arch index digest (`crane digest`), platforms, upstream
licence, signature mode and SBOM presence, as observed in the registries on
2026-10-14. `use` separates what the lab runs (`used`, `optional`) from chart
defaults it overrides or disables (`default-unused`) and denylist matches
(`denied`). All 50 resolved.

Required overrides (chart default -> lab pin):

| Component | Chart default | Lab pin |
|---|---|---|
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.13.3` | `v0.14.0` |
| OPA (Trino sidecar) | `openpolicyagent/opa:1.16.2-static` | `1.21.1-static` |
| PostgreSQL (CNPG) | operator default `18.6-system-trixie` | `17.11-standard-trixie` (PostgreSQL 17.11; `system` images are deprecated upstream) |
| Strimzi Kafka | chart lists 4.2.0 - 4.3.1 | `1.2.0-kafka-4.3.1` |
| Superset (optional) | `apachesuperset.docker.scarf.sh/apache/superset:6.1.0` (Scarf analytics proxy) | `docker.io/apache/superset:6.1.0` (same digest) |
| Keycloak operator / server | tags, `imagePullPolicy: Always` | digest pins, `IfNotPresent` |

Signatures observed (cosign-verifiable unless "none"):

- **keyless, sigstore bundle via OCI referrers (cosign v3):** Strimzi operator,
  kafka and maven-builder (identity: the `strimzi/strimzi-kafka-operator`
  `release.yml` workflow on branch `release-1.2.x`, issuer GitHub Actions) and
  kafka-bridge (`strimzi/strimzi-kafka-bridge` `release.yml` on
  `release-1.1.x`); OpenBao 2.7.1 (`release-images.yml` at tag `v2.7.1`). The
  Strimzi buildah and kaniko images carry hundreds of
  `build.yml@refs/heads/main` signatures (rebuilt base images). Note the
  Strimzi identities are branch refs, not tags: a verifier must pin the
  branch-ref identity, which is weaker than a tag.
- **keyless, legacy `.sig` tag:** trust-manager (`release.yaml` at `v0.25.0`)
  and its trust package image (`trust-package-release-debian-trixie.yaml` on
  `main`), SeaweedFS (`container_release_unified.yml` at `4.48`), OpenFGA
  (`release.yaml` at `v1.21.0`).
- **key:** cert-manager (Google Cloud KMS key, public key
  `cert-manager-pubkey-2021-09-20.pem`, upstream verifies with
  `--insecure-ignore-tlog`); `curlimages/curl` (unused).
- **none found:** kindest/node, Envoy Gateway and Envoy, CloudNativePG operator
  and PostgreSQL (CNPG carries SLSA provenance and SPDX SBOM attestations
  instead), Lakekeeper, Keycloak and its operator, Trino, OPA, Airflow, kafbat
  UI, Superset. For these the digest pin is the only integrity control; the
  lab's mirror must pull by digest.

SBOMs attached to the image: CloudNativePG operator, PostgreSQL (both tags) and
PgBouncer, kafbat UI, and the unused `library/postgres` and `library/redis`
(buildkit in-index SPDX attestations). Strimzi publishes its SBOM as a release asset
(`strimzi-sbom-1.2.0.tar.gz`), not attached. Everything else: none. Per-image
licence inventories therefore still need syft (5.7).

Non-OSI base layers (Red Hat UBI EULA): Keycloak and its operator (UBI 9
micro), Trino 483 (UBI 10 micro), Strimzi operator/kafka/bridge (UBI 9
minimal), Strimzi maven-builder (UBI 9 OpenJDK). Recorded per image in
`license.note`.

The mirror registry (`lab-registry`, added 2026-11-18 for `lab mirror`):
`docker.io/library/registry:3.1.2`, the distribution v3.1.2 release
(Apache-2.0), which kind's local-registry guide uses. It is a sibling
container, not rendered by any chart. No signature was found; the Docker
official image carries buildkit SPDX and SLSA v0.2 attestations in its index.
It is the only image `lab mirror` runs from upstream; everything else is
copied into it by digest with `crane copy` and verified with `crane digest`.

Built by the lab, with no published digest: `connect-iceberg` and
`fluid-runner` (`built_images`, digest `null`). `fluid-runner` is built on the
operator's machine by `lab build` from a base pinned by digest
(`python:3.13.18-slim-trixie`), and is never pushed anywhere. forge in it is
built from source, pinned by commit: `contents.forge` in the lock holds
the `Agenticstiger/forge-cli` commit and the sha256 of that commit's source
archive (BuildKit's `ADD --checksum` refuses any other bytes). forge's
dependencies and build tools install with `--require-hashes`; the probe
refuses an image built from another commit, and the image tag carries it
(`lab.local/fluid-runner:0.37.0-c826f3ae`). **To bump forge**: edit
`contents.forge` (commit, archive sha256, version) and the commit in
`platform/images/fluid-runner/requirements.in`, run
`platform/images/fluid-runner/compile.sh`, then the launcher tests and
`./lab build fluid-runner`; the steps are in
[platform/images/fluid-runner/README.md](../platform/images/fluid-runner/README.md).
Pinned: forge 0.37.0, the release (tag `v0.37.0`, commit `c826f3ae`, `release: true`), since 2026-11-22; before that forge-cli main
`b9c675e1` (0.32.1.dev0, unreleased). `quay.io/vakamo/lakekeeper-plus` is denied and
deliberately not resolved.

`spark-iceberg` (the `spark` add-on, built by `lab build spark-iceberg`):
`docker.io/apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`, pinned by index digest
`sha256:dbe5f3b3926b01fa0b818ae8463c2994bfd2945d8db96d2337e23dc62d6fa73d` (amd64 and
arm64; Ubuntu 22.04, JDK 17, Python 3.10, user `spark` uid 185), resolved 2026-11-22. The lab adds
Iceberg 1.12.0 (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`), `hadoop-aws` 3.3.4, the AWS SDK for
Java v1 bundle 1.12.262, `kafka-clients` 3.4.1, `commons-pool2` 2.11.1, the Spark 3.5.8 Kafka token
provider, and an offline Maven repository holding only the Spark 3.5.8 Kafka connector and its poms. Each file
is fetched from Maven Central with a sha256 the lock records (computed locally, the Maven `.sha1` checked).
Iceberg 1.12.0 reads and writes through Lakekeeper's REST catalog and S3A reaches SeaweedFS on this
combination, proven on a live lab on 2026-11-22. The `iceberg-spark-runtime-3.5_2.12` artifact is the one
for Spark 3.5. It is a lab-built image and is never pushed (digest `null` in `built_images`); everything in
it is Apache-2.0 (docs/spark-tier.md).

## 3a. Memory trims to fit the tier targets (2026-10-15)

The L0 footprint floor failed the real repository: core counted 9.16 GiB
against its 8 GiB VM target and orchestration 11.66 GiB against 11 GiB.
The rule is **trim to fit**. The tier targets in
`tiers.yaml` were not raised and the gate was not weakened (it was made
stricter in the same change: every container now needs a memory limit, heap
flags are read from every env/envFrom/ConfigMap/Secret source, HPAs count at
maxReplicas, and any unmodelled kind fails).

**Every value below is a lab-sized guess, not a measurement.** Nothing here
was measured under load. Measured resting figures are in
[proven.md](proven.md#measured-memory); a component that needs more gets a higher value.

| Component | Was (request / limit) | Now (request / limit) | Why this value |
|---|---|---|---|
| Trino coordinator (single node, `include-coordinator`) | 2Gi / 2560Mi; heap 70% (~1.75Gi); query.max-memory(-per-node) 768MB | 1Gi / 1536Mi; heap 70% (~1075Mi); 512MB / 512MB | Trino needs heap >= `query.max-memory-per-node` + heap headroom (30% by default): 512MB + ~322MB fits ~1075Mi. Enough for TPCH tiny and small Iceberg tables; far below Trino's production guidance. Worker values mirror it (0 workers render). |
| OPA sidecar | 256Mi / 512Mi | 128Mi / 256Mi | Serves a deny-all policy. A rows/masks and group-lookup bundle may need more: measure then. |
| Keycloak (CR) | 768Mi / 1Gi | unchanged | Already well under the operator default (1700Mi / 2Gi, tiers.yaml). Quarkus sizes its heap at 70% of the limit (~716Mi); we would not start it smaller without a measurement. |
| Keycloak operator | 256Mi / 384Mi | 256Mi / 320Mi | Upstream manifest asks 450Mi; one realm, one CR. |
| CNPG PostgreSQL (`lab-pg`, one instance) | 384Mi / 512Mi; shared_buffers 96MB; effective_cache_size 256MB | 256Mi / 384Mi; 64MB; 192MB | PostgreSQL's own default shared_buffers is 128MB; five small lab databases. max_connections 100 kept (pool sizes are not tuned). The footprint floor now fails a shared_buffers that does not fit the limit. |
| Lakekeeper | 128Mi / 256Mi | 96Mi / 192Mi | Rust binary; one warehouse. |
| OpenFGA | 96Mi / 192Mi | 64Mi / 128Mi | Go binary; one store, Postgres-backed. |
| cert-manager controller, cainjector | 64Mi / 128Mi each | 64Mi / 96Mi each | A handful of Certificates; the chart sets no default. |
| Envoy Gateway controller | 128Mi / 256Mi | 96Mi / 192Mi | One GatewayClass, one Gateway (chart default 256Mi request / 1Gi limit, section 1). |
| Lab Envoy proxy (EnvoyProxy) | 64Mi / 192Mi | 64Mi / 128Mi | One listener set, lab traffic only. |
| Airflow scheduler | 640Mi / 896Mi | 512Mi / 768Mi | LocalExecutor, parallelism 4, one generated DAG. |
| Airflow api-server | 512Mi / 768Mi | 384Mi / 640Mi | `api.workers: 1`. |
| Airflow dag-processor, migrations Job | 384Mi / 512Mi; 256Mi / 384Mi | unchanged | Both import Airflow in full; no headroom to take. |

Counted totals (limits/heaps + estimates + outside) against the targets:
core 7.41 of 8 GiB, streaming 9.28 of 13 GiB, orchestration 10.03 of 11 GiB, spark 9.91 of 11 GiB, e2e 13.28 of 14 GiB, full 11.91 of 16 GiB. The footprint gate fails when this line differs from what it counts;
`python3 tools/l0/footprint.py -v` prints every counted item.

## 4. Tools

CI pins with checksum URL and the linux/amd64 sha256 copied from it, in
`versions.lock.yaml` `tools:`: helm v3.22.0 (Helmfile with Helm 4 is
unverified, though the renders were also checked with local Helm
v4.3.0), helm-diff v3.15.15, helmfile v1.8.1, kind v0.33.0, kubeconform v0.8.0,
gitleaks v8.30.1, reuse 6.2.0 (PyPI, `--require-hashes`), trivy v0.74.0 (the
newest release at least 14 days old), crane v0.22.1 and yq v4.54.1.

## 5. Candidates (recorded, not in use)

Strimzi 1.3.0 (chart and images), Keycloak 26.8.1 (manifests commit and
images), OPA 1.21.2-static and OpenFGA v1.22.0 (chart 0.3.16), each with
digests in `candidates:`. They are 2-8 days old. They replace the current pins
only after a soak period and a green run, one commit per bump.

## 6. Offline schemas (`platform/schemas/`)

- `kubernetes/v1.36.4-standalone-strict/`: 27 kinds downloaded unmodified from
  `yannh/kubernetes-json-schema` at commit `817113f2` (Apache-2.0). The kinds
  are the union of what every vendored chart renders plus the ones the lab's
  own manifests will need (`kubernetes/kinds.txt`). Upstream publishes **no
  schema for `CustomResourceDefinition`**; it is listed in
  `kubernetes/MISSING.txt` as a reviewed exception. The schema check must name
  it explicitly (`-skip CustomResourceDefinition`) and rely on the chart
  digests for CRD integrity, never skip silently.
- `crds/<group>/<kind>_<version>.json`: 64 schemas generated offline from the
  53 CRDs inside the vendored charts by `tools/crd2jsonschema.py`, a
  standard-library port of kubeconform v0.8.0's `openapi2jsonschema.py`
  (same transform: `additionalProperties: false` below the root,
  `int-or-string` as `oneOf`).
- `regenerate.sh crds|core|sums|verify` rebuilds them from the pins in the
  lock. Only `core` uses the network. `verify` checks `SHA256SUMS` and that no
  unlisted file exists.

The schemas were checked against the defaults of every vendored chart with
`kubeconform -strict` and the flags in `versions.lock.yaml`
`schemas.kubeconform_flags`, offline: no invalid resources. A planted unknown
field in a `Gateway` was rejected, and a `strimzi.io/v1` `Kafka` and a CNPG
`Cluster` validated.

## 7. Receipts

```
Checked locally:
- versions of helm/helmfile/kind/kubeconform/gitleaks/reuse/trivy/crane/yq on this machine
Searched (2026-10-14):
- gh api: release dates + licences for envoyproxy/gateway, nginx/nginx-gateway-fabric,
  kgateway-dev/kgateway, traefik/traefik, projectcontour/contour, cilium/cilium
- kubernetes-sigs/gateway-api conformance/reports/v1.6/* (self-reports, Gateway API v1.6.1)
- Helm repo indexes (jetstack, cloudnative-pg, seaweedfs, openfga, trinodb, strimzi,
  airflow, kafbat, superset, lakekeeper, groundhog2k); OCI charts via helm pull
- lakekeeper/lakekeeper-charts @ cffd5b74 vs lakekeeper-0.12.0.tgz (diff -r)
- keycloak/keycloak-k8s-resources @ 26.8.0 (44077f21)
- registries via crane + the OCI referrers API: digests, platforms, cosign tags,
  sigstore bundles (Fulcio certificate SAN/issuer read with openssl), buildkit attestations
- cert-manager/website code-signing.md (KMS key); envoyproxy/gateway shared_types.go @ v1.9.2
- cloudnative-pg/postgres-containers README (system images deprecated)
- yannh/kubernetes-json-schema @ 817113f2; yannh/kubeconform scripts/openapi2jsonschema.py @ v0.8.0
Prior art reused:
- kubeconform's openapi2jsonschema transform (ported, Apache-2.0, attributed in the file header)
- yannh/kubernetes-json-schema schemas (vendored unmodified)
- upstream Keycloak kustomization.yml NamespaceTransformer semantics (re-done in a Helm template)
```
