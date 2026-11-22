<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

<!--
GENERATED from versions.lock.yaml. Do not edit the tables by hand: change the
lock, then regenerate. Each table below is the output of the yq (v4.54.1)
expression shown in the comment above it, run as
  yq --string-interpolation=false -r '<expression>' versions.lock.yaml
-->

# Third-party components

This repository is Apache-2.0 (see `LICENSE` and `NOTICE`). It contains, or
makes the lab run, the third-party components below. Each keeps its own
licence. For vendored files the licence was read from the licence text
itself, never from a hosting service's licence field; where a chart archive
ships no licence file, the source repository's LICENSE at the pinned commit
was read and copied next to the archive. For images, the licence shown is the
upstream project's licence as recorded in the lock.

Two kinds of use are listed separately:

- **Vendored** (distributed in this repository): the Helm chart cache in
  `platform/charts/` and the schemas in `platform/schemas/`.
- **Referenced at runtime** (pulled by digest when the lab runs, never
  stored in this repository): container images.

Generated from `versions.lock.yaml` (lock_version 1, resolved 2026-10-14).

## Vendored Helm charts

<!-- yq: .charts[] | "| `" + .name + "` | " + .version + " | " + .use + " | " + (.license.upstream_spdx // .license.chart_spdx) + " | " + (if .license.chart_spdx == "unlicensed" then "no licence file in the archive; source repository LICENSE at commit `" + (.license.upstream_commit | .[0:12]) + "`, copied to `" + .license.upstream_text + "`" elif .license.chart_file == "present" then "LICENSE inside the archive" else "`" + .file + "/" + (.license.chart_file | sub("present \((.*)\)"; "${1}")) + "`" end) + " | " + .license.upstream_repo + " |" -->

| Chart | Version | Use | Licence | Licence text | Upstream repository |
| --- | --- | --- | --- | --- | --- |
| `cert-manager` | v1.21.2 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `922a06aa49ee`, copied to `platform/charts/upstream-licenses/cert-manager-v1.21.2.LICENSE` | https://github.com/cert-manager/cert-manager |
| `trust-manager` | v0.25.0 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `ddeb17c70923`, copied to `platform/charts/upstream-licenses/trust-manager-v0.25.0.LICENSE` | https://github.com/cert-manager/trust-manager |
| `gateway-helm` | v1.9.2 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `f9f92a112a40`, copied to `platform/charts/upstream-licenses/gateway-helm-v1.9.2.LICENSE` | https://github.com/envoyproxy/gateway |
| `cloudnative-pg` | 0.29.1 | used | Apache-2.0 | LICENSE inside the archive | https://github.com/cloudnative-pg/charts |
| `seaweedfs` | 4.48.0 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `530be3e37337`, copied to `platform/charts/upstream-licenses/seaweedfs-4.48.0.LICENSE` | https://github.com/seaweedfs/seaweedfs |
| `lakekeeper` | 0.12.0+main.cffd5b74 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `cffd5b747321`, copied to `platform/charts/upstream-licenses/lakekeeper-0.12.0+main.cffd5b74.LICENSE` | https://github.com/lakekeeper/lakekeeper-charts |
| `openfga` | 0.3.15 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `c9fb3c73528e`, copied to `platform/charts/upstream-licenses/openfga-0.3.15.LICENSE` | https://github.com/openfga/helm-charts |
| `keycloak-operator` | 26.8.0 | used | Apache-2.0 | `platform/charts/keycloak-operator/upstream/LICENSE.txt` | https://github.com/keycloak/keycloak-k8s-resources |
| `trino` | 1.44.0 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `c524e513ddb4`, copied to `platform/charts/upstream-licenses/trino-1.44.0.LICENSE` | https://github.com/trinodb/charts |
| `strimzi-kafka-operator` | 1.2.0 | used | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `6c7b43c4af0d`, copied to `platform/charts/upstream-licenses/strimzi-kafka-operator-1.2.0.LICENSE` | https://github.com/strimzi/strimzi-kafka-operator |
| `airflow` | 1.22.0 | used | Apache-2.0 | LICENSE inside the archive | https://github.com/apache/airflow |
| `kafka-ui` | 1.6.5 | optional | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `9f372e85b324`, copied to `platform/charts/upstream-licenses/kafka-ui-1.6.5.LICENSE` | https://github.com/kafbat/helm-charts |
| `superset` | 0.22.8 | optional | Apache-2.0 | no licence file in the archive; source repository LICENSE at commit `9a02c4b611b7`, copied to `platform/charts/upstream-licenses/superset-0.22.8.LICENSE` | https://github.com/apache/superset |

`use: optional` charts are off by default. The `keycloak-operator` chart is a
small Agentics-authored wrapper (Apache-2.0) around the Keycloak Operator's
unmodified upstream manifests.

### Charts vendored inside those archives

<!-- yq: .charts[] | .name as $p | .subcharts[] | "| `" + $p + "` | `" + .name + "` | " + (.version | tostring) + " | " + .license + " | " + (if .enabled_by_lab then "rendered" else "kept off by lab values" end) + " |" -->

| Parent chart | Subchart | Version | Licence | In the lab |
| --- | --- | --- | --- | --- |
| `gateway-helm` | `crds` | 0.0.0 | Apache-2.0 | rendered |
| `cloudnative-pg` | `cluster` | 0.0 | Apache-2.0 | kept off by lab values |
| `lakekeeper` | `postgres` | 1.6.4 | MIT | kept off by lab values |
| `lakekeeper` | `openfga` | 0.3.10 | Apache-2.0 | kept off by lab values |
| `openfga` | `common` | 2.13.3 | Apache-2.0 | rendered |
| `openfga` | `postgresql` | 12.12.10 | Apache-2.0 | kept off by lab values |
| `openfga` | `mysql` | 9.6.0 | Apache-2.0 | kept off by lab values |
| `airflow` | `postgresql` | 13.2.24 | Apache-2.0 | kept off by lab values |
| `superset` | `postgresql` | 16.7.27 | Apache-2.0 | kept off by lab values |
| `superset` | `redis` | 17.9.4 | Apache-2.0 | kept off by lab values |

The subcharts kept off would render images the lab denies (Bitnami or
unmaintained database images). The Lakekeeper chart's `openfga` subchart
itself carries further Bitnami subcharts, which are never rendered either.

## Vendored schemas

| Path | Licence | Source |
| --- | --- | --- |
| `platform/schemas/kubernetes/v1.36.4-standalone-strict` | Apache-2.0 | https://github.com/yannh/kubernetes-json-schema at commit `817113f21a2643311cbc3e992bb44ffe1c416c54` |
| `platform/schemas/crds` | Apache-2.0 | generated offline from the CRDs inside the vendored charts by `platform/schemas/tools/crd2jsonschema.py` |

`platform/schemas/tools/crd2jsonschema.py` is derived from `scripts/openapi2jsonschema.py` in https://github.com/yannh/kubeconform at v0.8.0 (Apache-2.0).

## Container images the lab runs

Only images with `use: used` or `use: optional` are listed.
19 further images are chart defaults the lab does not run
(`default-unused`), and 7 are denied by policy and must never be rendered
(`denied`); both sets are in `versions.lock.yaml` with their digests so the
mirror and the denylist can match them.

<!-- yq: .images[] | select(.use == "used" or .use == "optional") | "| `" + .ref + "` | " + .component + " | " + .use + " | " + .license.spdx + " | " + (if ((.license.note // "") | test("UBI")) then "yes" else "no" end) + " | " + .license.source + " |" -->

| Image | Component | Use | Licence | Red Hat UBI base | Source |
| --- | --- | --- | --- | --- | --- |
| `docker.io/kindest/node:v1.36.4` | cluster | used | Apache-2.0 | no | https://github.com/kubernetes-sigs/kind |
| `quay.io/jetstack/cert-manager-controller:v1.21.2` | cert-manager | used | Apache-2.0 | no | https://github.com/cert-manager/cert-manager |
| `quay.io/jetstack/cert-manager-webhook:v1.21.2` | cert-manager | used | Apache-2.0 | no | https://github.com/cert-manager/cert-manager |
| `quay.io/jetstack/cert-manager-cainjector:v1.21.2` | cert-manager | used | Apache-2.0 | no | https://github.com/cert-manager/cert-manager |
| `quay.io/jetstack/cert-manager-startupapicheck:v1.21.2` | cert-manager | used | Apache-2.0 | no | https://github.com/cert-manager/cert-manager |
| `quay.io/jetstack/trust-manager:v0.25.0` | trust-manager | used | Apache-2.0 | no | https://github.com/cert-manager/trust-manager |
| `docker.io/envoyproxy/gateway:v1.9.2` | gateway | used | Apache-2.0 | no | https://github.com/envoyproxy/gateway |
| `docker.io/envoyproxy/envoy:distroless-v1.39.1` | gateway | used | Apache-2.0 | no | https://github.com/envoyproxy/envoy |
| `ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1` | cloudnative-pg | used | Apache-2.0 | no | https://github.com/cloudnative-pg/cloudnative-pg |
| `ghcr.io/cloudnative-pg/postgresql:17.11-standard-trixie` | cloudnative-pg | used | Apache-2.0 | no | https://github.com/cloudnative-pg/postgres-containers |
| `docker.io/chrislusf/seaweedfs:4.48` | seaweedfs | used | Apache-2.0 | no | https://github.com/seaweedfs/seaweedfs |
| `quay.io/lakekeeper/catalog:v0.14.0` | lakekeeper | used | Apache-2.0 | no | https://github.com/lakekeeper/lakekeeper |
| `docker.io/openfga/openfga:v1.21.0` | openfga | used | Apache-2.0 | no | https://github.com/openfga/openfga |
| `quay.io/keycloak/keycloak-operator:26.8.0` | keycloak | used | Apache-2.0 | yes | https://github.com/keycloak/keycloak |
| `quay.io/keycloak/keycloak:26.8.0` | keycloak | used | Apache-2.0 | yes | https://github.com/keycloak/keycloak |
| `docker.io/trinodb/trino:483` | trino | used | Apache-2.0 | yes | https://github.com/trinodb/trino |
| `docker.io/openpolicyagent/opa:1.21.1-static` | trino | used | Apache-2.0 | no | https://github.com/open-policy-agent/opa |
| `quay.io/strimzi/operator:1.2.0` | strimzi | used | Apache-2.0 | yes | https://github.com/strimzi/strimzi-kafka-operator |
| `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` | strimzi | used | Apache-2.0 | yes | https://github.com/strimzi/strimzi-kafka-operator |
| `docker.io/apache/airflow:3.2.2` | airflow | used | Apache-2.0 | no | https://github.com/apache/airflow |
| `quay.io/openbao/openbao:2.7.1` | openbao | used | MPL-2.0 | no | https://github.com/openbao/openbao |
| `docker.io/library/registry:3.1.2` | mirror | used | Apache-2.0 | no | https://github.com/distribution/distribution |
| `ghcr.io/kafbat/kafka-ui:v1.5.0` | kafbat-ui | optional | Apache-2.0 | no | https://github.com/kafbat/kafka-ui |
| `docker.io/apache/superset:6.1.0` | superset | optional | Apache-2.0 | no | https://github.com/apache/superset |

The licence column is the project's own licence. Every image also contains
operating-system packages under their own licences (including GPL and LGPL),
which a licence-file scan does not see. Before the lab publishes any image or
mirror bundle, it must produce an SBOM licence inventory for it and meet the
corresponding-source duties, or not publish it.

### Images the lab builds

<!-- yq: .built_images[] | "| `" + .id + "` | " + .component + " | " + (.base.ref // "see `contents` in the lock") + " | " + (if .digest == null then "no published digest" else .digest end) + " |" -->

| Image | Component | Base | Digest |
| --- | --- | --- | --- |
| `connect-iceberg` | strimzi | quay.io/strimzi/kafka:1.2.0-kafka-4.3.1 | no published digest |
| `fluid-runner` | runner | docker.io/library/python:3.13.18-slim-trixie | no published digest |
| `airflow-fluid` | airflow | docker.io/apache/airflow:3.2.2 | no published digest |
| `spark-iceberg` | spark | docker.io/apache/spark:3.5.8-scala2.12-java17-python3-ubuntu | no published digest |

The `spark-iceberg` image (the `spark` add-on tier, `platform/images/spark-iceberg`) is the official Apache Spark
image plus the files below, each fetched from Maven Central with a checked sha256
(`built_images[spark-iceberg].contents.files` in `versions.lock.yaml`). All of them are Apache-2.0. The lab builds
it on the operator's machine and never publishes it; the base image also holds operating-system packages under
their own licences.

| Component | Version | Licence | Source |
| --- | --- | --- | --- |
| Apache Spark (base image `apache/spark`) | 3.5.8 | Apache-2.0 | https://github.com/apache/spark |
| Apache Iceberg (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`) | 1.12.0 | Apache-2.0 | https://github.com/apache/iceberg |
| Spark Kafka connector (`spark-sql-kafka-0-10_2.12`, `spark-token-provider-kafka-0-10_2.12`, and the Spark and ASF parent poms) | 3.5.8 (parent pom `apache` 18) | Apache-2.0 | https://github.com/apache/spark |
| Apache Hadoop AWS (`hadoop-aws`) | 3.3.4 | Apache-2.0 | https://github.com/apache/hadoop |
| AWS SDK for Java v1 bundle (`aws-java-sdk-bundle`) | 1.12.262 | Apache-2.0 | https://github.com/aws/aws-sdk-java |
| Apache Kafka clients (`kafka-clients`) | 3.4.1 | Apache-2.0 | https://github.com/apache/kafka |
| Apache Commons Pool (`commons-pool2`) | 2.11.1 | Apache-2.0 | https://github.com/apache/commons-pool |

## Red Hat Universal Base Image (UBI) EULA

These images are built on a Red Hat UBI base layer: `quay.io/keycloak/keycloak-operator:26.8.0`, `quay.io/keycloak/keycloak:26.8.0`, `docker.io/trinodb/trino:483`, `quay.io/strimzi/operator:1.2.0`, `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`. So is the
lab-built `connect-iceberg` image, which derives from the Strimzi Kafka
image.

The UBI layer is covered by the Red Hat Universal Base Image End User
License Agreement, which is **not an OSI-approved open-source licence**. This
is a summary, not legal advice:

- Section 2: the unmodified EULA must be included in all distributions of
  container images sourced, built or otherwise derived from the UBI programs.
  If the UBI layer is modified, Red Hat trademarks must be removed. Nothing
  may imply Red Hat endorsement.
- Section 5: United States export-control representations apply.
- Section 7: the agreement is governed by New York law.

What the lab does about it:

- The lab distributes no container image and no mirror bundle, so no EULA
  copy is shipped.
- `connect-iceberg` must carry the unmodified EULA inside the image when it
  is built.
- Any mirror or air-gap bundle that contains these images must carry the
  EULA too.
- These duties apply to anyone who redistributes such an image.

## NOTICE texts

The Apache Airflow chart ships a NOTICE file; it is reproduced in this
repository's `NOTICE`. No other vendored chart archive contains a NOTICE file.
Lakekeeper's repository NOTICE is reproduced there too, because
`platform/identity/realm-import.yaml` adapts material from it (below).

## Adapted material in this repository's own files

| What | From | Licence |
| --- | --- | --- |
| Structure of `PRODUCTION-READINESS.md` (NO-GO banner, gate table, migration order) | AQUIFER, https://github.com/doochman/aquifer at `0a809b6e` | MIT, notice kept in `NOTICE` |
| `platform/schemas/tools/crd2jsonschema.py` | `scripts/openapi2jsonschema.py` in https://github.com/yannh/kubeconform at v0.8.0 | Apache-2.0 |
| `clientScopes` in `platform/identity/realm-import.yaml` (Keycloak's built-in scopes plus the `lakekeeper` audience scope, ids removed) | `examples/access-control-simple/keycloak/realm.json` in https://github.com/lakekeeper/lakekeeper at v0.14.0 | Apache-2.0, NOTICE kept in `NOTICE` |
| Shape of the SeaweedFS STS document in `labctl/credentials.py` (identities, `sts`, `roles`, `policies`) | `.github/seaweedfs-iam.json` in https://github.com/lakekeeper/lakekeeper at v0.14.0 (keys and the role policy are the lab's own) | Apache-2.0 |
| Local registry pattern in `labctl/mirror.py` and `labctl/cluster.py` (registry container on the kind network, containerd `certs.d` hosts) | https://kind.sigs.k8s.io/docs/user/local-registry/ (kind v0.33.0) | Apache-2.0 (idea only, no text copied) |

## Licence policy

Nothing under BUSL, SSPL, AGPL, OSL or a non-open-source vendor EULA, and
nothing without a licence (neither in the archive nor in its source
repository at the pinned commit), is vendored in this repository. The L0 licence gate reads the
licence text of every vendored artefact and fails on those; an artefact that a
profile only references at runtime may be allowed with its obligations
recorded. CI tools (helm, helmfile, kubeconform, gitleaks, reuse, trivy, crane,
yq) are run, never vendored; their pins are under `tools:` in
`versions.lock.yaml`.
