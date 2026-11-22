<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-FileCopyrightText: 2026 Christopher Ducci and the AQUIFER contributors
SPDX-License-Identifier: Apache-2.0 AND MIT

Structure (NO-GO banner, release-gate table, safe migration order) adapted from
AQUIFER's PRODUCTION-READINESS.md (https://github.com/doochman/aquifer, commit
0a809b6e, MIT). The content is rewritten for this lab; see NOTICE.
-->

# Production readiness

> **Status: NO-GO.** This repository is a test lab for local development
> only. `lab up` installs the `core` tier on kind, and `lab check` proves 9 of
> its 16 evidence records; there is no acceptance run. It is not approved for
> real data, for production use, or for exposure beyond the machine it runs
> on: every endpoint binds to 127.0.0.1. A green lab run is not production
> approval.

The lab exists to prove that `fluid` (data-product-forge) drives a sovereign,
on-prem-shaped stack through open protocols. It is built on one kind cluster
with one node. It can exercise production patterns (TLS, OIDC, policy
enforcement, signed artefacts), but it cannot show resilience across fault
domains, and several of its parts are labelled stand-ins for production
components (see "Substitutes" below).

## What the offline checks cover

The repository carries:

- `versions.lock.yaml`: every chart, image and tool pinned by version and
  digest, with its licence, signature mode and SBOM status as observed when
  it was pinned. Newer patch releases are recorded as candidates and are not used.
- A committed, digest-checked Helm chart cache, so `helmfile template` can
  render without the network.
- L0 checks that need no network and no cluster: render, schema validation,
  licence gate, image denylist, brand fence, secret scan, REUSE compliance,
  chart/app version lint and a memory footprint floor.

These checks prove nothing about runtime behaviour. Every runtime claim in
this repository is either an upstream fact with a source, a lab run recorded
in [docs/proven.md](docs/proven.md), or is marked unverified.

## What the lab runs

`lab mirror`, `lab up` and `lab down` (docs/lab-up.md). The runs and what
they showed are in [docs/proven.md](docs/proven.md). In short: the `core` tier
installs from the local mirror only, OpenBao runs beside the cluster with
persistent storage and TLS, and every endpoint is HTTPS at the lab Gateway.
`lab check` (docs/lab-up.md) proves these on evidence:
- the catalog vends temporary storage credentials;
- OpenBao survives a restart with its keys;
- Lakekeeper on OpenFGA denies a client with no grant;
- the node pulls only from the mirror;
- a wrong CA is refused, and Pod Security is enforced as recorded.

The other criteria fail and name what is missing.

## Release gates

"Current state" is as of the last commit that changed this file. A gate moves
only on evidence from a lab run, never on a design document.

| Gate | Required evidence | Current state |
| --- | --- | --- |
| Identity | Trino accepts Keycloak OAuth2 browser logins and JWTs for the CLI and dbt; Lakekeeper and OpenFGA deny an unauthorized request (proves the stack is not on allow-all); negative impersonation tests | Partly run. `lab check` (g): Lakekeeper on OpenFGA refused `lab-probe`, a signed-in client with no grant, and created nothing. Trino's OAuth2 and JWT logins and the impersonation tests have not run. |
| Authorization | Trino + OPA filter rows and mask columns from a forge-shaped policy; `$files` is denied; a failed or stale group lookup denies; OPA's API is bound to loopback | Not run. OPA 1.21.1 is pinned only; the policy packages do not exist. |
| Secrets and keys | OpenBao outside the cluster with persistent storage, TLS and scripted unseal; keys survive a restart; a canary secret never appears in a plan, state file, run record or log | Partly run. `lab up` runs OpenBao 2.7.1 outside the cluster with Raft storage on a named volume, TLS from the lab CA, scripted init and unseal (3 shares, threshold 2) and a revoked root token; every lab credential is generated into it. Lakekeeper's KV2 backend against OpenBao failed (docs/proven.md), so warehouse credentials sit in Lakekeeper's Postgres store, encrypted with a per-lab key held in OpenBao. `lab check` (e) restarted OpenBao. It came back sealed, was unsealed from the stored shares, and read every credential back unchanged. The canary search has not run. Dev mode is never used. |
| Transport | Every endpoint is HTTPS with a lab CA from cert-manager and trust-manager; a wrong CA bundle is rejected | Partly run. The lab CA comes from `lab up` (cert-manager issues from it, trust-manager distributes it); the Gateway serves `keycloak`, `lakekeeper`, `s3` and `trino.lab.test` over HTTPS, and pods verify against the lab CA. `lab check` (i): a client that trusts another CA is refused, and the Gateway has only HTTPS listeners. |
| Isolation | Default-deny egress in every namespace with a canary that must fail to reach the internet; Pod Security `restricted` except recorded exemptions; Kafka authorization on and a user without an ACL denied; one domain cannot reach another domain's topic or Secret | Partly run. Four core namespaces enforce `restricted`; the other five enforce `baseline`, with the measured violations recorded in `platform/namespaces.yaml`. Default-deny egress, the canary and the Kafka checks have not run. |
| Truthful stages | `lab accept` matches the expected status for every product, stage and tier, in both directions; no stage exits 0 on a skip; an `unsupported` capability exits non-zero under `--strict` | Not run. The expectations ledger has no live run. |
| Plan integrity | A tampered `plan.json`, bundle or saved `tfplan` is refused before any apply | Not run in this lab. |
| Telemetry and egress | Installation pulls only from the local mirror; SeaweedFS telemetry, OPA's version check, dbt's anonymous usage statistics and forge's own reporting (`FLUID_COMMAND_CENTER_ENABLED=false`) are off, and a sinkhole records zero requests from the lab | Partly run. The `core` install pulled only from the local mirror: the node has no upstream registry fallback, and an image missing from the mirror fails to pull. `lab check` (h) re-reads the node's pull config and finds every running image pinned by a lock digest. The telemetry opt-outs are set in values and not observed; the sinkhole has not run. |
| Supply chain | Every image pulled by digest from a local mirror; signatures verified where upstream signs; licence gate, image denylist and SBOM licence inventory per published image; Red Hat UBI EULA shipped with any derived image or mirror bundle | Partly run. `lab mirror` copies every pinned digest of a tier and verifies each copy; the `core` tier ran from it. Signatures are observed, not verified in a run. `fluid-runner` is built on the operator's machine by `lab build` from hash-pinned inputs, probed with no network against the lock, and loaded into the lab's node; `connect-iceberg` is not built. Neither has a published digest. Every image of the lab is open software: the `spark` add-on runs the lab-built `spark-iceberg` image (Apache Spark with Iceberg), and no image needs a licence exception or a cloud credential. No image is published, and the mirror is never pushed anywhere. |
| Capacity | `core` reaches ready in a VM of at most 8 GiB or fails loudly; add-ons reach ready on a 32 GB machine; peak memory and disk measured and published per tier; missing capacity fails the run | Partly measured: `core` at rest uses about 4.5 GiB (node plus sibling containers). The end-to-end run peaked at 7.28 GiB on the kind node. `core` has not been run inside an 8 GiB VM; `tiers.yaml` keeps its estimates. `lab up` refuses a tier that does not fit the VM. |
| Recovery | A consistent catalog plus object snapshot, restored into a clean cluster within an agreed RPO/RTO | Out of scope for the lab. No backup exists. |
| Availability | Multiple independent fault domains and tested failure and maintenance behaviour | Out of scope. One kind node; a kill-a-component check would only prove that failures are reported. |
| Operations | Delivered alerts, escalation, incident and restore runbooks with named owners | Out of scope. |
| Data authority | Real source freshness, revocation and ordering tests; governed privacy enforcement on real data | Out of scope. Synthetic data only. |

## Substitutes

Where the lab stands in for a production component, its conformance report
marks the cell `substrate: substitute` and names what it replaces. Only cells
proven on reference components count toward any published result.

| Lab component | Stands in for |
| --- | --- |
| SeaweedFS (S3 with STS) | Ceph RGW (the `lab-ceph` profile, CI only, is the reference) |
| One OpenBao container | The three-host key service |
| pbkdf2 for transit | A hardware- or service-backed transit key |
| ConfigMap policy fallback | Signed OPA bundles |

## Safe order before anyone considers production

1. Agree fault domains, the identity provider, alert ownership and RPO/RTO.
2. Prove a retained, off-host catalog-and-object backup with a clean-room restore.
3. Move secrets to an external, persistent key service with tested rotation and
   least-privilege service identities.
4. Turn on TLS and authenticated identities everywhere, then test every client
   (Trino CLI, dbt, the forge runner, Airflow) together.
5. Restrict networks and broker, catalog and object-store access, and keep the
   negative authorization tests in every run.
6. Verify image signatures against pinned trust roots, deliver alerts, and test
   fault-domain failures and realistic load before approving any deployment.

Keep canonical data-product contracts unchanged. Site differences belong in a
site profile, and product lifecycle work belongs to forge. Do not add parallel
build, policy, signing or scheduling engines to this repository.
