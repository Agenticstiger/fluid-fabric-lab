<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Identity (Keycloak realm)

Keycloak 26.8.0 runs from the Keycloak Operator (`platform/charts/keycloak-operator`,
release `keycloak-operator`) as the `Keycloak` CR in
`platform/components/keycloak/values.yaml` (release `keycloak`), on the lab
CNPG cluster, with resources set explicitly.

## The realm

`realm-import.yaml` is a second values file of the `keycloak` release. It
holds the `KeycloakRealmImport` for realm `fluid`, which the operator imports
once:

- `clientScopes`: Keycloak's built-in scopes plus `lakekeeper` (an audience
  mapper), taken from Lakekeeper's example realm at v0.14.0 with their ids
  removed. A realm import that lists scopes gets only those, so the built-ins
  are listed too (`basic` carries the `sub` claim).
- Clients, all confidential, client credentials only, with no committed
  secret: `lakekeeper` (the catalog's audience), `lab-admin` (bootstraps
  Lakekeeper and becomes its server admin), `trino` (Trino's Iceberg REST
  connection), `spark` (Spark jobs) and `lab-probe` (a principal with no
  grants, for the deny checks) and `fluid-pipeline` (a pipeline identity:
  dbt-trino through Trino; it alone may request scope `trino`).
- Each client secret is a `${...}` placeholder that the operator fills from
  Secret `keycloak/keycloak-realm-clients`, which `lab up` writes from OpenBao.
- One issuer URL for browsers and pods: `https://keycloak.lab.test/realms/fluid`,
  through the lab Gateway; pods resolve the name with the CoreDNS rewrite.

The Iceberg REST clients request scope `lakekeeper`, because Keycloak rejects a
scope it does not know and Iceberg's default scope is `catalog`.

Scope `trino` (an audience mapper, as `lakekeeper` has) puts `aud: trino` in a
token. Trino's JWT authentication requires that audience and the issuer
`https://keycloak.lab.test/realms/fluid`; the scope is an optional scope of
`fluid-pipeline` only, never a realm default (docs/trino-auth.md). The realm
import runs once, so `lab up` also reconciles the client and the scope on a
lab that was imported before they existed (`labctl/bootstrap.py`).

## Not provided

- Browser OAuth2 for Trino (`lab check` b-oauth2). The JWT audience for the
  Trino CLI and dbt is provided (`fluid-pipeline`, scope `trino`).
- One `fluid-apply-<domain>` client per domain (the profile's `oidc.clientId`).
- A dedicated read-only client for the OPA group lookup
  (`platform/policy/group-lookup`).
