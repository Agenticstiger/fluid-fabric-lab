<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Trino authentication and authorization

Status: written offline on 2026-11-19; `lab check` record `b-jwt` first passed against a live lab on 2026-11-22 ([docs/proven.md](proven.md#lab-check-results)).

A pipeline identity runs dbt-trino against the lab's Trino. Keycloak says who
it is (a JWT), Trino checks the token, OPA decides what the query may do, and
Lakekeeper trusts Trino as the engine.

## The JWT flow

1. `lab-dbt` (`platform/dbt/lab-dbt`, stdlib only, meant for forge's
   `DBT_EXECUTABLE`) asks Keycloak (`https://keycloak.lab.test/realms/fluid`)
   for a client-credentials token for the confidential client
   `fluid-pipeline`, scope `trino`. It reads the client id and secret from the
   environment (Secret `fluid-pipeline-client` in namespace `spark`), checks
   Keycloak's certificate against the lab CA, puts the token in
   `DBT_ENV_SECRET_FLUID_TRINO_JWT` (the prefix makes dbt scrub it from its logs) for the child only, and execs `dbt` with the same
   arguments. The secret never reaches argv, a log or the child.
2. dbt reads `profiles.yml` (profile `events_pipeline`, target `local`;
   ConfigMap `lab-dbt`), which sends the token as `method: jwt` to
   `trino.trino.svc:8443`.
3. Trino 483 verifies it
   ([JWT authentication](https://trino.io/docs/483/security/jwt.html)): the
   signature against Keycloak's JWKS
   (`.../protocol/openid-connect/certs`), `iss` =
   `https://keycloak.lab.test/realms/fluid`, `aud` contains `trino`. The user
   is the token's `azp` claim, which for a client-credentials token is the
   client id (`fluid-pipeline`). Anything else, or no token, gets 401.
4. Every authorization question goes to the OPA sidecar
   ([OPA access control](https://trino.io/docs/483/security/opa-access-control.html)).
5. Trino reaches Lakekeeper as its own client `trino`, which `lab up` grants
   `describe`, `select`, `create` and `modify` on `sales-wh`
   (`labctl/bootstrap.py: trino_grant`, idempotent). Trino is the trusted
   engine: Lakekeeper does not see the user, OPA decides per user.

Why it is shaped this way:
- JWT needs TLS and an internal-communication shared secret in Trino 483, so
  the coordinator serves HTTPS (certificate `trino-tls` from the lab CA) and
  the Gateway reaches port 8443 through a BackendTLSPolicy, as it does for
  Keycloak. Trino itself answers plain HTTP with 403 once authentication is
  on. `/v1/info` stays public, so probes and `lab check` (i) are unaffected.
- `azp` rather than `preferred_username`: both are the client id for a service
  account, `azp` needs no mapping pattern. The trade-off: a future human token
  would carry the client that asked for it, not the person. Browser login
  (`b-oauth2`) must therefore use its own mapping, and no user client may be
  given scope `trino`. Only `fluid-pipeline` has it.
- The audience scope `trino` is an optional scope on `fluid-pipeline` only
  (Keycloak's audience mapper, as the existing `lakekeeper` scope does); a
  token without it, or from another client, has no `aud: trino`.
- `fluid-pipeline` tokens live one hour (`access.token.lifespan` 3600): dbt
  holds one token for a whole run and has no refresh.

## The grants

`platform/policy/trino-lab/lab.rego` (package `trino`) allows only principals
listed in OPA data, from ConfigMap `trino-grants` (`data.json`, same
ConfigMap as the policy: the chart mounts one directory into the OPA
sidecar):

```json
{"principals": {"fluid-pipeline": {"lakehouse": {
  "events_raw": "read", "events_curated": "read", "events_marts": "write"}}}}
```

`write` implies `read`. Everything else is denied (`default allow := false`).
What dbt-trino needs, and so what is allowed:

| Needs | Operations (Trino 483 names) | Where |
|---|---|---|
| any query | `ExecuteQuery`, `ExecuteFunction` | a listed principal |
| dbt-trino's list-relations join and `dbt docs generate` | `AccessCatalog` on `system`; `SelectFromColumns` on `system.metadata.materialized_views` and `system.metadata.table_comments` only (dbt-trino 1.10.6 `macros/adapters.sql`, `catalog.sql`) | a listed principal; `FilterCatalogs` still hides `system` |
| see the catalog | `AccessCatalog`, `ShowSchemas`; `FilterCatalogs`, `FilterSchemas` (batch) | a catalog the principal has a grant in |
| list and describe | `ShowTables`, `ShowColumns`; `FilterTables`, `FilterColumns` (batch) | `read` or `write` schemas, and `information_schema` of a granted catalog |
| read | `SelectFromColumns` | `read` or `write` schemas |
| materialize | `CreateSchema`, `CreateTable` (CTAS), `InsertIntoTable`, `DropTable`, `RenameTable` | `write` schemas; a rename needs `write` at both ends |

Not allowed, on purpose: grants and roles (also off in the Trino plugin),
procedures, `DELETE`, schema drops, comments, views, session properties,
impersonation, query killing, and every other table of the `system` catalog.

Consequence for dbt: only `materialized: table` works for now. Views (dbt's
default materialization) and the incremental strategies that delete, update or
merge are denied by design, as is `persist_docs` (comments). The example
product's `dbt_project.yml` already sets `materialized: table`. Adding one of
the others is a policy change: add its operations to `lab.rego`, a test to
`lab_test.rego`, and a row to this table.

The policy has unit tests (`platform/policy/trino-lab/lab_test.rego`, run by
`opa test platform/policy/trino-lab`; CI leg `policy (opa test)` and
`tests/launcher`). They cover each statement dbt-trino sends for a table
model, the system-catalog reads, `FilterColumns` (the answer is column
indices), denies and an unknown operation.

The operation names and the `resource`, `targetResource` and
`filterResources` shapes come from Trino 483's OPA plugin (documentation and
`plugin/trino-opa` at tag 483). They were read, not run: the first live run
is the test. The policy is not copied from any other policy set.

To change a grant, edit the ConfigMap (`kubectl -n trino edit configmap
trino-grants`); the sidecar runs with `--watch` and reloads it in about a
minute. The lab does not generate `data.json` from forge's compiled policy
bindings; the policy file does not change for that.

## What (c) still needs

`rowFilters`, `columnMask` and `batchColumnMasks` exist and return nothing.
Criterion (c) adds, in this package: row filters and column masks driven by
per-product data from forge's bindings, and a group lookup whose failure
denies (`platform/policy/forge-rows-masks`, `platform/policy/group-lookup`),
then the signed bundle from SeaweedFS (f).

## Existing labs

The Keycloak realm is imported once, so a lab created before this change has
no client `fluid-pipeline` and no scope `trino`. `lab up` reconciles them
(`labctl/bootstrap.py: keycloak_reconcile`): it signs in to the master realm
with the operator's Secret `keycloak/keycloak-initial-admin` and, each step
guarded by a GET, creates scope `trino` with its audience mapper, creates the
client with the credential from OpenBao, and links the scope as an optional
scope of that client only. A rerun changes nothing. The scope is deliberately
not a realm default optional scope.
