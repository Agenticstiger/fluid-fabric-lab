<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Stage 8 in the lab: access policies onto Lakekeeper and OpenFGA

This tier maps a contract's access policies onto the lab's catalog. Stage 8 of forge's pipeline, policy-apply,
writes them as Lakekeeper grants, and Lakekeeper keeps those in OpenFGA.

## Where it stands

| Side | State |
|---|---|
| The lab | Ready. Lakekeeper runs with the OpenFGA authorization backend, and `lab check` (g) proves OpenFGA refuses a client with no grant, with a positive control. The target is named in one place: `labctl/policy.py` `target()` |
| forge | Not yet. On forge-cli main (`e37cdda5`), `fluid policy apply <bindings.json> --mode check\|enforce` applies compiled bindings to a warehouse provider; it has no Lakekeeper target |

## What the lab provides to forge

| | |
|---|---|
| Catalog | `https://lakekeeper.lab.test/catalog` (Iceberg REST), warehouse `sales-wh` |
| Grants | Lakekeeper's management API, `https://lakekeeper.lab.test/management`; authorization backend `openfga` |
| Sign-in | Keycloak realm `fluid`, `https://keycloak.lab.test/realms/fluid/protocol/openid-connect/token`, client credentials, scope `lakekeeper` |
| Client | `lab-admin` for now. Once forge's target lands, policy-apply gets a client of its own with grant rights only |
| Trust | the lab CA, `<state dir>/ca/ca.crt` |
| Secrets | by reference only: the client's secret is `client.lab-admin` in OpenBao's `kv/lab/credentials` |

## When forge's target lands

1. `lab accept` runs stage 8 with `target()`, in the form forge's Lakekeeper target reads.
2. `lab check` gains a record that a contract's reader grant lets that reader see a table, and that a client with no
   grant is still refused. That is (g)'s positive and negative pair, driven by the contract this time.
