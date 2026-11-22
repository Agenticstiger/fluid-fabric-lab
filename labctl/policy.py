# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Stage 8 in the lab: forge's policy-apply onto Lakekeeper and OpenFGA.

A contract's access policies land in the lab as Lakekeeper grants, which Lakekeeper keeps
in OpenFGA (criterion (g) of `lab check` proves OpenFGA enforces them). forge 0.37.0, the
pinned release, applies compiled bindings to Lakekeeper with `fluid policy apply`
(https://github.com/Agenticstiger/forge-cli/pull/993).

`target()` names the lab's side in one place (the catalog, the identity provider and the
realm client policy-apply acts as), so the stage that runs forge's pipeline in the lab
(`lab accept`) passes it to forge. Secrets are named by reference, never by value.
"""

from __future__ import annotations

from labctl import bootstrap, state

LAKEKEEPER = "https://lakekeeper.lab.test"
KEYCLOAK = "https://keycloak.lab.test"
# The realm client forge's policy-apply acts as: lab-admin, which manages the project and its
# warehouse.
POLICY_CLIENT = "lab-admin"


def target(lab: state.Lab) -> dict:
    """What forge's policy-apply needs to reach Lakekeeper in this lab."""
    return {
        "catalog_uri": f"{LAKEKEEPER}/catalog",
        "management_uri": f"{LAKEKEEPER}/management",
        "warehouse": bootstrap.WAREHOUSE,
        "authz_backend": "openfga",
        "token_endpoint": f"{KEYCLOAK}/realms/{bootstrap.REALM}/protocol/openid-connect/token",
        "scope": "lakekeeper",
        "client_id": POLICY_CLIENT,
        "client_secret_ref": f"openbao kv/lab/credentials, key client.{POLICY_CLIENT}",
        "ca_file": str(lab.state / "ca" / "ca.crt"),
    }
