<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Group lookup, `opa` route (placeholder; lab site glue)

Not implemented. This directory is for lab-owned Rego that resolves a principal's groups
with `http.send` to the Keycloak Admin API, through a dedicated **read-only**
Keycloak client, with a short cache. It **allows only on a positive answer**:
a failed, timed-out or stale lookup denies (a fail-open group lookup is a
known failure mode in other distributions).

This is site glue, outside forge; the site
profile only declares its endpoint (`groupLookup: {route: opa, ...}`). It is
written here rather than reusing a distribution's OSL-3.0 user-info-fetcher
image, which would carry OSL-3.0 obligations if redistributed.

Proved by `lab check` (c) and the negative check "a failed or stale group
lookup denies".
