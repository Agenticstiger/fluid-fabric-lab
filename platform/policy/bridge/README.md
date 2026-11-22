<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Lakekeeper OPA bridge (placeholder)

Not implemented. This directory is for the Rego that lets Trino's OPA sidecar ask Lakekeeper
(and so OpenFGA) whether a principal may see a catalog object: the `allow`
and `batch` decisions Trino calls at `/v1/data/trino/allow` and
`/v1/data/trino/batch` (`platform/components/trino/values.yaml`). Source:
Lakekeeper's published OPA bridge, adapted with attribution and pinned to the
Lakekeeper version in `versions.lock.yaml`.

It would be delivered as part of a signed bundle from a SeaweedFS prefix, not
as a ConfigMap. Today the Trino sidecar loads a deny-all placeholder policy,
so nothing is allowed by accident.

Proved by `lab check` (g): Lakekeeper on OpenFGA denies an unauthorized
request.
