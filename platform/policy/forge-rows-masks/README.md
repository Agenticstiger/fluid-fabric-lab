<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# forge rows / masks package (placeholder)

Not implemented. This directory is for a forge-shaped Rego package that answers Trino's
`rowFilters`, `columnMask` and `batchColumnMasks` decisions
(`/v1/data/trino/rowFilters`, `/v1/data/trino/columnMask`,
`/v1/data/trino/batchColumnMasks`, wired in
`platform/components/trino/values.yaml`), driven by the per-product data
documents forge's `policy-apply` stage emits as signed bundles.

Proved by `lab check` (c): it filters rows and masks columns, and a failed
group lookup denies. The restricted example product stays
`unsupported` in the expectations ledger. The sidecar runs a deny-all
placeholder.
