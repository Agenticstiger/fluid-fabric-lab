<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# PLANTED

The selftest unpacks the cached Lakekeeper chart to `products/lk/` in a copy
of the repository and points the Helmfile release at it. It renders, so only
the chart-source check can catch it: the render gate must FAIL there.
