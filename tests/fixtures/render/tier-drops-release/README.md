<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# PLANTED

The selftest deletes `trino` from the core tier in a copy of `tiers.yaml`.
Render, chart_app and footprint all measure against tiers.yaml, so the
independent record (each chart's `tier` in versions.lock.yaml) must make the
render gate FAIL.
