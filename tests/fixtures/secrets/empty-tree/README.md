<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# PLANTED

The selftest deletes every file of this fixture before the run: gitleaks on an
empty tree reports no findings, so secret_scan must FAIL instead of passing.
