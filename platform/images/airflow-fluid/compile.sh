#!/bin/sh
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# The airflow-fluid image installs exactly what the fluid-runner image installs
# (forge's dependencies, dbt-core, dbt-trino, DuckDB, and forge's build tools),
# so these files are copies of fluid-runner's, never compiled again here:
# the apache/airflow:3.2.2 image runs Python 3.13, the minor fluid-runner's
# hashes were compiled for. tests/launcher/test_build.py fails when a copy
# differs. Run this after fluid-runner/compile.sh.
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
for f in requirements.in requirements.txt requirements-build.txt record_forge.py; do
  cp "$here/../fluid-runner/$f" "$here/$f"
done
