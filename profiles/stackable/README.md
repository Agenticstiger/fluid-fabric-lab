<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Values-only overlay for a third-party data platform

This directory holds no overlay. It is where a values-only overlay goes
that swaps the Trino chart, OPA and the Airflow chart for a third-party
operator platform's operators. The L0 rules below apply to anything added
here.

The directory name is used only to identify the platform the overlay targets.
This project is not affiliated with, endorsed by or certified by that
platform's vendor, and makes no compatibility claim.

## Rules (enforced by L0)

- **Values files only, written by Agentics.** The only files allowed here are
  this README and Agentics-authored Helm values files, each carrying an
  Agentics SPDX header. An L0 check fails on any other file, and the REUSE
  check runs on this directory.
- **No derivative works of the platform's code.** No post-renderer or
  kustomize patches of rendered templates, no copied CRD YAML, no vendored
  charts. CRDs are fetched at runtime from the platform's own release.
- **Never patch the platform's operators.** A behaviour gap is reported as a
  capability gap, or fixed upstream.
- **Internal machines only.** The platform's licence (OSL-3.0) treats network
  use by others as distribution (section 5) and requires reasonable effort to
  obtain recipients' assent (section 9). So the overlay never runs in a shared
  or partner-facing environment without an OSL notice and an assent step, and
  never ships in an air-gap bundle.
- **No marks.** No third-party logos and no "compatible" or "certified" claims;
  names are used only nominatively.

Not legal advice.
