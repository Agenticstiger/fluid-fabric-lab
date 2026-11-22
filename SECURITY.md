<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Security policy

## Supported versions

None. The lab has no release. It is a NO-GO test lab
(see [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md)), so no version is
supported for production use, now or after a release.

| Version | Supported |
| --- | --- |
| none released | no |

## Reporting a vulnerability

Please do not report security problems in public issues, pull requests,
discussions or chat.

Use **GitHub Private Vulnerability Reporting**: open the repository's
**Security** tab and choose **Report a vulnerability**. The report stays
private between you and the maintainers until a fix and an advisory are
published.

While the repository is private, GitHub may not offer that button. If you do
not see it, open an issue in this repository that says only "security report:
please open a private channel", with no details. A maintainer will arrange a
private GitHub channel for the details (once the repository is public, that is
a draft security advisory).

Please include:

- what is affected (file, chart, image or check) and the commit you tested;
- how to reproduce it, and what an attacker gains;
- whether you have told anyone else.

We aim to acknowledge a report within five working days and to agree a
disclosure date with you. We credit reporters in the advisory unless you ask
us not to.

## What is in scope

- Files in this repository: the launcher, the L0 gates, Helmfile and values,
  policy packages, profiles, image build files and CI workflows.
- A gate that passes something it should fail (for example a denied licence or
  image getting through), or any check that exits 0 on a skip.
- Secrets, credentials or personal data committed to the repository.

## What is not in scope

- Vulnerabilities in upstream components (Trino, Keycloak, Lakekeeper,
  OpenFGA, OPA, OpenBao, Strimzi, SeaweedFS, CloudNativePG, cert-manager,
  Envoy Gateway, Airflow and the rest of `THIRD-PARTY.md`). Report those to
  the upstream project. If a pinned version is affected, tell us as well so we
  can move the pin.
- Vulnerabilities in data-product-forge (`fluid`). Report those through that
  project's own security policy.
- The lab's documented NO-GO limits (single node, lab defaults, no backups).
  These are known and listed in `PRODUCTION-READINESS.md`.
