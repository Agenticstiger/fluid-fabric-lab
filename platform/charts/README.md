<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Chart cache

Digest-checked copies of every upstream chart the lab renders, so
`helmfile template` works with no network. Pins, sources and licences are in
`../../versions.lock.yaml`; the reasoning is in `../../docs/pins.md`.

- `*.tgz`: archives exactly as `helm pull` returned them (sha256 equals the
  repository index digest), except `lakekeeper-0.12.0+main.cffd5b74.tgz`,
  packaged from `lakekeeper/lakekeeper-charts@cffd5b74` with its dependencies
  vendored.
- `*.tgz.prov`: upstream Helm provenance files where the project publishes them.
- `keycloak-operator/`: minimal local chart wrapping the Keycloak Operator's
  raw 26.8.0 manifests (upstream files unmodified under `crds/` and
  `upstream/`).
- `upstream-licenses/`: the source repository's LICENSE, read at the pinned
  commit, for archives that ship none.
- `SHA256SUMS`: every file above. Check it before rendering:
  `sha256sum -c SHA256SUMS` (Linux) or `shasum -a 256 -c SHA256SUMS` (macOS).

Do not edit an archive or an upstream file. A new pin is a new file, a new
`SHA256SUMS` line and a new `versions.lock.yaml` entry in one commit.
