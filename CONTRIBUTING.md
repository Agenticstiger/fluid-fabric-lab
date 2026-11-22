<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Contributing

Thank you for helping. This lab is NO-GO for any real use
(see [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md)). Please read the
rules below before opening a pull request; most of them are enforced by the
L0 checks.

## Licence and sign-off (DCO)

The lab is licensed under the [Apache License 2.0](LICENSE). Under its
section 5, a contribution you submit is licensed under the same terms
(inbound = outbound). There is no CLA.

Every commit must carry a Developer Certificate of Origin sign-off. By adding
it you certify the [DCO 1.1](https://developercertificate.org/): that you
wrote the change, or otherwise have the right to submit it under this
project's licence.

```sh
git commit -s -m "fix(gates): reject an empty licence file"
```

`-s` adds a line of the form `Signed-off-by: Your Name <address>`, using the
name and address from your git configuration (a GitHub `noreply` address is
fine). Use your real name. To sign off commits you already made:

```sh
git rebase --signoff main
```

A pull request whose commits are not all signed off cannot be merged.

## Ground rules

- **Every file has a licence and copyright.** Add a REUSE header to new files:

  ```text
  SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
  SPDX-License-Identifier: Apache-2.0
  ```

  (as comments in the file's own syntax). Files that cannot carry a comment,
  such as archives, generated JSON or fixtures, get an annotation in the
  nearest `REUSE.toml` instead. `reuse lint` must pass.
- **Third-party material keeps its own licence.** Copy only what the licence
  allows, keep the upstream LICENSE and any NOTICE, and record the source
  repository, commit and authors in the file header. Add the entry to
  `NOTICE` and `THIRD-PARTY.md`. Never vendor anything under BUSL, SSPL,
  AGPL, OSL, a non-open-source EULA, or with no licence at all; the licence
  gate reads the licence text and fails on these.
- **Pins live in `versions.lock.yaml`.** A new chart is a new archive in
  `platform/charts/`, a new line in its `SHA256SUMS` and a new lock entry, in
  one commit. Images are pinned by digest. Newer upstream patches go under
  `candidates:` first and replace a pin only after a soak period and a green
  run. Regenerate `THIRD-PARTY.md` when the lock changes.
- **Borrow before you build.** Look for an existing tool or upstream pattern
  first. When you adapt one, cite the source (repository, path, commit and
  licence) in a comment next to the adapted code.
- **Keep it light.** The `lab` launcher and the L0 gates use the Python
  standard library only (Python 3.10 or newer). Prefer the pinned CLI tools
  (helm, helmfile, kubeconform, reuse, gitleaks, trivy, crane, yq) over new
  code.
- **Never pass on a skip.** A check that cannot run, or a command that is not
  implemented yet, must exit non-zero with a clear message. A skip is never a
  pass.
- **No third-party marks in our names.** Organisation, repository, package,
  image, label and API-group names use no third-party product names. Mention
  other projects only to refer to them (nominative use), with no logos and no
  "compatible" or "certified" claims. The brand fence enforces this.
- **No secrets, no personal data.** Never commit credentials, tokens, keys or
  personal email addresses. Lab secrets are generated per machine and are
  git-ignored. `gitleaks` runs in L0.

## Running the L0 checks locally

L0 is the set of checks that runs on every pull request. It needs **no
network and no cluster**: everything renders from the committed chart cache
and validates against the vendored schemas. If an L0 check reaches the
network, that is a bug.

### Tools

Install the versions pinned under `tools:` in `versions.lock.yaml`:
helm, helmfile, kubeconform, gitleaks, reuse, trivy, crane and yq, plus
Python 3.10 or newer. CI pins Helm v3.22.0; Helmfile with Helm 4 is not yet
verified.

### Run everything

The `lab` launcher discovers the L0 gates in `tools/l0/` and runs them. It
exits non-zero if any gate fails, and also if a gate cannot run. A new gate
must be registered in `tools/l0/gates.conf`, with planted and real-repository
selftest cases and a CI leg; `lab l0` fails on an unregistered one.

```sh
./lab l0 --list        # the gates that would run
./lab l0               # run every L0 gate
./lab l0 <gate> ...    # run named gates only
./lab render --tier core   # helmfile template for one tier, offline
./lab doctor --tier core   # tool pins, runtime memory and disk for a tier
```

### Run a single tool by hand

These are useful when a check fails and you want the raw tool output.

```sh
# Chart cache integrity (macOS; use `sha256sum -c` on Linux)
(cd platform/charts && shasum -a 256 -c SHA256SUMS)

# Render every release from the cache
helmfile --file platform/helmfile.yaml.gotmpl template

# Licence and copyright information on every file
reuse lint

# Secret scan of the working tree
gitleaks dir --redact .
```

The kubeconform flags for the vendored schemas are under
`schemas.kubeconform_flags` in `versions.lock.yaml`.

## Pull requests

- Keep each pull request to one logical change, with a clear description of
  what changed and how you checked it.
- State what you did **not** verify. Claims about runtime behaviour need a
  dated lab run; until then, mark them unverified.
- All L0 checks must pass before review.
