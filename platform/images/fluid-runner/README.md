<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# fluid-runner image

The image every forge stage runs in: on the operator's machine, as a
Kubernetes Job, and from the generated Airflow DAG. forge is installed once, so no stage pays for a `pip install`.

forge comes from **source, pinned by commit**: the commit is a release tag's
commit (`release: true`, now forge 0.37.0), and a dev pin (an unreleased commit
of forge-cli main) uses the same mechanism (below).

| Piece | Version | How it is pinned |
|---|---|---|
| Base | `python:3.13.18-slim-trixie` | by digest (`ARG PYTHON_IMAGE`, and `base` in `versions.lock.yaml`) |
| forge (`data-product-forge[local,aws]`) | 0.37.0 (release tag `v0.37.0`, commit `c826f3ae`) | `contents.forge` in `versions.lock.yaml`: commit, and the sha256 of that commit's source archive |
| forge's dependencies, dbt-core, dbt-trino | dbt-core 1.10.23, dbt-trino 1.10.6 | `requirements.txt`: every distribution by version and sha256 |
| DuckDB | 1.5.6 | the same file |
| forge's build tools (setuptools, setuptools-scm, wheel) | in `requirements-build.txt` | by version and sha256; used in a build stage, not in the image |
| OpenTofu | 1.13.1 | the release's sha256 for each architecture, checked during the build |
| Providers | apache/iceberg 0.1.0, baptistegh/lakekeeper 0.4.10, keycloak/keycloak 5.10.0, hashicorp/kubernetes 3.3.0 | `tofu/.terraform.lock.hcl` (both Linux platforms), baked into a filesystem mirror |

`versions.lock.yaml` records the same versions as the `contents` of its
`built_images` entry `fluid-runner`. `lab build` holds the image to them, and
`tests/launcher/test_build.py` fails when these files and the lock disagree.

## Supply chain

- `pip install --require-hashes --no-deps --only-binary=:all: -r requirements.txt`:
  a distribution whose hash is not in the file cannot be installed, no
  dependency outside the file can be pulled in, and nothing is built from
  source (a source build would fetch build tools that no hash covers). `requirements.txt` is
  compiled from `requirements.in` for every platform, so it carries the
  hashes of every platform's wheels, and pip picks the one for the build
  platform.
- forge is built from the source archive of one commit of
  `Agenticstiger/forge-cli` (public). `ADD --checksum=sha256:...` makes BuildKit
  refuse the archive unless it matches `contents.forge.sha256`. A build stage
  makes a wheel from it with `pip wheel --no-deps --no-build-isolation`, so
  the only build tools are the hashed ones in `requirements-build.txt`. That
  step runs with `RUN --network=none` and `pip wheel --no-index`, so nothing
  can be fetched while forge is built. Only the wheel is installed in the
  image (with `--no-deps`: its dependencies came from `requirements.txt`, and
  `pip check` proves they are all there). A source archive has no `.git`, so
  setuptools-scm is told the version through
  `SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DATA_PRODUCT_FORGE`:
  `0.37.0` for the release pin in the lock (a dev pin would carry the
  commit: `0.32.1.dev0+gb9c675e1`, the lock's `version` plus `+g` and the
  commit's first 8 characters).
- What the image says about its forge is written from bytes, not from build
  args. `record_forge.py` writes `/opt/fluid-runner/forge/forge.json` in the
  build: the archive's sha256 computed from the file, the commit read from the
  archive's own pax header (`git archive` writes it, and GitHub's archives
  carry it), and the sha256 of the wheel it built. The build stops if that
  commit differs from the lock's. The wheel stays beside `forge.json`. The
  probe, running in the finished image, then compares the commit and archive
  sha256 with the lock, the kept wheel's sha256 with the record, and every
  file in the wheel's `RECORD` with the installed file. So an image whose
  forge was built from any other commit, or whose installed files were
  altered after install, is refused. (It is the image's own Python doing the
  reporting, so it shows the build did what the lock says; it is not an
  attestation.)
- OpenTofu is downloaded once and compared with the sha256 its release
  publishes. A mismatch stops the build.
- `tofu providers mirror` fills `/opt/fluid-runner/providers` for the image's
  platform. The build then runs `tofu init` from that mirror alone, which
  checks every package against the lock file. At run time `TF_CLI_CONFIG_FILE`
  points at a CLI config whose only installation source is the mirror, so a
  provider that is not baked in fails to install instead of being downloaded.

## Nothing phones home

`FLUID_COMMAND_CENTER_ENABLED=false`,
`DBT_SEND_ANONYMOUS_USAGE_STATS=false`, and `DBT_VERSION_CHECK=false` (dbt
otherwise asks PyPI for its latest version on `dbt --version`).
`PIP_NO_INDEX=1` keeps pip off the network at run time.

## Build, check, load

```sh
./lab build fluid-runner --name <lab>
```

1. `docker build` of this directory with BuildKit, for the Docker server's
   architecture. This step needs the network: PyPI, forge's source archive
   on github.com, the OpenTofu release and the provider registry.
   `.dockerignore` sends only the seven build inputs. The
   forge commit, archive sha256 and version reach the Dockerfile as
   `--build-arg`s that `lab build` reads from the lock; the Dockerfile has no
   copy of them.
2. A probe runs in the image (by id) with no network and is compared with the
   lock's `contents`: the versions, the forge commit and archive sha256 the
   image was built from (a different commit is refused, even at the same
   version), `tofu init` from the baked mirror, the
   telemetry switches, uid 10001, the `fluid` CLI and forge's Spark runner. An
   image that differs is not loaded. The probe checks that the build produced
   what the lock says; it is not an attestation, since the image's own Python
   does the reporting.
3. `kind load docker-image` into the lab's node, a check that the node holds
   the same layers, and a record in the lab's state directory (`images.json`)
   naming the node it went into. `lab down` drops the record with the cluster.

The tag is `lab.local/fluid-runner:<forge version>-<commit, 8 characters>`,
for example `lab.local/fluid-runner:0.37.0-c826f3ae`, so a new pin is a
new tag. No registry answers for
`lab.local`, and pods run the image with `imagePullPolicy: Never`, so a node
that lacks it fails to start the pod instead of pulling something else.

## Bumping forge (the common case)

One edit, then regenerate. In `versions.lock.yaml`, `built_images` entry
`fluid-runner`, change `contents.forge`:

```yaml
forge: {version: 0.37.0, commit: <40-character SHA>, sha256: <archive sha256>, release: true}
```

With `release: true` the commit must be the commit of tag `v<version>`; without it,
the commit must be in forge-cli `main`'s history (`compile.sh` checks either).

- `commit`: `gh api repos/Agenticstiger/forge-cli/commits/<ref> --jq .sha`
- `sha256`: `curl -sSfL https://github.com/Agenticstiger/forge-cli/archive/<commit>.tar.gz | shasum -a 256`
- `version`: the normalised PEP 440 version forge's next release is expected
  to have, such as `0.33.0.dev0` (`0.33.0rc1`, not `0.33.0.rc1`: installed
  metadata normalises it). The build appends `+g<commit 8>`.

Then put the same commit in `requirements.in` (the one
`data-product-forge[local,aws] @ ...` line, a GitHub archive URL) and
regenerate the hashes, which also picks up forge's dependency changes:

```sh
platform/images/fluid-runner/compile.sh              # needs uv, yq, gh, curl
platform/images/fluid-runner/compile.sh --allow-unmerged   # only for a commit not in main yet
```

`compile.sh` reads forge's pin from the lock (`python3 -m labctl.build forge
<field>`), so it is not typed twice. It downloads the archive itself, checks
its sha256 against the lock, and gives uv that verified local file, never the
URL. It writes the pin as the first line of `requirements.txt`
(`# forge commit <sha> archive-sha256 <sha256> version <v>`). Forgetting it is
caught: `tests/launcher/test_build.py` compares that line, `requirements.in`,
the Dockerfile and `lab build`'s args with the lock. Then
`python3 -m unittest discover -s tests/launcher` and
`./lab build fluid-runner --name <lab>`.

### What `release: true` changes

With `release: true` the build is told the exact version, with no `+g` part:
the installed distribution must report exactly that version, and the probe
refuses `<version>+g...`. `compile.sh` checks that tag `v<version>` resolves
to the pinned commit (an annotated tag is peeled) instead of checking main's
history, and an unknown key in `contents.forge` (such as a mistyped `relase`)
is refused. The commit is still recorded and checked, and the image tag stays
`lab.local/fluid-runner:<version>-<commit 8>`. A dev version (`.dev0`) with
`release: true` is refused. Installing the release from PyPI with hashes
instead (forge in `requirements.in` as `data-product-forge[local,aws]==<version>`,
without the forge-wheel stage) is possible, but it is a larger change than a
bump.

Archive checksums: GitHub makes the archive from the commit on request, so
its bytes could change if GitHub changes its archiver. The build then fails
at `ADD --checksum` (it never accepts different bytes); repeat the `sha256`
step and review that the source is the same commit.

## Updating any other pin

1. Change `requirements.in`, an `ARG` in the `Dockerfile` or
   `tofu/providers.tf`, and the `contents` in `versions.lock.yaml`.
2. Regenerate the hashes:
   - `platform/images/fluid-runner/compile.sh`
   - in `tofu/`: `tofu providers lock -platform=linux_amd64 -platform=linux_arm64`
3. Run `python3 -m unittest discover -s tests/launcher`, then
   `./lab build fluid-runner`.

## Not published

The image is built from this directory and stays on the machine that built
it. The lab publishes no image: there is no registry, no signed digest in the
lock, no SBOM licence inventory and no source offer for the Debian packages in
its base. Anyone who publishes the image takes on those duties.
