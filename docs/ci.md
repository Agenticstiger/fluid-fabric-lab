<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# CI

This repository has one workflow, [`.github/workflows/l0.yml`](../.github/workflows/l0.yml).
It runs the L0 checks on every pull
request and on every push to `main`. It can also be started by hand
(`workflow_dispatch`), for example for a merge commit that got no run, because
merges made with `GITHUB_TOKEN` do not trigger workflows. There are no path
filters: a required check that is skipped would stay pending.

The workflow token is read-only (`permissions: contents: read`). The
checkout does not keep credentials (`persist-credentials: false`).

## Jobs

Each row is one job, which is one leg of a matrix. Each job installs only
the tools it needs, then runs one check with the network removed (see
[No network](#no-network)). `yq` is installed in every job, because the
install step reads `versions.lock.yaml` with it.

| Job | Runs | Tools (besides yq) |
|---|---|---|
| `render` | `tools/l0/render.py` | helm, helmfile |
| `schema` | `tools/l0/schema.py` | helm, helmfile, kubeconform |
| `licence` | `tools/l0/licence.py` | trivy, reuse |
| `denylist` | `tools/l0/denylist.py` | helm, helmfile |
| `brand` | `tools/l0/brand_fence.py` | none |
| `reuse` | `tools/l0/reuse_lint.py` | reuse |
| `chart-app` | `tools/l0/chart_app.py` | helm, helmfile |
| `footprint` | `tools/l0/footprint.py` | helm, helmfile |
| `profile-schema` | `tools/l0/profile_schema.py` | none |
| `secrets` | `tools/l0/secret_scan.py` (gitleaks; full history) | gitleaks |
| `launcher-tests (3.10)`, `launcher-tests (3.14)` | `python3 -m unittest discover -s tests/launcher -v` (includes `opa test` on the Trino policy) | opa |
| `policy (opa test)` | `opa test platform/policy/trino-lab -v` | opa |
| `selftest` | `python3 tools/l0/selftest` (planted fixtures plus the real repository; full history) | helm, helmfile, kubeconform, trivy, reuse, gitleaks |

What each gate checks is in [`tools/l0/README.md`](../tools/l0/README.md).
Gate jobs run the gate script directly, so the job log has the gate's full
output. They give it the same environment that `./lab l0` gives a gate:
`LAB_ROOT`, `LAB_OFFLINE=1`, and every proxy variable pointed at the closed
port `127.0.0.1:9`. They run from the repository root with no arguments.

The `selftest` job also checks that the gates `./lab l0 --list` discovers are
exactly the `gate` legs in `l0.yml`. A new gate in `tools/l0/` fails CI until
it gets its own job, so a gate can never be skipped in CI without anyone
noticing.

Python is 3.10 in every job except `launcher-tests (3.14)`. 3.10 is the
lab's declared floor (`python3 >= 3.10`), so the gates and the launcher are
tested on the oldest version they claim to support. 3.14 covers the newest
stable release that has soaked; 3.15.0 is not used yet.

Nothing is skipped quietly. A job fails when a tool cannot be installed or
reports the wrong version, when the isolation cannot be proven, when a gate
fails, and when a gate cannot run (exit 2). A matrix leg with no command also
fails.

## Pinned actions

Every action is pinned to a full commit SHA. The tag is kept in a trailing
comment, which Dependabot reads and updates together with the SHA.

| Action | Tag | Commit | Released |
|---|---|---|---|
| `actions/checkout` | v7.0.1 | `3d3c42e5aac5ba805825da76410c181273ba90b1` | 2026-07-20 |
| `actions/setup-python` | v7.0.0 | `5fda3b95a4ea91299a34e894583c3862153e4b97` | 2026-07-20 |

Both SHAs were resolved on 2026-10-14 with:

```sh
gh api repos/actions/checkout/commits/v7.0.1 --jq .sha
gh api repos/actions/setup-python/commits/v7.0.0 --jq .sha
```

Both tags point directly at commits, not at annotated tag objects. The runner
image is pinned by name (`ubuntu-24.04`), never `ubuntu-latest`.

[`.github/dependabot.yml`](../.github/dependabot.yml) watches GitHub Actions
only. It runs weekly, puts all action bumps in one pull request, and has a
14-day cooldown, so a release is not proposed until it is 14 days old. This
is the same soak rule `versions.lock.yaml` applies to tools.

Charts, images, CLI tools and Python packages are deliberately not watched by
Dependabot. They change only through `versions.lock.yaml`, after a soak period
and a green run. Dependabot does not sign off its
commits. Under the DCO in [CONTRIBUTING.md](../CONTRIBUTING.md), a maintainer
squash-merges a Dependabot pull request with a `Signed-off-by` line, or the
DCO check is configured to exempt `dependabot[bot]`.

## Tools

The install step reads every tool from `versions.lock.yaml` (`tools:`). It
never uses a `latest` URL, and it never uses a tool that happens to be
installed on the runner image. For each release binary (helm, helmfile,
kubeconform, gitleaks, trivy, opa):

1. The download URL is the lock's `checksum_url` directory plus the lock's
   `linux_amd64.file`. The step refuses any host other than a GitHub release
   asset of the pinned version, or `get.helm.sh`.
2. The download must match the lock's `linux_amd64.sha256`
   (`sha256sum --check --strict`). This is the authority: the hash is
   committed and reviewed, so the download host is not trusted alone.
3. The upstream checksum file (`checksum_url`) must list that same sha256 for
   that same file. If the lock and the release ever disagree, CI fails instead
   of trusting either one.
4. The binary must report the pinned version.

**yq** reads the lock, so it cannot come from the lock. Its version and
sha256 are in the workflow (`YQ_VERSION`, `YQ_SHA256`). After the download is
verified, the step fails unless the lock's `yq` entry has the same values.
Change both together.

**reuse** comes from PyPI and is installed into its own virtualenv with
`pip --require-hashes`. Every dependency is pinned by hash, as the lock asks
("its runtime dependencies need their own hashes"). The requirements are in
the workflow (`REUSE_REQUIREMENTS`, `REUSE_BUILD_REQUIREMENTS`). Some details:

- The step checks that the pinned reuse version and sdist sha256 are the ones
  in `versions.lock.yaml`.
- reuse 6.2.0 ships one wheel, tagged `manylinux_2_41`, and ubuntu-24.04 has
  glibc 2.39. So reuse is built from its sdist, with poetry-core pinned by hash
  and no build isolation (build isolation would fetch an unpinned backend).
  Every other package must be a wheel (`--only-binary=:all:`).
- The hashes cover only the files pip can pick on CPython 3.10, linux x86_64,
  and the step refuses to install on any other Python.
- reuse is GPL-3.0-or-later and python-debian is GPL-2.0-or-later. Both are
  only run in CI and never vendored.

The dependency set was resolved on 2026-10-14 with uv. `--exclude-newer`
enforces the 14-day soak, and `--no-build` makes sure no package code ran
during resolution:

```sh
printf '%s\n' 'Jinja2>=3.0.0' 'license-expression>=21.6.14' 'python-debian>=0.1.48' \
  'tomlkit>=0.8' 'attrs>=23.2' 'click>=8.1' 'python-magic>=0.4.12' \
  'charset-normalizer>=2.0.5' > deps.in   # reuse 6.2.0 Requires-Dist + extra
uv pip compile --no-cache --no-build --generate-hashes --python-version 3.10 \
  --python-platform x86_64-manylinux_2_39 --exclude-newer 2026-09-30T00:00:00Z deps.in
printf 'poetry-core>=1.1.0\n' > build.in
uv pip compile --no-cache --no-build --generate-hashes --python-version 3.10 \
  --python-platform x86_64-manylinux_2_39 --exclude-newer 2026-09-30T00:00:00Z build.in
```

uv lists a hash for every file of each pinned version. The workflow keeps
only the `py3-none-any` wheels and the CPython 3.10 manylinux x86_64 wheels.
Every kept hash was checked against uv's output.

Neither helm-diff, kind nor crane is installed. `helmfile template` does not
need helm-diff, and no L0 check uses a cluster or a registry. They become CI
tools.

### Changing a tool pin

1. Edit the tool's entry in `versions.lock.yaml`: version, file, sha256 and
   `checksum_url`.
2. For yq, also edit `YQ_VERSION` and `YQ_SHA256` in `l0.yml`.
3. For reuse, re-run the resolution above with the new version, and update
   both requirement blocks in `l0.yml`.
4. Push. The install step fails if any of these disagree.

## No network

"No L0 check needs the network" is enforced, not assumed. Tools are installed
first, with the network available. Then the check runs in a new Linux network
namespace:

```sh
sudo unshare --net -- bash -c "$ISOLATE" isolate <uid> <gid> <user> <PATH> <HOME> <workspace> <ip> <command>
```

The `ISOLATE` script (in `l0.yml`) does this, all in the namespace the check
will use:

1. It brings up `lo` only. A new network namespace has no routes, no DNS
   server and no interface besides loopback, apart from tunnel devices some
   kernels add, which stay down and unconfigured. Loopback is up so that the
   gates' proxy trap behaves as it does on a laptop: `127.0.0.1:9` refuses
   the connection.
2. It proves the isolation, and stops the job if any of these hold:
   - an interface other than `lo` is up;
   - `ip route get` finds a route to github.com's IPv4 address, or to a
     public IPv6 address;
   - a TCP connection to github.com's address on port 443 opens;
   - `curl https://github.com` succeeds.
3. It drops root with `setpriv --reuid --regid --clear-groups --no-new-privs`
   and runs the check under `env -i`, with an explicit environment.
   - `--no-new-privs` means nothing in the check can regain root (sudo is
     setuid). So nothing can leave the namespace with `nsenter` or a second
     `unshare`.
   - `--clear-groups` removes the `docker` group. A container daemon lives
     outside the namespace and could fetch from the network for the check; a
     check cannot reach the daemon's socket without the group.

Before entering the namespace, the job runs the same probes on the runner
itself (a **positive control**), and these must **succeed**: github.com
resolves, TCP to port 443 connects, and HTTPS works. So a probe failing
inside the namespace shows that the network is gone, not that the probe is
broken. Both results are printed in the job log.

What the namespace does not cut: Unix sockets on the shared filesystem, for
example the host's `systemd-resolved` socket. A process could still resolve
a name through it, but it cannot connect anywhere. No L0 tool uses these
sockets. Dropping the groups closes the container daemon socket.

On a laptop there is no namespace. `./lab l0` and the gates themselves point
every proxy variable at `127.0.0.1:9`, so an HTTP(S) attempt by helm,
helmfile or Python fails instead of succeeding. The selftest proves this with
a chart pull that stops with `proxyconnect tcp: dial tcp 127.0.0.1:9`. On
Linux, you can reproduce the CI run exactly:

```sh
sudo unshare --net -- sh -c 'ip link set lo up && exec setpriv --reuid="$1" --regid="$2" --clear-groups --no-new-privs -- env PATH="$3" HOME="$4" ./lab l0' \
  sh "$(id -u)" "$(id -g)" "$PATH" "$HOME"
```

## Runners

The workflow uses GitHub-hosted `ubuntu-24.04` runners: 2 vCPU, 8 GB and 14 GB
of SSD on a private repository, which is enough for L0. No L0 job needs a
cluster.

The kind end-to-end runs per tier (`lab up`, `lab check`) do not fit a hosted
runner, so CI does not run them. If a self-hosted runner is added for them,
these rules apply:

- **Never attach a self-hosted runner to a public repository.** On a public
  repository, a pull request from a fork can run code on the runner.
- Give it a dedicated runner group, with access limited to this repository.
- Do not register it under labels these L0 jobs use; L0 stays on
  `ubuntu-24.04` hosted runners.
- The isolation above needs root (`sudo unshare`). If the runner cannot have
  passwordless sudo, run the checks in a container started with
  `--network none` instead, and keep the positive-control and probe steps.

## Required checks

Make these checks required on `main` under branch protection, by their job
names: `render`, `schema`, `licence`, `denylist`, `brand`, `reuse`,
`chart-app`, `footprint`, `profile-schema`, `secrets`,
`launcher-tests (3.10)`, `launcher-tests (3.14)` and `selftest`.

## Not in CI

These do not exist. Each would be a job that fails loudly, never one that
skips:

- the nightly kind end-to-end run per tier;
- the free-disk step before a cluster run;
- `lab mirror` into a local registry;
- building, signing and SBOM-scanning the lab's own images.

## Not verified

On a new fork, check these things in the first run's log:

- The `render`-based gates run with helm v3.22.0, the CI pin. A local run may
  use another helm version (see the helm entry in `versions.lock.yaml`).
- reuse builds from its sdist on ubuntu-24.04, and `python-magic` finds
  `libmagic` on the runner image.
- `sudo unshare --net` and `setpriv` work on the hosted runner image, and the
  isolation probes print as described.
