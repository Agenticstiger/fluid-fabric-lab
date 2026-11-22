# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# fluid-runner: the image every forge stage runs in (CLI, Kubernetes Job,
# generated Airflow DAG).
#
# - forge's dependencies, and everything else Python, from PyPI with
#   --require-hashes, --no-deps and --only-binary=:all:. requirements.txt pins
#   every distribution by version AND sha256 (uv pip compile
#   --generate-hashes --universal), so nothing unpinned can enter, and no
#   sdist build can pull in unhashed build tools.
# - forge itself is built from source: the source archive of one commit of
#   Agenticstiger/forge-cli (a release's tag commit, or a dev commit), fetched with ADD --checksum (BuildKit refuses it
#   unless its sha256 matches). It is built into a wheel in a stage of its own
#   with --no-deps --no-build-isolation, by build tools installed from the
#   hashed requirements-build.txt, and only the wheel reaches this image.
#   forge.json beside the wheel records what the build saw in the archive and
#   the wheel (their sha256, the commit in the archive's own header), not the
#   build args. FORGE_VERSION, FORGE_COMMIT and FORGE_SHA256 have no default: `lab build`
#   passes them from versions.lock.yaml (fluid-runner `contents.forge`), the
#   one place they are written.
# - OpenTofu 1.13.1 from its GitHub release, checked against the release's
#   SHA256SUMS values copied into the build args below.
# - A filesystem provider mirror for the four pinned providers. The last
#   build step runs `tofu init` from that mirror alone, which checks every
#   package against the committed .terraform.lock.hcl. TF_CLI_CONFIG_FILE
#   keeps the mirror the only installation source at run time.
# - Nothing phones home: FLUID_COMMAND_CENTER_ENABLED=false,
#   DBT_SEND_ANONYMOUS_USAGE_STATS=false, and DBT_VERSION_CHECK=false (dbt
#   otherwise asks PyPI for its latest version on `dbt --version`).
# - Non-root (uid 10001); pip has no index at run time (PIP_NO_INDEX=1).
#
# Needs BuildKit (TARGETARCH, the heredoc, ADD --checksum). Built on the operator's machine
# by `./lab build fluid-runner` and loaded
# into a lab's kind node. It is never pushed to a registry: the lab publishes no image.

ARG PYTHON_IMAGE=docker.io/library/python:3.13.18-slim-trixie@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90

FROM ${PYTHON_IMAGE} AS tofu
ARG TARGETARCH
ARG TOFU_VERSION=1.13.1
ARG TOFU_SHA256_amd64=378ada19d4bc70c43732004e8159be771b23b9a5afdf059e5f8a2b3fa2c70a69
ARG TOFU_SHA256_arm64=9c1ef375aa1852db0b2888aa921b640c71f8140d4682aa4fec99378a64fa7dc3
RUN set -eu; \
    case "$TARGETARCH" in amd64) sum="$TOFU_SHA256_amd64";; arm64) sum="$TOFU_SHA256_arm64";; \
      *) echo "unsupported arch $TARGETARCH" >&2; exit 1;; esac; \
    python3 - "$TOFU_VERSION" "$TARGETARCH" "$sum" <<'PY'
import hashlib, io, sys, tarfile, urllib.request
version, arch, want = sys.argv[1:4]
url = f"https://github.com/opentofu/opentofu/releases/download/v{version}/tofu_{version}_linux_{arch}.tar.gz"
data = urllib.request.urlopen(url, timeout=300).read()
got = hashlib.sha256(data).hexdigest()
if got != want:
    sys.exit(f"tofu {version} {arch}: sha256 {got}, expected {want}")
with tarfile.open(fileobj=io.BytesIO(data)) as tar:
    member = tar.getmember("tofu")
    with open("/usr/local/bin/tofu", "wb") as out:
        out.write(tar.extractfile(member).read())
PY
RUN chmod 0755 /usr/local/bin/tofu && tofu version

FROM tofu AS providers
ARG TARGETARCH
COPY tofu/providers.tf tofu/.terraform.lock.hcl /build/
WORKDIR /build
# Download only this image's platform; the lock holds hashes for both.
RUN tofu providers mirror -platform=linux_${TARGETARCH} /opt/fluid-runner/providers

FROM ${PYTHON_IMAGE} AS forge-wheel
ARG FORGE_VERSION
ARG FORGE_COMMIT
ARG FORGE_SHA256
RUN test -n "$FORGE_VERSION" -a -n "$FORGE_COMMIT" -a -n "$FORGE_SHA256" \
 || { echo "FORGE_VERSION, FORGE_COMMIT or FORGE_SHA256 is empty: build with ./lab build fluid-runner, which passes the forge pin from versions.lock.yaml" >&2; exit 1; }
COPY requirements-build.txt /tmp/requirements-build.txt
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check --require-hashes --no-deps \
      --only-binary=:all: -r /tmp/requirements-build.txt
ADD --checksum=sha256:${FORGE_SHA256} https://github.com/Agenticstiger/forge-cli/archive/${FORGE_COMMIT}.tar.gz /src/forge.tar.gz
COPY record_forge.py /usr/local/bin/record_forge.py
# A source archive has no .git: setuptools-scm is told the version (a dev pin carries the commit,
# 0.32.1.dev0+gb9c675e1; a release pin is the release, 0.37.0 now). No network: the build tools are installed
# and forge's dependencies are not needed to build it. record_forge.py then writes forge.json from the
# bytes: the archive's sha256, the commit in the archive's own pax header (git archive writes it) and
# the wheel's sha256. The probe checks the image against it.
RUN --network=none set -eu; \
    mkdir /src/forge /out; tar -xzf /src/forge.tar.gz -C /src/forge --strip-components=1; \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DATA_PRODUCT_FORGE="$FORGE_VERSION" \
    python3 -m pip wheel --no-cache-dir --disable-pip-version-check --no-deps --no-build-isolation --no-index \
      --wheel-dir /out /src/forge; \
    python3 -I /usr/local/bin/record_forge.py /src/forge.tar.gz /out "$FORGE_COMMIT" "$FORGE_VERSION" > /out/forge.json; \
    ls /out

FROM ${PYTHON_IMAGE}
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check --require-hashes --no-deps \
      --only-binary=:all: \
      -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt \
 && python3 -m pip check
# forge: the wheel built above, kept in /opt/fluid-runner/forge beside forge.json (the probe compares
# the installed files with it). Its dependencies are already installed from the hashed
# requirements.txt, so --no-deps; pip check proves they are all there.
COPY --from=forge-wheel /out/ /opt/fluid-runner/forge/
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check --no-deps --no-index /opt/fluid-runner/forge/*.whl \
 && python3 -m pip check
COPY --from=tofu /usr/local/bin/tofu /usr/local/bin/tofu
COPY --from=providers /opt/fluid-runner/providers /opt/fluid-runner/providers
COPY tofu/tofurc tofu/.terraform.lock.hcl tofu/providers.tf /opt/fluid-runner/tofu/
# The baked mirror must satisfy the committed lock: `tofu init` installs only
# from the mirror and checks each package against the lock's hashes, so a
# missing or altered provider fails the build here.
RUN set -eu; work="$(mktemp -d)"; \
    cp /opt/fluid-runner/tofu/providers.tf /opt/fluid-runner/tofu/.terraform.lock.hcl "$work/"; \
    TF_CLI_CONFIG_FILE=/opt/fluid-runner/tofu/tofurc tofu -chdir="$work" init -backend=false -input=false -no-color; \
    rm -rf "$work"
RUN useradd --uid 10001 --create-home --home-dir /home/runner --shell /usr/sbin/nologin runner
ENV FLUID_COMMAND_CENTER_ENABLED=false \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_VERSION_CHECK=false \
    TF_CLI_CONFIG_FILE=/opt/fluid-runner/tofu/tofurc \
    TF_IN_AUTOMATION=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_INDEX=1
USER 10001
WORKDIR /home/runner
ENTRYPOINT ["fluid"]
