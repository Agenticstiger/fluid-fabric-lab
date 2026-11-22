# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# airflow-fluid: the Airflow image of the orchestration tier, which runs
# forge's generated DAGs.
#
# - FROM the official apache/airflow image, by digest (`AIRFLOW_IMAGE`, and
#   `base` in versions.lock.yaml). Airflow, its providers and its package pins
#   are not touched.
# - forge, dbt-core, dbt-trino and DuckDB go into a venv of their own,
#   /opt/fluid/venv, from the same hashed requirements and the same pinned
#   forge source archive as the fluid-runner image (see
#   ../fluid-runner/Dockerfile): --require-hashes --no-deps --only-binary,
#   forge built to a wheel by hashed build tools in a stage of its own.
#   FORGE_VERSION, FORGE_COMMIT and FORGE_SHA256 have no default: `lab build`
#   passes them from versions.lock.yaml (fluid-runner `contents.forge`).
# - `fluid` and `dbt` are on PATH for the airflow user through symlinks in
#   /usr/local/bin. python and pip are not linked: `python3` stays Airflow's.
# - Nothing phones home: FLUID_COMMAND_CENTER_ENABLED=false,
#   DBT_SEND_ANONYMOUS_USAGE_STATS=false, DBT_VERSION_CHECK=false.
#
# Never pushed: `./lab build airflow-fluid` builds it on the operator's
# machine and loads it into a lab's kind node (docs/orchestration-tier.md).

ARG AIRFLOW_IMAGE=docker.io/apache/airflow:3.2.2@sha256:bef4d135ec7b64658cf64089973edd62cab8fd72203f280c7e0952213c17d912

FROM ${AIRFLOW_IMAGE} AS forge-wheel
ARG FORGE_VERSION
ARG FORGE_COMMIT
ARG FORGE_SHA256
USER root
RUN test -n "$FORGE_VERSION" -a -n "$FORGE_COMMIT" -a -n "$FORGE_SHA256" \
 || { echo "FORGE_VERSION, FORGE_COMMIT or FORGE_SHA256 is empty: build with ./lab build airflow-fluid, which passes the forge pin from versions.lock.yaml" >&2; exit 1; }
COPY requirements-build.txt /tmp/requirements-build.txt
RUN python3 -m venv /tmp/build \
 && /tmp/build/bin/python -m pip install --no-cache-dir --disable-pip-version-check --require-hashes --no-deps \
      --only-binary=:all: -r /tmp/requirements-build.txt
ADD --checksum=sha256:${FORGE_SHA256} https://github.com/Agenticstiger/forge-cli/archive/${FORGE_COMMIT}.tar.gz /src/forge.tar.gz
COPY record_forge.py /usr/local/bin/record_forge.py
# A source archive has no version control data: setuptools-scm is told the
# version (the release, e.g. 0.37.0; a dev pin carries the commit, 0.32.1.dev0+gb9c675e1), so the
# installed distribution says which source it came from. record_forge.py reads
# the commit from the archive's own header and refuses a wheel that is not this
# version; it writes forge.json beside the wheel, and both stay in the image
# (as in fluid-runner).
RUN --network=none set -eu; \
    mkdir /src/forge /out; tar -xzf /src/forge.tar.gz -C /src/forge --strip-components=1; \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DATA_PRODUCT_FORGE="$FORGE_VERSION" \
    /tmp/build/bin/python -m pip wheel --no-cache-dir --disable-pip-version-check --no-deps --no-build-isolation \
      --no-index --wheel-dir /out /src/forge; \
    python3 -I /usr/local/bin/record_forge.py /src/forge.tar.gz /out "$FORGE_COMMIT" "$FORGE_VERSION" > /out/forge.json; \
    ls /out

FROM ${AIRFLOW_IMAGE}
USER root
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/fluid/venv \
 && /opt/fluid/venv/bin/python -m pip install --no-cache-dir --disable-pip-version-check --require-hashes \
      --no-deps --only-binary=:all: -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt \
 && /opt/fluid/venv/bin/python -m pip check
# forge: the wheel built above, kept with forge.json in /opt/fluid/forge so the
# probe can compare every installed file with it. Its dependencies are already
# in the venv from the hashed requirements.txt, so --no-deps, and pip check
# proves they are all there.
COPY --from=forge-wheel /out/ /opt/fluid/forge/
RUN /opt/fluid/venv/bin/python -m pip install --no-cache-dir --disable-pip-version-check --no-deps --no-index \
      /opt/fluid/forge/*.whl \
 && /opt/fluid/venv/bin/python -m pip check \
 && ln -s /opt/fluid/venv/bin/fluid /usr/local/bin/fluid \
 && ln -s /opt/fluid/venv/bin/dbt /usr/local/bin/dbt \
 && chmod -R a+rX /opt/fluid
ENV FLUID_COMMAND_CENTER_ENABLED=false \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_VERSION_CHECK=false
# Back to the image's own user (uid 50000, gid 0) and entrypoint.
USER 50000
