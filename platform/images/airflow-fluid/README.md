<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# airflow-fluid image

The Airflow image of the orchestration tier (docs/orchestration-tier.md): the
official `apache/airflow:3.2.2` image, by digest, with forge, dbt-core,
dbt-trino and DuckDB added in a venv of their own, so `fluid apply` runs on the
scheduler.

| Piece | Version | How it is pinned |
|---|---|---|
| Base | `apache/airflow:3.2.2` (Python 3.13.13) | by digest (`ARG AIRFLOW_IMAGE`, `base` in `versions.lock.yaml`) |
| forge, dbt-core, dbt-trino, DuckDB | the fluid-runner pins | `contents.pins_from: fluid-runner` in `versions.lock.yaml`; one place to bump them |
| Their dependencies | | `requirements.txt`, a byte-for-byte copy of fluid-runner's (compiled for Python 3.13, which both images run) |

Layout: forge and its dependencies live in `/opt/fluid/venv`. `fluid` and
`dbt` are symlinked into `/usr/local/bin`; `python3` and `pip` stay Airflow's,
whose own package pins are untouched. `/opt/fluid/forge.json` records the
commit and archive sha256 forge was built from.

## Build, check, load

```sh
./lab build airflow-fluid --name <lab>
```

The same three steps as fluid-runner (`labctl/build.py`): `docker build`, a
probe with no network compared with the lock, `kind load docker-image` and a
record in the lab's state directory. The probe checks forge's version and
commit (the commit the lock pins, and the archive sha256), dbt and DuckDB, that
`fluid --version` runs from PATH, Airflow's version, and that Airflow imports
`S3DagBundle` from its Amazon provider. The tag is
`lab.local/airflow-fluid:<forge version>-<commit, 8 characters>`; the chart
runs it with `imagePullPolicy: Never`. The build needs the network once
(Docker Hub, PyPI, github.com).

## Bumping

Bump forge as `platform/images/fluid-runner/README.md` says, then run
`platform/images/airflow-fluid/compile.sh` to copy the regenerated
requirements, then `./lab build airflow-fluid`. To bump Airflow, change the
`airflow` image entry (digest) and this `base` in `versions.lock.yaml`, the
`AIRFLOW_IMAGE` ARG, and the chart pin together; recompile the requirements if
the base image's Python minor changed.

## Not published

Never pushed to a registry: the lab publishes no image, like fluid-runner.
