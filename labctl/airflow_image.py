# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""What `lab build airflow-fluid` holds the Airflow image to (labctl.build runs the steps).

The image is apache/airflow:3.2.2 with forge, dbt-core, dbt-trino and DuckDB in
a venv of their own (platform/images/airflow-fluid). The lock's `contents` for it
says `pins_from: fluid-runner` plus the Airflow version, so forge's pin is typed
once, in the fluid-runner entry; `resolve` turns that into the same shape
fluid-runner's `contents` has.
"""

from __future__ import annotations

from labctl import config

IMAGE = "airflow-fluid"
SOURCE_IMAGE = "fluid-runner"
UID = 50000                                  # the airflow user of the official image
VENV = "/opt/fluid/venv"
FORGE_SOURCE_RECORD = "/opt/fluid/forge/forge.json"   # written by the build, beside the kept wheel
PINNED = ("forge", "dbt-core", "dbt-trino", "duckdb")   # copied from the fluid-runner entry
ENV = {"FLUID_COMMAND_CENTER_ENABLED": "false", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
       "DBT_VERSION_CHECK": "false"}

# Runs in the image as `/opt/fluid/venv/bin/python -I -c PROBE <distributions> <env names> <forge record>`
# and always prints one JSON object: every failure inside becomes a field, never a traceback.
PROBE = r"""
import base64, csv, hashlib, importlib.metadata as md, io, json, os, pathlib, subprocess, sys, zipfile

AIRFLOW = (
    "import importlib.metadata as m, json\n"
    "from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle\n"
    "print(json.dumps({'airflow': m.version('apache-airflow'), "
    "'amazon': m.version('apache-airflow-providers-amazon'), 'bundle': S3DagBundle.__name__}))\n"
)


def run(argv, timeout):
    try:
        res = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return {"rc": res.returncode, "out": res.stdout, "tail": (res.stdout + res.stderr)[-600:]}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"rc": None, "out": "", "tail": f"{type(exc).__name__}: {exc}"}


dists, env_names = sys.argv[1].split(","), sys.argv[2].split(",")
out = {"uid": os.getuid(), "versions": {}, "env": {k: os.environ.get(k) for k in env_names}}
for dist in dists:
    try:
        out["versions"][dist] = md.version(dist)
    except md.PackageNotFoundError:
        out["versions"][dist] = None
# __FORGE_SOURCE__

try:
    out["forge_source"] = forge_source(sys.argv[3])
except Exception as exc:
    out["forge_source"] = {"error": f"{type(exc).__name__}: {exc}"}
cli = run(["fluid", "--version"], 60)
out["fluid_cli"] = {"rc": cli["rc"], "out": cli["tail"].strip()[-300:]}
dbt = run(["dbt", "--version"], 60)
out["dbt_cli"] = {"rc": dbt["rc"], "out": dbt["tail"].strip()[-300:]}
air = run(["python3", "-I", "-c", AIRFLOW], 120)
try:
    out["airflow"] = json.loads(air["out"].strip().splitlines()[-1]) if air["rc"] == 0 else None
except (ValueError, IndexError):
    out["airflow"] = None
out["airflow_tail"] = None if out["airflow"] else air["tail"]
print(json.dumps(out))
"""


def resolve(lock: dict, entry: dict) -> dict:
    """The entry with `contents` in the fluid-runner shape: forge, dbt and DuckDB from `pins_from`."""
    contents = entry.get("contents")
    if not isinstance(contents, dict) or contents.get("pins_from") != SOURCE_IMAGE or not contents.get("airflow"):
        raise config.ConfigError(f"{config.VERSIONS_LOCK} built_images[{IMAGE}] needs `contents` with "
                                 f"`pins_from: {SOURCE_IMAGE}` and `airflow`")
    source = next((b for b in lock.get("built_images") or [] if isinstance(b, dict) and b.get("id") == SOURCE_IMAGE),
                  None)
    pins = (source or {}).get("contents")
    missing = [k for k in PINNED if not isinstance(pins, dict) or not pins.get(k)]
    if missing:
        raise config.ConfigError(f"{config.VERSIONS_LOCK} built_images[{SOURCE_IMAGE}] contents lacks "
                                 + ", ".join(missing) + f" (built_images[{IMAGE}] takes them from there)")
    base = entry.get("base")
    if not isinstance(base, dict) or not base.get("ref") or not str(base.get("digest") or "").startswith("sha256:"):
        raise config.ConfigError(f"{config.VERSIONS_LOCK} built_images[{IMAGE}] needs a `base` with ref and digest")
    return {**entry, "contents": {**{k: pins[k] for k in PINNED}, "airflow": str(contents["airflow"])}}


def probe_argv(ref: str, arch: str, name: str, distributions: str, script: str,
               forge_record: str = FORGE_SOURCE_RECORD) -> list:
    return ["docker", "run", "--rm", "--name", name, "--platform", f"linux/{arch}", "--network", "none",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--entrypoint", f"{VENV}/bin/python", ref,
            "-I", "-c", script, distributions, ",".join(ENV), forge_record]
