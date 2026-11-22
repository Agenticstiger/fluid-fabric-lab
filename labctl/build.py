# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab build`: build a lab-owned image on this machine and check what is in it.

Lab-built images never leave the machine. They are not pushed to the mirror
or to any registry: the lab publishes no image. A lab's cluster
gets them with `kind load docker-image`, and its pods run them with
`imagePullPolicy: Never`, so no pull is ever attempted for them.

`lab build fluid-runner|airflow-fluid|spark-iceberg [--name LAB]` runs three steps (airflow-fluid, the
orchestration tier's Airflow image, and spark-iceberg, the spark tier's Spark image, are built and probed the same
way; their probes are in labctl.airflow_image and labctl.spark_image); the first failure stops it:

  build   `docker build` (BuildKit) of platform/images/fluid-runner for the
          Docker server's architecture. The Dockerfile pins its base by
          digest, installs wheels only, each with --require-hashes, checks
          OpenTofu's sha256, builds forge from the source archive of the one
          commit the lock pins (checked against the lock's sha256), and fails
          unless `tofu init` accepts the baked provider mirror against the
          committed lock file. The commit, the archive sha256 and the version
          reach the Dockerfile as build args from the lock, nowhere else.
  probe   runs the image, by id, with no network and compares it with the
          `contents` that versions.lock.yaml records for it: the forge, dbt
          and DuckDB versions, the commit forge was built from, OpenTofu's version, the providers (`tofu init`
          again, offline), the telemetry switches, the non-root user, the
          `fluid` CLI and forge's Spark runner. This checks that the build
          produced what the lock says. It is not an attestation: the image's
          own Python reports on the image.
  load    into the lab's kind node, then checks that the node holds the same
          layers, and records the image and the node it went into
          (<state>/images.json) for `lab check`. `lab down` drops the record
          with the cluster. A lab with no cluster yet is a warning: run
          `lab up`, then `lab build` again (the build is cached).
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Callable, Mapping, Optional

from labctl import airflow_image, cluster, config, doctor, down, proc, spark_image, state
from labctl.output import FAIL, PASS, WARN, check

IMAGES = {"fluid-runner": "platform/images/fluid-runner", "airflow-fluid": "platform/images/airflow-fluid",
          "spark-iceberg": spark_image.DIR}
BUILD_TIMEOUT_S = 1800
PROBE_TIMEOUT_S = 300   # above the fluid-runner probe's own subprocess timeouts (90 + 30 + 60 s)
SPARK_PROBE_TIMEOUT_S = 420   # above spark-iceberg's inner timeouts (120 + 240 s = 360 s)
LOAD_TIMEOUT_S = 600
RUNNER_UID = 10001
PROVIDER_HOST = "registry.opentofu.org"
FORGE_REPO = "Agenticstiger/forge-cli"
# Written by the Dockerfile from the archive and the wheel it built, not from the build args; the wheel sits beside it.
FORGE_SOURCE_RECORD = "/opt/fluid-runner/forge/forge.json"
# `docker version` Server.Arch values -> the Linux platform built for.
ARCHES = {"arm64": "arm64", "aarch64": "arm64", "amd64": "amd64", "x86_64": "amd64"}
# The distributions the probe reads, keyed by their name in the lock's `contents`.
DISTRIBUTIONS = {"forge": "data-product-forge", "dbt-core": "dbt-core", "dbt-trino": "dbt-trino",
                 "duckdb": "duckdb"}
# Environment the image must carry: nothing phones home, and OpenTofu and pip
# install from nothing but what is baked in.
ENV = {"FLUID_COMMAND_CENTER_ENABLED": "false", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
       "DBT_VERSION_CHECK": "false", "TF_CLI_CONFIG_FILE": "/opt/fluid-runner/tofu/tofurc", "PIP_NO_INDEX": "1"}

FORGE_SOURCE_FN = r"""
def forge_source(record_path):
    # What the build recorded, and proof from the image's own bytes: the kept wheel's sha256, and every file the
    # wheel's RECORD lists compared with what is installed.
    res = {"record": None, "wheel_sha256": None, "checked": 0, "mismatched": []}
    record = pathlib.Path(record_path)
    res["record"] = json.loads(record.read_text())
    wheels = sorted(record.parent.glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError(f"{len(wheels)} wheels beside the record, expected 1")
    data = wheels[0].read_bytes()
    res["wheel_sha256"] = hashlib.sha256(data).hexdigest()
    site = md.distribution("data-product-forge").locate_file("")
    zf = zipfile.ZipFile(io.BytesIO(data))
    name = next(n for n in zf.namelist() if n.endswith(".dist-info/RECORD"))
    for row in csv.reader(zf.read(name).decode().splitlines()):
        path, digest = row[0], row[1]
        if not digest or ".data/" in path or path.startswith(".."):
            continue
        algo, _, b64 = digest.partition("=")
        want = base64.urlsafe_b64decode(b64 + "=" * (-len(b64) % 4)).hex()
        try:
            got = hashlib.new(algo, (site / path).read_bytes()).hexdigest()
        except OSError:
            got = None
        res["checked"] += 1
        if got != want:
            res["mismatched"].append(path)
    return res
"""

# Runs inside the image (python3 -I -c PROBE <distributions> <env names> <forge record>) and
# always prints one JSON object: every failure inside becomes a field, never
# a traceback. It writes nothing but a temporary directory for `tofu init`.
PROBE_TEMPLATE = r"""
import base64, csv, hashlib, importlib.metadata as md, importlib.util, io, json, os, pathlib, shutil, subprocess, sys, tempfile, zipfile


def run(argv, timeout):
    try:
        res = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return {"rc": res.returncode, "out": res.stdout, "tail": (res.stdout + res.stderr)[-600:]}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"rc": None, "out": "", "tail": f"{type(exc).__name__}: {exc}"}


dists, env_names = sys.argv[1].split(","), sys.argv[2].split(",")
out = {"uid": os.getuid(), "versions": {}, "env": {k: os.environ.get(k) for k in env_names}, "tofu": None}
for dist in dists:
    try:
        out["versions"][dist] = md.version(dist)
    except md.PackageNotFoundError:
        out["versions"][dist] = None
try:
    work = tempfile.mkdtemp()
    for name in ("providers.tf", ".terraform.lock.hcl"):
        shutil.copy(os.path.join("/opt/fluid-runner/tofu", name), work)
except OSError as exc:
    out["tofu_init"] = {"rc": None, "tail": f"{type(exc).__name__}: {exc}"}
else:
    init = run(["tofu", "-chdir=" + work, "init", "-backend=false", "-input=false", "-no-color"], 90)
    out["tofu_init"] = {"rc": init["rc"], "tail": init["tail"]}
    ver = run(["tofu", "-chdir=" + work, "version", "-json"], 30)
    try:
        out["tofu"] = json.loads(ver["out"]) if ver["rc"] == 0 else None
    except ValueError:
        out["tofu"] = None
spec = importlib.util.find_spec("fluid_build")
base = pathlib.Path(list(spec.submodule_search_locations)[0]) if spec else None


# __FORGE_SOURCE__


try:
    out["forge_source"] = forge_source(sys.argv[3])
except Exception as exc:
    out["forge_source"] = {"error": f"{type(exc).__name__}: {exc}"}
out["spark_runner"] = bool(base and (base / "spark" / "runner" / "forge_spark_runner.py").is_file())
cli = run(["fluid", "--version"], 60)
out["fluid_cli"] = {"rc": cli["rc"], "out": cli["tail"].strip()[-300:]}
print(json.dumps(out))
"""


PROBE = PROBE_TEMPLATE.replace("# __FORGE_SOURCE__", FORGE_SOURCE_FN)


class BuildError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


def _norm(version) -> str:
    return str(version or "").strip().lstrip("v")


def _mapping(value) -> dict:
    return value if isinstance(value, dict) else {}


def lock_entry(lock: dict, image: str) -> dict:
    """The image's `built_images` entry; it must say what the image contains."""
    for entry in lock.get("built_images") or []:
        if isinstance(entry, dict) and entry.get("id") == image:
            if image == airflow_image.IMAGE:
                return airflow_image.resolve(lock, entry)
            if image == spark_image.IMAGE:
                return spark_image.resolve(lock, entry)
            contents = entry.get("contents")
            if not isinstance(contents, dict) or not isinstance(contents.get("providers"), dict):
                raise config.ConfigError(f"{config.VERSIONS_LOCK} built_images[{image}] needs `contents`, a mapping "
                                         "with a `providers` mapping")
            missing = [k for k in (*DISTRIBUTIONS, "opentofu", "providers") if not contents.get(k)]
            if missing:
                raise config.ConfigError(f"{config.VERSIONS_LOCK} built_images[{image}] contents lacks "
                                         + ", ".join(missing))
            forge_pin(contents)
            return entry
    raise config.ConfigError(f"{config.VERSIONS_LOCK} has no built_images entry {image!r}")


def forge_pin(contents: dict) -> dict:
    """The forge source the lock pins: {version, commit, sha256, release, local_version, url}.

    `local_version` is the PEP 440 version the build tells setuptools-scm
    (a source archive has no .git) and the one the installed distribution
    must report: `version` plus `+g` and the commit's first 8 characters, or
    exactly `version` when the entry says `release: true` (a release tag's
    commit, built as the release it is).
    """
    pin = contents.get("forge")
    if not isinstance(pin, dict):
        raise config.ConfigError(f"{config.VERSIONS_LOCK} fluid-runner contents.forge must be a mapping "
                                 "with version, commit and sha256 (forge is built from source, pinned by commit)")
    unknown = sorted(set(pin) - {"version", "commit", "sha256", "release"})
    if unknown:
        raise config.ConfigError(f"contents.forge has unknown key(s) {', '.join(map(str, unknown))}; "
                                 "allowed: version, commit, sha256, release")
    version, commit, sha = (str(pin.get(k) or "") for k in ("version", "commit", "sha256"))
    release = pin.get("release", False)
    if not isinstance(release, bool):
        raise config.ConfigError(f"contents.forge.release {release!r} must be true or false")
    # PEP 440 in the form installed metadata reports it (no dot before a/b/rc), so the two compare equal.
    if not re.fullmatch(r"\d+(\.\d+)*((a|b|rc)\d+)?(\.dev\d+)?", version):
        raise config.ConfigError(f"contents.forge.version {version!r} is not a normalised PEP 440 version "
                                 "without a local part (such as 0.33.0, 0.33.0rc1 or 0.33.1.dev0)")
    if release and "dev" in version:
        raise config.ConfigError(f"contents.forge.release is true, but version {version!r} is a dev version")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise config.ConfigError(f"contents.forge.commit {commit!r} is not a full 40-character lowercase SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise config.ConfigError(f"contents.forge.sha256 {sha!r} is not a 64-character lowercase sha256")
    return {"version": version, "commit": commit, "sha256": sha, "release": release,
            "local_version": version if release else f"{version}+g{commit[:8]}",
            "url": f"https://github.com/{FORGE_REPO}/archive/{commit}.tar.gz"}


def pin_header(contents: dict) -> str:
    """The first line of requirements.txt, written by compile.sh: a bump of the lock that was not followed by
    compile.sh no longer matches it, and a test says so."""
    pin = forge_pin(contents)
    return (f"# forge commit {pin['commit']} archive-sha256 {pin['sha256']} version {pin['local_version']}")


def input_problem(image_dir: Path, contents: dict) -> Optional[str]:
    """Offline: does the build's input match the lock? None when it does; else why not.

    A lock bump that skipped compile.sh leaves requirements.txt's pin line or requirements.in's forge line behind,
    and the image would be built from one commit and described by another."""
    pin = forge_pin(contents)
    try:
        header = (image_dir / "requirements.txt").read_text(encoding="utf-8").splitlines()[:1]
        lines = (image_dir / "requirements.in").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        return f"cannot read the build's requirements files: {exc}"
    if header != [pin_header(contents)]:
        return (f"requirements.txt was not compiled for the lock's forge pin (its first line is "
                f"{header[0] if header else 'missing'!r}, expected {pin_header(contents)!r})")
    forge = [ln for ln in lines if ln.startswith("data-product-forge")]
    if forge != [f"data-product-forge[local,aws] @ {pin['url']}"]:
        return f"requirements.in must have exactly one line `data-product-forge[local,aws] @ {pin['url']}`; has {forge}"
    return None


def want_version(contents: dict, key: str) -> str:
    """The version the image must report for one `contents` key."""
    return forge_pin(contents)["local_version"] if key == "forge" else _norm(contents[key])


def tag_for(image: str, contents: dict) -> str:
    """A name no registry answers for: `lab.local` is not a real registry."""
    pin = forge_pin(contents)
    return f"lab.local/{image}:{pin['version']}-{pin['commit'][:8]}"


def compare(contents: dict, found: dict, image: str = "fluid-runner") -> list:
    """Every way the probed image differs from the lock's `contents`, one line each."""
    if image == airflow_image.IMAGE:
        return _airflow_problems(contents, found)
    if image == spark_image.IMAGE:
        return spark_image.compare(contents, found)
    return (_version_problems(contents, found) + _forge_problems(contents, found)
            + _tofu_problems(contents, found) + _runtime_problems(found))


def _version_problems(contents: dict, found: dict) -> list:
    versions = _mapping(found.get("versions"))
    return [f"{dist} {versions.get(dist) or 'missing'}, the lock says {want_version(contents, key)}"
            for key, dist in DISTRIBUTIONS.items() if _norm(versions.get(dist)) != want_version(contents, key)]


def _forge_problems(contents: dict, found: dict) -> list:
    """forge must be built from the locked commit's archive, and what is installed must be that build's wheel."""
    pin = forge_pin(contents)
    source = _mapping(found.get("forge_source"))
    record = _mapping(source.get("record"))
    if source.get("error") or not record:
        return [f"the image does not record which forge source it was built from: {source.get('error') or 'nothing'}"]
    problems = [f"forge was built from {label} {record.get(key) or 'missing'}, the lock says {pin['commit' if key == 'commit' else 'sha256']}"
                for label, key in (("commit", "commit"), ("archive sha256", "archive_sha256"))
                if record.get(key) != pin["commit" if key == "commit" else "sha256"]]
    if not source.get("wheel_sha256") or source.get("wheel_sha256") != record.get("wheel_sha256"):
        problems.append(f"the wheel kept in the image has sha256 {source.get('wheel_sha256') or 'missing'}, "
                        f"the build recorded {record.get('wheel_sha256') or 'missing'}")
    checked, mismatched = source.get("checked"), source.get("mismatched")
    if not isinstance(checked, int) or checked < 1 or not isinstance(mismatched, list):
        problems.append("no installed forge file was compared with the wheel")
    elif mismatched:
        problems.append(f"{len(mismatched)} installed forge file(s) differ from the recorded wheel, "
                        f"such as {mismatched[0]}")
    return problems


def _tofu_problems(contents: dict, found: dict) -> list:
    problems = []
    tofu = _mapping(found.get("tofu"))
    if _norm(tofu.get("terraform_version")) != _norm(contents["opentofu"]):
        problems.append(f"OpenTofu {tofu.get('terraform_version') or 'missing'}, "
                        f"the lock says {contents['opentofu']}")
    init = _mapping(found.get("tofu_init"))
    if init.get("rc") != 0:
        problems.append("`tofu init` from the baked mirror failed: " + str(init.get("tail") or "no output").strip())
    want = {f"{PROVIDER_HOST}/{src}": _norm(v) for src, v in contents["providers"].items()}
    have = {src: _norm(v) for src, v in _mapping(tofu.get("provider_selections")).items()}
    if have != want:
        problems.append(f"providers {have or 'none'}, the lock says {want}")
    return problems


def _runtime_problems(found: dict) -> list:
    problems = []
    if found.get("uid") != RUNNER_UID:
        problems.append(f"runs as uid {found.get('uid')}, not {RUNNER_UID}")
    env = _mapping(found.get("env"))
    for name, value in ENV.items():
        if env.get(name) != value:
            problems.append(f"{name} is {env.get(name)!r}, not {value!r}")
    if not found.get("spark_runner"):
        problems.append("forge's Spark runner (fluid_build/spark/runner/forge_spark_runner.py) is missing")
    cli = _mapping(found.get("fluid_cli"))
    if cli.get("rc") != 0:
        problems.append(f"`fluid --version` failed: {cli.get('out') or 'no output'}")
    return problems


def _airflow_problems(contents: dict, found: dict) -> list:
    """airflow-fluid: forge, dbt and DuckDB as the lock pins them, Airflow untouched, `fluid` on PATH."""
    problems = _version_problems(contents, found) + _forge_problems(contents, found)
    if found.get("uid") != airflow_image.UID:
        problems.append(f"runs as uid {found.get('uid')}, not {airflow_image.UID}")
    env = _mapping(found.get("env"))
    problems += [f"{n} is {env.get(n)!r}, not {v!r}" for n, v in airflow_image.ENV.items() if env.get(n) != v]
    for tool in ("fluid", "dbt"):
        cli = _mapping(found.get(f"{tool}_cli"))
        if cli.get("rc") != 0:
            problems.append(f"`{tool} --version` failed on the airflow user's PATH: {cli.get('out') or 'no output'}")
    air = _mapping(found.get("airflow"))
    if not air:
        problems.append("Airflow does not import S3DagBundle from its Amazon provider: "
                        + str(found.get("airflow_tail") or "no output").strip()[-300:])
    elif _norm(air.get("airflow")) != _norm(contents["airflow"]):
        problems.append(f"Airflow {air.get('airflow')}, the lock says {contents['airflow']}")
    return problems


def docker_arch(runner: proc.Runner) -> str:
    """The Linux architecture the Docker server runs, which a kind node on it runs too."""
    res = runner(["docker", "version", "--format", "{{.Server.Arch}}"], timeout=60)
    arch = ARCHES.get((res.stdout or "").strip()) if res.ok else None
    if arch is None:
        raise BuildError(f"could not read the Docker server's architecture: "
                         f"{res.output_tail(300).strip() or 'no answer'}", "Start the container runtime and retry.")
    return arch


def build_args(contents: dict) -> list:
    """The Dockerfile's forge ARGs, fed from the lock so they are never typed twice."""
    pin = forge_pin(contents)
    return [a for k, v in (("FORGE_VERSION", pin["local_version"]), ("FORGE_COMMIT", pin["commit"]),
                           ("FORGE_SHA256", pin["sha256"])) for a in ("--build-arg", f"{k}={v}")]


def build_argv(root: Path, image: str, tag: str, arch: str, contents: dict) -> list:
    args = spark_image.build_args(contents) if image == spark_image.IMAGE else build_args(contents)
    return ["docker", "build", "--platform", f"linux/{arch}", *args, "--tag", tag, str(root / IMAGES[image])]


def probe_argv(ref: str, arch: str, name: str, image: str = "fluid-runner", entry: Optional[dict] = None,
               conf_text: str = "") -> list:
    if image == spark_image.IMAGE:
        return spark_image.probe_argv(ref, arch, name, entry or {}, conf_text)
    if image == airflow_image.IMAGE:
        return airflow_image.probe_argv(ref, arch, name, ",".join(DISTRIBUTIONS.values()),
                                        airflow_image.PROBE.replace("# __FORGE_SOURCE__", FORGE_SOURCE_FN))
    return ["docker", "run", "--rm", "--name", name, "--platform", f"linux/{arch}", "--network", "none",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--entrypoint", "python3", ref,
            "-I", "-c", PROBE, ",".join(DISTRIBUTIONS.values()), ",".join(ENV), FORGE_SOURCE_RECORD]


def image_facts(runner: proc.Runner, tag: str) -> dict:
    res = runner(["docker", "image", "inspect", tag, "--format", "{{json .}}"], timeout=60)
    if not res.ok:
        raise BuildError(f"docker image inspect {tag}: {res.output_tail(300).strip() or 'no answer'}")
    try:
        data = json.loads(res.stdout)
        return {"tag": tag, "id": data["Id"], "layers": data["RootFS"]["Layers"],
                "platform": f"{data['Os']}/{data['Architecture']}", "created": data.get("Created")}
    except (ValueError, KeyError, TypeError) as exc:
        raise BuildError(f"docker image inspect {tag} gave an unexpected answer: {exc}") from exc


def probe(runner: proc.Runner, ref: str, arch: str, image: str = "fluid-runner", entry: Optional[dict] = None,
          conf_text: str = "") -> dict:
    name = f"fluid-fabric-lab-probe-{os.getpid()}-{int(time.time())}"
    timeout = SPARK_PROBE_TIMEOUT_S if image == spark_image.IMAGE else PROBE_TIMEOUT_S
    res = runner(probe_argv(ref, arch, name, image, entry, conf_text), timeout=timeout)
    if res.timed_out:
        rm = runner(["docker", "rm", "-f", name], timeout=60)
        gone = ("was removed" if rm.ok else
                f"could not be removed ({rm.output_tail(200).strip()}); remove it with `docker rm -f {name}`")
        raise BuildError(f"the probe did not finish within {timeout} s; its container {name} {gone}")
    lines = (res.stdout or "").strip().splitlines()
    if not res.ok or not lines:
        raise BuildError(f"the probe did not run in {ref}: {res.output_tail(600).strip() or 'no output'}")
    try:
        found = json.loads(lines[-1])
    except ValueError as exc:
        raise BuildError(f"the probe printed no JSON: {lines[-1][:300]}") from exc
    if not isinstance(found, dict):
        raise BuildError(f"the probe printed {type(found).__name__}, not an object")
    return found


def node_layers(runner: proc.Runner, lab: state.Lab, tag: str) -> Optional[list]:
    res = runner(["docker", "exec", lab.node, "crictl", "inspecti", "-o", "json", tag], timeout=60)
    if not res.ok:
        return None
    try:
        return json.loads(res.stdout)["info"]["imageSpec"]["rootfs"]["diff_ids"]
    except (ValueError, KeyError, TypeError):
        return None


def _read_record(lab: state.Lab) -> dict:
    """The image record; an unreadable one is replaced (it only describes what was loaded)."""
    try:
        return state.read_json(lab.images_record) or {}
    except (OSError, ValueError, state.StateError):
        return {}


def _forget(lab: state.Lab, image: str) -> None:
    """After a failed load the node may hold anything under the tag: drop its record."""
    try:
        if lab.images_record.is_file():
            record = _read_record(lab)
            if record.pop(image, None) is not None:
                state.write_json_private(lab.images_record, record)
    except OSError:
        pass  # the record is compared with the node before use (`lab check`), never trusted alone


def load(runner: proc.Runner, lab: state.Lab, image: str, facts: dict) -> dict:
    """Load the image into the lab's node and prove the node holds the same layers."""
    again = f"`./lab build {image} --name {lab.name}`"
    try:
        present = cluster.exists(runner, lab)
    except cluster.ClusterError as exc:
        return check("build:load", FAIL, f"{exc}; nothing is loaded",
                     f"Install kind at its pin (`./lab doctor`) and start the container runtime, then run {again}.")
    if not present:
        return check("build:load", WARN, f"built, not loaded: lab {lab.name} has no kind cluster",
                     f"Run `./lab up --name {lab.name}`, then {again} (the build is cached).")
    if not down.owns_state(lab):
        return check("build:load", FAIL, f"kind cluster {lab.cluster} has no lab state in {lab.state}; "
                     "nothing is loaded", "`lab build` loads only into labs that `lab up` created.")
    node = runner(["docker", "inspect", "--format", "{{.Id}}", lab.node], timeout=60)
    if not node.ok:
        return check("build:load", FAIL, f"could not inspect node {lab.node}: {node.output_tail(300).strip()}",
                     f"Check the cluster with `kind get nodes --name {lab.cluster}`, then run {again}.")
    res = runner(["kind", "load", "docker-image", facts["tag"], "--name", lab.cluster], timeout=LOAD_TIMEOUT_S)
    layers = node_layers(runner, lab, facts["tag"]) if res.ok else None
    if not res.ok or layers != facts["layers"]:
        _forget(lab, image)
        why = (f"kind load docker-image failed: {res.output_tail(600).strip()}" if not res.ok
               else f"node {lab.node} does not hold the layers of {facts['tag']}")
        return check("build:load", FAIL, why,
                     f"Check the node with `docker exec {lab.node} crictl images`, then run {again}.")
    try:
        record = _read_record(lab)
        record[image] = {**facts, "cluster": lab.cluster, "node": lab.node, "node_id": node.stdout.strip()}
        state.write_json_private(lab.images_record, record)
    except OSError as exc:
        return check("build:load", FAIL, f"loaded, but {lab.images_record} could not be written: {exc}",
                     f"Fix the state directory's permissions, then run {again}.")
    return check("build:load", PASS, f"loaded into kind cluster {lab.cluster}: the node holds the same "
                 f"{len(layers)} layers; recorded in {lab.images_record}")


def run_build(root: Path, image: str, *, name: str = state.DEFAULT_NAME, runner: proc.Runner = proc.run,
              env: Optional[Mapping[str, str]] = None, clock: Callable[[], float] = time.monotonic,
              log: Callable[[str], None] = lambda _m: None) -> dict:
    """Build, probe and load one lab-owned image. Raises state.StateError for a bad state directory."""
    env = os.environ if env is None else env
    lab = state.lab_for(root, name, env)
    checks: list = []
    doc = {"command": "build", "image": image, "name": lab.name, "ok": False, "checks": checks}
    if image not in IMAGES:
        checks.append(check("build:config", FAIL, f"no buildable image {image!r}; known: {', '.join(IMAGES)}"))
        return doc
    rt = doctor.select_runtime(None, env, proc.which)
    if rt != "docker":
        checks.append(check("build:config", FAIL, f"the lab builds with the docker runtime only (selected: {rt})",
                            "Unset KIND_EXPERIMENTAL_PROVIDER and use Docker Desktop, Colima, Rancher "
                            "Desktop (moby) or Docker Engine."))
        return doc
    try:
        entry = lock_entry(config.load_yaml(root / config.VERSIONS_LOCK, runner), image)
        contents = entry["contents"]
        if image == spark_image.IMAGE:
            stale = spark_image.input_problem(root, entry)
            advice = f"Make {IMAGES[image]}/Dockerfile and {spark_image.CONF} agree with {config.VERSIONS_LOCK}."
        else:
            stale = input_problem(root / IMAGES[image], contents)
            advice = f"Run {IMAGES[image]}/compile.sh after editing forge's pin in {config.VERSIONS_LOCK}."
        if stale:
            raise BuildError(stale, advice)
        conf_text = (root / spark_image.CONF).read_text(encoding="utf-8") if image == spark_image.IMAGE else ""
        arch = docker_arch(runner)
    except OSError as exc:
        checks.append(check("build:config", FAIL, f"cannot read the build's input: {exc}"))
        return doc
    except config.ConfigError as exc:
        checks.append(check("build:config", FAIL, str(exc)))
        return doc
    except BuildError as exc:
        checks.append(check("build:config", FAIL, str(exc), exc.advice))
        return doc
    tag = spark_image.tag_for(entry) if image == spark_image.IMAGE else tag_for(image, contents)
    doc["tag"] = tag
    log(f"lab build: building {tag} for linux/{arch}; the first build downloads about "
        f"{'350' if image == spark_image.IMAGE else '300'} MB ...")
    started = clock()
    res = runner(build_argv(root, image, tag, arch, contents), env={**env, "DOCKER_BUILDKIT": "1"}, timeout=BUILD_TIMEOUT_S)
    if not res.ok:
        checks.append(check("build:docker", FAIL, f"docker build of {IMAGES[image]} failed: "
                            f"{res.output_tail(1500).strip() or 'no output'}",
                            ("The build needs the network (Docker Hub, Maven Central) and BuildKit. A checksum "
                             "mismatch stops it on purpose." if image == spark_image.IMAGE else
                             "The build needs the network (PyPI, forge's source archive on github.com, "
                             "the OpenTofu release, the provider registry) "
                             "and BuildKit. A hash or checksum mismatch stops it on purpose.")))
        return doc
    try:
        facts = image_facts(runner, tag)
    except BuildError as exc:
        checks.append(check("build:docker", FAIL, str(exc), exc.advice))
        return doc
    checks.append(check("build:docker", PASS, f"built {tag} ({facts['platform']}) in {clock() - started:.0f} s"))
    log(f"lab build: probing {facts['id'][:19]} with no network ...")
    try:
        found = probe(runner, facts["id"], arch, image, entry, conf_text)
    except BuildError as exc:
        checks.append(check("build:probe", FAIL, str(exc), exc.advice))
        return doc
    problems = compare(contents, found, image)
    if problems:
        checks.append(check("build:probe", FAIL, "; ".join(problems),
                            f"Fix {IMAGES[image]} or the lock's built_images entry, then build again."))
        return doc
    try:
        moved = image_facts(runner, tag)["id"] != facts["id"]
    except BuildError as exc:
        checks.append(check("build:probe", FAIL, str(exc), exc.advice))
        return doc
    if moved:
        checks.append(check("build:probe", FAIL, f"{tag} changed while it was being probed; nothing is loaded",
                            "Run `lab build` again with no other build of this tag running."))
        return doc
    if image == spark_image.IMAGE:
        facts["versions"] = {"spark": contents["spark"], "iceberg": contents["iceberg"],
                             "kafka-connector": contents["kafka-connector"]}
        checks.append(check("build:probe", PASS, f"with no network: Spark {contents['spark']}, Iceberg "
                            f"{contents['iceberg']}, Kafka connector {contents['kafka-connector']}; "
                            f"{len(contents['files'])} files match the lock's sha256; spark-submit with the lab's "
                            f"properties loaded {len(spark_image.CLASSES)} classes "
                            f"({', '.join(c.rsplit('.', 1)[1] for c in spark_image.CLASSES)}); uid {spark_image.UID}"))
        log(f"lab build: loading {tag} into lab {lab.name} ...")
        checks.append(load(runner, lab, image, facts))
        doc["ok"] = all(c["status"] != FAIL for c in checks)
        return doc
    facts["forge_commit"] = found["forge_source"]["record"]["commit"]
    forge_note = (f"forge commit {forge_pin(contents)['commit'][:8]}; archive sha256 and wheel match the record; "
                  f"{found['forge_source']['checked']} installed files match the wheel")
    if image == airflow_image.IMAGE:
        air = found["airflow"]
        facts["versions"] = {**found["versions"], "airflow": air["airflow"], "airflow-amazon": air["amazon"]}
        checks.append(check("build:probe", PASS, "with no network: " + ", ".join(
            f"{k} {want_version(contents, k)}" for k in DISTRIBUTIONS)
            + f" ({forge_note}); Airflow {air['airflow']} imports {air['bundle']} (Amazon provider {air['amazon']}); "
            f"`fluid` and `dbt` run from the airflow user's PATH; uid {airflow_image.UID}; telemetry off"))
    else:
        facts["versions"] = {**found["versions"], "opentofu": found["tofu"]["terraform_version"],
                             "providers": found["tofu"]["provider_selections"]}
        checks.append(check("build:probe", PASS, "with no network: " + ", ".join(
            f"{k} {want_version(contents, k)}" for k in (*DISTRIBUTIONS, "opentofu"))
            + f" ({forge_note})"
            + f"; {len(contents['providers'])} providers installed by `tofu init` from the baked mirror; "
            f"uid {RUNNER_UID}; telemetry off; `fluid` and forge's Spark runner present"))
    log(f"lab build: loading {tag} into lab {lab.name} ...")
    checks.append(load(runner, lab, image, facts))
    doc["ok"] = all(c["status"] != FAIL for c in checks)
    return doc


def main(argv: list) -> int:
    """`python3 -m labctl.build forge <field>`: the lock's forge pin for ./compile.sh (field: version, commit,
    sha256, url, local_version, release, header)."""
    fields = ("version", "commit", "sha256", "url", "local_version", "release", "header")
    if len(argv) != 2 or argv[0] != "forge" or argv[1] not in fields:
        print(f"usage: python3 -m labctl.build forge {{{'|'.join(fields)}}}", file=sys.stderr)
        return 2
    try:
        root = Path(__file__).resolve().parents[1]
        contents = lock_entry(config.load_yaml(root / config.VERSIONS_LOCK), "fluid-runner")["contents"]
        value = pin_header(contents) if argv[1] == "header" else forge_pin(contents)[argv[1]]
        print(str(value).lower() if isinstance(value, bool) else value)
    except config.ConfigError as exc:
        print(f"lab build: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
