# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""What `lab build spark-iceberg` holds the Spark image to (labctl.build runs the steps).

The image is the official apache/spark 3.5.8 image with the Iceberg runtime, the S3A connector,
the Kafka client and an offline Maven repository for the Spark Kafka connector added
(platform/images/spark-iceberg). The lock's `contents.files` lists every added file with its
Maven coordinate, URL, path and sha256: it is the only place those are written. `build_args`
turns it into the Dockerfile's build args, and the probe compares the image with it.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from labctl import config

IMAGE = "spark-iceberg"
UID = 185                                   # the `spark` user of the apache/spark image
DIR = "platform/images/spark-iceberg"
CONF = "platform/spark/spark-defaults.apache.conf"
SPARK_BIN = "/opt/spark/bin/spark-submit"
# The classes the probe loads with spark-submit's own classpath, offline: the Kafka source, the Kafka
# client, the Iceberg catalog, the AWS SDK v2 S3 client (inside the Iceberg AWS bundle), S3A and commons-pool2.
CLASSES = ("org.apache.spark.sql.kafka010.KafkaSourceProvider", "org.apache.kafka.clients.consumer.KafkaConsumer",
           "org.apache.iceberg.spark.SparkCatalog", "software.amazon.awssdk.services.s3.S3Client",
           "org.apache.hadoop.fs.s3a.S3AFileSystem", "org.apache.commons.pool2.impl.GenericKeyedObjectPool")
_ID = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
_SHA = re.compile(r"[0-9a-f]{64}")

# Runs in the image as `python3 -I -c PROBE <files JSON> <properties text> <classes>` with no network and
# always prints one JSON object: every failure inside becomes a field, never a traceback.
PROBE = r"""
import hashlib, json, os, subprocess, sys, tempfile

files, props, classes = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3].split(",")
SUBMIT = sys.argv[4]
APP = '''
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("lab-probe").getOrCreate()
jvm = spark._jvm
loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
for name in sys.argv[1].split(","):
    try:
        loader.loadClass(name)
        print("LAB-PROBE-CLASS ok " + name)
    except Exception as exc:
        print("LAB-PROBE-CLASS missing " + name + " " + str(exc)[:200])
spark.stop()
'''


def run(argv, timeout, env=None):
    try:
        res = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env)
        return {"rc": res.returncode, "out": res.stdout, "err": res.stderr}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"rc": None, "out": "", "err": f"{type(exc).__name__}: {exc}"}


out = {"uid": os.getuid(), "files": {}}
for entry in files:
    try:
        with open(entry["path"], "rb") as handle:
            out["files"][entry["path"]] = {"sha256": hashlib.sha256(handle.read()).hexdigest(),
                                          "mode": oct(os.stat(entry["path"]).st_mode & 0o777)}
    except OSError as exc:
        out["files"][entry["path"]] = {"error": f"{type(exc).__name__}: {exc.strerror}"}
env = {**os.environ, "SPARK_LOCAL_HOSTNAME": "localhost"}
ver = run([SUBMIT, "--version"], 120, env)
out["version"] = {"rc": ver["rc"], "text": (ver["out"] + ver["err"])[-1500:]}
work = tempfile.mkdtemp()
conf = os.path.join(work, "spark-defaults.conf")
app = os.path.join(work, "probe_classes.py")
with open(conf, "w") as handle:
    handle.write(props)
with open(app, "w") as handle:
    handle.write(APP)
res = run([SUBMIT, "--master", "local[1]", "--properties-file", conf, app, ",".join(classes)], 240, env)
out["classes"] = {}
for line in res["out"].splitlines():
    if line.startswith("LAB-PROBE-CLASS "):
        state, _, rest = line[len("LAB-PROBE-CLASS "):].partition(" ")
        name = rest.split(" ", 1)[0]
        out["classes"][name] = state == "ok"
out["submit"] = {"rc": res["rc"], "tail": (res["out"] + res["err"])[-1200:]}
print(json.dumps(out))
"""


def _entry_problem(entry: dict) -> str:
    for key in ("id", "coordinate", "url", "path", "sha256"):
        if not isinstance(entry.get(key), str) or not entry[key]:
            return f"has no `{key}`"
    if not _ID.fullmatch(entry["id"]):
        return f"id {entry['id']!r} must be lower-case words joined by `-`"
    if not _SHA.fullmatch(entry["sha256"]):
        return f"sha256 {entry['sha256']!r} is not a 64-character lowercase sha256"
    if not entry["url"].startswith("https://") or not entry["path"].startswith("/opt/"):
        return "needs an https `url` and an absolute /opt/ `path`"
    return ""


def resolve(lock: dict, entry: dict) -> dict:
    """The entry, checked: base with ref and digest, the three versions, and a well-formed `files` list."""
    where = f"{config.VERSIONS_LOCK} built_images[{IMAGE}]"
    contents = entry.get("contents")
    if not isinstance(contents, dict):
        raise config.ConfigError(f"{where} needs `contents`, a mapping")
    missing = [k for k in ("spark", "iceberg", "kafka-connector") if not contents.get(k)]
    if missing:
        raise config.ConfigError(f"{where} contents lacks {', '.join(missing)}")
    files = contents.get("files")
    if not isinstance(files, list) or not files or not all(isinstance(f, dict) for f in files):
        raise config.ConfigError(f"{where} contents.files must be a list of mappings")
    for f in files:
        problem = _entry_problem(f)
        if problem:
            raise config.ConfigError(f"{where} contents.files[{f.get('id')}] {problem}")
    for key in ("id", "path"):
        seen = [f[key] for f in files]
        if len(set(seen)) != len(seen):
            raise config.ConfigError(f"{where} contents.files has a repeated {key}")
    base = entry.get("base")
    if not isinstance(base, dict) or not base.get("ref") or not str(base.get("digest") or "").startswith("sha256:"):
        raise config.ConfigError(f"{where} needs a `base` with ref and digest")
    return entry


def arg_name(file_id: str, what: str) -> str:
    return f"{file_id.upper().replace('-', '_')}_{what}"


def build_args(contents: dict) -> list:
    """The Dockerfile's <ID>_URL and <ID>_SHA256 ARGs, fed from the lock so they are never typed twice."""
    args = []
    for f in contents["files"]:
        for what, value in (("URL", f["url"]), ("SHA256", f["sha256"])):
            args += ["--build-arg", f"{arg_name(f['id'], what)}={value}"]
    return args


def tag_for(entry: dict) -> str:
    """A name no registry answers for. The last part follows the base digest and every file's sha256, so a
    change to either gives a new tag (the node never keeps an old image under the new name)."""
    contents = entry["contents"]
    seed = entry["base"]["digest"] + "".join(f"{f['path']}={f['sha256']}" for f in contents["files"])
    return (f"lab.local/{IMAGE}:{contents['spark']}-iceberg{contents['iceberg']}-"
            f"{hashlib.sha256(seed.encode()).hexdigest()[:8]}")


def input_problem(root: Path, entry: dict) -> str:
    """Offline: does the build's input match the lock? "" when it does; else why not.

    The Dockerfile names every build arg and the path each file lands at; the properties file's `spark.jars`
    names the two Iceberg jars. A lock bump that skipped one of them would build an image the conf cannot use."""
    contents = entry["contents"]
    try:
        text = (root / DIR / "Dockerfile").read_text(encoding="utf-8")
        conf = (root / CONF).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        return f"cannot read the build's input: {exc}"
    base = f"{entry['base']['ref']}@{entry['base']['digest']}"
    # Whole lines only: a commented line or a substring of a longer one proves nothing.
    bases = re.findall(r"^ARG SPARK_IMAGE=(.*)$", text, re.M)
    if bases != [base]:
        return f"the Dockerfile's `ARG SPARK_IMAGE=` is not the lock's base, {base} (found {bases or 'none'})"
    if not re.search(r"^FROM \$\{SPARK_IMAGE\}$", text, re.M):
        return "the Dockerfile has no `FROM ${SPARK_IMAGE}` line"
    loose = [ln for ln in re.findall(r"^ADD .*$", text, re.M) if "--checksum=sha256:${" not in ln]
    if loose:
        return f"the Dockerfile has an ADD without a checked sha256: `{loose[0]}`"
    for f in contents["files"]:
        want = (f"ADD --chmod=0644 --checksum=sha256:${{{arg_name(f['id'], 'SHA256')}}} "
                f"${{{arg_name(f['id'], 'URL')}}} {f['path']}")
        if not re.search("^" + re.escape(want) + "$", text, re.M):
            return f"the Dockerfile has no line `{want}` for file {f['id']} of the lock"
        for what in ("URL", "SHA256"):
            if not re.search(f"^ARG {arg_name(f['id'], what)}$", text, re.M):
                return f"the Dockerfile has no `ARG {arg_name(f['id'], what)}` (no default is allowed)"
    defaults = sorted(n for n in re.findall(r"^ARG\s+([A-Za-z0-9_]+)=", text, re.M | re.I) if n != "SPARK_IMAGE")
    if defaults:
        return f"the Dockerfile gives {', '.join(defaults)} a default; the lock is the only place they are written"
    jars = next((ln.split(None, 1)[1].strip() for ln in conf.splitlines() if ln.startswith("spark.jars ")), "")
    want_jars = ",".join(f["path"] for f in contents["files"] if f["id"] in ("iceberg-spark-runtime",
                                                                            "iceberg-aws-bundle"))
    if jars != want_jars:
        return f"{CONF} says `spark.jars {jars}`, the lock's Iceberg jars are {want_jars}"
    return ""


def probe_argv(ref: str, arch: str, name: str, entry: dict, conf_text: str) -> list:
    contents = entry["contents"]
    files = [{"id": f["id"], "path": f["path"]} for f in contents["files"]]
    return ["docker", "run", "--rm", "--name", name, "--platform", f"linux/{arch}", "--network", "none",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--entrypoint", "python3", ref,
            "-I", "-c", PROBE, json.dumps(files), conf_text, ",".join(CLASSES), SPARK_BIN]


def compare(contents: dict, found: dict) -> list:
    """Every way the probed image differs from the lock's `contents`, one line each."""
    problems = []
    if found.get("uid") != UID:
        problems.append(f"runs as uid {found.get('uid')}, not {UID}")
    version = found.get("version") if isinstance(found.get("version"), dict) else {}
    if version.get("rc") != 0 or not re.search(rf"version\s+{re.escape(str(contents['spark']))}\b",
                                               str(version.get("text") or "")):
        problems.append(f"`spark-submit --version` does not say {contents['spark']}: "
                        f"{str(version.get('text') or 'no output').strip()[-200:]}")
    files = found.get("files") if isinstance(found.get("files"), dict) else {}
    for f in contents["files"]:
        got = files.get(f["path"]) if isinstance(files.get(f["path"]), dict) else {}
        if got.get("error"):
            problems.append(f"{f['path']}: {got['error']}")
        elif got.get("sha256") != f["sha256"]:
            problems.append(f"{f['path']} has sha256 {got.get('sha256') or 'missing'}, the lock says {f['sha256']}")
        elif got.get("mode") not in ("0o644",):
            problems.append(f"{f['path']} has mode {got.get('mode')}, not 0o644 (the pod's uid must read it)")
    classes = found.get("classes") if isinstance(found.get("classes"), dict) else {}
    absent = [c for c in CLASSES if classes.get(c) is not True]
    submit = found.get("submit") if isinstance(found.get("submit"), dict) else {}
    if absent or submit.get("rc") != 0:
        problems.append("spark-submit with the lab's properties and no network did not load "
                        + (", ".join(absent) if absent else "its classes")
                        + f" (exit {submit.get('rc')}): {str(submit.get('tail') or 'no output').strip()[-400:]}")
    return problems
