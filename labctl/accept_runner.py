# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The `cli` runner of `lab accept`: one `fluid ship --stage N` per exec in the fluid-host pod.

This is how generated CI runs forge, one stage per step. The product directory is
copied into the pod as a tar stream on exec stdin (base64 inside it: labctl.proc
passes text, and a tar archive is binary), under /work/accept/<run-id>/<product>.
Every stage then runs in its own `kubectl exec`, and the ship report forge writes
(runtime/ship-report.json, merged by FLUID_RUN_ID) is read back after each one.

What goes into an exec's argv is fixed here: FLUID_ENV, FLUID_RUN_ID, VALIDATE_STRICT=true,
VERIFY_STRICT=true and the operator's FLUID_*_ALLOWLIST values from
profiles/<profile>/operator/<env>.env. Nothing is taken from the launcher's own
environment, so a secret there cannot reach an argv.

The pod's own environment is not trusted either: forge reads variables that weaken a stage
(POLICY_APPLY_MODE, APPLY_MODE, ALLOW_DATA_LOSS, NO_VERIFY_DIGEST, DIFF_EXIT_ON_DRIFT,
SCHEDULE_SYNC_DRY_RUN, RUN_STAGE_<N>_<NAME>, the *_ALLOWLIST of another operator). FORGE_WRAPPER
unsets every variable of the pod except a short allow set (KEEP_NAMES, KEEP_PREFIXES), by name,
so no value is ever put in an argv. Each forge exec also runs under `timeout`, so a stage the
launcher gave up on does not keep running in the pod.
"""

from __future__ import annotations

import base64
import io
import json
import re
import tarfile
import time
from pathlib import Path
from typing import Callable, Mapping, Optional

from labctl import accept, bootstrap, http, kube, openbao, proc
from labctl.accept import (AcceptError, Observed, Product, Run, Site, Step, StepResult, error_cell, map_status,
                           oneline, strict_guard)

WORK_ROOT = "/work/accept"
SHIP_REPORT = "runtime/ship-report.json"
MAX_TAR_BYTES = 20 * 1024 * 1024
# Directories and files that are forge's own output or a developer's, never part of the product's input.
EXCLUDE_DIRS = {".git", "runtime", "dist", ".fluid", "__pycache__", ".venv", "node_modules", "target"}
EXCLUDE_FILES = {".DS_Store"}
STAGE_TIMEOUT_S = {7: 3600}
DEFAULT_STAGE_TIMEOUT_S = 1800
POLL_INTERVAL_S = 5
CATALOG_PATH = "/catalog/v1"
TIMEOUT_MARGIN_S = 30          # the remote `timeout` fires before the launcher's own deadline
EXIT_TIMEOUT = 124             # what `timeout` exits with; the watchdog fallback maps its kill to it
_RUN_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,62}$")

# The only variables of the pod that reach forge (everything else is unset, by name).
KEEP_NAMES = ("PATH", "HOME", "TMPDIR", "TZ", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE",
              "CURL_CA_BUNDLE", "DBT_PROFILES_DIR", "JAVA_HOME", "SPARK_HOME", "KUBERNETES_SERVICE_HOST",
              "KUBERNETES_SERVICE_PORT")
KEEP_PREFIXES = ("LC_", "FLUID_", "AWS_", "LAKEKEEPER_")   # the lab's own, except FLUID_*_ALLOWLIST (operator's file only)

CD_WRAPPER = 'cd "$1" && shift && exec "$@"'
FORGE_WRAPPER = r"""dir=$1; limit=$2; shift 2
cd "$dir" || exit 126
for name in $(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p'); do
  case "$name" in
    %(names)s) ;;
    FLUID_*_ALLOWLIST) set -- -u "$name" "$@" ;;
    %(prefixes)s) ;;
    *) set -- -u "$name" "$@" ;;
  esac
done
if command -v timeout >/dev/null 2>&1; then exec timeout -k 20 "$limit" env "$@"; fi
env "$@" &
pid=$!
( sleep "$limit"; kill -TERM "$pid"; sleep 10; kill -KILL "$pid" ) >/dev/null 2>&1 &
dog=$!
wait "$pid"; rc=$?
kill "$dog" >/dev/null 2>&1
if [ "$rc" = 143 ] || [ "$rc" = 137 ]; then rc=124; fi
exit "$rc"
""" % {"names": "|".join(KEEP_NAMES), "prefixes": "|".join(p + "*" for p in KEEP_PREFIXES)}
# Exit 3 when a process of another run still has this product's directory as its working directory.
LIVE_CHECK = r"""for p in /proc/[0-9]*; do
  cwd=$(readlink "$p/cwd" 2>/dev/null) || continue
  case "$cwd" in "$1"/*/"$2"|"$1"/*/"$2"/*) echo "${p#/proc/}"; exit 3 ;; esac
done
exit 0
"""


def ship_argv(number: int, site: Site, env: str, contract: str = "contract.fluid.yaml") -> list:
    """`fluid ship` for one stage, with the flags the generated CI passes. Always strict, no override."""
    argv = ["fluid", "ship", contract, "--env", env, "--strict", "--verify-strict", "--stage", str(number)]
    if number == accept.APPLY:
        argv.append("--yes")
    elif number == accept.PUBLISH:
        argv += ["--publish", "--target", str(site.publish_target)]
    elif number == accept.SCHEDULE_SYNC:
        argv += ["--schedule-sync", "--scheduler", str(site.scheduler_kind),
                 "--scheduler-destination", str(site.scheduler_destination)]
    return strict_guard(argv)


def apply_build_argv(build_id: str, env: str, contract: str = "contract.fluid.yaml") -> list:
    """The command the generated DAG's single task runs, once per build, by hand."""
    return strict_guard(["fluid", "apply", contract, "--env", env, "--mode", "amend-and-build",
                         "--build-id", build_id, "--yes"])


def safe_tail(res: proc.Result, limit: int = 600) -> str:
    """The end of a command's output, redacted BEFORE it is cut: a secret cut in two would no longer be recognised."""
    return accept.redact((res.stdout or "") + (res.stderr or ""))[-limit:]


def env_prefix(env: str, run_id: str, allowlists: Mapping[str, str]) -> list:
    accept.refuse_weakened_strict(allowlists, "the operator's allowlists")
    return ["env", f"FLUID_ENV={env}", f"FLUID_RUN_ID={run_id}", "VALIDATE_STRICT=true", "VERIFY_STRICT=true",
            *[f"{k}={v}" for k, v in sorted(allowlists.items())]]


def product_tar(directory: Path) -> bytes:
    """The product's input files as a deterministic tar (names relative, owner 0, mtime 0)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for path in sorted(directory.rglob("*")):
            rel = path.relative_to(directory)
            if any(part in EXCLUDE_DIRS for part in rel.parts) or path.name in EXCLUDE_FILES or path.suffix == ".pyc":
                continue
            if path.is_symlink():
                raise AcceptError(f"{path} is a symbolic link; a product is copied as plain files")
            info = tarfile.TarInfo(rel.as_posix())
            info.mtime, info.uid, info.gid, info.uname, info.gname = 0, 0, 0, "", ""
            if path.is_dir():
                info.type, info.mode = tarfile.DIRTYPE, 0o755
                tar.addfile(info)
            elif path.is_file():
                data = path.read_bytes()
                info.size, info.mode = len(data), 0o755 if path.stat().st_mode & 0o111 else 0o644
                tar.addfile(info, io.BytesIO(data))
    if buf.tell() > MAX_TAR_BYTES:
        raise AcceptError(f"{directory} is {buf.tell() // 1024} KiB as a tar; the cli runner copies at most "
                          f"{MAX_TAR_BYTES // 1024} KiB")
    return buf.getvalue()


def observe_stage(number: int, exit_code: int, output_tail: str, report_text: Optional[str], argv: list,
                  timed_out: bool = False, run_id: Optional[str] = None) -> Observed:
    """One stage, from ship's exit code and the report it left: the ledger's word for it, or an error cell.

    The stage was asked for with `--stage N`, so a record that says it did not run (`not-run`) is the harness's
    problem, not a result: forge exits 0 without writing a report when RUN_STAGE_<N>_* is false in its environment,
    and a failed report write leaves an earlier call's record behind. The report must also be this run's.
    """
    name = accept.STAGE_NAMES[number - 1]
    if timed_out or exit_code == EXIT_TIMEOUT:
        return error_cell(f"stage {number} {name} timed out", argv)
    try:
        doc = json.loads(report_text) if report_text else None
    except ValueError:
        doc = None
    records = [r for r in (doc or {}).get("stages") or [] if isinstance(r, dict)] if isinstance(doc, dict) else []
    rec = next((r for r in records if r.get("number") == number), None)
    if run_id is not None and isinstance(doc, dict) and doc.get("run_id") != run_id:
        return error_cell(f"the ship report belongs to run {doc.get('run_id')!r}, not {run_id!r}: it is not this stage's",
                          argv)
    if rec is None:
        why = "no ship report" if doc is None else f"the ship report has no record of stage {number}"
        return error_cell(f"{why} (fluid ship exited {exit_code}): {oneline(output_tail)}", argv)
    if rec.get("name") != name:
        return error_cell(f"the ship report names stage {number} {rec.get('name')!r}, not {name!r}", argv)
    ship_status = str(rec.get("status"))
    skip_reason = rec.get("skip_reason")
    reason = rec.get("failure_reason") if ship_status == "failed" else skip_reason
    try:
        status = map_status(ship_status, skip_reason)
    except ValueError as exc:
        return error_cell(str(exc), argv)
    if status == "pending":
        return error_cell(f"stage {number} {name} was selected with --stage but forge did not run it "
                          f"({ship_status}, exit {exit_code}): {oneline(skip_reason or output_tail, 200)}", argv)
    if exit_code != 0 and status != "failed":
        return error_cell(f"fluid ship exited {exit_code} but its report says stage {number} {ship_status}", argv)
    if status == "failed" and not reason:
        reason = f"exit code {rec.get('exit_code', exit_code)}: {oneline(output_tail, 200)}"
    if status == "passed" and not reason:
        reason = "the stage passed"
    return Observed(status, str(reason or ""), ship_status=ship_status, exit_code=rec.get("exit_code", exit_code),
                    argv=argv)


class LabCatalog:
    """Does a table exist in Lakekeeper's REST catalog? Through the lab's own client code, as `lab check` does.

    Opens OpenBao (read-only, for lab-admin's client secret) and a port-forward to the Gateway on first use,
    and opens them again once if the forward died under a poll.
    """

    def __init__(self, lab, kubectl: kube.Kube, runner: proc.Runner, opener: Optional[Callable] = None):
        self.lab, self.kube, self.runner = lab, kubectl, runner
        self.client: Optional[bootstrap.LabClient] = None
        self.forward = None
        self.opener = opener or self._open_lab

    def _open_lab(self) -> tuple:
        cafile = self.lab.state / "ca" / "ca.crt"
        bao = openbao.Bao(openbao.host_port(self.runner, self.lab.openbao), str(cafile))
        openbao.open_existing(bao, self.lab)
        creds = bao.kv_get(openbao.KV_MOUNT, "lab/credentials") or {}
        if not creds:
            raise AcceptError("OpenBao has no lab credentials (kv/lab/credentials is empty)")
        forward = bootstrap.gateway(self.kube)
        forward.__enter__()                      # raises (after closing itself) when the forward does not start
        return bootstrap.LabClient(cafile, forward.port, creds), forward

    def _open(self) -> bootstrap.LabClient:
        if self.client is None:
            self.client, self.forward = self.opener()
        return self.client

    def _reset(self) -> None:
        if self.forward is not None:
            self.forward.close()
        self.client = self.forward = None

    def table_exists(self, warehouse: str, namespace: str, table: str) -> bool:
        try:
            return self._ask(warehouse, namespace, table)
        except (OSError, http.HTTPError):        # a dead port-forward: open a new one, once
            self._reset()
            return self._ask(warehouse, namespace, table)

    def _ask(self, warehouse: str, namespace: str, table: str) -> bool:
        client = self._open()
        auth = {"Authorization": f"Bearer {client.token('lab-admin')}"}
        cfg = client.https("GET", "lakekeeper.lab.test", f"{CATALOG_PATH}/config?warehouse={warehouse}", headers=auth)
        body = cfg.json() if cfg.status == 200 else {}
        prefix = ((body.get("overrides") or {}).get("prefix") or (body.get("defaults") or {}).get("prefix"))
        if not prefix:
            raise AcceptError(f"Lakekeeper has no warehouse {warehouse!r} (HTTP {cfg.status})")
        found = client.https("GET", "lakekeeper.lab.test",
                             f"{CATALOG_PATH}/{prefix}/namespaces/{namespace}/tables/{table}", headers=auth, timeout=60)
        if found.status == 200:
            return True
        if found.status == 404:
            return False
        raise AcceptError(f"Lakekeeper loadTable {namespace}.{table}: HTTP {found.status} {found.text(200)}")

    def close(self) -> None:
        self._reset()


class CliRunner(accept.Runner):
    name = "cli"

    def __init__(self, kubectl, site: Site, *, env: str, run_id: str, allowlists: Mapping[str, str],
                 catalog=None, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 pod_timeout_s: int = 180, keep: bool = False):
        if not _RUN_ID.match(run_id):
            raise AcceptError(f"run id {run_id!r} is not usable as a directory name")
        self.kube, self.site, self.env, self.run_id = kubectl, site, env, run_id
        self.allowlists = dict(allowlists)
        accept.refuse_weakened_strict(self.allowlists, "the operator's allowlists")
        self.keep, self.notes = keep, []
        self.catalog = catalog
        self.clock, self.sleep, self.pod_timeout_s = clock, sleep, pod_timeout_s
        self._pod: Optional[str] = None

    # ---- the pod

    def pod(self) -> str:
        if self._pod is None:
            dep = self.kube.get("deployment", self.site.host_deployment, self.site.host_namespace)
            if not dep:
                raise AcceptError(f"no Deployment {self.site.host_namespace}/{self.site.host_deployment} (fluid-host)",
                                  "The cli runner needs the fluid-host Deployment of the spark tier.")
            labels = ((dep.get("spec") or {}).get("selector") or {}).get("matchLabels") or {}
            if not labels:
                raise AcceptError(f"Deployment {self.site.host_namespace}/{self.site.host_deployment} has no selector")
            selector = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            self._pod = self.kube.first_pod(self.site.host_namespace, selector, self.pod_timeout_s)
        return self._pod

    def workdir(self, product: Product) -> str:
        return f"{WORK_ROOT}/{self.run_id}/{product.id}"

    def exec(self, argv: list, *, cwd: Optional[str] = None, input: Optional[str] = None,  # noqa: A002
             timeout: float = 120) -> proc.Result:
        command = ["sh", "-c", CD_WRAPPER, "sh", cwd, *argv] if cwd else list(argv)
        return self.kube.exec(self.site.host_namespace, self.pod(), command, input=input, timeout=timeout)

    def forge(self, argv: list, *, cwd: str, timeout: float) -> proc.Result:
        """A forge command in the product's directory, in the pod's environment cleaned by FORGE_WRAPPER, under `timeout`."""
        strict_guard(argv)
        limit = max(int(timeout) - TIMEOUT_MARGIN_S, 1)
        command = ["sh", "-c", FORGE_WRAPPER, "sh", cwd, str(limit), *argv]
        return self.kube.exec(self.site.host_namespace, self.pod(), command, timeout=timeout)

    # ---- runner interface

    def version(self) -> str:
        res = self.exec(["fluid", "--version"], timeout=120)
        text = (res.stdout or "").strip() or res.output_tail(200).strip()
        if not res.ok or not text:
            raise AcceptError(f"`fluid --version` in fluid-host failed: {oneline(text or 'no output')}")
        return text

    def prepare(self, product: Product) -> None:
        workdir = self.workdir(product)
        live = self.exec(["sh", "-c", LIVE_CHECK, "sh", WORK_ROOT, product.id], timeout=60)
        if live.returncode == 3:
            raise AcceptError(f"a process of an earlier run of {product.id} is still running in the pod "
                              f"(pid {safe_tail(live, 40).strip()}): its directory is in use",
                              "Wait for it, or stop it with `kubectl exec`, then run again.")
        payload = base64.b64encode(product_tar(product.directory)).decode("ascii")
        res = self.exec(["sh", "-c", 'mkdir -p "$1" && base64 -d | tar -xf - -C "$1"', "sh", workdir],
                        input=payload, timeout=300)
        if not res.ok:
            raise AcceptError(f"copying {product.id} into {workdir} failed: {oneline(safe_tail(res, 400))}")

    def stage(self, product: Product, number: int) -> Observed:
        workdir = self.workdir(product)
        argv = env_prefix(self.env, self.run_id, self.allowlists) + ship_argv(number, self.site, self.env, product.contract)
        res = self.forge(argv, cwd=workdir, timeout=STAGE_TIMEOUT_S.get(number, DEFAULT_STAGE_TIMEOUT_S))
        report = self.exec(["cat", SHIP_REPORT], cwd=workdir, timeout=60)
        return observe_stage(number, res.returncode, safe_tail(res, 600), report.stdout if report.ok else None, argv,
                             timed_out=res.timed_out, run_id=self.run_id)

    def step(self, product: Product, step: Step) -> StepResult:
        label = step.label()
        if step.kind == "apply_build":
            argv = env_prefix(self.env, self.run_id, self.allowlists) + apply_build_argv(
                step.args["id"], self.env, product.contract)
            res = self.forge(argv, cwd=self.workdir(product), timeout=STAGE_TIMEOUT_S[accept.APPLY])
            return StepResult(label, res.ok, "build applied" if res.ok else oneline(safe_tail(res, 400)), argv)
        a = step.args
        if self.catalog is None:
            return StepResult(label, False, "no catalog client is available to wait on a table")
        deadline = self.clock() + a["timeout_s"]
        last = ""
        while True:
            try:
                if self.catalog.table_exists(a["warehouse"], a["namespace"], a["table"]):
                    return StepResult(label, True, "the table exists")
                last = "not found yet"
            except (AcceptError, http.HTTPError, kube.KubeError, openbao.BaoError, bootstrap.BootstrapError, OSError) as exc:
                last = oneline(exc, 200)
            if self.clock() >= deadline:
                return StepResult(label, False, f"{a['namespace']}.{a['table']} was not there after {a['timeout_s']} s ({last})")
            self.sleep(POLL_INTERVAL_S)

    def close(self) -> None:
        if self.catalog is not None and hasattr(self.catalog, "close"):
            self.catalog.close()
        if self._pod is None:
            return
        path = f"{WORK_ROOT}/{self.run_id}"
        if self.keep:
            self.notes.append(f"the product directories were kept in the pod, under {path} (--keep).")
            return
        res = self.exec(["rm", "-rf", "--", path], timeout=120)
        if not res.ok:
            self.notes.append(f"{path} could not be removed from the pod: {oneline(safe_tail(res, 200))}")


def cli_runner(root: Path, lab, run: Run, kubectl: kube.Kube, runner: proc.Runner,
               env_vars: Mapping[str, str], keep: bool = False) -> CliRunner:
    """The real runner for one run: operator allowlists from the profile, a catalog client for wait_table."""
    allowlists = accept.parse_operator_env(run.site.directory / "operator" / f"{run.env}.env")
    if not allowlists:
        run.notes.append(f"profiles/{run.site.profile}/operator/{run.env}.env is absent or empty: no operator "
                         "allowlists were passed to forge.")
    return CliRunner(kubectl, run.site, env=run.env, run_id=run.run_id, allowlists=allowlists,
                     catalog=LabCatalog(lab, kubectl, runner), keep=keep)
