# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab accept`: run forge's 11 stages on each example product and compare every result with the ledger.

The ledger (profiles/<profile>/expected/<tier>.yaml) holds one cell per
product x runner x stage. A run passes only if every observed status equals the
ledger, in both directions: an unexpected `passed` fails the run exactly as an
unexpected `failed` does, and a cell on one side only is a mismatch too.

This module is the engine. It knows the stages, the status mapping, the
comparison, the report and `--update-ledger`. How a stage is run belongs to a
runner (labctl/accept_runner.py: `cli`, which execs `fluid ship --stage N` in
the fluid-host pod). The `dag` runner comes with the orchestration tier.

Strict is not negotiable: nothing here passes `--no-strict`, and a run refuses
to start when VALIDATE_STRICT or VERIFY_STRICT is set to a false word.
"""

from __future__ import annotations

import datetime
import difflib
import hashlib
import importlib.util
import json
import os
import re
import secrets
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence

from labctl import config, kube, proc, state
from labctl.output import FAIL, PASS, check

STAGES = (
    (1, "bundle"), (2, "validate"), (3, "generate-artifacts"), (4, "validate-artifacts"), (5, "diff"),
    (6, "plan"), (7, "apply"), (8, "policy-apply"), (9, "verify"), (10, "publish"), (11, "schedule-sync"),
)
STAGE_NAMES = tuple(name for _, name in STAGES)
APPLY, POLICY_APPLY, PUBLISH, SCHEDULE_SYNC = 7, 8, 10, 11

DEFAULT_PROFILE = "lab-kind"
DEFAULT_ENV = "local"
RUNNERS_NOW = ("cli",)
RUNNERS_LATER = {
    "dag": "the dag runner runs the generated Airflow DAG, so it needs the orchestration tier (it is not available)",
}
REPORTS_DIR = "conformance/reports"
LEDGER_STATUSES = ("passed", "failed", "unsupported", "not_checked", "not_applicable", "pending")

# forge's ship statuses (fluid_build/cli/_ship_report.py: passed, failed, skipped, not-run) -> the ledger's.
# A skip is `unsupported` only when forge's own reason says the capability is absent on this platform;
# any other skip is `not_checked`. `skipped` is therefore resolved by `map_status`, not by this table.
# forge writes `not-run` (hyphen); `not_run` is accepted too.
SHIP_TO_LEDGER = {"passed": "passed", "failed": "failed", "not-run": "pending", "not_run": "pending"}
UNSUPPORTED_REASON = tuple(re.compile(p, re.I) for p in (
    r"\bnot supported\b", r"\bunsupported\b", r"\bnot available (?:on|for|in) (?:this|the) (?:platform|target|runtime)",
    r"\bno (?:such )?(?:target|provider|backend) (?:for|on|available)\b",
))
# What a ledger cell says when the harness itself decides (the reasonCode that `--update-ledger` writes).
DEFAULT_REASON_CODE = {
    "passed": "stage_passed", "failed": "stage_failed", "unsupported": "forge_skip_unsupported",
    "not_checked": "forge_skip", "pending": "stage_not_run", "not_applicable": "not_applicable",
}

_ID = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
_FALSE_WORDS = {"false", "0", "no", "off", "n", "f", "disabled"}
STRICT_VARS = ("VALIDATE_STRICT", "VERIFY_STRICT")
NO_STRICT_FLAGS = ("--no-strict", "--no-verify-strict")
_ALLOWLIST_KEY = re.compile(r"^FLUID_[A-Z0-9_]*_ALLOWLIST$")


class AcceptError(Exception):
    """The run cannot start, or could not be evaluated (exit 1)."""

    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


class LedgerRefused(AcceptError):
    """--update-ledger declined to write: the run is not a trustworthy result."""


class AcceptUsageError(AcceptError):
    """A request accept does not offer (exit 2)."""


# --------------------------------------------------------------------------- redaction

_REDACTIONS = (
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)", re.S), "***"),
    (re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{6,}"), r"\1 ***"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]*"), "***"),
    (re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), "***"),
    (re.compile(r"\bLAB[A-Z0-9]{17}\b"), "***"),
    (re.compile(r"(://[^/\s:@]*):[^/\s@]+@"), r"\1:***@"),
    (re.compile(r"(?i)\b([A-Za-z0-9_.-]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|"
                r"credential)[A-Za-z0-9_.-]*)([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;\"'}\]]+)"), r"\1\2***"),
)


def redact(text: object) -> str:
    """`text` with anything that looks like a secret replaced by `***` (reports are committed)."""
    out = str(text)
    for pattern, replacement in _REDACTIONS:
        out = pattern.sub(replacement, out)
    return out


def oneline(text: object, limit: int = 300) -> str:
    flat = " ".join(redact(text).split())
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."


# --------------------------------------------------------------------------- reasons

REASONED = ("failed", "unsupported", "not_checked")      # statuses whose cell carries forge's own words
_FP_MARK = re.compile(r"\[fp:([0-9a-f]{10})\]")
FP_WORDS = 16


def _key(text: object) -> str:
    """The first FP_WORDS alphabetic words of a reason, lower case: digits and punctuation drop out."""
    return " ".join(re.findall(r"[a-z]+", redact(text).lower())[:FP_WORDS])


def fingerprint(text: object) -> str:
    return hashlib.sha256(_key(text).encode()).hexdigest()[:10]


def reason_matches(ledger_reason: str, observed_reason: str) -> bool:
    """Does the ledger cell's reason stand for what forge said? A `[fp:<10 hex>]` marker must equal the
    fingerprint of forge's reason; without a marker, forge's first words must appear in the ledger's prose."""
    marked = _FP_MARK.search(ledger_reason or "")
    if marked:
        return marked.group(1) == fingerprint(observed_reason)
    key = _key(observed_reason)
    return bool(key) and f" {key} " in f" {_key_all(ledger_reason)} "


def _key_all(text: object) -> str:
    return " ".join(re.findall(r"[a-z]+", str(text).lower()))


# --------------------------------------------------------------------------- status mapping


def map_status(ship_status: str, skip_reason: Optional[str] = None) -> str:
    """forge's stage status -> the ledger's. Raises ValueError for a status forge does not write."""
    if ship_status == "skipped":
        return "unsupported" if any(p.search(skip_reason or "") for p in UNSUPPORTED_REASON) else "not_checked"
    if ship_status in SHIP_TO_LEDGER:
        return SHIP_TO_LEDGER[ship_status]
    raise ValueError(f"forge reported the stage status {ship_status!r}, which the lab has no mapping for")


# --------------------------------------------------------------------------- strict


def refuse_weakened_strict(env: Mapping[str, str], where: str = "the environment") -> None:
    """Never run with strict switched off: a false VALIDATE_STRICT or VERIFY_STRICT stops the run."""
    for var in STRICT_VARS:
        if str(env.get(var, "")).strip().lower() in _FALSE_WORDS:
            raise AcceptError(f"{var} is {env[var]!r} in {where}: lab accept always runs strict",
                              f"Unset {var}. There is no override.")


def strict_guard(argv: Sequence[str]) -> list:
    """`argv` unchanged, unless it carries a flag that weakens strict (then AcceptError)."""
    bad = [a for a in argv if a in NO_STRICT_FLAGS]
    if bad:
        raise AcceptError(f"refusing to run {bad[0]}: lab accept always runs strict")
    return list(argv)


# --------------------------------------------------------------------------- site, ledger, products


@dataclass(frozen=True)
class Site:
    profile: str
    tier: str
    directory: Path
    ledger_path: Path
    host_namespace: str           # fluid-host's namespace and Deployment (endpoints.spark.host)
    host_deployment: str
    publish_target: Optional[str]
    scheduler_kind: Optional[str]
    scheduler_destination: Optional[str]
    tiers: tuple = ()

    def absent(self, stage: int) -> Optional[str]:
        """Why a stage cannot run on this site at all (None when it can)."""
        if stage == PUBLISH and not self.publish_target:
            return "the site has no publish target (endpoints.publish.defaultTarget is null); the stage was not run"
        if stage == SCHEDULE_SYNC and not (self.scheduler_kind and self.scheduler_destination):
            return "the site has no scheduler (endpoints.scheduler); the stage was not run"
        return None


@dataclass(frozen=True)
class Step:
    kind: str                 # apply_build | wait_table
    args: dict

    def label(self) -> str:
        if self.kind == "apply_build":
            return f"apply_build {self.args['id']}"
        a = self.args
        return f"wait_table {a['warehouse']}/{a['namespace']}.{a['table']}"


@dataclass(frozen=True)
class Product:
    id: str
    directory: Path
    steps: tuple = ()
    contract: str = "contract.fluid.yaml"


def _l0_jsonschema(root: Path):
    """tools/l0/_jsonschema.py, the repository's own stdlib validator (borrowed, not rewritten)."""
    path = root / "tools" / "l0" / "_jsonschema.py"
    if not path.is_file():
        raise AcceptError(f"{path} not found: lab accept validates the profile with the L0 schema validator")
    spec = importlib.util.spec_from_file_location("_lab_l0_jsonschema", path)
    module = importlib.util.module_from_spec(spec)
    keep, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = keep
    return module


def schema_errors(root: Path, schema_file: str, doc: object) -> list:
    path = root / "profiles" / "schema" / schema_file
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AcceptError(f"{path}: {exc}") from exc
    return _l0_jsonschema(root).Validator(schema).errors(doc)


def _must_be_valid(root: Path, schema_file: str, doc: object, label: str) -> None:
    errors = schema_errors(root, schema_file, doc)
    if errors:
        raise AcceptError(f"{label} does not match {schema_file}: " + "; ".join(errors[:5])
                          + (f" (+{len(errors) - 5} more)" if len(errors) > 5 else ""))


def load_site(root: Path, profile: str, tier: Optional[str], runner: proc.Runner = proc.run) -> Site:
    if not _ID.match(profile or ""):
        raise AcceptUsageError(f"profile {profile!r} is not a profile name")
    directory = root / "profiles" / profile
    path = directory / "site.yaml"
    if not path.is_file():
        found = sorted(p.parent.name for p in (root / "profiles").glob("*/site.yaml"))
        raise AcceptUsageError(f"no profile {profile!r} (profiles/{profile}/site.yaml); profiles: {', '.join(found) or 'none'}")
    doc = config.load_yaml(path, runner)
    _must_be_valid(root, "site.schema.json", doc, f"profiles/{profile}/site.yaml")
    supported = (doc.get("tiers") or {}).get("supported") or {}
    tier = tier or (doc.get("tiers") or {}).get("default")
    if tier not in supported:
        raise AcceptUsageError(f"profile {profile} has no tier {tier!r}; it supports: {', '.join(supported)}")
    ledger_path = (directory / supported[tier]["expectations"]).resolve()
    if directory.resolve() not in ledger_path.parents:
        raise AcceptError(f"the ledger of tier {tier} is outside profiles/{profile}: {ledger_path}")
    ep = doc.get("endpoints") or {}
    host = str((ep.get("spark") or {}).get("host") or "")
    namespace, _, deployment = host.partition("/")
    if not (re.fullmatch(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?", namespace)
            and re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", deployment)):
        raise AcceptError(f"profiles/{profile}/site.yaml endpoints.spark.host is {host!r}: the cli runner needs "
                          "<namespace>/<deployment> of fluid-host")
    sched = ep.get("scheduler") or {}
    return Site(profile=profile, tier=tier, directory=directory, ledger_path=ledger_path,
                host_namespace=namespace, host_deployment=deployment,
                publish_target=(ep.get("publish") or {}).get("defaultTarget") or None,
                scheduler_kind=sched.get("kind"), scheduler_destination=sched.get("dagsDestination"),
                tiers=tuple(supported))


def load_ledger(root: Path, site: Site, runner: proc.Runner = proc.run) -> dict:
    doc = config.load_yaml(site.ledger_path, runner)
    where = site.ledger_path.relative_to(root.resolve()).as_posix() if root.resolve() in site.ledger_path.parents \
        else str(site.ledger_path)
    _must_be_valid(root, "expectations.schema.json", doc, where)
    if doc.get("profile") != site.profile or doc.get("tier") != site.tier:
        raise AcceptError(f"{where} is the ledger of {doc.get('profile')}/{doc.get('tier')}, "
                          f"not {site.profile}/{site.tier}")
    return doc


def parse_operator_env(path: Path) -> dict:
    """The operator's allowlists (profiles/<profile>/operator/<env>.env). Allowlists only, never a secret."""
    out: dict = {}
    if not path.is_file():
        return out
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not sep or not _ALLOWLIST_KEY.match(key):
            raise AcceptError(f"{path.name} line {number}: only FLUID_*_ALLOWLIST variables are accepted "
                              "(operator allowlists are passed to forge; secrets never are)")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if not value or re.search(r"[\s\x00-\x1f]", value):
            raise AcceptError(f"{path.name} line {number}: {key} must be one value without spaces")
        out[key] = value
    refuse_weakened_strict(out, path.name)
    return out


_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,126}$")
_BUILD = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
STEP_KINDS = ("apply_build", "wait_table")
WAIT_KEYS = ("warehouse", "namespace", "table", "timeout_s")


def parse_accept_yaml(doc: object, builds: Sequence[str], where: str = "accept.yaml") -> tuple:
    """Validate a product's accept.yaml strictly; the steps in order. AcceptError names the first fault."""
    def bad(msg: str) -> AcceptError:
        return AcceptError(f"{where}: {msg}")

    if not isinstance(doc, dict):
        raise bad("must be a mapping")
    extra = sorted(set(doc) - {"schemaVersion", "steps"})
    if extra:
        raise bad(f"unknown key {extra[0]!r} (allowed: schemaVersion, steps)")
    if doc.get("schemaVersion") != 1 or isinstance(doc.get("schemaVersion"), bool):
        raise bad("schemaVersion must be 1")
    steps = doc.get("steps")
    if not isinstance(steps, list) or not steps:
        raise bad("steps must be a non-empty list")
    out = []
    for n, item in enumerate(steps, 1):
        if not isinstance(item, dict) or len(item) != 1:
            raise bad(f"step {n} must be a mapping with exactly one key ({', '.join(STEP_KINDS)})")
        (kind, value), = item.items()
        if kind == "kafka_produce":
            raise bad(f"step {n}: kafka_produce is not available yet (it needs the streaming tier)")
        if kind not in STEP_KINDS:
            raise bad(f"step {n}: unknown step {kind!r} (allowed: {', '.join(STEP_KINDS)})")
        if kind == "apply_build":
            if not isinstance(value, str) or not _BUILD.match(value):
                raise bad(f"step {n}: apply_build takes a build id")
            if value not in builds:
                raise bad(f"step {n}: apply_build {value!r} is not a build of the contract ({', '.join(builds)})")
            out.append(Step(kind, {"id": value}))
            continue
        if not isinstance(value, dict):
            raise bad(f"step {n}: wait_table takes a mapping of {', '.join(WAIT_KEYS)}")
        missing = [k for k in WAIT_KEYS if k not in value]
        unknown = sorted(set(value) - set(WAIT_KEYS))
        if missing or unknown:
            raise bad(f"step {n}: wait_table " + (f"is missing {', '.join(missing)}" if missing else
                                                  f"has unknown key {unknown[0]!r}"))
        for key in ("warehouse", "namespace", "table"):
            if not isinstance(value[key], str) or not _NAME.match(value[key]):
                raise bad(f"step {n}: wait_table {key} {value[key]!r} is not a name")
        t = value["timeout_s"]
        if isinstance(t, bool) or not isinstance(t, int) or not 1 <= t <= 3600:
            raise bad(f"step {n}: wait_table timeout_s must be a whole number of seconds from 1 to 3600")
        out.append(Step(kind, {k: value[k] for k in WAIT_KEYS}))
    return tuple(out)


def load_products(root: Path, ids: Sequence[str], runner: proc.Runner = proc.run) -> list:
    products, missing = [], []
    for pid in ids:
        directory = root / "products" / pid
        contract = directory / "contract.fluid.yaml"
        if not contract.is_file():
            missing.append(pid)
            continue
        steps: tuple = ()
        hook = directory / "accept.yaml"
        if hook.is_file():
            builds = [str(b.get("id")) for b in config.load_yaml(contract, runner).get("builds") or []
                      if isinstance(b, dict)]
            steps = parse_accept_yaml(config.load_yaml(hook, runner), builds, f"products/{pid}/accept.yaml")
        products.append(Product(pid, directory, steps))
    if missing:
        raise AcceptError("the ledger lists products with no products/<id>/contract.fluid.yaml: " + ", ".join(missing),
                          "Add the product, or run only the ones that exist with --product.")
    return products


# --------------------------------------------------------------------------- observations


@dataclass
class Observed:
    """What one stage did, in the ledger's words."""

    status: str
    reason: str = ""
    reason_code: str = ""
    ship_status: str = ""
    exit_code: Optional[int] = None
    error: bool = False          # the harness could not observe the stage: never equal to the ledger
    argv: list = field(default_factory=list)

    def as_dict(self) -> dict:
        out = {"status": self.status, "reason": oneline(self.reason, 600), "shipStatus": self.ship_status or None,
               "exitCode": self.exit_code, "error": self.error, "argv": [redact(a) for a in self.argv]}
        if self.reason_code:
            out["reasonCode"] = self.reason_code
        return out


@dataclass
class StepResult:
    label: str
    ok: bool
    summary: str
    argv: list = field(default_factory=list)


def error_cell(reason: str, argv: Optional[list] = None) -> Observed:
    return Observed("failed", reason, "harness_error", error=True, argv=argv or [])


def not_run(reason: str, code: str = "stage_not_run", error: bool = False) -> Observed:
    """A stage that did not run. `error` when the cause was the harness: the cell is then not a result."""
    return Observed("pending", reason, code, error=error)


class Runner:
    """What a runner offers the engine. `cli` is the only one available (labctl/accept_runner.py)."""

    name = ""

    def version(self) -> str:  # pragma: no cover - interface
        raise NotImplementedError

    def prepare(self, product: Product) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def stage(self, product: Product, number: int) -> Observed:  # pragma: no cover - interface
        raise NotImplementedError

    def step(self, product: Product, step: Step) -> StepResult:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:
        pass


def run_product(runner: Runner, product: Product, site: Site, log: Callable[[str], None] = lambda _m: None) -> tuple:
    """Run the 11 stages and the product's steps: ({stage: Observed}, [StepResult], [error text]).

    Order: stages 1 to 7; then the accept.yaml steps (batch tables must exist before
    stage 8 can grant on them); then stages 8 to 11. The first failed stage, or failed
    step, stops the run like a CI job: later stages are `pending`. A stage the site cannot
    run (no publish target, no scheduler) is `pending` with reasonCode `addon_absent`.
    """
    observed: dict = {}
    results: list = []
    errors: list = []
    stopped = ""
    harness = False     # the stop was the harness's fault: the cells after it are not results either
    try:
        runner.prepare(product)
    except Exception as exc:  # the harness could not even start: every cell is unobserved
        why = f"could not prepare {product.id} for the {runner.name} runner: {oneline(exc)}"
        errors.append(why)
        stopped, harness = why, True
    for number, name in STAGES:
        absent = site.absent(number)
        if absent:
            observed[name] = not_run(absent, "addon_absent")
            continue
        if stopped:
            observed[name] = not_run(f"not run: {stopped}", "earlier_stage_failed", error=harness)
            continue
        log(f"lab accept: {product.id} stage {number} {name} ...")
        try:
            obs = runner.stage(product, number)
        except Exception as exc:  # a runner bug or a lost pod must not look like a stage result
            obs = error_cell(f"the {runner.name} runner failed on stage {number}: {oneline(exc)}")
        observed[name] = obs
        if obs.error:
            errors.append(f"{product.id}/{runner.name}/{name}: {oneline(obs.reason)}")
        if obs.status == "failed" or obs.error:
            stopped, harness = f"stage {number} {name} " + ("could not be observed" if obs.error else "failed"), obs.error
        if number == APPLY and obs.status == "passed" and product.steps and not stopped:
            for step in product.steps:
                log(f"lab accept: {product.id} step {step.label()} ...")
                try:
                    res = runner.step(product, step)
                except Exception as exc:
                    res = StepResult(step.label(), False, f"the {runner.name} runner failed: {oneline(exc)}")
                results.append(res)
                if not res.ok:
                    stopped, harness = f"accept.yaml step {step.label()} failed", True
                    errors.append(f"{product.id}/{runner.name}/step {step.label()}: {oneline(res.summary)}")
                    break
    return observed, results, errors


# --------------------------------------------------------------------------- comparison


def compare(product: str, runner: str, expected: Mapping[str, dict], observed: Mapping[str, Observed]) -> tuple:
    """Both directions: (cells {stage: dict}, mismatch lines).

    A cell on one side only is a mismatch; an unexpected `passed` is one exactly as an unexpected `failed` is;
    an observation the harness could not make never equals the ledger.
    """
    cells: dict = {}
    mismatches: list = []
    order = [s for s in STAGE_NAMES if s in expected or s in observed]
    order += sorted((set(expected) | set(observed)) - set(order))
    for stage in order:
        exp, obs = expected.get(stage), observed.get(stage)
        want = exp["status"] if exp else None
        got = obs.status if obs else None
        ok = exp is not None and obs is not None and not obs.error and want == got
        changed = ok and got in REASONED and not reason_matches(exp.get("reason", ""), obs.reason)
        ok = ok and not changed
        cells[stage] = {"expected": ({"status": want, "reasonCode": exp.get("reasonCode")} if exp else None),
                        "observed": obs.as_dict() if obs else None, "match": ok}
        if changed:
            mismatches.append(f"{product}/{runner}/{stage}: expected {want}, observed {got} (reason changed: forge said "
                              f"\"{oneline(obs.reason, 160)}\", which the ledger reason neither quotes nor marks "
                              f"[fp:{fingerprint(obs.reason)}])")
        elif not ok:
            why = oneline(obs.reason) if obs and obs.reason else ("no result" if obs is None else "no reason given")
            seen = "(nothing)" if obs is None else (f"error, not observed [{got}]" if obs.error else got)
            mismatches.append(f"{product}/{runner}/{stage}: expected {want or '(no cell)'}, observed {seen} ({why})")
    return cells, mismatches


# --------------------------------------------------------------------------- run


@dataclass
class Run:
    run_id: str
    date: str
    site: Site
    ledger: dict
    products: list
    runners: list
    env: str
    cells: dict = field(default_factory=dict)          # (product, runner) -> {stage: cell dict}
    steps: dict = field(default_factory=dict)          # (product, runner) -> [StepResult]
    mismatches: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    observed: dict = field(default_factory=dict)       # (product, runner) -> {stage: Observed}
    forge: dict = field(default_factory=dict)          # runner -> `fluid --version` text
    notes: list = field(default_factory=list)
    commit: str = "unknown"
    dirty: bool = False

    @property
    def ok(self) -> bool:
        return not self.mismatches and not self.errors


def new_run_id(now: datetime.datetime) -> str:
    return f"acc-{now:%Y%m%d%H%M%S}-{secrets.token_hex(3)}"


def git_state(root: Path, runner: proc.Runner) -> tuple:
    head = runner(["git", "-C", str(root), "rev-parse", "HEAD"], timeout=30)
    if not head.ok or not re.fullmatch(r"[0-9a-f]{40}", head.stdout.strip()):
        return "unknown", False
    status = runner(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"], timeout=30)
    return head.stdout.strip(), bool(status.ok and status.stdout.strip())


def check_runner_request(name: str) -> None:
    if name in RUNNERS_LATER:
        raise AcceptUsageError(f"the {name} runner is not available yet: {RUNNERS_LATER[name]}. "
                               f"Use --runner {RUNNERS_NOW[0]}.")
    if name not in RUNNERS_NOW:
        raise AcceptUsageError(f"unknown runner {name!r}; runners: {', '.join(RUNNERS_NOW + tuple(RUNNERS_LATER))}")


def prepare_run(root: Path, *, profile: str, tier: Optional[str], product_ids: Sequence[str], runner_names: Sequence[str],
                env: str, env_vars: Mapping[str, str], now: datetime.datetime, run_id: Optional[str] = None,
                proc_runner: proc.Runner = proc.run) -> Run:
    """Everything that can be refused before the first exec: nothing here touches the cluster."""
    for name in runner_names:
        check_runner_request(name)
    if not _ID.match(env or ""):
        raise AcceptUsageError(f"--env {env!r} is not an environment name")
    refuse_weakened_strict(env_vars)
    site = load_site(root, profile, tier, proc_runner)
    ledger = load_ledger(root, site, proc_runner)
    declared = ledger.get("runners") or []
    for name in runner_names:
        if name not in declared:
            raise AcceptUsageError(f"the ledger of {site.profile}/{site.tier} declares the runners "
                                   f"{', '.join(declared)}, not {name}")
    ledger_products = list(ledger["products"])
    product_ids = list(dict.fromkeys(product_ids))
    unknown = [p for p in product_ids if p not in ledger_products]
    if unknown:
        raise AcceptUsageError(f"the ledger of {site.profile}/{site.tier} has no product {unknown[0]!r}; "
                               f"products: {', '.join(ledger_products)}")
    products = load_products(root, list(product_ids) or ledger_products, proc_runner)
    commit, dirty = git_state(root, proc_runner)
    run = Run(run_id=run_id or new_run_id(now), date=f"{now:%Y-%m-%d}", site=site, ledger=ledger, products=products,
              runners=list(runner_names), env=env, commit=commit, dirty=dirty)
    if not _ID.match(run.run_id):
        raise AcceptError(f"run id {run.run_id!r} is not usable in a path")
    for name, why in RUNNERS_LATER.items():
        if name in declared and name not in runner_names:
            run.notes.append(f"runner {name} was not run, so its ledger cells were not compared: {why}.")
    for stage in (PUBLISH, SCHEDULE_SYNC):
        absent = site.absent(stage)
        if absent:
            run.notes.append(f"stage {stage} {STAGE_NAMES[stage - 1]}: {absent}; recorded as pending "
                             "(reasonCode addon_absent).")
    return run


def execute(run: Run, runners: Mapping[str, Runner], log: Callable[[str], None] = lambda _m: None) -> Run:
    """Run every selected product on every selected runner and compare with the ledger."""
    for rname in run.runners:
        runner = runners[rname]
        try:
            run.forge[rname] = oneline(runner.version(), 200)
        except Exception as exc:
            run.forge[rname] = "unknown"
            run.errors.append(f"{rname}: could not read `fluid --version`: {oneline(exc)}")
        for product in run.products:
            observed, steps, errors = run_product(runner, product, run.site, log)
            expected = run.ledger["products"][product.id]["runners"][rname]
            cells, mismatches = compare(product.id, rname, expected, observed)
            key = (product.id, rname)
            run.observed[key], run.steps[key], run.cells[key] = observed, steps, cells
            run.mismatches += mismatches
            run.errors += errors
    return run


# --------------------------------------------------------------------------- report


def report_stem(run: Run) -> str:
    return f"{run.date}-{run.site.profile}-{run.site.tier}-{run.run_id}"


def report_doc(run: Run) -> dict:
    products: dict = {}
    for (pid, rname), cells in run.cells.items():
        products.setdefault(pid, {})[rname] = {
            "cells": cells,
            "steps": [{"step": s.label, "ok": s.ok, "summary": oneline(s.summary, 600), "argv": [redact(a) for a in s.argv]}
                      for s in run.steps[(pid, rname)]],
        }
    return {
        "schemaVersion": 1, "runId": run.run_id, "date": run.date, "ok": run.ok,
        "profile": run.site.profile, "tier": run.site.tier, "env": run.env, "runners": run.runners,
        "lab": {"commit": run.commit, "dirty": run.dirty},
        "forge": {"fluidVersion": dict(run.forge)},
        "strict": True,
        "mismatches": [oneline(m, 700) for m in run.mismatches],
        "errors": [oneline(e, 700) for e in run.errors],
        "notes": list(run.notes),
        "products": products,
    }


def report_markdown(doc: dict) -> str:
    out = [f"# lab accept {doc['runId']}", "",
           f"- Result: **{'MATCH' if doc['ok'] else 'MISMATCH'}** ({len(doc['mismatches'])} mismatches, "
           f"{len(doc['errors'])} errors)",
           f"- Profile and tier: {doc['profile']} / {doc['tier']}, env {doc['env']}, runners {', '.join(doc['runners'])}, "
           "strict on",
           f"- Date: {doc['date']}; lab commit {doc['lab']['commit']}{' (dirty tree)' if doc['lab']['dirty'] else ''}",
           "- forge: " + "; ".join(f"{r}: {v}" for r, v in doc["forge"]["fluidVersion"].items()), ""]
    for pid, runners in doc["products"].items():
        for rname, body in runners.items():
            out += [f"## {pid} / {rname}", "", "| stage | expected | observed | match | forge's reason |", "|---|---|---|---|---|"]
            for stage, cell in body["cells"].items():
                exp = (cell["expected"] or {}).get("status", "(no cell)")
                obs = cell["observed"] or {}
                reason = str(obs.get("reason") or "").replace("|", "/")
                out.append(f"| {stage} | {exp} | {obs.get('status', '(nothing)')}{' (error)' if obs.get('error') else ''} | "
                           f"{'yes' if cell['match'] else '**NO**'} | {reason[:160]} |")
            out.append("")
            if body["steps"]:
                out += ["Steps between stage 7 and stage 8:", ""]
                out += [f"- {'ok' if s['ok'] else 'FAILED'}: {s['step']}: {s['summary'][:200]}" for s in body["steps"]]
                out.append("")
            out += ["Exact argv per stage (after `cd <workdir>`; operator allowlists only, no secrets):", "", "```"]
            for stage, cell in body["cells"].items():
                argv = (cell["observed"] or {}).get("argv")
                if argv:
                    out.append(f"{stage}: " + " ".join(argv))
            out += ["```", ""]
    if doc["mismatches"]:
        out += ["## Mismatches", ""] + [f"- {m}" for m in doc["mismatches"]] + [""]
    if doc["errors"]:
        out += ["## Errors (the harness could not observe)", ""] + [f"- {e}" for e in doc["errors"]] + [""]
    if doc["notes"]:
        out += ["## Notes", ""] + [f"- {n}" for n in doc["notes"]] + [""]
    return "\n".join(out)


def write_report(root: Path, run: Run) -> tuple:
    """conformance/reports/<date>-<profile>-<tier>-<run-id>.json and .md. Never overwrites."""
    directory = root / REPORTS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    doc = report_doc(run)
    paths = []
    for suffix, text in ((".json", json.dumps(doc, indent=2) + "\n"), (".md", report_markdown(doc))):
        path = directory / (report_stem(run) + suffix)
        try:
            with open(path, "x", encoding="utf-8") as handle:
                handle.write(text)
        except FileExistsError as exc:
            raise AcceptError(f"{path} already exists; a report is evidence and is never overwritten") from exc
        paths.append(path)
    return paths[0], paths[1]


# --------------------------------------------------------------------------- --update-ledger

def split_flow_cell(line: str) -> Optional[tuple]:
    """(indent, stage, trailing comment) of `<indent><stage>: {...}  # comment`, braces and quotes aware; else None."""
    m = re.match(r"^(\s*)([a-z][a-z-]*):\s*\{", line)
    if not m:
        return None
    depth, quote, i = 0, "", m.end() - 1
    while i < len(line):
        c = line[i]
        if quote:
            if c == "\\" and quote == '"':
                i += 1
            elif c == quote:
                quote = ""
        elif c in "\"'":
            quote = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                tail = line[i + 1:]
                if tail.strip() and not tail.lstrip().startswith("#"):
                    return None
                return m.group(1), m.group(2), tail.rstrip()
        i += 1
    return None


_KEY_LINE = re.compile(r"^(?P<indent>\s*)(?P<key>[A-Za-z0-9_-]+):(?:\s|$)")


def _flow_cell(cell: dict) -> str:
    def q(text: str) -> str:
        return json.dumps(text, ensure_ascii=False)

    parts = [f"status: {cell['status']}", f"reasonCode: {cell['reasonCode']}", f"reason: {q(cell['reason'])}"]
    if cell.get("capability"):
        parts.append(f"capability: {cell['capability']}")
    if cell.get("evidence"):
        ev = cell["evidence"]
        parts.append(f"evidence: {{run: {ev['run']}, date: \"{ev['date']}\", report: {ev['report']}}}")
    return "{" + ", ".join(parts) + "}"


def new_cell(old: dict, obs: Observed, run: Run, report_rel: str, runner: str) -> dict:
    body = (f"Observed on {run.run_id} with {run.forge.get(runner, 'forge version unknown')}: "
            + (oneline(obs.reason, 400) or "the stage passed"))[:850]
    if obs.status in REASONED:
        body += f" [fp:{fingerprint(obs.reason)}]"
    cell = {"status": obs.status, "reasonCode": obs.reason_code or DEFAULT_REASON_CODE[obs.status], "reason": body}
    if old.get("capability"):
        cell["capability"] = old["capability"]
    if obs.status == "passed":
        cell["evidence"] = {"run": run.run_id, "date": run.date, "report": report_rel}
    return cell


def ledger_changes(run: Run) -> list:
    """[(product, runner, stage, old cell, Observed)]: cells whose status differs, or whose forge reason changed."""
    changes = []
    for (pid, rname), observed in run.observed.items():
        expected = run.ledger["products"][pid]["runners"][rname]
        for stage, obs in observed.items():
            old = expected.get(stage)
            if old is None:
                continue
            same = old["status"] == obs.status
            if same and not (obs.status in REASONED and not reason_matches(old.get("reason", ""), obs.reason)):
                continue
            changes.append((pid, rname, stage, old, obs))
    return changes


def edit_ledger_text(text: str, replacements: Mapping[tuple, str]) -> str:
    """Replace single-line flow cells `<stage>: {...}` at products.<p>.runners.<r>.<stage`; nothing else moves."""
    lines = text.split("\n")
    stack: list = []
    done: set = set()
    for i, line in enumerate(lines):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = _KEY_LINE.match(line)
        if not m:
            continue
        indent = len(m.group("indent"))
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = tuple(k for _, k in stack) + (m.group("key"),)
        if len(path) == 5 and path[0] == "products" and path[2] == "runners" and (path[1], path[3], path[4]) in replacements:
            cell = split_flow_cell(line)
            if not cell:
                raise AcceptError(f"the ledger cell {path[1]}/{path[3]}/{path[4]} is not a one-line flow mapping, "
                                  "so --update-ledger cannot edit it safely; edit it by hand")
            key = (path[1], path[3], path[4])
            lines[i] = f"{cell[0]}{cell[1]}: {replacements[key]}{cell[2]}"
            done.add(key)
        stack.append((indent, m.group("key")))
    missing = set(replacements) - done
    if missing:
        raise AcceptError("ledger cell(s) not found in the file: " + ", ".join("/".join(k) for k in sorted(missing)))
    return "\n".join(lines)


def update_ledger(root: Path, run: Run, report_rel: str, proc_runner: proc.Runner = proc.run) -> tuple:
    """Write every cell whose status differs, or whose forge reason changed. Returns (diff text, number of cells).

    Refused (AcceptError, nothing written) when the run had any error: a cell the harness could not observe, and the
    cells that follow it, are not results.

    A minimal text edit: comments, order and every untouched line stay as they were. The edited
    file is parsed again, must equal the old ledger plus exactly these cells, and must pass the
    schema, or nothing is written.
    """
    if run.errors or any(o.error for cells in run.observed.values() for o in cells.values()):
        raise LedgerRefused(f"the run had {len(run.errors)} error(s): cells the harness could not observe, or that depend on "
                          "one, are not results, so the ledger was not touched", "Fix the cause, then run again.")
    changes = ledger_changes(run)
    if not changes:
        return "", 0
    path = run.site.ledger_path
    old_text = path.read_text(encoding="utf-8")
    cells = {(p, r, s): new_cell(old, obs, run, report_rel, r) for p, r, s, old, obs in changes}
    new_text = edit_ledger_text(old_text, {k: _flow_cell(v) for k, v in cells.items()})
    with tempfile.TemporaryDirectory() as tmp:
        probe = Path(tmp) / path.name
        probe.write_text(new_text, encoding="utf-8")
        new_doc = config.load_yaml(probe, proc_runner)
    want = json.loads(json.dumps(run.ledger))
    for (p, r, s), cell in cells.items():
        want["products"][p]["runners"][r][s] = cell
    if new_doc != want:
        raise AcceptError("the edited ledger does not equal the old ledger plus the observed cells; nothing was written")
    errors = schema_errors(root, "expectations.schema.json", new_doc)
    if errors:
        raise AcceptError("the edited ledger does not match expectations.schema.json (" + "; ".join(errors[:3])
                          + "); nothing was written")
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(new_text, encoding="utf-8")
    os.replace(tmp_path, path)
    diff = "".join(difflib.unified_diff(old_text.splitlines(True), new_text.splitlines(True),
                                        f"a/{path.name}", f"b/{path.name}", n=0))
    return diff, len(cells)


# --------------------------------------------------------------------------- command


def run_accept(root: Path, *, name: str = state.DEFAULT_NAME, profile: str = DEFAULT_PROFILE, tier: Optional[str] = None,
               products: Sequence[str] = (), runner: str = "cli", env: str = DEFAULT_ENV, do_update_ledger: bool = False,
               env_vars: Optional[Mapping[str, str]] = None, proc_runner: proc.Runner = proc.run,
               keep: bool = False, runner_factory: Optional[Callable[[Run], Runner]] = None,
               now: Optional[Callable[[], datetime.datetime]] = None, run_id: Optional[str] = None,
               log: Callable[[str], None] = lambda _m: None) -> dict:
    """The whole command. Raises AcceptUsageError for a request it refuses; AcceptError is a failed run."""
    env_vars = os.environ if env_vars is None else env_vars
    moment = (now or (lambda: datetime.datetime.now(datetime.timezone.utc)))()
    root = Path(root)
    run = prepare_run(root, profile=profile, tier=tier, product_ids=products, runner_names=[runner], env=env,
                      env_vars=env_vars, now=moment, run_id=run_id, proc_runner=proc_runner)
    if runner_factory is None:
        from labctl import accept_runner

        lab = state.lab_for(root, name, env_vars)
        if not lab.kubeconfig.is_file():
            raise AcceptError(f"lab {lab.name!r} has no kubeconfig at {lab.kubeconfig}",
                              f"Run `./lab up --name {lab.name}` first (the fluid-host pod must be running).")

        def runner_factory(r: Run) -> Runner:
            return accept_runner.cli_runner(root, lab, r, kube.Kube(lab, proc_runner), proc_runner, env_vars, keep=keep)

    runners = {runner: runner_factory(run)}
    try:
        execute(run, runners, log)
    finally:
        for r in runners.values():
            r.close()
            run.notes += list(getattr(r, "notes", []))
    json_path, md_path = write_report(root, run)
    rel = json_path.relative_to(root).as_posix()
    checks = []
    for (pid, rname), cells in run.cells.items():
        for stage, cell in cells.items():
            label = f"{pid}/{rname}/{stage}"
            obs = cell["observed"] or {}
            if cell["match"]:
                checks.append(check(label, PASS, f"observed {obs['status']}, as the ledger expects"))
            else:
                line = next(m for m in run.mismatches if m.startswith(label + ":"))
                checks.append(check(label, FAIL, line[len(label) + 2:]))
    for err in run.errors:
        if not any(err.startswith(c["id"]) for c in checks if c["status"] == FAIL):
            checks.append(check("accept", FAIL, err))
    doc = {"command": "accept", "ok": run.ok, "run_id": run.run_id, "profile": run.site.profile, "tier": run.site.tier,
           "report": rel, "report_md": md_path.relative_to(root).as_posix(), "checks": checks,
           "mismatches": run.mismatches, "notes": run.notes}
    if do_update_ledger:
        led = {"path": run.site.ledger_path.relative_to(root.resolve()).as_posix()
               if root.resolve() in run.site.ledger_path.parents else str(run.site.ledger_path),
               "cells_updated": 0, "diff": ""}
        try:
            led["diff"], led["cells_updated"] = update_ledger(root, run, rel, proc_runner)
        except LedgerRefused as exc:
            led["refused"] = str(exc)
        doc["ledger"] = led
    return doc
