<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Acceptance: `lab accept`

`lab accept` runs forge's eleven `fluid ship` stages on each example product and compares what it
observed with the profile's expectations ledger. It has not run on a live
lab yet: its first run needs the parts listed under
[What the first live run needs](#what-the-first-live-run-needs).

```sh
./lab accept --name fluid-fabric-lab                       # profile lab-kind, its default tier, runner cli, env local
./lab accept --product events-pipeline --json              # one product, machine-readable
./lab accept --update-ledger                               # also write the observed statuses into the ledger
```

| Flag | Meaning |
|---|---|
| `--name` | the lab whose `fluid-host` pod runs forge (default `fluid-fabric-lab`) |
| `--profile` | the profile under `profiles/` (default `lab-kind`, the profile of the kind lab) |
| `--tier` | a tier of that profile's `site.yaml` (default: its `tiers.default`) |
| `--product ID` | only this product of the ledger (repeatable; default: all of them) |
| `--runner` | `cli` only; `dag` is refused with exit 2 |
| `--env` | forge's environment of the contracts (default `local`) |
| `--keep` | leave `/work/accept/<run-id>` in the pod afterwards (default: remove it when the run ends) |
| `--update-ledger` | write each observed status or forge reason that differs into the ledger (below); refused when the run had any error |
| `--json` | the result as JSON |

Exit codes: 0 when every compared cell equals the ledger; 1 on a mismatch, or when the run could not
observe a cell, or could not start; 2 for a request it does not offer (the `dag`
runner, an unknown product or tier).

The ledgers of `lab-kind` (`profiles/lab-kind/expected/`) list the lab's two authored products, `events-pipeline` and
`stream-batch`, and four products that are not authored (`batch-dbt-trino` and three others). Every cell
is `pending`. Without `--product`, `lab accept` refuses the run (exit 1) and names the products that have no
`products/<id>/contract.fluid.yaml`: run `./lab accept --product events-pipeline` until they exist. The default tier of
`lab-kind` is `core`, which has no `fluid-host` pod, so the run also needs a lab that has the spark tier.

## What it runs

For each product and each runner, in this order:

1. **Stages 1 to 7**: `bundle`, `validate`, `generate-artifacts`, `validate-artifacts`, `diff`, `plan`, `apply`.
2. **The product's `accept.yaml` steps**, if it has one (below).
3. **Stages 8 to 11**: `policy-apply`, `verify`, `publish`, `schedule-sync`.

The order has a reason. Stage 7 runs in apply mode `amend`, which defines the stream and the tables but
runs no batch, so the batch tables do not exist yet. Stage 8 grants on tables, so they must exist first.
The `accept.yaml` steps run each build once, as the generated Airflow DAG's single task does
(`fluid apply ... --mode amend-and-build --build-id <id> --yes`), and wait until the catalog lists its table.

The first stage that fails stops the product, like a CI job: the stages after it are recorded as `pending`
(reasonCode `earlier_stage_failed`). A step that fails stops it too, and counts as an error of the run
whatever the ledger says (a step is not a ledger cell, so a failed one must not hide behind a `pending`).
When the stop was the harness's doing (a stage it could not observe, a failed step, a failed copy into the pod), the
cells after it are error cells too, not `pending` results: they were never run, so they say nothing.

A stage the site cannot run at all is not run. With `endpoints.publish.defaultTarget: null` there is no
stage 10; with no `endpoints.scheduler` there is no stage 11. Each is recorded as `pending` with the
reasonCode `addon_absent` (the one the ledger uses for the same case), and the report says so in its notes.

## The `cli` runner

The runner runs inside the `fluid-host` pod of the spark tier. Its coordinates are the site's
`endpoints.spark.host`, `<namespace>/<deployment>` (`spark/fluid-host` for lab-kind). It finds the pod from
the Deployment's selector and uses `kubectl exec` through `labctl/kube.py`, against the lab's own
kubeconfig.

- **Copy.** The product directory goes in as a tar stream on exec stdin (base64-wrapped, because
  `labctl.proc` passes text and a tar is binary) under `/work/accept/<run-id>/<product>`. `runtime/`,
  `dist/`, `.fluid/`, `.git/` and caches are left out: they are forge's output, not input. A symbolic link
  is refused, and a file's exec bit is kept. A run refuses to start while a process of an earlier run of the same
  product still has its directory as working directory. The run's directory is removed when the run ends (also
  after a failure), unless `--keep` is given; the report notes a kept directory.
- **One exec per stage**: `fluid ship contract.fluid.yaml --env <env> --strict --verify-strict --stage N`,
  in the product's directory, as CI runs one stage per step. Stage 7 adds `--yes`. Stage 10 adds
  `--publish --target <the site's publish target>`. Stage 11 adds
  `--schedule-sync --scheduler <kind> --scheduler-destination <dags root>`, both from the site profile.
- **The environment** of each forge exec is `FLUID_ENV`, `FLUID_RUN_ID`, `VALIDATE_STRICT=true`,
  `VERIFY_STRICT=true` and the operator's allowlists from `profiles/<profile>/operator/<env>.env`. That file
  may hold `FLUID_*_ALLOWLIST` variables only; anything else is refused. Nothing is taken from the
  launcher's own environment, so no secret can reach an argv.
- **The pod's environment is cleaned.** forge reads variables that weaken a stage (`POLICY_APPLY_MODE`,
  `APPLY_MODE`, `ALLOW_DATA_LOSS`, `NO_VERIFY_DIGEST`, `DIFF_EXIT_ON_DRIFT`, `SCHEDULE_SYNC_DRY_RUN`,
  `RUN_STAGE_<N>_<NAME>`, another operator's allowlists), and a pod may carry them. Each forge exec unsets, by
  name, every variable of the pod except an allow set: `PATH`, `HOME`, `TMPDIR`, `TZ`, `LANG`, `LC_*`, the CA
  bundle variables, `DBT_PROFILES_DIR`, `JAVA_HOME`, `SPARK_HOME`, `KUBERNETES_SERVICE_*`, and the lab's own
  `FLUID_*` (but never a `FLUID_*_ALLOWLIST`: those come from the operator's file only), `AWS_*` and `LAKEKEEPER_*`
  (`KEEP_NAMES` and `KEEP_PREFIXES` in `labctl/accept_runner.py`). Values are never put in an argv. Whether
  fluid-host needs another name is not known until a live run.
- **A deadline in the pod.** Each forge exec runs under `timeout` (a watchdog loop where the image has none), 30 s
  before the launcher gives up, so a stage the launcher abandoned does not keep running. Exit 124 is a timeout:
  an error cell.
- **Read back.** After each stage the runner reads `runtime/ship-report.json`, which forge merges by
  `FLUID_RUN_ID`, and takes the stage's record. A missing report, a missing or misnamed record, an exit code
  that contradicts the record, or a timeout is an **error** cell: the harness could not observe the stage, so
  the cell never equals the ledger, whatever the ledger says. So are a record that says `not-run` for a stage
  that was asked for with `--stage N` (forge exits 0 without a report when `RUN_STAGE_<N>_*` is false), and a
  report whose `run_id` is not this run's `FLUID_RUN_ID` (a stale report).

## `accept.yaml`

A product may carry `products/<product>/accept.yaml`. It is validated strictly in code
(`labctl/accept.py`, `parse_accept_yaml`); any fault stops the run before anything is executed.

```yaml
schemaVersion: 1                 # required, must be 1
steps:                           # required, not empty; run in order, between stage 7 and stage 8
  - apply_build: daily-curated   # a build id of the contract
  - wait_table: {warehouse: sales-wh, namespace: events_curated, table: events_daily, timeout_s: 900}
```

| Step | Does | Rules |
|---|---|---|
| `apply_build: <id>` | `fluid apply contract.fluid.yaml --env <env> --mode amend-and-build --build-id <id> --yes` in `fluid-host` | `<id>` must be a build of the contract |
| `wait_table: {warehouse, namespace, table, timeout_s}` | polls Lakekeeper's REST catalog (`GET .../namespaces/<ns>/tables/<table>`) every 5 s until it answers 200, as lab-admin, through the lab client (`labctl.bootstrap.LabClient`, the way `lab check` reaches the Gateway) | all four keys required; `timeout_s` a whole number, 1 to 3600 |

Unknown keys and unknown steps are refused. `kafka_produce` (put test rows on the Kafka topic) is not
available: it needs the streaming tier (lab #8). The `events-pipeline` file applies `daily-curated`, waits
for `events_curated.events_daily`, applies `hourly-marts`, waits for `events_marts.events_hourly`; the stream
has no step because stage 7 keeps it running.

## Status mapping

forge writes one record per stage in `ship-report.json` with a `status`. The ledger has its own words.
The mapping is explicit (`labctl/accept.py`, `map_status`) and tested:

| forge's ship status | Ledger status |
|---|---|
| `passed` | `passed` |
| `failed` | `failed` |
| `skipped`, and the skip reason says the capability is absent on this platform (it contains "not supported", "unsupported", "not available on this platform" or "no target for ...") | `unsupported` |
| `skipped`, any other reason | `not_checked`, carrying forge's reason |
| `not-run` (forge's spelling; `not_run` is read too) | `pending` |
| anything else | an error cell: the lab never guesses a status |

A skip is never a pass. forge's own skip reasons today ("SCHEDULER is blank", "... does not exist (no policy
bindings to apply)") say a stage had nothing to do, not that the platform lacks it, so they map to
`not_checked`; the `unsupported` patterns are a short list in `UNSUPPORTED_REASON` that a live run may need to extend. `not_applicable` is never observed, only written by hand in the ledger.

## The comparison

A run passes only if every observed status equals the ledger, in both directions. For every
product, runner and stage of the selected products and runners:

- an observed status that differs from the ledger's is a mismatch, whichever way: an unexpected `passed`
  fails the run exactly as an unexpected `failed` does;
- a cell the ledger has and the run did not observe (or the reverse) is a mismatch;
- an error cell is a mismatch.

### The reason rule

A `failed`, `unsupported` or `not_checked` cell also names a reason, and the status alone would let a ledger cell
that says "fails because runtime local is unreleased" match any later failure. So when the statuses agree and the
status is one of those three, forge's reason must match the ledger cell's `reason`:

1. **Fingerprint.** If the ledger reason contains `[fp:<10 hex digits>]`, it must equal the fingerprint of forge's
   reason: the SHA-256 of its first 16 alphabetic words, lower case (digits, punctuation and case drop out, so
   timings and ids do not matter). `--update-ledger` writes this marker.
2. **Quotation.** Without a marker, forge's first 16 alphabetic words must appear, in order, in the ledger reason's
   prose.

Otherwise the cell is a mismatch, `expected failed, observed failed (reason changed: forge said "...", which the
ledger reason neither quotes nor marks [fp:...])`. A hand-written reason that only paraphrases forge therefore fails
its first live run; `--update-ledger` rewrites that cell with the observed reason and the marker. `passed` and
`pending` cells are compared by status only.

Every mismatch is one line, `product/runner/stage: expected X, observed Y (reason)`. The ledger's other runner
is not compared when it was not run: the orchestration and full ledgers of `lab-kind` declare `dag`, which needs the orchestration tier, so a `--runner cli` run says in the report that the `dag` cells were not compared. The
`dag` runner exits 2.

## The report

Every run writes `conformance/reports/<YYYY-MM-DD>-<profile>-<tier>-<run-id>.json` and a short `.md`
beside it, whatever the result ([conformance/README.md](../conformance/README.md): they are evidence and are
committed on purpose). The run id (`acc-<UTC time>-<random>`) is also the `FLUID_RUN_ID`. A report holds:

- the run id, the date, the profile, tier, env and runners, and `strict: true`;
- forge's `fluid --version` as the runner saw it, and the git commit of the lab (and whether the tree was dirty);
- every observed cell with forge's own reason, its ship status and exit code, next to the ledger's cell;
- the comparison: each mismatch as `product/runner/stage: expected X, observed Y (reason)`, the errors, the steps;
- the exact argv of each stage (operator allowlists and flags; no secret ever goes into an argv) and the notes
  (stages not run because the site has no publish target or scheduler, runners not compared).

Everything passes through `redact` first, and redaction comes before any truncation: bearer tokens, JWTs,
`password=`/`token=`/`secret=` pairs in shell form and in JSON form (`{"password": "..."}`), AWS and lab
access keys, URL credentials and PEM private keys become `***`. A report is created exclusively and never
overwritten.

## `--update-ledger`

After a run, `--update-ledger` writes the observed status of every cell that **differs** from the ledger into
`profiles/<profile>/expected/<tier>.yaml`:

- a `passed` cell gets `evidence: {run, date, report}` pointing at this run's report; any other cell gets the
  observed reason, with no evidence;
- the reasonCode is the harness's (`stage_passed`, `stage_failed`, `forge_skip_unsupported`, `forge_skip`,
  `stage_not_run`, `addon_absent`, `earlier_stage_failed`), and a cell's `capability` is kept;
- cells that already agree (status, and for `failed`, `unsupported` and `not_checked` the reason rule above) are not
  touched, nor are the other runner's cells;
- **a run with any error never updates the ledger**: a cell the harness could not observe, and the cells after
  it, are not results. The output says the ledger was not touched, and the file does not change;
- the edit is a minimal text edit of the one-line `stage: {...}` cells, so comments, order and every other line
  stay as they were. The result is parsed again, must equal the old ledger plus exactly these cells and must
  pass `expectations.schema.json`; otherwise nothing is written. A cell that is not a one-line mapping stops it.

It prints the diff. `lab accept` never commits: changing a cell needs a commit that names the capability
change behind it (the ledger's own rule), and that is yours to write after reading the diff. Updating the
ledger from a run that was meant to *check* it defeats the check, so do it deliberately, once a result is
understood.

## Strict, with no override

Every `fluid ship` stage runs with `--strict` and `--verify-strict`, and the exec's environment sets
`VALIDATE_STRICT=true` and `VERIFY_STRICT=true`. The code never passes `--no-strict` or `--no-verify-strict`
(an argv with either is refused before it is sent), and the run does not start when `VALIDATE_STRICT` or
`VERIFY_STRICT` is set to a false word (`false`, `0`, `no`, `off`) in the launcher's environment or in the
operator's file. There is no flag to relax it.

## What the first live run needs

The first live run is not done ([proven.md](proven.md#not-proven-yet)). It needs:

- **`fluid-host`**: a Deployment `spark/fluid-host` with forge on a forge release that has
  `runtime: local` (forge 0.33.0 and later), the operator's `lab-spark-submit`, `/work` writable, and `base64` and `tar`.
- **The Kafka fixture**: the stream `stream-raw` reads topic
  `events.source`, and a `kafka_produce` step to put rows on it does not exist.
- **A Trino JWT for dbt-trino**: `hourly-marts` runs dbt-trino against Trino, so dbt needs a Keycloak
  token (`lab check` record `b-jwt` proves Trino accepts one).
- the `spark` tier up and `lab check` green for it, and Lakekeeper grants for the `spark` and `trino` clients on
  the product's namespaces.

## Not offered

- **`dag`**: the same stages through the generated Airflow DAG.
- **`kafka_produce`** and the other fixtures of the streaming tier.
