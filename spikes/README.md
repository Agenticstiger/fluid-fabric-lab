<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# PG spikes

The proof gates PG-1 to PG-8, run as throwaway
pods against a lab. Each spike is one script. `run.py` runs it in the
fluid-runner image in namespace `spark` (Pod Security `restricted`) and prints
its one result line:

```sh
python3 spikes/run.py --name <lab> pg1
```

`run.py` exits 0 only when the spike ran to its end: a finding is in the line,
not in the exit code. A pod that cannot start fails at once. On exit, Ctrl-C
and SIGTERM included, `run.py` then deletes the pod, the ConfigMap that holds
the scripts and, for PG-6, its copy of the `tofu` role's password.

They need a lab with the spark tier (`./lab up --tier spark`) and the runner
image (`./lab build fluid-runner`). PG-1 and PG-2 read `raw.events`, which
`./lab check --tier spark` seeds with 3 rows. The results
are in [docs/proven.md](../docs/proven.md#pg-spikes).

| Spike | Script | What it runs |
|---|---|---|
| PG-1 | `pg1.py` | DuckDB 1.5.6 `iceberg` ATTACH to Lakekeeper over OAuth2 with a plain `duckdb.connect`, as the `spark` client, then SELECT and INSERT with vended credentials and no `AWS_*` variable |
| PG-2 | `pg2.py` | The same inside forge's `secure_duckdb_connect`, with the ATTACH in `before_lock`. Then REST and S3 reads after the configuration is locked, and three things the sandbox itself must refuse with DuckDB's typed refusal: a local file, another bucket, and unlocking |
| PG-5 | `pg5.sh` | The `apache/iceberg` OpenTofu provider v0.1.0 against Lakekeeper with a bearer token: create (checked over REST), a second plan, a nullable add, an incompatible change, two column drops, and destroy (checked over REST) |
| PG-6 | `pg6.sh` | OpenTofu 1.13.1 with the `pg` backend (TLS verified against CNPG's CA) and pbkdf2 state and plan encryption. It looks for a sensitive canary in the saved plan (and checks the plan is not a plain zip), in `show -json`, and in the state row Postgres stores (and checks that row is encrypted). It also checks that `show -json` is stable and that a quoted schema name works |

## Leftovers

What the spikes write stays in the lab:
- PG-1 adds row `d1` to `raw.events` on each run. The next `./lab check --tier spark` replaces the table's rows.
- PG-5 deletes its namespace `pg5` at the end. A run killed before then leaves it, and the next run deletes it first.
- PG-6 leaves schema `"Lab Spike-PG6"`, with its encrypted state row, in database `tofu_state`. To drop it:
  `kubectl -n lab-db exec lab-pg-1 -c postgres -- psql -d tofu_state -c 'DROP SCHEMA "Lab Spike-PG6" CASCADE'`.

PG-3 (dbt-trino through the OPA bridge), PG-4 (Rook-Ceph RGW), PG-7 (Strimzi
through `kubernetes_manifest`) and PG-8 (the Key Service pilot) are not here.
Each needs a part the lab does not run.

DuckDB downloads its `iceberg` and `httpfs` extensions from
extensions.duckdb.org when a spike starts, and checks their signatures. The
runner image does not carry them.
