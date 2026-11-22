<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# conformance

Evidence from `lab accept` runs. `reports/` holds one pair of files per run:

```
reports/<YYYY-MM-DD>-<profile>-<tier>-<run-id>.json   the full record
reports/<YYYY-MM-DD>-<profile>-<tier>-<run-id>.md     the same, short, for people
```

A report says: the run id, forge's `fluid --version` as the runner saw it, the git commit of
the lab, every observed cell with forge's own reason, the comparison with the expectations
ledger (every mismatch as `product/runner/stage: expected X, observed Y (reason)`), and the
exact argv of each stage. Secrets are redacted before anything is written.

These files are evidence, and they are committed on purpose. A `passed` cell of a ledger
(`profiles/<profile>/expected/<tier>.yaml`) points at the report that proved it
(`evidence.report`), so a report is never edited, renamed or deleted once a ledger cites it.
A run that did not match is kept too: it shows what the lab observed.

`lab accept` creates a report with exclusive-create and never overwrites one. How to read a
report, the status mapping and `--update-ledger`: [docs/lab-accept.md](../docs/lab-accept.md).
