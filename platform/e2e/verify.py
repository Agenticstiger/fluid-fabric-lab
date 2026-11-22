# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Read back what one `lab check --tier e2e` run landed, and print it as one JSON line.

Runs on the spark tier's image, as the `spark` client, through catalog `fluid`
(Lakekeeper's Iceberg REST catalog with vended credentials), like the builds it
checks. Usage: spark-submit verify.py <run id>. The run id reaches Spark as a
named parameter, never inside the SQL text.
"""

import json
import re
import sys

from pyspark.sql import SparkSession

RAW, DAILY = "fluid.e2e_raw.events", "fluid.e2e_curated.events_daily"

run = sys.argv[1] if len(sys.argv) == 2 else ""
if not re.fullmatch(r"e2e-[0-9a-z-]{4,60}", run):
    sys.exit(f"verify.py: {run!r} is not a run id of `lab check --tier e2e`")
spark = SparkSession.builder.getOrCreate()


def rows(sql, **args):
    return spark.sql(sql, args=args).collect()


def snapshots(table):
    return rows(f"SELECT count(*) AS n FROM {table}.snapshots")[0]["n"]


raw = rows(f"SELECT count(*) AS n, count(DISTINCT event_id) AS ids, CAST(min(event_time) AS STRING) AS first "
           f"FROM {RAW} WHERE run_id = :run", run=run)[0]
daily = rows(f"SELECT CAST(event_date AS STRING) AS d, events FROM {DAILY} WHERE run_id = :run ORDER BY d", run=run)
print("LAB-E2E-RESULT " + json.dumps({
    "run": run,
    "raw": {"rows": raw["n"], "event_ids": raw["ids"], "first": raw["first"], "snapshots": snapshots(RAW)},
    "daily": {"rows": {r["d"]: r["events"] for r in daily}, "snapshots": snapshots(DAILY)},
    "spark": spark.version,
}, sort_keys=True))
