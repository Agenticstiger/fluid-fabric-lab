# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Seed the probe contract's source table, raw.events, and empty its sink, curated.events_daily.

Run through lab-spark-entry.sh on the spark-iceberg image, catalog `fluid` declared in spark-defaults
(Lakekeeper REST, vended credentials). Rerunnable: it replaces raw.events' rows and
deletes every row of the sink, so whatever the sink holds afterwards was written by
this run's runner. It prints the sink's snapshot count for verify.py to compare.
"""

import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
for statement in (
    "CREATE NAMESPACE IF NOT EXISTS fluid.raw",
    "CREATE NAMESPACE IF NOT EXISTS fluid.curated",
    "CREATE TABLE IF NOT EXISTS fluid.raw.events (event_id string NOT NULL, user_id string, event_time timestamp)"
    " USING iceberg",
    "CREATE TABLE IF NOT EXISTS fluid.curated.events_daily (event_date date NOT NULL, events bigint) USING iceberg",
    "INSERT OVERWRITE fluid.raw.events VALUES"
    " ('e1', 'u1', TIMESTAMP '2026-11-17 10:00:00'),"
    " ('e2', 'u2', TIMESTAMP '2026-11-17 11:00:00'),"
    " ('e3', 'u1', TIMESTAMP '2026-11-18 09:30:00')",
    "DELETE FROM fluid.curated.events_daily",
):
    spark.sql(statement)
snapshots = spark.sql("SELECT count(*) AS n FROM fluid.curated.events_daily.snapshots").collect()[0]["n"]
print("LAB-SPARK-SEEDED " + json.dumps({"rows": spark.table("fluid.raw.events").count(), "sink_rows":
                                        spark.table("fluid.curated.events_daily").count(), "sink_snapshots": snapshots}))
