# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Read the runner's sink back and print it as one JSON line for `lab check`."""

import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
rows = spark.sql("SELECT CAST(event_date AS STRING) AS d, events FROM fluid.curated.events_daily ORDER BY d").collect()
snapshots = spark.sql("SELECT count(*) AS n FROM fluid.curated.events_daily.snapshots").collect()[0]["n"]
print("LAB-SPARK-RESULT " + json.dumps({"rows": {r["d"]: r["events"] for r in rows}, "snapshots": snapshots,
                                         "spark": spark.version}))
