<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# spark-iceberg image

The Spark image of the `spark` add-on tier (docs/spark-tier.md): the official
`apache/spark` 3.5.8 image, by digest, with the Iceberg runtime, the S3A
connector, the Kafka client and an offline Maven repository for the Spark Kafka
connector added. forge's Spark runner runs in it with `spark-submit` and a
local master. It holds open software only (all Apache-2.0) and needs no AWS
credentials.

| Piece | Version | How it is pinned |
|---|---|---|
| Base | `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu` (Ubuntu 22.04, JDK 17, Python 3.10) | by index digest (`ARG SPARK_IMAGE`, `base` in `versions.lock.yaml`); amd64 and arm64 |
| Iceberg | `iceberg-spark-runtime-3.5_2.12` and `iceberg-aws-bundle` 1.12.0 | `contents.files` in `versions.lock.yaml`: Maven coordinate, URL, sha256 |
| S3A | `hadoop-aws` 3.3.4 and the AWS SDK for Java v1 bundle 1.12.262 | the same |
| Kafka | `kafka-clients` 3.4.1, `commons-pool2` 2.11.1, `spark-token-provider-kafka` 3.5.8, and `spark-sql-kafka-0-10` 3.5.8 with its poms | the same |

Every file is fetched from Maven Central with `ADD --checksum`, so BuildKit
refuses it unless its sha256 matches. The URL and the sha256 of each one reach
the Dockerfile as build args from the lock (`<ID>_URL`, `<ID>_SHA256`) and have
no default: the lock is the only place they are written. The path each file
lands at is written in the Dockerfile and in the lock, and a test
(`tests/launcher/test_spark_image.py`) keeps the two equal.

## Layout

| Path | Holds |
|---|---|
| `/opt/lab/jars/` | the two Iceberg jars; the lab's properties file names them in `spark.jars` |
| `/opt/spark/jars/` | what must always be on the classpath: hadoop-aws and the AWS SDK v1 bundle (spark-submit needs them to fetch an `s3://` application before `spark.jars` load), the Kafka client, commons-pool2 and Spark's Kafka token provider |
| `/opt/lab/maven/` | an offline Maven repository with only the Spark Kafka connector, its pom and its two parent poms |
| `/opt/lab/ivysettings.xml` | points Ivy at that repository and nothing else |

forge asks for the Kafka connector through `spark.jars.packages` for any Kafka
build, and for the Iceberg jar path of another image in `spark.jars`. The operator's properties file
(`platform/spark/spark-defaults.apache.conf`) replaces `spark.jars` and names
the connector at this image's Spark version, so Ivy resolves it from
`/opt/lab/maven` with no network. The connector's dependencies are already in
`/opt/spark/jars` and are excluded (`spark.jars.excludes`).

The image runs as uid 185 (`spark`), the base image's own user.

## Build, check, load

```sh
./lab build spark-iceberg --name <lab>
```

The same three steps as fluid-runner (`labctl/build.py`, `labctl/spark_image.py`):
`docker build`, a probe with no network, `kind load docker-image` and a record
in the lab's state directory. The probe runs the image by id with
`--network none` and fails unless:

- the uid is 185 and `spark-submit --version` says 3.5.8;
- every file in the image has the sha256 the lock records;
- `spark-submit` with the lab's properties file loads
  `org.apache.spark.sql.kafka010.KafkaSourceProvider`,
  `org.apache.kafka.clients.consumer.KafkaConsumer`,
  `org.apache.iceberg.spark.SparkCatalog`,
  `software.amazon.awssdk.services.s3.S3Client`,
  `org.apache.hadoop.fs.s3a.S3AFileSystem` and
  `org.apache.commons.pool2.impl.GenericKeyedObjectPool`.

The tag is `lab.local/spark-iceberg:<Spark version>-iceberg<Iceberg version>-<8 characters>`
(the last part follows the base digest and every file's sha256); the pods run
it with `imagePullPolicy: Never`. The build needs the network once (Docker Hub,
Maven Central).

## Bumping

Change the version, URL, path and sha256 of the file in
`versions.lock.yaml` (`built_images[spark-iceberg].contents.files`), the `ADD`
line and the `ARG`s in the Dockerfile, and `spark.jars` and `spark.jars.packages`
in `platform/spark/spark-defaults.apache.conf`. Compute each sha256 yourself
from the downloaded file and compare it with the `.sha1` Maven publishes. To bump
the base, change `base` in the lock and `ARG SPARK_IMAGE`. `lab build` refuses a
Dockerfile that disagrees with the lock before it starts `docker build`.

## Not published

Never pushed to a registry: the lab publishes no image, like fluid-runner.
