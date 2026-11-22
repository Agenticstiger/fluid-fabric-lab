# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# spark-iceberg: the Spark image of the `spark` add-on tier. Apache Spark with
# the Iceberg runtime, the S3A connector and the Kafka connector, so forge's
# Spark runner runs on open software only (docs/spark-tier.md).
#
# - FROM the official apache/spark image, by digest (`SPARK_IMAGE`, and `base`
#   in versions.lock.yaml). Spark, its Python and its JDK are not touched.
# - Every jar and pom is fetched with ADD --checksum (BuildKit refuses it
#   unless its sha256 matches). The URL and the sha256 of each one have no
#   default: `lab build` passes them as build args from versions.lock.yaml
#   (`built_images[spark-iceberg].contents.files`), the one place they are
#   written. The path each file lands at is written here and in the lock, and
#   a test keeps the two equal.
# - /opt/lab/jars holds the two jars `spark.jars` names (the Iceberg Spark
#   runtime and the Iceberg AWS bundle). /opt/spark/jars gets the jars that
#   must always be on the classpath: hadoop-aws and the AWS SDK for Java v1
#   bundle (spark-submit reads an s3:// application with them before
#   spark.jars load), the Kafka client, commons-pool2 and Spark's Kafka token
#   provider.
# - /opt/lab/maven is an offline Maven repository that holds only the Spark
#   Kafka connector, its pom and its two parent poms. forge asks for the
#   connector with spark.jars.packages; Ivy resolves it from there
#   (/opt/lab/ivysettings.xml), so a Kafka build needs no network. The
#   connector's dependencies are already in /opt/spark/jars and are excluded
#   by spark.jars.excludes in the lab's properties file.
# - Runs as uid 185, the image's own `spark` user.
#
# Needs BuildKit (ADD --checksum). Built on the operator's machine by
# `./lab build spark-iceberg` and loaded into a lab's kind node. It is never
# pushed to a registry: the lab publishes no image.

ARG SPARK_IMAGE=docker.io/apache/spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:dbe5f3b3926b01fa0b818ae8463c2994bfd2945d8db96d2337e23dc62d6fa73d

FROM ${SPARK_IMAGE}

ARG ICEBERG_SPARK_RUNTIME_URL
ARG ICEBERG_SPARK_RUNTIME_SHA256
ARG ICEBERG_AWS_BUNDLE_URL
ARG ICEBERG_AWS_BUNDLE_SHA256
ARG HADOOP_AWS_URL
ARG HADOOP_AWS_SHA256
ARG AWS_JAVA_SDK_BUNDLE_URL
ARG AWS_JAVA_SDK_BUNDLE_SHA256
ARG KAFKA_CLIENTS_URL
ARG KAFKA_CLIENTS_SHA256
ARG COMMONS_POOL2_URL
ARG COMMONS_POOL2_SHA256
ARG SPARK_TOKEN_PROVIDER_KAFKA_URL
ARG SPARK_TOKEN_PROVIDER_KAFKA_SHA256
ARG SPARK_SQL_KAFKA_JAR_URL
ARG SPARK_SQL_KAFKA_JAR_SHA256
ARG SPARK_SQL_KAFKA_POM_URL
ARG SPARK_SQL_KAFKA_POM_SHA256
ARG SPARK_PARENT_POM_URL
ARG SPARK_PARENT_POM_SHA256
ARG APACHE_PARENT_POM_URL
ARG APACHE_PARENT_POM_SHA256

# The directories are made here: ADD would create them with the file's mode (0644), which no user can enter.
USER root
RUN mkdir -p /opt/lab/jars \
      /opt/lab/maven/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8 \
      /opt/lab/maven/org/apache/spark/spark-parent_2.12/3.5.8 \
      /opt/lab/maven/org/apache/apache/18 \
 && chmod -R 0755 /opt/lab

ADD --chmod=0644 --checksum=sha256:${ICEBERG_SPARK_RUNTIME_SHA256} ${ICEBERG_SPARK_RUNTIME_URL} /opt/lab/jars/iceberg-spark-runtime-3.5_2.12-1.12.0.jar
ADD --chmod=0644 --checksum=sha256:${ICEBERG_AWS_BUNDLE_SHA256} ${ICEBERG_AWS_BUNDLE_URL} /opt/lab/jars/iceberg-aws-bundle-1.12.0.jar

ADD --chmod=0644 --checksum=sha256:${HADOOP_AWS_SHA256} ${HADOOP_AWS_URL} /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD --chmod=0644 --checksum=sha256:${AWS_JAVA_SDK_BUNDLE_SHA256} ${AWS_JAVA_SDK_BUNDLE_URL} /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
ADD --chmod=0644 --checksum=sha256:${KAFKA_CLIENTS_SHA256} ${KAFKA_CLIENTS_URL} /opt/spark/jars/kafka-clients-3.4.1.jar
ADD --chmod=0644 --checksum=sha256:${COMMONS_POOL2_SHA256} ${COMMONS_POOL2_URL} /opt/spark/jars/commons-pool2-2.11.1.jar
ADD --chmod=0644 --checksum=sha256:${SPARK_TOKEN_PROVIDER_KAFKA_SHA256} ${SPARK_TOKEN_PROVIDER_KAFKA_URL} /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar

ADD --chmod=0644 --checksum=sha256:${SPARK_SQL_KAFKA_JAR_SHA256} ${SPARK_SQL_KAFKA_JAR_URL} /opt/lab/maven/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar
ADD --chmod=0644 --checksum=sha256:${SPARK_SQL_KAFKA_POM_SHA256} ${SPARK_SQL_KAFKA_POM_URL} /opt/lab/maven/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.pom
ADD --chmod=0644 --checksum=sha256:${SPARK_PARENT_POM_SHA256} ${SPARK_PARENT_POM_URL} /opt/lab/maven/org/apache/spark/spark-parent_2.12/3.5.8/spark-parent_2.12-3.5.8.pom
ADD --chmod=0644 --checksum=sha256:${APACHE_PARENT_POM_SHA256} ${APACHE_PARENT_POM_URL} /opt/lab/maven/org/apache/apache/18/apache-18.pom

COPY --chmod=0644 ivysettings.xml /opt/lab/ivysettings.xml

USER 185
