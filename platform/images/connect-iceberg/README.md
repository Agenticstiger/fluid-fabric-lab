<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# connect-iceberg image (not built)

The lab does not build this image. `versions.lock.yaml` records it under
`built_images` with `digest: null`: the Apache Iceberg Kafka Connect sink
1.12.0 on the pinned `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` base, by
digest. The streaming tier runs no KafkaConnect cluster, so nothing uses it,
and `lab check` criterion (d) fails and says so.

Rules for a build of it:
- It derives from a Red Hat UBI image, so the unmodified UBI EULA ships inside
  it and with any mirror or bundle that contains it, and an SBOM licence
  inventory is produced for it.
- It is pulled by digest only, like every other image.

Iceberg 1.12.0 on the Kafka 4.x Connect runtime is unverified.
