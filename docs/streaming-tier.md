<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# The `streaming` add-on tier: Kafka for the raw stream

The `streaming` tier carries the raw Kafka stream of an example product: forge's
Spark Structured Streaming job reads a Kafka topic and writes Iceberg. This step
brings up Kafka and proves it. KafkaConnect is not part of it yet.

```sh
./lab mirror --tier streaming              # the Strimzi operator and Kafka images, by digest
./lab up --tier streaming --name <lab>     # core, plus namespaces `strimzi` and `kafka`
./lab check --tier streaming --name <lab>  # every core record, plus `streaming`
```

Tiers stack: `lab up --tier streaming` syncs only the streaming releases, so an
already installed `spark` add-on stays as it is.

## What runs

| Piece | Where |
|---|---|
| Strimzi 1.2.0 operator | namespace `strimzi`, watches namespace `kafka` only |
| Kafka cluster `lab-kafka` (Kafka 4.3.1, KRaft) | namespace `kafka`: one KafkaNodePool node `dual` with the controller and broker roles, 5 Gi volume, heap 256m to 512m, 1 Gi limit |
| Entity operator (topic and user operators) | namespace `kafka`, 256 Mi limit each |
| Certificate `lab-kafka-listener` | cert-manager, ClusterIssuer `lab-ca`, for the `tls` listener |
| KafkaTopics `lab-check.events` (open), `lab-check.restricted` (ACLs) | one partition, one replica |
| KafkaUsers `lab-check-writer` (Write and Describe on `lab-check.restricted`, Describe on the cluster), `lab-check-nobody` (no ACL), `kafka-ui` (Describe and DescribeConfigs on the cluster only, for the optional kafbat UI) | SCRAM-SHA-512; Strimzi generates each password into a Secret of the same name in `kafka` |

Every image is referenced by its lock digest: the Kafka image
(`spec.kafka.image`), both entity-operator containers, and the operator's own
image environment. The Kafka CR follows Strimzi 1.2.0's own examples
(`examples/kafka/kafka-single-node.yaml`, `examples/security/scram-sha-512-auth`)
and its documentation on listener certificates.

## Listeners and authorization

| Listener | Port | TLS | Authentication | Why |
|---|---|---|---|---|
| `plain` | 9092 | no | none | forge's Spark Kafka source reads this one |
| `tls` | 9093 | certificate from the lab CA | SCRAM-SHA-512 | clients that can authenticate |

Authorization is `simple` with `allow.everyone.if.no.acl.found=true`. That
setting is per resource: a resource with no ACL at all is open to everyone, and
a resource with any ACL is enforced. So the lab puts an ACL on the **cluster**
resource (`Describe` for `lab-check-writer`, and `kafka-ui`'s own); without it,
anonymous clients on `plain` would hold every cluster operation, including
CreateAcls, DeleteAcls, AlterUserScramCredentials and AlterConfigs, and could
undo the topic ACLs.

**What an anonymous client on `plain` can and cannot do** (User:ANONYMOUS, no
credentials):

| Can | Cannot |
|---|---|
| Read, write and describe any topic that has no ACL, such as `lab-check.events` (checked: produce and consume) | Any cluster operation: add or delete ACLs, alter SCRAM credentials or configs (checked: adding an ACL is refused with ClusterAuthorizationException) |
| Use consumer groups that have no ACL (not checked: the check consumes without a group) | Read, write or describe a topic that has ACLs, such as `lab-check.restricted` (checked: the read is refused with TopicAuthorizationException) |
| Use transactional ids that have no ACL (not checked) | |

Anything new that is created with no ACL, including every topic a product adds
afterwards, is open on `plain` until it gets one. The rule is per matching ACL, so
an ACL on `*` or on a prefix closes every topic it matches to anonymous clients
too. A live run showed it: a `kafka-ui` ACL on
topic `*` made `lab-check.events` invisible to the anonymous check, so `kafka-ui`
holds cluster ACLs only. That also means the UI is not read-only: like every
principal, it can write the topics that have no ACL. The check's plain producer sets
`enable.idempotence=false`: with an ACL on the cluster resource, whether an
anonymous idempotent producer is still allowed depends on how the authorizer
answers "Write on any topic" for a principal with no ACLs, and the check does
not rely on that. Whether forge's stream needs idempotence is open.

**Recorded gap: forge's Kafka source has no SCRAM or mTLS.** forge 0.32.0
(`fluid_build/spark/compiler.py`, `_kafka`) supports only `auth: none` and
`msk-iam`. So the stream must use the unauthenticated `plain` listener, and
inside the lab anyone who can reach the `kafka` service has the access in the
left column above. The `tls` listener shows that authenticated access works
and that ACLs are enforced, but forge cannot use it yet.

## What `lab check --tier streaming` runs

One Job in namespace `kafka` with Pod Security `restricted`, in the pinned Strimzi
Kafka image and with its own console clients. It passes only on all four:

1. 20 JSON records carrying this run's unique marker go to `lab-check.events` over
   `plain`, and exactly those 20 are consumed back (from the offset before the
   produce, so earlier runs' records never count).
2. `lab-check-writer`, over `tls` with SCRAM, writes to `lab-check.restricted`.
   This is the positive control.
3. `lab-check-nobody`, the same way, is refused there with a topic authorization
   error. A failed login does not count as a refusal.
4. An anonymous client on `plain` is refused adding an ACL (a cluster operation:
   ClusterAuthorizationException) and is refused reading `lab-check.restricted`
   (TopicAuthorizationException, no record leaked).

Before the Job runs, the check waits for the Kafka cluster, both topics and both
users to be Ready. The passwords reach the pod only as `secretKeyRef`
environment variables, and are redacted from every log line the launcher prints.
Like the spark Job: names are unique, a finished Job is left to its one-hour TTL,
a Job that does not finish is deleted, logs are read whole, and a stuck container
fails the check at once. The Job runs in `kafka`, which also has standing pods,
so `ready` ignores Jobs labelled `fluid-fabric-lab/check`.

## Not in this step

- **KafkaConnect** (two clusters, `sales` and `hr`): the `connect-iceberg` image
  is not built (`versions.lock.yaml` built_images, digest null), and a Connect
  cluster on the plain Strimzi image would sink nothing. Criterion (d) stays
  failing until it lands.
- **The kafbat UI** (`addons.kafbat-ui`, optional, off by default) is configured
  for the `tls` listener with the `kafka-ui` user and the lab CA. Its render is
  checked offline; it has not been started in a lab.
- **`strimzi`** (the operator's namespace) stays `baseline`, unmeasured.

## Evidence

Each result is from a run against a live lab.

| Run | Result |
|---|---|
| `./lab mirror --tier streaming` | PASS, 21 images, each digest verified |
| `./lab up --tier streaming` | PASS 10/10 in 268 s. It synced the strimzi operator and the `kafka` lab-objects release, the `spark` add-on was left as installed, and `lab-kafka` became Ready |
| `./lab check --tier streaming` | `streaming` PASS. In `quay.io/strimzi/kafka`, with its own console clients: 20 JSON records carrying the run's marker went to `lab-check.events` over `plain` and came back, exactly those. `lab-check-writer` wrote to `lab-check.restricted` over TLS (lab CA) with SCRAM-SHA-512. `lab-check-nobody`, which has no ACL there, was refused with a topic authorization error. An anonymous client on `plain` was refused adding an ACL and reading `lab-check.restricted`. The core records were unchanged: 9 PASS, plus the 7 that fail by design |
| Pod Security | Namespace `kafka` enforces `restricted`, and the broker, the entity operator and the check Job all run under it |
| Memory | The node went from 5.9 GiB (core and spark at rest) to 7.03 GiB: Kafka adds about 1.1 GiB at rest. The footprint gate counts 9.28 GiB of limits for the tier, against its 13 GiB target |

Two points about how the check judges the cluster:

- **ACLs that match a topic close it to anonymous clients.** `KafkaUser kafka-ui` once held ACLs on topic `*` and
  group `*`. An ACL that matches a resource switches off `allow.everyone.if.no.acl.found` for it, and `*` matches
  every topic and group, so anonymous clients on `plain` lost `lab-check.events`. `kafka-ui` now holds cluster ACLs
  only, and a test fails on any topic or group ACL that matches `lab-check.events`.
- **The anonymous read is judged on the error, not the exit code.** `kafka-console-consumer.sh` logs the
  `TopicAuthorizationException` and exits 0. The `ANONREAD` step passes only on a topic authorization error and no
  leaked record, whatever the exit code (0, 1 or a `timeout` 124); `tests/launcher/test_streaming.py` pins the
  observed shape.
