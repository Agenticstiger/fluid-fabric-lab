<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# OpenBao (lab Key Service stand-in)

OpenBao 2.7.1 runs as a **sibling container on the lab's Docker network,
outside the cluster**. It stands in for a production three-host Key Service,
so every conformance cell that depends on it is marked
`substrate: substitute`.

| File | What it is |
|---|---|
| `config.hcl` | Server config: persistent Raft storage at `/openbao/file`, TLS listener (TLS 1.2+), no UI, no telemetry |
| `labctl/openbao.py` | How `lab up` runs, initialises, unseals and configures it, over the HTTP API |

## How `lab up` runs it

- Image `quay.io/openbao/openbao:2.7.1` by digest, pulled from the lab mirror.
- Container `<lab>-openbao` on the lab network `<lab>-net`, `--read-only`, every
  capability dropped, `no-new-privileges`, 256 MiB, restart `unless-stopped`.
- Raft data on volume `<lab>-openbao-data`, mounted at the image's own data
  directory, `/openbao/file`, which its non-root `openbao` user (uid 100)
  owns. A new volume inherits that ownership, so no root step is needed.
- TLS key pair issued by the lab CA on the host (`labctl/pki.py`), for
  `openbao.lab.test`, `<lab>-openbao`, `localhost` and `127.0.0.1`.
- One host port, an ephemeral one on 127.0.0.1, so it never clashes with a
  local Vault or OpenBao on 8200. Pods use `https://openbao.lab.test:8200`
  (a CoreDNS rewrite to the container name), so no chart value depends on the
  lab name.

## Rules

- **Never dev mode.** `bao server -dev` is in-memory, auto-unsealed and
  non-TLS, and "will lose data on every restart", which would wipe the
  lab credentials and the transit keys.
- `init` runs once with 3 key shares and threshold 2. The shares and the
  `lab-admin` password live in `<state>/openbao/init.json` (mode 0600, in the
  0700 state directory, outside the repository). That is a lab-only
  convenience: a production Key Service never keeps unseal material next to
  the server.
- The root token is used once, to create the `lab-admin` userpass login and
  its policy, and is then revoked. No root token survives `lab up`.
- Clients verify TLS against the lab CA. `-tls-skip-verify` is never used, and
  the launcher talks to the HTTP API (keys and tokens in request bodies,
  never on a command line).
- Split policies with no server-wide wildcard: `lab-admin` manages the lab's
  `kv/` and `transit/` mounts and reads and writes `kv/data/lab/*` only.

## Mounts

| Mount | Holds |
|---|---|
| `kv/` (KV v2) | `kv/lab/credentials`: every credential `lab up` generated (labctl/credentials.py), including Lakekeeper's secret-store encryption key. The Kubernetes Secrets are rendered from it. |
| `userpass/` | the `lab-admin` login |

Transit keys per domain and Kubernetes auth for the runner are not set up;
the state-encryption spike (PG-6) did not use them.

## Lakekeeper's KV2 backend: recorded as failed

Check (e) asks for Lakekeeper's KV2 secret backend against OpenBao,
"or the failure recorded". It fails with Lakekeeper v0.14.0 and OpenBao 2.7.1: Lakekeeper's Vault client parses `GET /v1/sys/health` and
requires the field `performance_standby`, which OpenBao does not return. The
secrets health check fails, Lakekeeper's `/health` answers 503, and the
liveness probe restarts the pod in a loop. The lab therefore uses Lakekeeper's
Postgres secret store with a per-lab encryption key kept in OpenBao
(`platform/components/lakekeeper/values.yaml`). Moving back needs a
Lakekeeper release whose client tolerates OpenBao's health response.

## Proved by `lab check` (e)

OpenBao survives a container restart with its keys (Raft data on the named
volume) and unseals from the stored shares.
