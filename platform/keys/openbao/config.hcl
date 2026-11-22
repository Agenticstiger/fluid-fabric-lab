# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# OpenBao 2.7.1 server config for the lab Key Service stand-in: a sibling
# container on the lab's Docker network, OUTSIDE the cluster. Persistent Raft storage and a TLS listener. Dev mode is never
# used: it is in-memory, auto-unsealed and non-TLS, and loses every key on
# restart (which would wipe the lab credentials and the transit keys).
#
# Paths are inside the container; labctl/openbao.py mounts them.

ui            = false
disable_mlock = true   # recommended with integrated (Raft) storage; the container also runs without IPC_LOCK

# Pods reach the API as openbao.lab.test (a CoreDNS rewrite to the container's
# name on the lab's network, so the name does not depend on the lab name);
# `lab` on the host uses 127.0.0.1. The TLS certificate carries both.
api_addr     = "https://openbao.lab.test:8200"
cluster_addr = "https://openbao.lab.test:8201"

storage "raft" {
  path    = "/openbao/file"   # the image's own data directory, owned by its openbao user
  node_id = "lab-openbao-1"
}

listener "tcp" {
  # Listens on the container's own interfaces (the lab network and the
  # published port). The host side is published on 127.0.0.1 only
  # (labctl/openbao.py), never on a LAN interface.
  address         = "0.0.0.0:8200"
  cluster_address = "0.0.0.0:8201"
  tls_cert_file   = "/openbao/tls/tls.crt"
  tls_key_file    = "/openbao/tls/tls.key"
  tls_min_version = "tls12"
}

# No telemetry block: nothing is exported or sent anywhere.
log_level  = "info"
log_format = "json"
