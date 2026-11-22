# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# PLANTED (four ways to listen beyond the machine). L0 selftest: the exposure
# gate must FAIL on each (run with --launcher).
def host_network():
    return ["docker", "run", "-d", "--network", "host", "openbao:2"]


def attached_publish():
    return ["docker", "run", "-d", "-p8200:8200", "openbao:2"]


def formatted_publish(port):
    return ["docker", "run", "-d", f"--publish={port}:8200", "openbao:2"]


def forward_everywhere():
    return ["kubectl", "port-forward", "-n", "trino", "svc/trino", ":8443", "--address", "0.0.0.0"]
