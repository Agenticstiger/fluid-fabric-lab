# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# PLANTED (a Docker port published on every interface). L0 selftest: the
# exposure gate must FAIL (run with --launcher).
def start(port):
    return ["docker", "run", "-d", "-p", f"{port}:5000", "registry:3"]
