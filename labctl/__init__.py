# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""labctl: the fluid-fabric-lab launcher (standard library only).

Commands: doctor, render, l0, mirror, build, up, down, check, e2e, accept.
"""

__version__ = "0.0.0"

# Exit codes shared by every command.
EXIT_OK = 0
EXIT_FAIL = 1  # a check ran and failed (including "could not be evaluated")
EXIT_USAGE = 2  # bad arguments, or a request the command does not offer
