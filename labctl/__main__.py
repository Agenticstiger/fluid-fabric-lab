# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Allow `python3 -m labctl ...` as an alternative to `./lab ...`."""

import sys

from labctl.cli import main

sys.exit(main())
