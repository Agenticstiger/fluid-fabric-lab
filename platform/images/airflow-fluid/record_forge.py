# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Build step of the fluid-runner image: print forge.json, what the build saw in the forge source archive and wheel.

    python3 -I record_forge.py <archive.tar.gz> <wheel dir> <commit> <version>

The commit is the one `git archive` wrote into the archive's own pax header, not the build arg; the build stops
if they differ, or if the wheel is not named for <version>. The probe compares forge.json with versions.lock.yaml.
"""

import hashlib
import json
import sys
import tarfile
from pathlib import Path

archive, wheel_dir, commit, version = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
with tarfile.open(archive) as tar:
    in_archive = (tar.pax_headers or {}).get("comment", "")
if in_archive != commit:
    sys.exit(f"forge archive says commit {in_archive or 'nothing'}, the build was asked for {commit}")
wheels = sorted(wheel_dir.glob("*.whl"))
if len(wheels) != 1:
    sys.exit(f"expected one wheel in {wheel_dir}, found {[w.name for w in wheels]}")
if not wheels[0].name.startswith(f"data_product_forge-{version}-"):
    sys.exit(f"wheel {wheels[0].name} is not data_product_forge {version}")
print(json.dumps({
    "commit": in_archive,
    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    "wheel": wheels[0].name,
    "wheel_sha256": hashlib.sha256(wheels[0].read_bytes()).hexdigest(),
}, indent=2, sort_keys=True))
