# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The stage-8 seam names the Lakekeeper target the lab actually runs, and no secret value."""

import tempfile
import unittest
from pathlib import Path

import support  # noqa: F401
from support import REPO_ROOT

from labctl import bootstrap, check, credentials, policy, state


class Seam(unittest.TestCase):
    def test_the_target_is_the_labs_own(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab = state.lab_for(REPO_ROOT, "ffl-t", {"LAB_STATE_DIR": str(Path(tmp) / "state")})
            found = policy.target(lab)
        self.assertEqual(found["warehouse"], bootstrap.WAREHOUSE)
        self.assertIn(f"/realms/{bootstrap.REALM}/", found["token_endpoint"])
        self.assertIn(found["client_id"], credentials.CLIENTS)
        self.assertTrue(found["ca_file"].endswith("/ca/ca.crt"))
        self.assertIn("lakekeeper.lab.test", check.ENDPOINTS)       # the host criterion (i) checks
        self.assertNotIn("secret", " ".join(k for k in found if k != "client_secret_ref"))
        self.assertTrue(found["client_secret_ref"].startswith("openbao "))   # a reference, not a value


if __name__ == "__main__":
    unittest.main()
