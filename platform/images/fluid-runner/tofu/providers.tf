# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# The OpenTofu providers baked into the fluid-runner image's filesystem
# mirror. Versions are exact; the hashes for
# linux_amd64 and linux_arm64 are in .terraform.lock.hcl next to this file.
terraform {
  required_providers {
    iceberg    = { source = "apache/iceberg", version = "0.1.0" }
    lakekeeper = { source = "baptistegh/lakekeeper", version = "0.4.10" }
    keycloak   = { source = "keycloak/keycloak", version = "5.10.0" }
    kubernetes = { source = "hashicorp/kubernetes", version = "3.3.0" }
  }
}
