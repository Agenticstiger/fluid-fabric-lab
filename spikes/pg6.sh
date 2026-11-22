#!/bin/sh
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# PG-6: OpenTofu 1.13.1, `pg` backend with state and plan encryption (pbkdf2). It counts a sensitive
# canary in the saved tfplan, `show -json` and `state pull` (run.py adds the row Postgres stores),
# checks that `show -json` is stable, and that schema_name quoting works. PG_CONN_STR (no password:
# lib/pq reads PGPASSWORD) comes from spikes/run.py.
set -eu
work=$(mktemp -d); cd "$work"
cat > main.tf <<'HCL'
variable "passphrase" {
  type      = string
  sensitive = true
}
variable "canary" {
  type      = string
  sensitive = true
}
terraform {
  backend "pg" {
    schema_name = "Lab Spike-PG6"
  }
  encryption {
    key_provider "pbkdf2" "lab" {
      passphrase = var.passphrase
    }
    method "aes_gcm" "lab" {
      keys = key_provider.pbkdf2.lab
    }
    state {
      method   = method.aes_gcm.lab
      enforced = true
    }
    plan {
      method   = method.aes_gcm.lab
      enforced = true
    }
  }
}
resource "terraform_data" "canary" {
  input = var.canary
}
output "canary" {
  value     = var.canary
  sensitive = true
}
HCL
export TF_VAR_canary="CANARY-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
tofu init -input=false -no-color >/dev/null
tofu plan -input=false -no-color -out=tfplan >/dev/null
plan_clear=$(grep -c "$TF_VAR_canary" tfplan || true)
# An unencrypted saved plan is a zip, whose deflated entries would hide the canary from grep too.
plan_zip=$(python3 -I -c 'import sys, zipfile; print(str(zipfile.is_zipfile(sys.argv[1])).lower())' tfplan)
tofu apply -input=false -no-color tfplan >/dev/null
tofu show -json > show1.json
tofu show -json > show2.json
show_has=$(grep -c "$TF_VAR_canary" show1.json || true)
stable=$(cmp -s show1.json show2.json && echo true || echo false)
tofu state pull > pulled.json
pulled_has=$(grep -c "$TF_VAR_canary" pulled.json || true)
printf 'PG6 {"tofu": "%s", "plan_cleartext_hits": %s, "plan_is_plain_zip": %s, "show_json_cleartext_hits": %s, "show_json_stable": %s, "state_pull_cleartext_hits": %s, "canary_prefix": "%s"}\n' \
  "$(tofu version | head -1)" "$plan_clear" "$plan_zip" "$show_has" "$stable" "$pulled_has" "$(echo "$TF_VAR_canary" | cut -c1-7)"
echo "PG6-CANARY $TF_VAR_canary"
