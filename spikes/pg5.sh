#!/bin/sh
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# PG-5: apache/iceberg provider v0.1.0 against Lakekeeper with a bearer token: create, an empty second
# plan, a nullable add, an incompatible type change, two column drops (back to the first schema, and to
# a new one), destroy, and what each plans as.
# Exits non-zero when the spike could not run its steps; what the steps plan and do is the finding.
set -eu
work=$(mktemp -d); cd "$work"
cp /opt/fluid-runner/tofu/.terraform.lock.hcl .
TF_VAR_token=$(python3 -I -c '
import json, os, urllib.parse, urllib.request
body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": os.environ["LAKEKEEPER_CLIENT_ID"],
                               "client_secret": os.environ["LAKEKEEPER_CLIENT_SECRET"], "scope": "lakekeeper"}).encode()
r = urllib.request.urlopen("https://keycloak.lab.test/realms/fluid/protocol/openid-connect/token", body, timeout=30)
print(json.load(r)["access_token"])')
export TF_VAR_token
cat > rest.py <<'PY'
# What Lakekeeper holds for namespace pg5, read (or, for "clean", deleted) over its REST API.
import json, os, sys, urllib.error, urllib.request
h = {"Authorization": "Bearer " + os.environ["TF_VAR_token"]}
def call(method, path):
    req = urllib.request.Request("https://lakekeeper.lab.test/catalog" + path, headers=h, method=method)
    try:
        return urllib.request.urlopen(req, timeout=30).status
    except urllib.error.HTTPError as exc:
        return exc.code
cfg = json.load(urllib.request.urlopen(urllib.request.Request(
    "https://lakekeeper.lab.test/catalog/v1/config?warehouse=sales-wh", headers=h), timeout=30))
base = "/v1/" + ((cfg.get("overrides") or {}).get("prefix") or cfg["defaults"]["prefix"]) + "/namespaces/pg5"
if sys.argv[1] == "clean":   # what a killed run left: the table first, then the namespace
    print("table", call("DELETE", base + "/tables/t?purgeRequested=true"), "namespace", call("DELETE", base))
else:
    print("namespace", call("GET", base), "table", call("GET", base + "/tables/t"))
PY
module() {  # $1: the column list of table t
cat > main.tf <<HCL
terraform {
  required_providers {
    iceberg = { source = "apache/iceberg", version = "0.1.0" }
  }
}
variable "token" {
  type      = string
  sensitive = true
}
provider "iceberg" {
  catalog_uri = "https://lakekeeper.lab.test/catalog"
  warehouse   = "sales-wh"
  token       = var.token
}
resource "iceberg_namespace" "pg5" {
  name = ["pg5"]
}
resource "iceberg_table" "t" {
  namespace = iceberg_namespace.pg5.name
  name      = "t"
  schema    = { fields = [$1] }
}
HCL
}
actions() {  # the planned actions of iceberg_table.t, from a saved plan
  rm -f p.tfplan
  tofu plan -input=false -no-color -out=p.tfplan >/dev/null || { echo "plan failed"; return 0; }
  tofu show -json p.tfplan | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(",".join(a for r in d.get("resource_changes",[]) if r["address"]=="iceberg_table.t" for a in r["change"]["actions"]) or "none")'
}
apply_saved() {  # apply p.tfplan: "ok", or the error's first words
  tofu apply -auto-approve -input=false -no-color p.tfplan >apply.log 2>&1 && echo ok \
    || tr '\n"' '  ' <apply.log | sed 's/.*Error: //' | cut -c1-240
}
A='{ id = 1, name = "a", type = "long", required = false }'
B="$A, { id = 2, name = \"b\", type = \"string\", required = false }"
C='{ id = 1, name = "a", type = "string", required = false }, { id = 2, name = "b", type = "string", required = false }'
D='{ id = 2, name = "b", type = "string", required = false }'
module "$A"
tofu init -input=false -no-color >/dev/null
pre_clean=$(python3 -I rest.py clean)
create=$(tofu apply -auto-approve -input=false -no-color >/dev/null 2>&1 && echo ok || echo failed)
after_create=$(python3 -I rest.py get)
second=$(actions)
module "$B"; nullable_add=$(actions); apply_b=$(apply_saved)
again=$(actions)
module "$C"; incompatible=$(actions); apply_c=$(apply_saved)
module "$A"; drop_to_old=$(actions); apply_drop_to_old=$(apply_saved)
module "$D"; drop_to_new=$(actions); apply_drop_to_new=$(apply_saved)
destroy=$(tofu destroy -auto-approve -input=false -no-color >/dev/null 2>&1 && echo ok || echo failed)
left=$(python3 -I rest.py get)
printf 'PG5 {"pre_clean": "%s", "create": "%s", "after_create": "%s", "second_plan": "%s", "nullable_add": "%s", "apply_nullable_add": "%s", "plan_after_add": "%s", "incompatible_change": "%s", "apply_incompatible": "%s", "drop_b_to_first_schema": "%s", "apply_drop_b": "%s", "drop_a_to_new_schema": "%s", "apply_drop_a": "%s", "destroy": "%s", "after_destroy": "%s"}\n' \
  "$pre_clean" "$create" "$after_create" "$second" "$nullable_add" "$apply_b" "$again" "$incompatible" "$apply_c" \
  "$drop_to_old" "$apply_drop_to_old" "$drop_to_new" "$apply_drop_to_new" "$destroy" "$left"
# The spike ran only if the table existed between create and destroy, and nothing was left after.
[ "$create" = ok ] && [ "$after_create" = "namespace 200 table 200" ] && [ "$destroy" = ok ] \
  && [ "$left" = "namespace 404 table 404" ]
