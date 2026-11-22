# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# The lab's Trino access policy (package trino, the package every URI in
# platform/components/trino/values.yaml reads).
#
# Input shape and operation names: Trino 483's OPA access control
# (https://trino.io/docs/483/security/opa-access-control.html) and its
# plugin source, plugin/trino-opa (OpaAccessControl.java, schema/*), tag 483.
# Written from those, not copied from any other policy set.
#
# Who may do what comes from OPA data, not from this file: data.principals,
# loaded from the same ConfigMap (trino-grants, key data.json):
#   {"principals": {"<authenticated user>": {"<catalog>": {"<schema>": "read" | "write"}}}}
# "write" implies "read". The user is context.identity.user: for a JWT it is
# the token's principal (`azp`, the client id), so only the Keycloak clients
# listed in data.principals reach anything. The grants come from the ConfigMap, not
# from forge's compiled policy bindings.
#
# Everything not allowed here is denied. Sections 4 and 5 are where the row
# filters and column masks of `lab check` (c) land.
package trino

import rego.v1

default allow := false

# ---- 1. identity and grants

principal := input.context.identity.user

# Undefined for a user not listed in data.principals, so every rule below fails.
grants := data.principals[principal]

known if is_object(grants)

level(catalog, schema) := grants[catalog][schema]

can_read(catalog, schema) if level(catalog, schema) in {"read", "write"}

# Trino's metadata queries (dbt-trino lists relations through it) read it in every catalog the user has.
can_read(catalog, "information_schema") if is_object(grants[catalog])

can_write(catalog, schema) if level(catalog, schema) == "write"

op := input.action.operation

# ---- 2. single decisions (opa.policy.uri)

allow if {
	op in {"ExecuteQuery", "ExecuteFunction"}
	known
}

allow if {
	op in {"AccessCatalog", "ShowSchemas"}
	is_object(grants[input.action.resource.catalog.name])
}

allow if {
	op == "ShowTables"
	schema := input.action.resource.schema
	can_read(schema.catalogName, schema.schemaName)
}

allow if {
	op in {"ShowColumns", "SelectFromColumns"}
	table := input.action.resource.table
	can_read(table.catalogName, table.schemaName)
}

allow if {
	op == "CreateSchema"
	schema := input.action.resource.schema
	can_write(schema.catalogName, schema.schemaName)
}

allow if {
	op in {"CreateTable", "DropTable", "InsertIntoTable"}
	table := input.action.resource.table
	can_write(table.catalogName, table.schemaName)
}

# dbt-trino swaps a rebuilt table in by renaming: both ends need write.
allow if {
	op == "RenameTable"
	source := input.action.resource.table
	target := input.action.targetResource.table
	can_write(source.catalogName, source.schemaName)
	can_write(target.catalogName, target.schemaName)
}

# dbt-trino 1.10.6 reads exactly two tables of the system catalog: its list-relations query
# joins system.metadata.materialized_views (macros/adapters.sql) and `dbt docs generate`
# reads system.metadata.table_comments (macros/catalog.sql). Trino 483 checks AccessCatalog
# on `system` first. Nothing else of `system` is allowed, and FilterCatalogs keeps hiding it.
dbt_system_tables := {"materialized_views", "table_comments"}

allow if {
	op == "AccessCatalog"
	known
	input.action.resource.catalog.name == "system"
}

allow if {
	op == "SelectFromColumns"
	known
	table := input.action.resource.table
	table.catalogName == "system"
	table.schemaName == "metadata"
	table.tableName in dbt_system_tables
}

# ---- 3. batch decisions (opa.policy.batched-uri): the indices of the items that may be seen

batch contains i if {
	some i, item in input.action.filterResources
	visible(op, item)
}

visible("FilterCatalogs", item) if is_object(grants[item.catalog.name])

visible("FilterSchemas", item) if can_read(item.schema.catalogName, item.schema.schemaName)

visible("FilterTables", item) if can_read(item.table.catalogName, item.table.schemaName)

# FilterColumns is one table whose `columns` array is filtered: the answer is column indices.
batch contains i if {
	op == "FilterColumns"
	some item in input.action.filterResources
	can_read(item.table.catalogName, item.table.schemaName)
	some i, _ in item.table.columns
}

# ---- 4. row filters (opa.policy.row-filters-uri): none yet, (c) adds them

rowFilters := []

# ---- 5. column masks (column-masking and batch-column-masking URIs): none yet, (c) adds them

columnMask := {}

batchColumnMasks := []
