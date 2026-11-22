# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# `opa test platform/policy/trino-lab` (CI leg `policy`, and tests/launcher).
# One test per thing dbt-trino 1.10.6 asks Trino for with a materialized: table
# model, per read of the system catalog, per deny that matters, and the batch
# endpoint. The grants here are the shape of data.json, not its values.
package trino_test

import rego.v1

grants := {"principals": {
	"pipe": {"lakehouse": {"raw": "read", "curated": "read", "marts": "write"}},
	"empty": {},
}}

ctx(user) := {"identity": {"user": user}}

decide(user, action) if {
	data.trino.allow with input as {"context": ctx(user), "action": action}
		with data.principals as grants.principals
}

batch_of(user, action) := result if {
	result := data.trino.batch with input as {"context": ctx(user), "action": action}
		with data.principals as grants.principals
}

table(schema, name) := {"table": {"catalogName": "lakehouse", "schemaName": schema, "tableName": name}}

schema_of(name) := {"schema": {"catalogName": "lakehouse", "schemaName": name}}

system_table(schema, name) := {"table": {"catalogName": "system", "schemaName": schema, "tableName": name}}

# ---- any query

test_a_listed_principal_may_execute_a_query if decide("pipe", {"operation": "ExecuteQuery"})

test_a_principal_with_no_grants_may_still_execute_a_query_it_cannot_use if decide("empty", {"operation": "ExecuteQuery"})

test_an_unlisted_principal_may_not_execute_a_query if not decide("stranger", {"operation": "ExecuteQuery"})

test_a_request_with_no_identity_is_denied if not data.trino.allow with input as {"action": {"operation": "ExecuteQuery"}}
	with data.principals as grants.principals

test_functions_run_for_a_listed_principal_only if {
	decide("pipe", {"operation": "ExecuteFunction"})
	not decide("stranger", {"operation": "ExecuteFunction"})
}

# ---- catalogs and schemas

test_the_granted_catalog_is_accessible if decide("pipe", {"operation": "AccessCatalog", "resource": {"catalog": {"name": "lakehouse"}}})

test_another_catalog_is_not if not decide("pipe", {"operation": "AccessCatalog", "resource": {"catalog": {"name": "tpch"}}})

test_show_schemas_needs_a_granted_catalog if {
	decide("pipe", {"operation": "ShowSchemas", "resource": {"catalog": {"name": "lakehouse"}}})
	not decide("pipe", {"operation": "ShowSchemas", "resource": {"catalog": {"name": "tpch"}}})
	not decide("empty", {"operation": "ShowSchemas", "resource": {"catalog": {"name": "lakehouse"}}})
}

test_show_tables_in_read_and_write_schemas if {
	decide("pipe", {"operation": "ShowTables", "resource": schema_of("raw")})
	decide("pipe", {"operation": "ShowTables", "resource": schema_of("marts")})
	decide("pipe", {"operation": "ShowTables", "resource": schema_of("information_schema")})
}

test_show_tables_in_an_ungranted_schema_is_denied if not decide("pipe", {"operation": "ShowTables", "resource": schema_of("lab_check")})

# ---- dbt-trino reads

test_select_from_read_and_write_schemas if {
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("raw", "events")})
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("curated", "events_daily")})
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("marts", "events_hourly")})
}

test_select_from_an_ungranted_schema_is_denied if not decide("pipe", {"operation": "SelectFromColumns", "resource": table("lab_check", "probe")})

test_list_relations_reads_information_schema_of_a_granted_catalog if {
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("information_schema", "tables")})
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("information_schema", "columns")})
	decide("pipe", {"operation": "SelectFromColumns", "resource": table("information_schema", "schemata")})
	not decide("empty", {"operation": "SelectFromColumns", "resource": table("information_schema", "tables")})
}

test_show_columns_follows_the_schema_level if {
	decide("pipe", {"operation": "ShowColumns", "resource": table("marts", "events_hourly")})
	not decide("pipe", {"operation": "ShowColumns", "resource": table("lab_check", "probe")})
}

# ---- the system catalog: exactly what dbt-trino 1.10.6 queries

test_the_system_catalog_is_accessible_to_a_listed_principal if {
	decide("pipe", {"operation": "AccessCatalog", "resource": {"catalog": {"name": "system"}}})
	not decide("stranger", {"operation": "AccessCatalog", "resource": {"catalog": {"name": "system"}}})
}

test_the_list_relations_join_reads_materialized_views if decide("pipe", {"operation": "SelectFromColumns", "resource": system_table("metadata", "materialized_views")})

test_docs_generate_reads_table_comments if decide("pipe", {"operation": "SelectFromColumns", "resource": system_table("metadata", "table_comments")})

test_no_other_system_table_is_readable if {
	not decide("pipe", {"operation": "SelectFromColumns", "resource": system_table("metadata", "catalogs")})
	not decide("pipe", {"operation": "SelectFromColumns", "resource": system_table("runtime", "queries")})
	not decide("pipe", {"operation": "SelectFromColumns", "resource": system_table("runtime", "materialized_views")})
	not decide("stranger", {"operation": "SelectFromColumns", "resource": system_table("metadata", "materialized_views")})
}

test_the_system_catalog_is_never_writable if {
	not decide("pipe", {"operation": "CreateTable", "resource": system_table("metadata", "materialized_views")})
	not decide("pipe", {"operation": "DropTable", "resource": system_table("metadata", "materialized_views")})
}

# ---- dbt-trino table materialization

test_create_schema_if_absent_in_a_write_schema_only if {
	decide("pipe", {"operation": "CreateSchema", "resource": schema_of("marts")})
	not decide("pipe", {"operation": "CreateSchema", "resource": schema_of("raw")})
	not decide("pipe", {"operation": "CreateSchema", "resource": schema_of("brand_new")})
}

test_create_table_as_insert_and_drop_in_a_write_schema if {
	every op in ["CreateTable", "InsertIntoTable", "DropTable"] {
		decide("pipe", {"operation": op, "resource": table("marts", "events_hourly__dbt_tmp")})
	}
}

test_writes_to_a_read_schema_are_denied if {
	every op in ["CreateTable", "InsertIntoTable", "DropTable"] {
		not decide("pipe", {"operation": op, "resource": table("raw", "events")})
		not decide("pipe", {"operation": op, "resource": table("curated", "events_daily")})
	}
}

test_rename_needs_write_at_both_ends if {
	decide("pipe", {"operation": "RenameTable", "resource": table("marts", "events_hourly__dbt_tmp"), "targetResource": table("marts", "events_hourly")})
	not decide("pipe", {"operation": "RenameTable", "resource": table("marts", "events_hourly"), "targetResource": table("raw", "events_hourly")})
	not decide("pipe", {"operation": "RenameTable", "resource": table("raw", "events"), "targetResource": table("marts", "events")})
	not decide("pipe", {"operation": "RenameTable", "resource": table("marts", "events_hourly")}) # no target
}

# ---- denied by design, and unknown operations

test_views_deletes_and_changes_to_columns_are_denied if {
	every op in ["CreateView", "DropView", "DeleteFromTable", "UpdateTableColumns", "TruncateTable", "AddColumn", "DropColumn", "AlterColumn", "SetTableComment", "SetColumnComment", "CreateMaterializedView", "DropSchema", "ExecuteProcedure", "ExecuteTableProcedure", "ShowCreateTable", "SetSystemSessionProperty", "ImpersonateUser", "WriteSystemInformation", "KillQueryOwnedBy"] {
		not decide("pipe", {"operation": op, "resource": table("marts", "events_hourly")})
	}
}

test_an_unknown_operation_is_denied if not decide("pipe", {"operation": "SomethingTrino484Adds", "resource": table("marts", "events_hourly")})

test_a_missing_operation_is_denied if not decide("pipe", {})

# ---- batch endpoint

test_filter_catalogs_shows_the_granted_catalog_and_hides_system if {
	items := [{"catalog": {"name": "lakehouse"}}, {"catalog": {"name": "system"}}, {"catalog": {"name": "tpch"}}]
	batch_of("pipe", {"operation": "FilterCatalogs", "filterResources": items}) == {0}
}

test_filter_schemas_keeps_granted_schemas_and_information_schema if {
	items := [schema_of("raw"), schema_of("lab_check"), schema_of("information_schema"), schema_of("marts")]
	batch_of("pipe", {"operation": "FilterSchemas", "filterResources": items}) == {0, 2, 3}
}

test_filter_tables_follows_the_schema if {
	items := [table("raw", "events"), table("lab_check", "probe"), table("marts", "events_hourly")]
	batch_of("pipe", {"operation": "FilterTables", "filterResources": items}) == {0, 2}
}

test_filter_columns_returns_the_indices_of_the_columns_of_a_readable_table if {
	cols := ["id", "ts", "payload"]
	readable := {"table": {"catalogName": "lakehouse", "schemaName": "raw", "tableName": "events", "columns": cols}}
	batch_of("pipe", {"operation": "FilterColumns", "filterResources": [readable]}) == {0, 1, 2}
}

test_filter_columns_of_an_unreadable_table_is_empty if {
	hidden := {"table": {"catalogName": "lakehouse", "schemaName": "lab_check", "tableName": "probe", "columns": ["x", "y"]}}
	batch_of("pipe", {"operation": "FilterColumns", "filterResources": [hidden]}) == set()
}

test_batch_for_an_unlisted_principal_or_an_unknown_operation_is_empty if {
	items := [schema_of("raw"), schema_of("marts")]
	batch_of("stranger", {"operation": "FilterSchemas", "filterResources": items}) == set()
	batch_of("pipe", {"operation": "FilterFunctions", "filterResources": items}) == set()
}

# ---- the endpoints (c) will fill

test_no_row_filters_or_masks_yet if {
	data.trino.rowFilters == [] with input as {}
	data.trino.columnMask == {} with input as {}
	data.trino.batchColumnMasks == [] with input as {}
}
