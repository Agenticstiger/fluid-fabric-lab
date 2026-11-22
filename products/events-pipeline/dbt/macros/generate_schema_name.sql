-- SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
-- SPDX-License-Identifier: Apache-2.0
{#- The contract names the schema (events_marts): use it as written, not prefixed
    with the target's schema (dbt's documented override, "Custom schemas"). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
