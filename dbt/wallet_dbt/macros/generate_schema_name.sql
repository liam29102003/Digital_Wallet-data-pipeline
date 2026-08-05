{#
  dbt's default generate_schema_name macro concatenates the target schema
  with any custom +schema config, e.g. +schema: silver becomes the
  physical schema "dbt_dev_silver" rather than "silver". For a Medallion
  layout where Bronze/Silver/Gold need to be distinct, predictable
  top-level schemas in Unity Catalog, that's not what we want.

  This is dbt's own documented override pattern: when a model sets a
  custom schema, use exactly that name. When it doesn't, fall back to
  the profile's default target schema (e.g. dev sandbox models with no
  explicit layer).
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
