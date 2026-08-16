{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the custom schema (e.g. BQ_STAGING_DATASET / BQ_MARTS_DATASET) as-is,
        instead of dbt's default "<target_dataset>_<custom_schema>" concatenation. -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
