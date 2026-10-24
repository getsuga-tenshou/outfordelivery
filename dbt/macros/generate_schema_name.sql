{#- Snowflake: a model's +schema is used as is (STAGING from the profile, MARTS from dbt_project.yml).
    Every other adapter writes to the profile's schema, so DuckDB keeps using main. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.type == 'snowflake' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
