{#- Warehouse-specific pieces behind adapter.dispatch, so the models stay portable.
    default__ keeps the DuckDB behaviour the project had before (other adapters, such as athena, were never supported). -#}

{% macro ofd_silver_events() %}
    {{ return(adapter.dispatch('ofd_silver_events', 'outfordelivery')()) }}
{% endmacro %}

{% macro duckdb__ofd_silver_events() %}
    read_parquet('{{ var("silver_events_path") }}/**/*.parquet', hive_partitioning = true)
{% endmacro %}

{% macro default__ofd_silver_events() %}
    {{ outfordelivery.duckdb__ofd_silver_events() }}
{% endmacro %}

{% macro snowflake__ofd_silver_events() %}
    {{ source('lake', 'silver_parcel_events') }}
{% endmacro %}


{% macro ofd_parcels() %}
    {{ return(adapter.dispatch('ofd_parcels', 'outfordelivery')()) }}
{% endmacro %}

{#- JSON Lines from producer batches, or Parquet (a file or glob) from a downloaded Silver export. -#}
{% macro duckdb__ofd_parcels() %}
    {%- if var("parcels_path").endswith(".parquet") %}
    read_parquet('{{ var("parcels_path") }}')
    {%- else %}
    read_json_auto('{{ var("parcels_path") }}')
    {%- endif %}
{% endmacro %}

{% macro default__ofd_parcels() %}
    {{ outfordelivery.duckdb__ofd_parcels() }}
{% endmacro %}

{% macro snowflake__ofd_parcels() %}
    {{ source('lake', 'silver_parcels') }}
{% endmacro %}


{#- Seconds from start_ts to end_ts, with sub-second precision (DuckDB datediff would count whole-second boundaries). -#}
{% macro ofd_seconds_between(start_ts, end_ts) %}
    {{ return(adapter.dispatch('ofd_seconds_between', 'outfordelivery')(start_ts, end_ts)) }}
{% endmacro %}

{% macro duckdb__ofd_seconds_between(start_ts, end_ts) %}
    (epoch({{ end_ts }}) - epoch({{ start_ts }}))
{% endmacro %}

{% macro default__ofd_seconds_between(start_ts, end_ts) %}
    (datediff('millisecond', {{ start_ts }}, {{ end_ts }}) / 1000.0)
{% endmacro %}
