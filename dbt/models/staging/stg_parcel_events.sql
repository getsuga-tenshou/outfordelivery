select
    event_id,
    parcel_id,
    event_type,
    status,
    hub_id,
    cast(lat as double) as lat,
    cast(lon as double) as lon,
    cast(event_ts as timestamp) as event_ts,
    cast(version as integer) as version,
    weather_bad,
    cast(event_date as date) as event_date
from read_parquet('{{ var("silver_events_path") }}/**/*.parquet', hive_partitioning = true)
