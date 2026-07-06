select
    coalesce(weather_bad, false) as bad_weather,
    count(*) filter (where event_type = 'OUT_FOR_DELIVERY') as delivery_attempts,
    count(*) filter (where event_type = 'DELIVERY_FAILED') as failures,
    round(
        count(*) filter (where event_type = 'DELIVERY_FAILED') * 1.0
        / nullif(count(*) filter (where event_type = 'OUT_FOR_DELIVERY'), 0),
        4
    ) as failure_rate
from {{ ref('stg_parcel_events') }}
group by 1
order by 1
