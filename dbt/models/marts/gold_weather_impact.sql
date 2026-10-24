select
    coalesce(weather_bad, false) as bad_weather,
    sum(case when event_type = 'OUT_FOR_DELIVERY' then 1 else 0 end) as delivery_attempts,
    sum(case when event_type = 'DELIVERY_FAILED' then 1 else 0 end) as failures,
    round(
        sum(case when event_type = 'DELIVERY_FAILED' then 1 else 0 end) * 1.0
        / nullif(sum(case when event_type = 'OUT_FOR_DELIVERY' then 1 else 0 end), 0),
        4
    ) as failure_rate
from {{ ref('stg_parcel_events') }}
group by 1
order by 1
