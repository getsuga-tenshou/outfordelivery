with outcome as (
    select
        parcel_id,
        any_value(destination_region) as region,
        any_value(created_at) as created_at,
        any_value(sla_hours) as sla_hours,
        max(case when event_type = 'DELIVERED' then event_ts end) as delivered_ts
    from {{ ref('fct_parcel_events') }}
    group by parcel_id
)
select
    coalesce(region, 'Unknown') as region,
    count(*) as parcels,
    count(delivered_ts) as delivered,
    sum(
        case when delivered_ts is not null
             and (epoch(delivered_ts) - epoch(created_at)) * {{ var('time_acceleration', 120) }} <= sla_hours * 3600
             then 1 else 0 end
    ) as on_time,
    round(
        sum(
            case when delivered_ts is not null
                 and (epoch(delivered_ts) - epoch(created_at)) * {{ var('time_acceleration', 120) }} <= sla_hours * 3600
                 then 1 else 0 end
        ) * 1.0 / nullif(count(delivered_ts), 0),
        4
    ) as on_time_rate
from outcome
group by 1
order by parcels desc
