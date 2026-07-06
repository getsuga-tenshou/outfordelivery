with per_parcel as (
    select
        parcel_id,
        any_value(destination_region) as region,
        max(case when event_type = 'DELIVERY_FAILED' then 1 else 0 end) as had_failure,
        max(case when event_type = 'RETURNED' then 1 else 0 end) as returned
    from {{ ref('fct_parcel_events') }}
    group by parcel_id
)
select
    coalesce(region, 'Unknown') as region,
    count(*) as parcels,
    sum(had_failure) as parcels_with_failure,
    sum(returned) as returned,
    round(sum(had_failure) * 1.0 / count(*), 4) as failure_rate,
    round(sum(returned) * 1.0 / count(*), 4) as return_rate
from per_parcel
group by 1
order by parcels desc
