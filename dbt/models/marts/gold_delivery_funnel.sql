select
    event_type,
    count(*) as events,
    count(distinct parcel_id) as parcels
from {{ ref('stg_parcel_events') }}
group by event_type
order by parcels desc
