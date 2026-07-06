select
    e.event_id,
    e.parcel_id,
    e.event_type,
    e.status,
    e.hub_id,
    e.lat,
    e.lon,
    e.event_ts,
    e.version,
    e.weather_bad,
    e.event_date,
    p.service_level,
    p.sla_hours,
    p.created_at,
    p.promised_by,
    p.destination_region,
    p.carrier,
    p.origin_hub
from {{ ref('stg_parcel_events') }} e
left join {{ ref('stg_parcels') }} p using (parcel_id)
