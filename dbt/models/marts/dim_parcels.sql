select
    parcel_id,
    origin_hub,
    destination_postcode,
    destination_region,
    carrier,
    service_level,
    sla_hours,
    created_at,
    promised_by
from {{ ref('stg_parcels') }}
