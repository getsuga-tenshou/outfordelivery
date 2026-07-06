select
    parcel_id,
    origin_hub,
    destination_postcode,
    destination_region,
    carrier,
    service_level,
    cast(sla_hours as integer) as sla_hours,
    cast(created_at as timestamp) as created_at,
    cast(promised_by as timestamp) as promised_by
from read_json_auto('{{ var("parcels_path") }}')
qualify row_number() over (partition by parcel_id order by created_at desc) = 1
