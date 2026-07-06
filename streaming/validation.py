from __future__ import annotations

from datetime import datetime, timedelta, timezone


def coord_in_bbox(lat: float, lon: float, bbox: dict) -> bool:
    return bbox["lat_min"] <= lat <= bbox["lat_max"] and bbox["lon_min"] <= lon <= bbox["lon_max"]


def is_known_hub(hub_id: str, hub_ids) -> bool:
    return hub_id in set(hub_ids)


def not_in_future(event_ts: datetime, now: datetime | None = None, tolerance_minutes: int = 5) -> bool:
    now = now or datetime.now(timezone.utc)
    return event_ts <= now + timedelta(minutes=tolerance_minutes)


def is_valid_event(event: dict, bbox: dict, hub_ids, now: datetime | None = None) -> bool:
    return (
        is_known_hub(event["hub_id"], hub_ids)
        and coord_in_bbox(event["lat"], event["lon"], bbox)
        and not_in_future(event["event_ts"], now)
    )
