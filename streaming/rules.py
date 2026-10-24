from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

STATE_FIELDS = ["version", "event_type", "status", "event_ts", "hub_id", "lat", "lon", "weather_bad"]
TERMINAL_STATUSES = {"DELIVERED", "RETURNED"}


@dataclass(frozen=True)
class Rules:
    hub_ids: list[str]
    bbox: dict
    avro_schema_json: str
    future_tolerance_minutes: int = 5


def load_rules(repo: Path) -> Rules:
    network = yaml.safe_load((repo / "config" / "network.yaml").read_text())
    return Rules(
        hub_ids=[h["id"] for h in network["hubs"]],
        bbox=network["nl_bounding_box"],
        avro_schema_json=(repo / "schemas" / "parcel_event.avsc").read_text(),
    )


def valid_event_condition(rules: Rules):
    from pyspark.sql import functions as F

    bbox = rules.bbox
    return (
        F.col("hub_id").isin(rules.hub_ids)
        & F.col("lat").between(bbox["lat_min"], bbox["lat_max"])
        & F.col("lon").between(bbox["lon_min"], bbox["lon_max"])
        & (F.col("event_ts") <= F.current_timestamp() + F.expr(f"INTERVAL {rules.future_tolerance_minutes} MINUTES"))
    )


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def checked_acceleration(value: float) -> float:
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("time_acceleration must be finite and positive")
    return value


def elapsed_simulated_seconds(created_at: datetime, event_ts: datetime, accel: float) -> float:
    return (as_utc(event_ts) - as_utc(created_at)).total_seconds() * checked_acceleration(accel)


def is_breached(status, created_at, sla_hours, event_ts, accel: float) -> bool:
    """SLA check on the simulation clock, with the event's own time as "now". Unknown parcels are never breached."""
    if status in TERMINAL_STATUSES or created_at is None or sla_hours is None or event_ts is None:
        return False
    elapsed = elapsed_simulated_seconds(created_at, event_ts, accel)
    return elapsed > int(sla_hours) * 3600


def load_time_acceleration(repo: Path) -> float:
    config = yaml.safe_load((repo / "config" / "simulation.yaml").read_text(encoding="utf-8"))
    return checked_acceleration(config["time_acceleration"])


def load_parcel_dimension(path: Path) -> dict:
    """parcel_id -> (created_at, sla_hours) from parcels.jsonl, keeping the latest created_at per parcel."""
    dimension: dict = {}
    if not path.exists():
        return dimension
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parcel = json.loads(line)
        created_at = datetime.fromisoformat(parcel["created_at"])
        known = dimension.get(parcel["parcel_id"])
        if known is None or as_utc(created_at) >= as_utc(known[0]):
            dimension[parcel["parcel_id"]] = (created_at, int(parcel["sla_hours"]))
    return dimension


def add_breached(rows: list[dict], dimension: dict, accel: float) -> list[dict]:
    """Set row["breached"] on each latest-state row; dimension maps parcel_id to (created_at, sla_hours)."""
    for row in rows:
        created_at, sla_hours = dimension.get(row["parcel_id"], (None, None))
        row["breached"] = is_breached(row.get("status"), created_at, sla_hours, row.get("event_ts"), accel)
    return rows
