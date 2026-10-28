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
    return parse_parcel_dimension(path.read_text(encoding="utf-8")) if path.exists() else {}


def parse_parcel_dimension(text: str) -> dict:
    dimension: dict = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parcel = json.loads(line)
        created_at = datetime.fromisoformat(parcel["created_at"])
        known = dimension.get(parcel["parcel_id"])
        if known is None or as_utc(created_at) >= as_utc(known[0]):
            dimension[parcel["parcel_id"]] = (created_at, int(parcel["sla_hours"]))
    return dimension


def load_s3_parcel_dimension(s3, uri: str) -> dict:
    """Same as load_parcel_dimension for an s3://bucket/key object; a missing object gives an empty dimension."""
    text = _read_s3_text(s3, uri)
    return parse_parcel_dimension(text) if text is not None else {}


def _read_s3_text(s3, uri: str) -> str | None:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        return body.read().decode("utf-8")
    finally:
        body.close()


def parse_weather_cache(text: str) -> dict[str, bool | None]:
    """Read the producer's hub observations without treating unknown weather as clear."""
    observations = json.loads(text).get("observations", {})
    weather = {}
    for hub, obs in observations.items():
        bad = obs.get("bad")
        if bad is not None and not isinstance(bad, bool):
            raise ValueError(f"Invalid weather state for hub {hub}: expected boolean or null")
        weather[hub] = None if obs.get("source") == "unknown" else bad
    return weather


def load_s3_weather(s3, uri: str) -> dict[str, bool | None]:
    """Reload a producer weather cache; missing objects give unknown weather, other errors fail the batch."""
    text = _read_s3_text(s3, uri)
    return parse_weather_cache(text) if text is not None else {}


def enrich_weather(events, weather: dict):
    """Attach the current hub snapshot, preserving every event and null for absent hubs."""
    from pyspark.sql import functions as F

    lookup = events.sparkSession.createDataFrame(list(weather.items()), "hub_id string, weather_bad boolean")
    return events.drop("weather_bad").join(F.broadcast(lookup), "hub_id", "left")


def prepare_glue_stream(events, rules: Rules):
    """Validate and deduplicate the Glue stream before its per-batch enrichment."""
    from pyspark.sql import functions as F

    # Glue 4 stores the full row in dedup state. Keep its existing schema across job upgrades.
    return (events.withColumn("_valid", valid_event_condition(rules))
            .withColumn("weather_bad", F.lit(None).cast("boolean"))
            .withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"]))


def add_breached(rows: list[dict], dimension: dict, accel: float) -> list[dict]:
    """Set row["breached"] on each latest-state row; dimension maps parcel_id to (created_at, sla_hours)."""
    for row in rows:
        created_at, sla_hours = dimension.get(row["parcel_id"], (None, None))
        row["breached"] = is_breached(row.get("status"), created_at, sla_hours, row.get("event_ts"), accel)
    return rows
