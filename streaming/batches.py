from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastavro.validation import validate

from streaming.rules import as_utc, checked_acceleration

REPO = Path(__file__).resolve().parents[1]
DATA_FILES = ("events.jsonl", "parcels.jsonl", "weather.json")
SCHEMA_VERSION = 1


def json_bytes(value) -> bytes:
    def timestamp(value):
        if isinstance(value, datetime):
            return as_utc(value).isoformat()
        raise TypeError(f"Cannot serialize {type(value)}")

    return (json.dumps(value, sort_keys=True, allow_nan=False, default=timestamp) + "\n").encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sync_directory(path: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def durable_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    _sync_directory(path.parent)


def atomic_json(path: Path, value) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    durable_write(temporary, json_bytes(value))
    os.replace(temporary, path)
    _sync_directory(path.parent)


def safe_batch_id(batch_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", batch_id):
        raise ValueError("batch_id must be 1-100 letters, digits, underscores or hyphens")
    return batch_id


@contextmanager
def file_lock(path: Path):
    """OS lock releases on process death; the persistent lock file is harmless."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            def lock():
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

            def unlock():
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def lock():
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def unlock():
                fcntl.flock(handle, fcntl.LOCK_UN)
        try:
            lock()
        except OSError as exc:
            raise RuntimeError(f"Another process holds {path}") from exc
        try:
            yield
        finally:
            unlock()


def _schema_rows(rows: list[dict], name: str, timestamps: tuple[str, ...]) -> None:
    schema = json.loads((REPO / "schemas" / name).read_text(encoding="utf-8"))
    for row in rows:
        typed = dict(row)
        for field in timestamps:
            value = typed[field]
            typed[field] = as_utc(datetime.fromisoformat(value) if isinstance(value, str) else value)
        validate(typed, schema)


def validate_payload(events: list[dict], parcels: list[dict], weather: dict, acceleration: float) -> None:
    acceleration = checked_acceleration(acceleration)
    _schema_rows(events, "parcel_event.avsc", ("event_ts",))
    _schema_rows(parcels, "parcel.avsc", ("created_at", "promised_by"))
    dimensions = {row["parcel_id"]: row for row in parcels}
    if len(dimensions) != len(parcels):
        raise ValueError("Duplicate parcel dimensions")
    ids = {event["event_id"] for event in events}
    if len(ids) != len(events):
        raise ValueError("Duplicate event IDs")
    if {event["parcel_id"] for event in events} != set(dimensions):
        raise ValueError("Missing or unmatched parcel dimensions; use the producer's saved context")
    for parcel in parcels:
        created = as_utc(datetime.fromisoformat(parcel["created_at"]))
        deadline = as_utc(datetime.fromisoformat(parcel["promised_by"]))
        if deadline != created + timedelta(seconds=parcel["sla_hours"] * 3600 / acceleration):
            raise ValueError("Parcel deadline disagrees with time_acceleration; regenerate with the current producer")
    contexts = weather["event_context"]
    snapshots = weather["snapshots"]
    if set(contexts) != ids or set(contexts.values()) != set(snapshots):
        raise ValueError("Missing or unmatched event weather context")
    for snapshot_id, snapshot in snapshots.items():
        if digest(json_bytes(snapshot)) != snapshot_id:
            raise ValueError("Weather snapshot checksum mismatch")
        if snapshot["time_acceleration"] != acceleration:
            raise ValueError("Weather snapshot time_acceleration mismatch")
    for event in events:
        observations = snapshots[contexts[event["event_id"]]]["observations"]
        origin = dimensions[event["parcel_id"]]["origin_hub"]
        for hub in {origin, event["hub_id"]}:
            obs = observations.get(hub)
            if not obs or not obs.get("source") or obs.get("hub_id") != hub:
                raise ValueError(f"Missing weather for hub {hub}; provide the captured snapshot")
            if not isinstance(obs.get("bad"), bool) and not (obs["source"] == "unknown" and obs.get("bad") is None):
                raise ValueError(f"Invalid weather state for hub {hub}")


def read_batch(path: Path, expected_acceleration: float) -> dict:
    path = Path(path)
    raw = (path / "manifest.json").read_bytes()
    if (path / "COMPLETE").read_text(encoding="utf-8").strip() != digest(raw):
        raise ValueError("Missing or invalid batch completion marker")
    manifest = json.loads(raw)
    if manifest["schema_version"] != SCHEMA_VERSION or manifest["batch_id"] != path.name:
        raise ValueError("Unsupported batch schema or mismatched batch ID")
    if manifest["time_acceleration"] != checked_acceleration(expected_acceleration):
        raise ValueError("Batch time_acceleration differs from this run's configuration")
    if set(manifest["files"]) != set(DATA_FILES):
        raise ValueError("Batch must contain exactly events, parcels and weather")
    payload = {}
    for name in DATA_FILES:
        data = (path / name).read_bytes()
        if manifest["files"][name] != {"sha256": digest(data), "bytes": len(data)}:
            raise ValueError(f"Batch checksum/size mismatch: {name}")
        payload[name] = data
    events = [json.loads(line) for line in payload["events.jsonl"].splitlines()]
    parcels = [json.loads(line) for line in payload["parcels.jsonl"].splitlines()]
    weather = json.loads(payload["weather.json"])
    counts = {"events": len(events), "parcels": len(parcels), "weather_snapshots": len(weather["snapshots"])}
    if manifest["counts"] != counts:
        raise ValueError("Batch row counts mismatch")
    validate_payload(events, parcels, weather, expected_acceleration)
    return {"manifest": manifest, "events": events, "parcels": parcels, "weather": weather}


def publish_batch(root: Path, batch_id: str, events: list[dict], parcels: list[dict], weather: dict,
                  acceleration: float, source: dict) -> Path:
    batch_id = safe_batch_id(batch_id)
    root = Path(root)
    with file_lock(root / ".locks" / f"{batch_id}.lock"):
        target = root / batch_id
        if target.exists():
            raise FileExistsError(f"Batch {target} already exists; reuse its manifest instead of regenerating it")
        # Normalize JSON timestamps before validating and writing the paired datasets.
        events, parcels = json.loads(json_bytes(events)), json.loads(json_bytes(parcels))
        validate_payload(events, parcels, weather, acceleration)
        staging = root / f".{batch_id}.{uuid.uuid4().hex}.partial"
        staging.mkdir()
        files = {
            "events.jsonl": b"".join(json_bytes(row) for row in events),
            "parcels.jsonl": b"".join(json_bytes(row) for row in parcels),
            "weather.json": json_bytes(weather),
        }
        for name, data in files.items():
            durable_write(staging / name, data)
            if (staging / name).read_bytes() != data:
                raise OSError(f"Batch file verification failed: {name}")
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "batch_id": batch_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "time_acceleration": checked_acceleration(acceleration),
            "counts": {"events": len(events), "parcels": len(parcels), "weather_snapshots": len(weather["snapshots"])},
            "files": {name: {"sha256": digest(data), "bytes": len(data)} for name, data in files.items()},
            "source": source,
        }
        raw = json_bytes(manifest)
        durable_write(staging / "manifest.json", raw)
        durable_write(staging / "COMPLETE", (digest(raw) + "\n").encode())
        os.replace(staging, target)
        _sync_directory(root)
        return target


def enrich_events(batch: dict) -> list[dict]:
    """Use only captured weather, for ticket 17's batch ingestion and local parity checks."""
    weather = batch["weather"]
    result = []
    for event in batch["events"]:
        snapshot_id = weather["event_context"][event["event_id"]]
        obs = weather["snapshots"][snapshot_id]["observations"][event["hub_id"]]
        result.append({**event, "weather_bad": obs["bad"], "weather_source": obs["source"]})
    return result


class ProducerContext:
    """Immutable weather snapshots and per-event dimension records saved before Kafka send."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def save_weather(self, snapshot: dict) -> str:
        snapshot_id = digest(json_bytes(snapshot))
        atomic_json(self.root / "weather" / f"{snapshot_id}.json", snapshot)
        return snapshot_id

    def save_event(self, event: dict, parcel: dict, snapshot_id: str, acceleration: float) -> None:
        path = self.root / "events" / f"{digest(event['event_id'].encode())}.json"
        record = {"event_id": event["event_id"], "parcel": parcel, "weather_id": snapshot_id,
                  "time_acceleration": checked_acceleration(acceleration)}
        if path.exists() and json.loads(path.read_bytes()) != record:
            raise ValueError("Event ID already has different producer context")
        atomic_json(path, record)

    def pair(self, events: list[dict], acceleration: float) -> tuple[list[dict], dict]:
        parcels, contexts, snapshots = {}, {}, {}
        for event in events:
            path = self.root / "events" / f"{digest(event['event_id'].encode())}.json"
            if not path.exists():
                raise ValueError(f"Missing producer context for event {event['event_id']}; retain --context-dir from production")
            record = json.loads(path.read_bytes())
            if record["event_id"] != event["event_id"] or record["parcel"]["parcel_id"] != event["parcel_id"]:
                raise ValueError("Producer context identity mismatch")
            if record["time_acceleration"] != acceleration:
                raise ValueError("Producer time_acceleration differs from exporter configuration")
            parcel = record["parcel"]
            known = parcels.setdefault(event["parcel_id"], parcel)
            if known != parcel:
                raise ValueError("Conflicting dimensions for one parcel ID; export separate producer runs")
            snapshot_id = record["weather_id"]
            contexts[event["event_id"]] = snapshot_id
            if snapshot_id not in snapshots:
                snapshots[snapshot_id] = json.loads((self.root / "weather" / f"{snapshot_id}.json").read_bytes())
        return list(parcels.values()), {"snapshots": snapshots, "event_context": contexts}
