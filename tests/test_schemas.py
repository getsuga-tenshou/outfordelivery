import json
from pathlib import Path

from fastavro import parse_schema

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = REPO_ROOT / "schemas"


def _load(filename: str) -> dict:
    return json.loads((SCHEMA_DIR / filename).read_text(encoding="utf-8"))


def test_parcel_event_schema_parses():
    raw = _load("parcel_event.avsc")
    parse_schema(raw)
    names = {field["name"] for field in raw["fields"]}
    assert {"event_id", "parcel_id", "event_type", "event_ts", "version"} <= names


def test_parcel_schema_parses():
    raw = _load("parcel.avsc")
    parse_schema(raw)
    names = {field["name"] for field in raw["fields"]}
    assert {"parcel_id", "service_level", "sla_hours", "promised_by"} <= names


def test_event_type_symbols_cover_state_machine():
    raw = _load("parcel_event.avsc")
    event_type = next(f for f in raw["fields"] if f["name"] == "event_type")
    symbols = set(event_type["type"]["symbols"])
    expected = {
        "CREATED",
        "LABEL_PRINTED",
        "COLLECTED",
        "AT_SORTING_HUB",
        "IN_TRANSIT",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "DELIVERY_FAILED",
        "RESCHEDULED",
        "RETURNED",
    }
    assert expected <= symbols
