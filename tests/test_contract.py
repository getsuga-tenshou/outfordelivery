import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastavro import parse_schema, schemaless_writer

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA = parse_schema(json.loads((REPO_ROOT / "schemas" / "parcel_event.avsc").read_text(encoding="utf-8")))


def valid_event() -> dict:
    return {
        "event_id": "evt-1",
        "parcel_id": "P1000000001",
        "event_type": "CREATED",
        "hub_id": "AMS",
        "lat": 52.3,
        "lon": 4.9,
        "event_ts": datetime.now(timezone.utc),
        "status": "CREATED",
        "version": 1,
    }


def encode(event: dict) -> bytes:
    buffer = io.BytesIO()
    schemaless_writer(buffer, SCHEMA, event)
    return buffer.getvalue()


def test_valid_event_is_accepted():
    assert encode(valid_event())


def test_invalid_event_type_is_rejected():
    bad = valid_event()
    bad["event_type"] = "TELEPORTED"
    with pytest.raises(Exception):
        encode(bad)


def test_missing_required_field_is_rejected():
    bad = valid_event()
    del bad["parcel_id"]
    with pytest.raises(Exception):
        encode(bad)


def test_wrong_type_is_rejected():
    bad = valid_event()
    bad["lat"] = "north"
    with pytest.raises(Exception):
        encode(bad)
