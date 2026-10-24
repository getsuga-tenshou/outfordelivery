from pathlib import Path

import pytest

from streaming.databricks_helpers import (
    checkpoint_path,
    ddl_from_avro,
    events_dir,
    parse_trigger,
    table_names,
)
from streaming.rules import load_rules

REPO = Path(__file__).resolve().parent.parent


def test_parse_trigger():
    assert parse_trigger("availableNow") == {"availableNow": True}
    assert parse_trigger("processingTime=30 seconds") == {"processingTime": "30 seconds"}
    with pytest.raises(ValueError):
        parse_trigger("continuous")


def test_paths_and_table_names():
    assert checkpoint_path("c", "bronze") == "/Volumes/c/ops/checkpoints/bronze"
    assert events_dir("c") == "/Volumes/c/ops/raw/events/"
    assert table_names("c")["bronze"] == "c.bronze.parcel_events"
    assert table_names("c")["dlq"] == "c.bronze.dlq_parcel_events"


def test_ddl_from_avro_matches_event_schema():
    ddl = ddl_from_avro(load_rules(REPO).avro_schema_json)
    assert ddl.split(", ") == [
        "event_id string",
        "parcel_id string",
        "event_type string",
        "hub_id string",
        "lat double",
        "lon double",
        "event_ts string",
        "status string",
        "version int",
    ]
