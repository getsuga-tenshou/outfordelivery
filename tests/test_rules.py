from pathlib import Path

import pytest

from streaming.rules import STATE_FIELDS, load_rules, valid_event_condition

REPO = Path(__file__).resolve().parent.parent


def test_load_rules_reads_network_and_schema():
    rules = load_rules(REPO)
    assert rules.hub_ids == ["AMS", "RTM", "UTR", "EIN", "DHG", "GRO", "ZWO", "MST"]
    assert rules.bbox == {"lat_min": 50.75, "lat_max": 53.55, "lon_min": 3.36, "lon_max": 7.23}
    assert '"name":"ParcelEvent"' in rules.avro_schema_json.replace(" ", "")
    assert rules.future_tolerance_minutes == 5


def test_state_fields_cover_dynamo_attributes():
    assert STATE_FIELDS[0] == "version"
    assert {"status", "event_type", "event_ts", "hub_id", "lat", "lon", "weather_bad"} <= set(STATE_FIELDS)


def test_valid_event_condition_builds():
    pytest.importorskip("pyspark")
    assert valid_event_condition(load_rules(REPO)) is not None
