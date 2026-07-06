import io
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from fastavro import parse_schema, schemaless_writer

from simulator import statemachine as sm
from simulator.engine import SimulationEngine
from simulator.network import load_network

REPO_ROOT = Path(__file__).resolve().parents[1]


def make_engine(seed=11):
    config = yaml.safe_load((REPO_ROOT / "config" / "simulation.yaml").read_text())
    return SimulationEngine(config, load_network(), seed=seed)


def group_by_parcel(events):
    grouped = defaultdict(list)
    for event in events:
        grouped[event["parcel_id"]].append(event)
    for evs in grouped.values():
        evs.sort(key=lambda e: e["version"])
    return grouped


def test_versions_are_contiguous_and_transitions_valid():
    events = make_engine().generate(300)
    for evs in group_by_parcel(events).values():
        assert [e["version"] for e in evs] == list(range(1, len(evs) + 1))
        for a, b in zip(evs, evs[1:]):
            assert sm.is_valid_transition(a["event_type"], b["event_type"]), (
                f"{a['event_type']} -> {b['event_type']}"
            )


def test_every_parcel_reaches_a_terminal_state():
    events = make_engine().generate(300)
    for evs in group_by_parcel(events).values():
        assert evs[-1]["event_type"] in sm.TERMINAL


def test_events_are_schema_conformant():
    schema = parse_schema(json.loads((REPO_ROOT / "schemas" / "parcel_event.avsc").read_text()))
    for event in make_engine().generate(100):
        buffer = io.BytesIO()
        schemaless_writer(buffer, schema, event)
        assert buffer.getvalue()


def test_coordinates_within_netherlands():
    net = load_network()
    box = net.bbox
    for event in make_engine().generate(200):
        assert box["lat_min"] <= event["lat"] <= box["lat_max"]
        assert box["lon_min"] <= event["lon"] <= box["lon_max"]


def test_promised_by_equals_created_plus_sla():
    engine = make_engine()
    now = datetime.now(timezone.utc)
    for _ in range(50):
        parcel = engine.build_parcel(now)
        assert parcel.promised_by == now + timedelta(hours=parcel.sla_hours)


def test_failure_and_return_rates_are_plausible():
    grouped = group_by_parcel(make_engine(seed=3).generate(3000))
    n = len(grouped)
    failed = sum(1 for evs in grouped.values() if any(e["event_type"] == sm.DELIVERY_FAILED for e in evs))
    returned = sum(1 for evs in grouped.values() if evs[-1]["event_type"] == sm.RETURNED)

    assert 0.02 <= failed / n <= 0.09

    assert returned / n <= 0.03
