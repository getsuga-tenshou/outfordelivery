import json
import random
from datetime import datetime, timezone

import pytest

from simulator.engine import SimulationEngine
from simulator.network import load_network
from simulator.simulate import _parcel_record, load_sim_config
from simulator.weather import WeatherProvider, load_weather_config
from streaming.batches import digest, json_bytes


@pytest.fixture
def paired_records():
    config = load_sim_config()
    config["time_acceleration"] = 37.0
    reference = datetime(2026, 10, 20, 8, tzinfo=timezone.utc)
    weather = WeatherProvider(load_weather_config(), load_network(), rng=random.Random(1), offline=True)
    weather.fetch()
    snapshot = weather.snapshot(37, reference)
    snapshot_id = digest(json_bytes(snapshot))
    engine = SimulationEngine(config, load_network(), weather_is_bad=weather.is_bad, seed=8)
    parcels = []
    events = engine.generate(4, reference, on_parcel=lambda parcel: parcels.append(_parcel_record(parcel)))
    events = json.loads(json_bytes(events))
    return events, parcels, {"snapshots": {snapshot_id: snapshot},
                             "event_context": {event["event_id"]: snapshot_id for event in events}}
