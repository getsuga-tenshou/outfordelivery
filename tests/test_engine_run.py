import json
import random
from collections import defaultdict
from types import SimpleNamespace

import pytest

from simulator import engine as engine_module
from simulator import producer as producer_module
from simulator import simulate, weather as weather_module
from simulator import statemachine as sm
from simulator.engine import SimulationEngine
from simulator.network import load_network
from simulator.simulate import load_sim_config
from streaming.batches import ProducerContext, publish_batch, read_batch


class Clock:
    def __init__(self):
        self.now = 100.0
        self.calls = 0

    def monotonic(self):
        self.calls += 1
        assert self.calls < 1000, "simulator did not terminate within the controlled-clock bound"
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(engine_module, "wallclock", clock)
    return clock


@pytest.fixture
def live_engine(clock):
    engine = SimulationEngine(load_sim_config(), load_network())
    engine.time_acceleration = 3600
    engine.np_rng = SimpleNamespace(exponential=lambda scale: 0.5)
    engine.plan_events = lambda parcel: [(0, sm.CREATED), (1, sm.DELIVERED)]
    return engine, clock


def test_duration_stops_admission_at_deadline_and_drains(live_engine):
    engine, clock = live_engine
    admitted, events = [], []

    def on_parcel(parcel):
        assert clock.now < 100.5, "parcel admitted at or after the duration deadline"
        admitted.append(parcel)

    count = engine.run(lambda key, event: events.append(event), on_parcel=on_parcel, max_runtime_s=0.5)

    assert len(admitted) == 1
    assert count == len(events) == 2
    assert [event["status"] for event in events] == [sm.CREATED, sm.DELIVERED]
    assert clock.now == 101.0


def test_duration_is_rechecked_during_parcel_admission(live_engine):
    engine, clock = live_engine
    admitted = []

    def on_parcel(parcel):
        assert clock.now < 100.5, "slow admission created a parcel after the deadline"
        admitted.append(parcel)
        clock.now += 0.6

    assert engine.run(lambda key, event: None, on_parcel=on_parcel, max_runtime_s=0.5) == 2
    assert len(admitted) == 1
    assert clock.now == 101.0


def test_duration_does_not_catch_up_arrivals_after_slow_emit(live_engine):
    engine, clock = live_engine
    events = []

    def emit(key, event):
        events.append(event)
        if len(events) == 1:
            clock.now += 2

    assert engine.run(emit, max_runtime_s=0.5) == 2
    assert len({event["parcel_id"] for event in events}) == 1
    assert events[-1]["status"] == sm.DELIVERED


def test_zero_duration_admits_no_parcels(live_engine):
    engine, clock = live_engine
    parcels = []

    assert engine.run(lambda key, event: pytest.fail("unexpected event"),
                      on_parcel=parcels.append, max_runtime_s=0) == 0
    assert parcels == []
    assert clock.now == 100


def test_duration_exits_at_deadline_when_no_events_remain(live_engine):
    engine, clock = live_engine
    engine.plan_events = lambda parcel: [(0, sm.DELIVERED)]
    events = []

    assert engine.run(lambda key, event: events.append(event), max_runtime_s=0.01) == 1
    assert len(events) == 1
    assert clock.now == pytest.approx(100.01)


@pytest.mark.parametrize(("duration", "max_parcels", "expected"), [
    (None, 2, 2), (10, 2, 2), (0.25, 10, 1), (0.75, None, 2),
])
def test_duration_and_parcel_limit_drain_whichever_closes_admission_first(
    live_engine, duration, max_parcels, expected,
):
    engine, clock = live_engine
    events = []

    count = engine.run(lambda key, event: events.append(event),
                       max_parcels=max_parcels, max_runtime_s=duration)

    assert count == expected * 2
    assert len({event["parcel_id"] for event in events}) == expected
    assert {e["parcel_id"] for e in events if e["status"] == sm.DELIVERED} == {e["parcel_id"] for e in events}
    assert clock.now == 101 + (expected - 1) * 0.5


@pytest.mark.parametrize("duration", [-1, float("nan"), float("inf"), float("-inf")])
def test_invalid_duration_is_rejected(live_engine, duration):
    engine, _ = live_engine
    with pytest.raises(ValueError, match="finite non-negative"):
        engine.run(lambda key, event: None, max_runtime_s=duration)


@pytest.mark.parametrize("duration", ["-1", "nan", "inf", "-inf"])
def test_cli_rejects_invalid_duration_before_producer_setup(monkeypatch, capsys, duration):
    monkeypatch.setattr(simulate, "load_env", lambda: None)
    monkeypatch.setattr(simulate, "run_produce", lambda args: pytest.fail("producer must not start"))
    with pytest.raises(SystemExit) as exc:
        simulate.main(["--produce", f"--duration={duration}"])
    assert exc.value.code == 2
    assert "finite non-negative" in capsys.readouterr().err


@pytest.mark.parametrize(("failed", "still_queued", "expected_exit"), [(0, 0, 0), (1, 0, 1), (0, 1, 1)])
def test_producer_duration_drains_flushes_and_preserves_export_context(
    tmp_path, monkeypatch, clock, failed, still_queued, expected_exit,
):
    config = load_sim_config()
    config["time_acceleration"] = 3600
    config["arrivals"]["new_parcels_per_minute"] = 120
    weather_config = weather_module.load_weather_config()
    provider_class = weather_module.WeatherProvider
    monkeypatch.setattr(simulate, "load_env", lambda: None)
    monkeypatch.setattr(simulate, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(simulate, "load_sim_config", lambda: config)
    monkeypatch.setattr(weather_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(weather_module, "load_weather_config", lambda: weather_config)
    monkeypatch.setattr(weather_module, "WeatherProvider",
                        lambda *args: provider_class(*args, rng=random.Random(11), offline=True))
    store = ProducerContext(tmp_path / "context")
    retained = store.root / "retained.txt"
    retained.parent.mkdir()
    retained.write_bytes(b"keep earlier producer context")
    dimensions_path = tmp_path / "data" / "parcels.jsonl"
    dimensions_path.parent.mkdir()
    old_dimension = b'{"parcel_id":"retained-from-an-earlier-run"}\n'
    dimensions_path.write_bytes(old_dimension)
    events, flush_times = [], []

    class Producer:
        delivered = 0

        def __init__(self, *args, **kwargs):
            self.failed = failed

        def produce(self, key, event):
            assert key == event["parcel_id"]
            store.pair([event], 3600)  # Context must be durable before sending, including during drain.
            events.append(event)
            self.delivered += 1

        def flush(self):
            flush_times.append(clock.now)
            return still_queued

    monkeypatch.setattr(producer_module, "EventProducer", Producer)
    assert simulate.main(["--produce", "--duration", "0.75", "--seed", "11",
                          "--context-dir", str(store.root)]) == expected_exit

    assert len(flush_times) == 1
    assert flush_times[0] > 100.75
    grouped = defaultdict(list)
    for event in events:
        grouped[event["parcel_id"]].append(event)
    assert grouped
    for journey in grouped.values():
        assert journey[0]["status"] == sm.CREATED
        assert journey[-1]["status"] in sm.TERMINAL
        assert [event["version"] for event in journey] == list(range(1, len(journey) + 1))
        assert all(sm.is_valid_transition(a["status"], b["status"]) for a, b in zip(journey, journey[1:]))
    dimensions, weather = store.pair(events, 3600)
    path = publish_batch(tmp_path / "batches", "duration-check", events, dimensions, weather, 3600,
                         {"kind": "replay"})
    batch = read_batch(path, 3600)
    assert batch["manifest"]["counts"] == {
        "events": len(events), "parcels": len(grouped), "weather_snapshots": 1,
    }
    assert retained.read_bytes() == b"keep earlier producer context"
    assert dimensions_path.read_bytes().startswith(old_dimension)
    assert [json.loads(line) for line in dimensions_path.read_text().splitlines()[1:]] == dimensions
