import copy
from datetime import datetime

import pytest

from streaming import batches
from streaming.batches import ProducerContext, enrich_events, file_lock, publish_batch, read_batch


def publish(root, records, batch_id="fixture"):
    return publish_batch(root, batch_id, *records, 37, {"kind": "replay"})


def test_manifest_rejects_mismatch_and_corruption(tmp_path, paired_records):
    path = publish(tmp_path, paired_records)
    with pytest.raises(ValueError, match="time_acceleration"):
        read_batch(path, 120)
    (path / "events.jsonl").write_bytes(b"corrupt\n")
    with pytest.raises(ValueError, match="checksum"):
        read_batch(path, 37)


@pytest.mark.parametrize("missing", ["dimensions", "weather", "acceleration", "deadline"])
def test_incomplete_pair_cannot_be_published(tmp_path, paired_records, missing):
    events, parcels, weather = copy.deepcopy(paired_records)
    if missing == "dimensions":
        parcels.pop()
    elif missing == "weather":
        weather["event_context"].pop(events[0]["event_id"])
    elif missing == "deadline":
        parcels[0]["promised_by"] = parcels[0]["created_at"]
    else:
        weather["snapshots"][next(iter(weather["snapshots"]))]["time_acceleration"] = 120
    with pytest.raises(ValueError):
        publish(tmp_path, (events, parcels, weather))
    assert not (tmp_path / "fixture").exists()
    assert not list(tmp_path.glob("*/COMPLETE"))


@pytest.mark.parametrize("failure_point", ["weather.json", "COMPLETE", "rename"])
def test_interrupted_publication_is_invisible_and_retryable(tmp_path, paired_records, monkeypatch, failure_point):
    original_write, original_replace = batches.durable_write, batches.os.replace

    def fail_write(path, data):
        if path.name == failure_point:
            raise OSError("injected disk failure")
        original_write(path, data)

    def fail_replace(source, target):
        if failure_point == "rename":
            raise OSError("injected rename failure")
        original_replace(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(batches, "durable_write", fail_write)
        patch.setattr(batches.os, "replace", fail_replace)
        with pytest.raises(OSError, match="injected"):
            publish(tmp_path, paired_records)
    assert not (tmp_path / "fixture").exists()
    path = publish(tmp_path, paired_records)
    assert read_batch(path, 37)["manifest"]["counts"]["parcels"] == 4


def test_os_lock_serializes_writers_and_releases_after_failure(tmp_path):
    path = tmp_path / "export.lock"
    with pytest.raises(ValueError):
        with file_lock(path):
            with pytest.raises(RuntimeError, match="Another process"):
                with file_lock(path):
                    pytest.fail("concurrent lock")
            raise ValueError("simulated failure")
    with file_lock(path):
        pass


def test_context_is_exact_and_weather_is_never_refetched(tmp_path, paired_records):
    events, parcels, weather = paired_records
    store = ProducerContext(tmp_path / "context")
    snapshot_id, snapshot = next(iter(weather["snapshots"].items()))
    assert store.save_weather(snapshot) == snapshot_id
    dimensions = {parcel["parcel_id"]: parcel for parcel in parcels}
    for event in events:
        store.save_event(event, dimensions[event["parcel_id"]], snapshot_id, 37)
    paired, captured = store.pair(events, 37)
    path = publish_batch(tmp_path, "paired", events, paired, captured, 37, {"kind": "replay"})
    enriched = enrich_events(read_batch(path, 37))
    for event in enriched:
        observation = snapshot["observations"][event["hub_id"]]
        assert event["weather_bad"] is observation["bad"]
        assert event["weather_source"] == observation["source"] == "synthetic"
    with pytest.raises(ValueError, match="time_acceleration"):
        store.pair(events, 120)
    with pytest.raises(ValueError, match="Missing producer context"):
        store.pair([{**events[0], "event_id": "absent"}], 37)


def test_explicit_unknown_weather_stays_unknown(tmp_path, paired_records):
    events, parcels, weather = copy.deepcopy(paired_records)
    snapshot = next(iter(weather["snapshots"].values()))
    for obs in snapshot["observations"].values():
        obs.update(source="unknown", bad=None, temperature=None, wind_speed=None, precipitation=None)
    snapshot_id = batches.digest(batches.json_bytes(snapshot))
    weather = {"snapshots": {snapshot_id: snapshot},
               "event_context": {event["event_id"]: snapshot_id for event in events}}
    batch = read_batch(publish(tmp_path, (events, parcels, weather)), 37)
    assert all(event["weather_bad"] is None for event in enrich_events(batch))


def test_context_rejects_conflicting_dimension(tmp_path, paired_records):
    events, parcels, weather = paired_records
    store = ProducerContext(tmp_path)
    snapshot_id = store.save_weather(next(iter(weather["snapshots"].values())))
    first = events[0]
    parcel = next(parcel for parcel in parcels if parcel["parcel_id"] == first["parcel_id"])
    store.save_event(first, parcel, snapshot_id, 37)
    with pytest.raises(ValueError, match="different producer context"):
        store.save_event(first, {**parcel, "carrier": "changed"}, snapshot_id, 37)


def test_batch_paths_cannot_escape_root(tmp_path, paired_records):
    for bad_id in ("../escape", "a/b", ".partial", "a\\b", ""):
        with pytest.raises(ValueError, match="batch_id"):
            publish(tmp_path, paired_records, bad_id)


def test_versions_and_compressed_timestamps_are_monotonic(paired_records):
    events, parcels, _ = paired_records
    for parcel in parcels:
        timeline = [event for event in events if event["parcel_id"] == parcel["parcel_id"]]
        times = [datetime.fromisoformat(event["event_ts"]) for event in timeline]
        assert times == sorted(times)
        assert [event["version"] for event in timeline] == list(range(1, len(timeline) + 1))
        assert times[0] == datetime.fromisoformat(parcel["created_at"])
