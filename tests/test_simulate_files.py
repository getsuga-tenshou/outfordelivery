import json
from datetime import datetime, timedelta, timezone

import pytest

from simulator.network import load_network
from simulator.simulate import load_sim_config, main
from streaming.batches import enrich_events, read_batch
from streaming.validation import is_valid_event, not_in_future

REFERENCE = datetime(2026, 10, 24, 8, tzinfo=timezone.utc)


def test_files_sink_writes_complete_paired_batch(tmp_path):
    assert main(["--sink", "files", "--out", str(tmp_path), "--batch-id", "fixture",
                 "--count", "30", "--seed", "1", "--reference-time", REFERENCE.isoformat()]) == 0
    batch = read_batch(tmp_path / "fixture", 120)
    events, parcels = batch["events"], batch["parcels"]
    assert len(parcels) == 30
    dimensions = {parcel["parcel_id"]: parcel for parcel in parcels}
    assert {event["parcel_id"] for event in events} == set(dimensions)
    net = load_network()
    for event in events:
        event = {**event, "event_ts": datetime.fromisoformat(event["event_ts"])}
        assert is_valid_event(event, net.bbox, [hub.id for hub in net.hubs], REFERENCE)
        assert event["event_ts"] >= datetime.fromisoformat(dimensions[event["parcel_id"]]["created_at"])
    assert max(datetime.fromisoformat(event["event_ts"]) for event in events) == REFERENCE
    assert not not_in_future(REFERENCE + timedelta(minutes=5, microseconds=1), REFERENCE)
    assert all(event["weather_source"] == "synthetic" for event in enrich_events(batch))
    assert main(["--verify-batch", str(tmp_path / "fixture")]) == 0


def test_replay_never_overwrites_a_completed_batch(tmp_path):
    args = ["--sink", "files", "--out", str(tmp_path), "--batch-id", "same", "--count", "1"]
    main(args)
    original = (tmp_path / "same" / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError, match="reuse"):
        main(args)
    assert (tmp_path / "same" / "manifest.json").read_bytes() == original


def test_files_sink_default_stays_inside_ignored_data(tmp_path, monkeypatch):
    config = load_sim_config()
    monkeypatch.setattr("simulator.simulate.REPO_ROOT", tmp_path)
    monkeypatch.setattr("simulator.simulate.load_sim_config", lambda: config)
    assert main(["--sink", "files", "--batch-id", "default", "--count", "1"]) == 0
    path = tmp_path / "data" / "batches" / "default" / "manifest.json"
    assert json.loads(path.read_bytes())["counts"]["parcels"] == 1
