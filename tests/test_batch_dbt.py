import copy
import json
import shutil
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest
import yaml

from scripts import run_dbt
from streaming.batches import digest, enrich_events, json_bytes, publish_batch, read_batch
from streaming.rules import is_breached

REPO = Path(__file__).resolve().parents[1]


# Exported Silver snapshots carry parcel dimensions as Parquet; producer batches carry JSON Lines.
@pytest.mark.parametrize("parcels_format", ["jsonl", "parquet"])
def test_same_paired_snapshot_matches_python_and_real_duckdb_dbt(tmp_path, paired_records, monkeypatch, parcels_format):
    original, parcels, weather = copy.deepcopy(paired_records)
    acceleration = 40.0
    created = datetime(2026, 10, 20, 8, tzinfo=timezone.utc)
    events = []
    expected = defaultdict(lambda: {"parcels": 0, "delivered": 0, "on_time": 0})
    for index, parcel in enumerate(parcels):
        parcel["created_at"] = created.isoformat()
        deadline = created + timedelta(seconds=parcel["sla_hours"] * 3600 / acceleration)
        parcel["promised_by"] = deadline.isoformat()
        first = next(event for event in original if event["parcel_id"] == parcel["parcel_id"])
        status = "RETURNED" if index == 3 else "DELIVERED"
        # Exact boundary, one millisecond late, early delivery, and a terminal return.
        outcome_ts = deadline + timedelta(milliseconds=[0, 1, -1, 1000][index])
        outcome = {**first, "event_id": first["event_id"] + "-outcome", "event_type": status,
                   "status": status, "version": 2, "event_ts": outcome_ts.isoformat()}
        events.extend([{**first, "event_ts": created.isoformat()}, outcome])
        late = is_breached("OUT_FOR_DELIVERY", created, parcel["sla_hours"], outcome_ts, acceleration)
        assert late is (index in (1, 3))
        assert not is_breached(status, created, parcel["sla_hours"], outcome_ts, acceleration)
        region = expected[parcel["destination_region"]]
        region["parcels"] += 1
        region["delivered"] += status == "DELIVERED"
        region["on_time"] += status == "DELIVERED" and not late
    snapshot = next(iter(weather["snapshots"].values()))
    snapshot["time_acceleration"] = acceleration
    snapshot_id = digest(json_bytes(snapshot))
    weather = {"snapshots": {snapshot_id: snapshot},
               "event_context": {event["event_id"]: snapshot_id for event in events}}
    path = publish_batch(tmp_path / "batches", "boundaries", events, parcels, weather, acceleration, {"kind": "replay"})
    batch = read_batch(path, acceleration)
    enriched_path = tmp_path / "enriched.jsonl"
    enriched_path.write_bytes(b"".join(json_bytes(event) for event in enrich_events(batch)))
    silver = tmp_path / "silver"
    silver.mkdir()
    with duckdb.connect() as con:
        con.sql("select event_id, parcel_id, event_type, status, hub_id, lat, lon, "
                "cast(event_ts as timestamp) as event_ts, version, weather_bad, "
                "cast(event_ts as date) as event_date from read_json_auto(?)",
                params=[str(enriched_path)]).write_parquet(str(silver / "events.parquet"))
        parcels_path = (path / "parcels.jsonl").as_posix()
        if parcels_format == "parquet":
            (tmp_path / "parcels").mkdir()
            con.sql("select * replace (cast(created_at as timestamp) as created_at, "
                    "cast(promised_by as timestamp) as promised_by) from read_json_auto(?)",
                    params=[parcels_path]).write_parquet(str(tmp_path / "parcels" / "part-0.parquet"))
            parcels_path = (tmp_path / "parcels" / "*.parquet").as_posix()
    project = tmp_path / "dbt"
    shutil.copytree(REPO / "dbt", project,
                    ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", "*.duckdb", "*.wal"))
    database = tmp_path / "marts.duckdb"
    (project / "profiles.yml").write_text(yaml.safe_dump({"outfordelivery": {
        "target": "dev", "outputs": {"dev": {"type": "duckdb", "path": str(database), "threads": 1}}
    }}), encoding="utf-8")
    variables = {"time_acceleration": batch["manifest"]["time_acceleration"],
                 "silver_events_path": silver.as_posix(), "parcels_path": parcels_path}
    monkeypatch.setattr(run_dbt, "load_time_acceleration", lambda repo: acceleration)
    assert run_dbt.main(["build", "--batch", str(path / "manifest.json"), "--project-dir", str(project),
                         "--profiles-dir", str(project), "--target", "dev", "--vars", json.dumps(variables)]) == 0
    with duckdb.connect(str(database), read_only=True) as con:
        rows = con.execute("select region, parcels, delivered, on_time, on_time_rate from gold_on_time_by_region").fetchall()
    assert len(rows) == len(expected)
    for region, count, delivered, on_time, rate in rows:
        assert {"parcels": count, "delivered": delivered, "on_time": on_time} == expected[region]
        assert rate == (round(on_time / delivered, 4) if delivered else None)


def test_dbt_cannot_override_the_shared_clock_or_accept_an_incompatible_batch(tmp_path, paired_records, monkeypatch):
    def unexpected_run(*args, **kwargs):
        raise AssertionError("dbt must not start on an incompatible clock")

    monkeypatch.setattr(run_dbt.subprocess, "run", unexpected_run)
    assert run_dbt.main(["build", "--vars", '{"time_acceleration": 1}']) == 2
    path = publish_batch(tmp_path, "other-clock", *paired_records, 37, {"kind": "replay"})
    assert run_dbt.main(["build", "--batch", str(path)]) == 2
