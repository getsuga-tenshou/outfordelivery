from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts import schedule_sync
from streaming.batches import atomic_json, digest, file_lock, publish_batch


@pytest.fixture
def scheduler(tmp_path, monkeypatch):
    monkeypatch.setattr(schedule_sync, "load_time_acceleration", lambda repo: 37)
    batches = tmp_path / "input batches"
    batches.mkdir()
    config = tmp_path / "scheduler" / "config.json"
    exports = tmp_path / "saved exports"
    return config, batches, exports


def initialize(scheduler):
    config, batches, exports = scheduler
    schedule_sync.initialize(config, batches, exports, 123, "ofd", "outfordelivery")


def publish(scheduler, paired_records, batch_id="stream-one", source="redpanda"):
    return publish_batch(scheduler[1], batch_id, *paired_records, 37, {"kind": source})


def complete(command, log):
    def value(flag):
        return command[command.index(flag) + 1]

    state_path = Path(value("--exports-dir")) / ".runs" / f"{value('--export-id')}.json"
    if command[2] == "sync":
        state = {"export_id": value("--export-id"),
                 "input_hash": digest(Path(value("--manifest")).read_bytes())}
    else:
        state = json.loads(state_path.read_bytes())
    atomic_json(state_path, {**state, "stage": "complete", "dbt_status": "success"})
    log.write_text("coordinator completed\n", encoding="utf-8")
    return 0


def no_cloud(*args):
    pytest.fail("Idle, invalid or blocked work must not invoke the coordinator")


def test_initialization_excludes_all_retained_ids_without_cloud_or_data_mutation(scheduler, paired_records):
    old = publish(scheduler, paired_records, "old-stream")
    unfinished = scheduler[1] / "unfinished-old"
    unfinished.mkdir()
    before = {path: path.read_bytes() for path in old.iterdir()}
    initialize(scheduler)
    state = schedule_sync.tick(scheduler[0], runner=no_cloud)
    assert state["status"] == "idle"
    assert state["excluded_batch_ids"] == ["old-stream", "unfinished-old"]
    assert all(path.read_bytes() == content for path, content in before.items())
    assert not scheduler[2].exists()
    with pytest.raises(ValueError, match="already initialized"):
        initialize(scheduler)


def test_new_stream_batch_completes_once_and_replays_and_partials_are_ignored(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records, "replay", source="replay")
    (scheduler[1] / "unfinished-new").mkdir()
    actual = publish(scheduler, paired_records)
    before = {path: path.read_bytes() for path in actual.iterdir()}
    calls = []

    def runner(command, log):
        calls.append(command)
        return complete(command, log)

    first = schedule_sync.tick(scheduler[0], runner=runner)
    assert first["status"] == "complete" and first["runs"][actual.name]["attempts"] == 1
    assert calls[0][2] == "sync" and str(actual / "manifest.json") in calls[0]
    assert schedule_sync.tick(scheduler[0], runner=no_cloud)["status"] == "idle"
    assert all(path.read_bytes() == content for path, content in before.items())


def test_failed_attempt_resumes_original_export_before_newer_or_corrupt_work(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records)
    calls = []

    def failure(command, log):
        calls.append(command)
        complete(command, log)
        run = next((scheduler[2] / ".runs").glob("*.json"))
        state = json.loads(run.read_bytes())
        atomic_json(run, {**state, "stage": "loaded", "dbt_status": "failed"})
        return 2

    failed = schedule_sync.tick(scheduler[0], runner=failure)
    assert failed["status"] == "failed" and failed["runs"]["stream-one"]["status"] == "failed"
    bad = scheduler[1] / "aaa-corrupt-new"
    bad.mkdir()
    (bad / "COMPLETE").write_text("invalid")

    def recovery(command, log):
        calls.append(command)
        return complete(command, log)

    recovered = schedule_sync.tick(scheduler[0], runner=recovery)
    assert recovered["status"] == "complete"
    assert recovered["runs"]["stream-one"]["attempts"] == 2
    assert calls[1][2] == "resume" and "--manifest" not in calls[1]
    assert calls[0][calls[0].index("--export-id") + 1] == calls[1][calls[1].index("--export-id") + 1]
    assert len(list((scheduler[0].parent / "logs").glob("*.log"))) == 2


def test_crash_before_coordinator_state_retries_sync_with_identical_identity(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records)
    calls = []

    def crash(command, log):
        calls.append(command)
        raise OSError("private connector details")

    with pytest.raises(OSError):
        schedule_sync.tick(scheduler[0], runner=crash)
    state = json.loads((scheduler[0].parent / "state.json").read_bytes())
    assert state["last_error"] == "OSError"
    assert "private connector details" not in json.dumps(state)

    def recover(command, log):
        calls.append(command)
        return complete(command, log)

    assert schedule_sync.tick(scheduler[0], runner=recover)["status"] == "complete"
    assert calls[0] == calls[1]


@pytest.mark.parametrize("fault", ["checksum", "missing-payload", "clock", "unsafe-id"])
def test_invalid_completed_inputs_never_start_cloud_work(scheduler, paired_records, fault):
    initialize(scheduler)
    actual = publish(scheduler, paired_records)
    if fault == "checksum":
        (actual / "COMPLETE").write_text("invalid")
    elif fault == "missing-payload":
        (actual / "parcels.jsonl").unlink()
    elif fault == "clock":
        manifest = json.loads((actual / "manifest.json").read_bytes())
        manifest["time_acceleration"] = 120
        atomic_json(actual / "manifest.json", manifest)
        (actual / "COMPLETE").write_text(digest((actual / "manifest.json").read_bytes()))
    else:
        actual.rename(actual.with_name("invalid.name"))
    with pytest.raises((ValueError, FileNotFoundError)):
        schedule_sync.tick(scheduler[0], runner=no_cloud)
    assert not scheduler[2].exists()


def test_pending_missing_input_blocks_later_batches(scheduler, paired_records):
    initialize(scheduler)
    actual = publish(scheduler, paired_records)
    assert schedule_sync.tick(scheduler[0], runner=lambda *args: 2)["status"] == "failed"
    (actual / "COMPLETE").unlink()
    publish(scheduler, paired_records, "later-stream")
    with pytest.raises(ValueError, match="Pending scheduled batch"):
        schedule_sync.tick(scheduler[0], runner=no_cloud)


def test_completed_identity_cannot_be_reassigned(scheduler, paired_records):
    initialize(scheduler)
    actual = publish(scheduler, paired_records)
    schedule_sync.tick(scheduler[0], runner=complete)
    manifest = json.loads((actual / "manifest.json").read_bytes())
    manifest["created_at"] = "2026-10-27T00:00:00+00:00"
    atomic_json(actual / "manifest.json", manifest)
    with pytest.raises(ValueError, match="identity changed"):
        schedule_sync.tick(scheduler[0], runner=no_cloud)


def test_overlapping_tick_is_rejected_and_does_not_touch_state(scheduler):
    initialize(scheduler)
    state = scheduler[0].parent / "state.json"
    before = state.read_bytes()
    with file_lock(scheduler[0].parent / ".scheduler.lock"), pytest.raises(RuntimeError, match="Another process"):
        schedule_sync.tick(scheduler[0], runner=no_cloud)
    assert state.read_bytes() == before


def test_lost_or_edited_state_fails_closed(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records)
    config = json.loads(scheduler[0].read_bytes())
    atomic_json(scheduler[0], {**config, "job_id": 999})
    with pytest.raises(ValueError, match="configuration/state changed"):
        schedule_sync.tick(scheduler[0], runner=no_cloud)
    atomic_json(scheduler[0], config)
    (scheduler[0].parent / "state.json").unlink()
    with pytest.raises(FileNotFoundError):
        schedule_sync.tick(scheduler[0], runner=no_cloud)


def test_zero_exit_without_successful_coordinator_state_does_not_complete(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records)
    with pytest.raises(FileNotFoundError):
        schedule_sync.tick(scheduler[0], runner=lambda *args: 0)
    state = json.loads((scheduler[0].parent / "state.json").read_bytes())
    assert state["status"] == "failed" and state["runs"]["stream-one"]["status"] == "pending"


def test_empty_stream_batch_never_starts_compute(scheduler):
    initialize(scheduler)
    publish_batch(scheduler[1], "empty-stream", [], [], {"snapshots": {}, "event_context": {}},
                  37, {"kind": "redpanda"})
    state = schedule_sync.tick(scheduler[0], runner=no_cloud)
    assert state["status"] == "empty" and state["runs"]["empty-stream"]["status"] == "empty"
    assert schedule_sync.tick(scheduler[0], runner=no_cloud)["status"] == "idle"
    assert not scheduler[2].exists()


def test_oldest_completed_batch_runs_first_with_one_transfer_per_tick(scheduler, paired_records):
    initialize(scheduler)
    publish(scheduler, paired_records, "aaa-newer")
    older = publish(scheduler, paired_records, "zzz-older")
    manifest = json.loads((older / "manifest.json").read_bytes())
    manifest["created_at"] = "2026-10-27T00:00:00+00:00"
    atomic_json(older / "manifest.json", manifest)
    (older / "COMPLETE").write_text(digest((older / "manifest.json").read_bytes()))
    first = schedule_sync.tick(scheduler[0], runner=complete)
    assert set(first["runs"]) == {"zzz-older"}
    second = schedule_sync.tick(scheduler[0], runner=complete)
    assert len(second["runs"]) == 2
    assert all(run["status"] == "complete" for run in second["runs"].values())


def test_status_detects_config_drift_without_touching_state(scheduler, monkeypatch, capsys):
    initialize(scheduler)
    monkeypatch.setattr(schedule_sync, "load_env", lambda: None)
    state = scheduler[0].parent / "state.json"
    before = state.read_bytes()
    config = json.loads(scheduler[0].read_bytes())
    atomic_json(scheduler[0], {**config, "job_id": 999})
    assert schedule_sync.main(["status", "--config", str(scheduler[0])]) == 2
    assert "ValueError" in capsys.readouterr().err
    assert state.read_bytes() == before


def test_subprocess_receives_paths_as_arguments_and_captures_diagnostics(tmp_path, monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        kwargs["stdout"].write(b"retained diagnostics\n")
        return subprocess.CompletedProcess(command, 2)

    monkeypatch.setattr(subprocess, "run", run)
    command = ["python.exe", "script with spaces.py", "resume"]
    log = tmp_path / "attempt.log"
    assert schedule_sync.run_coordinator(command, log) == 2
    assert calls[0][0] == command and calls[0][1]["cwd"] == schedule_sync.REPO
    assert log.read_text() == "retained diagnostics\n"
