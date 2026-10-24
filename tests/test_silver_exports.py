from __future__ import annotations

import copy
import io
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from databricks.sdk.errors import NotFound

from streaming.batches import digest, json_bytes
from streaming.rules import as_utc
from streaming.silver_exports import (
    COLUMNS,
    cleanup_export,
    download_export,
    export_path,
    file_metadata,
    read_export,
)


@pytest.fixture
def snapshot(tmp_path, paired_records):
    events, parcels, _ = copy.deepcopy(paired_records)
    root = tmp_path / "exports" / "snapshot"
    types = {"string": pa.string(), "int": pa.int32(), "double": pa.float64(), "boolean": pa.bool_(),
             "timestamp": pa.timestamp("us", tz="UTC"), "date": pa.date32()}
    events = [{**event, "event_ts": as_utc(datetime.fromisoformat(event["event_ts"])),
               "event_date": as_utc(datetime.fromisoformat(event["event_ts"])).date(), "weather_bad": None}
              for event in events]
    for parcel in parcels:
        for name in ("created_at", "promised_by"):
            parcel[name] = as_utc(datetime.fromisoformat(parcel[name]))
    datasets = {}
    for kind, rows in (("events", events), ("parcels", parcels)):
        file = root / kind / "part-0.parquet"
        file.parent.mkdir(parents=True)
        schema = pa.schema([(name, types[typ]) for name, typ, _ in COLUMNS[kind]])
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), file)
        datasets[kind] = {"rows": len(rows), "delta_version": 5,
                          "schema": [[name, typ] for name, typ, _ in COLUMNS[kind]],
                          "files": {f"{kind}/{file.name}": file_metadata(file)}}
    timestamps = [row["event_ts"] for row in events]
    datasets["events"]["event_time"] = {"min": min(timestamps).isoformat(), "max": max(timestamps).isoformat()}
    manifest = {"schema_version": 1, "mode": "full_snapshot", "export_id": root.name,
                "time_acceleration": 37, "created_at": "2026-10-24T00:00:00+00:00",
                "upstream": {"run_id": "123", "selected_batch_id": "paired",
                             "batches": [{"batch_id": "paired", "manifest_hash": "a" * 64}]}, "datasets": datasets}
    rewrite_manifest(root, manifest)
    return root


def rewrite_manifest(path, manifest):
    raw = json_bytes(manifest)
    (path / "manifest.json").write_bytes(raw)
    (path / "COMPLETE").write_text(digest(raw) + "\n")


class Files:
    def __init__(self, snapshot):
        self.remote = export_path("c", snapshot.name)
        self.data = {f"{self.remote}/{file.relative_to(snapshot).as_posix()}": file.read_bytes()
                     for file in snapshot.rglob("*") if file.is_file()}
        self.failure = None
        self.downloaded = []

    def download(self, path):
        if path.endswith(self.failure or "never-match"):
            raise OSError("injected partial download")
        self.downloaded.append(path)
        if path not in self.data:
            raise NotFound("missing")
        return SimpleNamespace(contents=io.BytesIO(self.data[path]))

    def create_directory(self, path):
        pass

    def upload(self, path, content, overwrite=False):
        assert not overwrite and path not in self.data
        self.data[path] = content.read()


def test_download_retains_verified_files_and_resume_reuses_saved_files(snapshot, tmp_path):
    files = Files(snapshot)
    root = tmp_path / "download"
    files.failure = "parcels/part-0.parquet"
    with pytest.raises(OSError, match="partial"):
        download_export(files, "c", snapshot.name, root, 37)
    local = root / snapshot.name
    assert not (local / "COMPLETE").exists()
    saved_event = (local / "events/part-0.parquet").stat().st_mtime_ns
    files.failure = None
    assert download_export(files, "c", snapshot.name, root, 37) == local
    assert read_export(local, 37) == read_export(snapshot, 37)
    assert (local / "events/part-0.parquet").stat().st_mtime_ns == saved_event
    assert download_export(files, "c", snapshot.name, root, 37) == local


@pytest.mark.parametrize("fault", ["missing", "bytes", "schema", "orphan", "duplicate", "acceleration", "bounds", "deadline"])
def test_invalid_snapshots_fail_before_publication(snapshot, fault):
    manifest = json.loads((snapshot / "manifest.json").read_bytes())
    file = snapshot / "events/part-0.parquet"
    if fault == "missing":
        file.unlink()
    elif fault == "bytes":
        file.write_bytes(b"corrupt")
    elif fault == "acceleration":
        manifest["time_acceleration"] = 120
    elif fault == "bounds":
        manifest["datasets"]["events"]["event_time"]["max"] = "2099-01-01T00:00:00+00:00"
    else:
        table = pq.ParquetFile(file).read()
        rows = table.to_pylist()
        if fault == "schema":
            table = table.drop(["status"])
        elif fault == "orphan":
            rows[0]["parcel_id"] = "absent"
            table = pa.Table.from_pylist(rows, schema=table.schema)
        elif fault == "duplicate":
            rows[1]["event_id"] = rows[0]["event_id"]
            table = pa.Table.from_pylist(rows, schema=table.schema)
        else:
            file = snapshot / "parcels/part-0.parquet"
            table = pq.ParquetFile(file).read()
            rows = table.to_pylist()
            rows[0]["promised_by"] = rows[0]["created_at"]
            table = pa.Table.from_pylist(rows, schema=table.schema)
        pq.write_table(table, file)
        kind = file.parent.name
        manifest["datasets"][kind]["files"][f"{kind}/{file.name}"] = file_metadata(file)
    rewrite_manifest(snapshot, manifest)
    with pytest.raises((ValueError, FileNotFoundError)):
        read_export(snapshot, 37)


def test_remote_corruption_and_identity_conflict_do_not_complete(snapshot, tmp_path):
    files = Files(snapshot)
    files.data[files.remote + "/events/part-0.parquet"] = b"bad remote bytes"
    root = tmp_path / "download"
    with pytest.raises(ValueError, match="checksum"):
        download_export(files, "c", snapshot.name, root, 37)
    assert not (root / snapshot.name / "COMPLETE").exists()
    files = Files(snapshot)
    download_export(files, "c", snapshot.name, root, 37)
    manifest = read_export(snapshot, 37)
    manifest["upstream"]["run_id"] = "456"
    raw = json_bytes(manifest)
    files.data[files.remote + "/manifest.json"] = raw
    files.data[files.remote + "/COMPLETE"] = digest(raw).encode()
    with pytest.raises(ValueError, match="different local bytes"):
        download_export(files, "c", snapshot.name, root, 37)


def test_cleanup_preserves_last_verified_copy_and_rejects_path_escape(snapshot):
    with pytest.raises(ValueError, match="last verified"):
        cleanup_export(snapshot.parent, snapshot.name, 37)
    with pytest.raises(ValueError):
        cleanup_export(snapshot.parent, "../escape", 37)


def test_freshness_uses_successful_audit_without_rewriting_event_time():
    import yaml
    from jinja2 import Environment

    text = (Path(__file__).resolve().parents[1] / "dbt/models/staging/_sources.yml").read_text()
    for mode in ("native", "iceberg"):
        rendered = Environment().from_string(text).render(target=SimpleNamespace(type="snowflake"),
                                                          var=lambda *args: mode)
        tables = yaml.safe_load(rendered)["sources"][0]["tables"]
        for table in tables:
            query = table["config"]["loaded_at_query"]
            assert ("LOAD_AUDIT" in query) == (mode == "native")
            assert "freshness" in table["config"]


class LocalWarehouse:
    """Run publication DML in a real transactional database; adapt only Snowflake-specific transport/commands."""

    def __init__(self, path):
        import duckdb

        from streaming.snowflake_load import LAKE, SF_TYPES, TABLES

        self.db = duckdb.connect()
        self.db.execute("SET TimeZone='UTC'")
        self.db.execute("ATTACH ':memory:' AS OUTFORDELIVERY")
        self.db.execute("CREATE SCHEMA OUTFORDELIVERY.LAKE")
        self.path = path
        self.fail = None
        self.partial_copy = False
        self.sql = []
        for kind, columns in COLUMNS.items():
            definitions = ", ".join(f"{name} {SF_TYPES[typ]}" for name, typ, _ in columns)
            definitions = definitions.replace("NUMBER(38,0)", "BIGINT").replace("TIMESTAMP_NTZ(6)", "TIMESTAMP")
            for prefix in ("", "LOADING_"):
                self.db.execute(f"CREATE TABLE {LAKE}.{prefix}{TABLES[kind]} ({definitions})")
        self.db.execute(f"CREATE TABLE {LAKE}.LOAD_LOCK (ID INTEGER, OWNER VARCHAR)")
        self.db.execute(f"INSERT INTO {LAKE}.LOAD_LOCK VALUES (1,'seed')")
        self.db.execute(f"CREATE TABLE {LAKE}.LOAD_AUDIT (EXPORT_ID VARCHAR, MANIFEST_HASH VARCHAR, LOADED_AT TIMESTAMP, "
                        "TIME_ACCELERATION DOUBLE, EVENTS_VERSION BIGINT, PARCELS_VERSION BIGINT, EVENTS_COUNT BIGINT, "
                        "PARCELS_COUNT BIGINT, EVENT_MIN TIMESTAMP, EVENT_MAX TIMESTAMP, MANIFEST_JSON VARCHAR)")

    def cursor(self):
        return WarehouseCursor(self)

    def close(self):
        pass

    def published(self):
        return [self.db.execute(f"SELECT * FROM OUTFORDELIVERY.LAKE.{name} ORDER BY 1").fetchall()
                for name in ("SILVER_PARCEL_EVENTS", "SILVER_PARCELS", "LOAD_AUDIT")]


class WarehouseCursor:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.result = []
        self.rowcount = -1

    def execute(self, sql, params=None):
        import re

        warehouse = self.warehouse
        warehouse.sql.append(sql)
        if warehouse.fail and warehouse.fail in sql:
            raise RuntimeError("injected Snowflake statement failure")
        if sql == "SELECT CURRENT_ROLE()":
            self.result = [("OFD_LOADER",)]
        elif sql.startswith("ALTER SESSION"):
            self.result = []
        elif sql.startswith("COPY INTO"):
            kind = "events" if "LOADING_SILVER_PARCEL_EVENTS" in sql else "parcels"
            table = sql.split()[2]
            names = ", ".join(name for name, _, _ in COLUMNS[kind])
            filename = re.search(r"FILES=\('([^']+)'\)", sql)[1]
            prefix = re.search(r"FROM (@[^)]+)\)", sql)[1]
            manifest_hash = digest((warehouse.path / "manifest.json").read_bytes())
            stage_root = f"@OUTFORDELIVERY.LAKE.SILVER_EXPORTS/{warehouse.path.name}/{manifest_hash}/"
            relative = prefix.removeprefix(stage_root) + filename
            source = warehouse.path / relative
            suffix = " LIMIT 1" if warehouse.partial_copy else ""
            warehouse.db.execute(f"INSERT INTO {table} SELECT {names} FROM read_parquet(?)" + suffix, [str(source)])
            self.result = [(str(source), "LOADED", 1, 1, 0, 0)]
        else:
            result = warehouse.db.execute(sql.replace("%s", "?").replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"), params or [])
            self.result = result.fetchall()
            if sql.startswith("UPDATE"):
                self.rowcount = self.result[0][0]
        return self

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None

    def close(self):
        pass


def local_load(warehouse, snapshot, acceleration=37):
    from streaming.snowflake_load import load_export

    warehouse.path = snapshot
    return load_export(warehouse, snapshot, acceleration, upload=lambda *args: None)


def next_snapshot(snapshot):
    import shutil

    path = snapshot.with_name("next")
    shutil.copytree(snapshot, path)
    manifest = json.loads((path / "manifest.json").read_bytes())
    manifest["export_id"] = path.name
    for kind in COLUMNS:
        file = path / kind / "part-0.parquet"
        table = pq.ParquetFile(file).read()
        rows = table.to_pylist()
        rows[0]["status" if kind == "events" else "carrier"] = "CHANGED"
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), file)
        manifest["datasets"][kind]["files"][f"{kind}/{file.name}"] = file_metadata(file)
        manifest["datasets"][kind]["delta_version"] += 1
    rewrite_manifest(path, manifest)
    return path


def test_full_snapshot_publication_preserves_timestamps_nulls_and_repeat_freshness(snapshot):
    warehouse = LocalWarehouse(snapshot)
    assert local_load(warehouse, snapshot)
    before = warehouse.published()
    assert before[0] and before[1] and len(before[2]) == 1
    assert any(row[9] is None for row in before[0])
    assert not local_load(warehouse, snapshot)
    assert warehouse.published() == before
    assert not any(sql.startswith(("TRUNCATE", "CREATE", "ALTER TABLE")) for sql in warehouse.sql)
    changed = next_snapshot(snapshot)
    assert local_load(warehouse, changed)
    assert warehouse.published()[0] != before[0]
    assert warehouse.published()[1] != before[1]
    assert len(warehouse.published()[2]) == 2


@pytest.mark.parametrize("failure", ["COPY INTO OUTFORDELIVERY.LAKE.LOADING_SILVER_PARCELS",
                                     "INSERT INTO OUTFORDELIVERY.LAKE.SILVER_PARCELS",
                                     "INSERT INTO OUTFORDELIVERY.LAKE.LOAD_AUDIT", "partial_copy"])
def test_copy_or_publication_failure_rolls_back_both_datasets_and_audit(snapshot, failure):
    warehouse = LocalWarehouse(snapshot)
    local_load(warehouse, snapshot)
    before = warehouse.published()
    newer = next_snapshot(snapshot)
    if failure == "partial_copy":
        warehouse.partial_copy = True
    else:
        warehouse.fail = failure
    with pytest.raises((RuntimeError, ValueError)):
        local_load(warehouse, newer)
    assert warehouse.published() == before
    assert warehouse.sql[-1] == "ROLLBACK"
    warehouse.fail, warehouse.partial_copy = None, False
    assert local_load(warehouse, newer)
    assert len(warehouse.published()[2]) == 2


def test_same_id_changed_manifest_and_missing_lock_fail_without_publication(snapshot):
    warehouse = LocalWarehouse(snapshot)
    local_load(warehouse, snapshot)
    before = warehouse.published()
    manifest = read_export(snapshot, 37)
    manifest["upstream"]["run_id"] = "different-run"
    rewrite_manifest(snapshot, manifest)
    with pytest.raises(ValueError, match="different manifest"):
        local_load(warehouse, snapshot)
    assert warehouse.published() == before
    newer = next_snapshot(snapshot)
    warehouse.db.execute("DELETE FROM OUTFORDELIVERY.LAKE.LOAD_LOCK")
    with pytest.raises(ValueError, match="exactly one"):
        local_load(warehouse, newer)
    assert warehouse.published() == before


def test_second_warehouse_session_cannot_publish_while_first_holds_transaction_lock(snapshot):
    warehouse = LocalWarehouse(snapshot)
    local_load(warehouse, snapshot)
    before = warehouse.published()
    other = copy.copy(warehouse)
    other.db = warehouse.db.cursor()
    warehouse.db.execute("BEGIN")
    warehouse.db.execute("UPDATE OUTFORDELIVERY.LAKE.LOAD_LOCK SET OWNER='first-runner' WHERE ID=1")
    try:
        with pytest.raises(Exception, match="[Cc]onflict"):
            local_load(other, next_snapshot(snapshot))
    finally:
        warehouse.db.execute("ROLLBACK")
    assert warehouse.published() == before


def test_dbt_failure_preserves_successful_load_and_resume_rebuilds_without_reloading(snapshot, monkeypatch):
    from scripts import sync_snowflake

    warehouse = LocalWarehouse(snapshot)
    monkeypatch.setattr(sync_snowflake, "load_export", local_load)
    calls = []

    def failed_dbt(args):
        calls.append(args)
        return 1 if args[0] == "build" else 0

    with pytest.raises(RuntimeError, match="loaded successfully"):
        sync_snowflake.coordinate(snapshot.parent, snapshot.name, 37,
                                  connection_factory=lambda: warehouse, dbt_runner=failed_dbt)
    before = warehouse.published()
    state_file = snapshot.parent / ".runs" / "snapshot.json"
    state = json.loads(state_file.read_bytes())
    assert state["stage"] == "loaded" and state["dbt_status"] == "failed"
    assert all(json.loads(args[-1])["time_acceleration"] == 37 for args in calls)
    sync_snowflake.coordinate(snapshot.parent, snapshot.name, 37, connection_factory=lambda: warehouse,
                              dbt_runner=lambda args: 0)
    assert warehouse.published() == before
    assert json.loads(state_file.read_bytes())["dbt_status"] == "success"


def test_failed_load_never_starts_dbt_and_second_local_runner_is_rejected(snapshot, monkeypatch):
    from scripts import sync_snowflake
    from streaming.batches import file_lock

    warehouse = LocalWarehouse(snapshot)
    warehouse.fail = "COPY INTO"
    monkeypatch.setattr(sync_snowflake, "load_export", local_load)
    with pytest.raises(RuntimeError, match="injected"):
        sync_snowflake.coordinate(snapshot.parent, snapshot.name, 37, connection_factory=lambda: warehouse,
                                  dbt_runner=lambda args: pytest.fail("dbt after failed load"))
    assert not any(warehouse.published())
    with file_lock(snapshot.parent / ".sync.lock"), pytest.raises(RuntimeError, match="Another process"):
        sync_snowflake.coordinate(snapshot.parent, snapshot.name, 37,
                                  connection_factory=lambda: pytest.fail("second runner"))


def test_coordinator_uploads_once_waits_and_resumes_the_same_run(snapshot, paired_records, tmp_path, monkeypatch):
    from scripts import sync_snowflake
    from streaming.batches import publish_batch

    batch = publish_batch(tmp_path / "inputs", "paired", *paired_records, 37, {"kind": "replay"})
    manifest = read_export(snapshot, 37)
    manifest["upstream"]["batches"][0]["manifest_hash"] = digest((batch / "manifest.json").read_bytes())
    rewrite_manifest(snapshot, manifest)
    files = Files(snapshot)
    starts, waits = [], []

    def start(**kwargs):
        starts.append(kwargs)
        return SimpleNamespace(response=SimpleNamespace(run_id=123))

    def wait(run_id, **kwargs):
        waits.append(run_id)
        if len(waits) == 1:
            raise OSError("interrupted wait")
        return SimpleNamespace(state=SimpleNamespace(result_state=SimpleNamespace(value="SUCCESS")))

    client = SimpleNamespace(files=files, jobs=SimpleNamespace(run_now=start, wait_get_run_job_terminated_or_skipped=wait))
    warehouse = LocalWarehouse(snapshot)
    monkeypatch.setattr(sync_snowflake, "load_export", local_load)
    root = tmp_path / "saved"
    with pytest.raises(OSError, match="interrupted"):
        sync_snowflake.coordinate(root, "snapshot", 37, manifest_path=batch / "manifest.json",
                                  client=client, catalog="c", job_id=999,
                                  connection_factory=lambda: pytest.fail("load before successful job"))
    assert not (root / "snapshot" / "COMPLETE").exists()
    assert len(starts) == 1 and len(starts[0]["idempotency_token"]) == 64
    result = sync_snowflake.coordinate(root, "snapshot", 37, client=client,
                                       connection_factory=lambda: warehouse, dbt_runner=lambda args: 0)
    assert result == root / "snapshot" and len(starts) == 1 and waits == [123, 123]
    assert read_export(result, 37) == manifest
    assert len(warehouse.published()[2]) == 1


def test_put_uses_saved_files_with_spaces_and_checks_existing_stage_bytes(snapshot, tmp_path):
    import shlex
    import shutil

    from streaming.snowflake_load import stage_files

    local = tmp_path / "directory with spaces" / snapshot.name
    shutil.copytree(snapshot, local)

    class Stage:
        def __init__(self):
            self.data = {}
            self.sources = []

        def execute(self, sql):
            command = shlex.split(sql)
            if command[0] == "PUT":
                source = Path(command[1].removeprefix("file://"))
                self.sources.append(source)
                target = command[2] + source.name
                status = "SKIPPED" if target in self.data else "UPLOADED"
                self.data.setdefault(target, source.read_bytes())
                self.result = [(None, None, None, None, None, None, status, None)]
            else:
                assert command[0] == "GET"
                destination = Path(command[2].removeprefix("file://")) / command[1].rsplit("/", 1)[-1]
                destination.write_bytes(self.data[command[1]])
                self.result = [(str(destination), destination.stat().st_size, "DOWNLOADED", None)]

        def fetchall(self):
            return self.result

    stage = Stage()
    manifest = read_export(local, 37)
    checksum = digest((local / "manifest.json").read_bytes())
    stage_files(stage, local, manifest, checksum)
    assert set(stage.sources) == {local / name for dataset in manifest["datasets"].values() for name in dataset["files"]}
    stage_files(stage, local, manifest, checksum)
    stage.data[next(iter(stage.data))] = b"corrupted prior upload"
    with pytest.raises(ValueError, match="staged checksum"):
        stage_files(stage, local, manifest, checksum)
