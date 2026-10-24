from __future__ import annotations

import copy
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from streaming import databricks_batches
from streaming.batches import digest, json_bytes, publish_batch, read_batch

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.getenv("OFD_TEST_DATABRICKS_DELTA") != "1", reason="opt-in real Spark/Delta test")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("ticket17-delta")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.databricks.delta.snapshotPartitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    for schema in ("bronze", "silver", "ops"):
        session.sql(f"CREATE DATABASE {schema}")
    yield session
    session.stop()


def run_job(name, monkeypatch, repo, *args):
    path = REPO / "databricks/jobs" / f"{name}.py"
    spec = spec_from_file_location(f"job_{name}", path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "get_secret"):
        monkeypatch.setattr(module, "get_secret", lambda *a, **kw: pytest.fail("unexpected secret access"))
    monkeypatch.setattr(sys, "argv", [str(path), "--repo", str(repo), "--catalog", "spark_catalog", *args])
    assert module.main() == 0


def test_paired_jobs_delta_retry_quarantine_weather_and_silver(spark, tmp_path, paired_records, monkeypatch):
    monkeypatch.syspath_prepend(str(REPO / "streaming"))
    repo = tmp_path / "repo"
    for name in ("config", "schemas"):
        shutil.copytree(REPO / name, repo / name)
    import yaml

    config = repo / "config/simulation.yaml"
    cfg = yaml.safe_load(config.read_text())
    cfg["time_acceleration"] = 37
    config.write_text(yaml.safe_dump(cfg))
    events, parcels, weather = copy.deepcopy(paired_records)
    events[0]["lat"] = 99.0
    events[1]["event_ts"] = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    events[2]["hub_id"] = "UNKNOWN"
    dimensions = {row["parcel_id"]: row for row in parcels}
    for event, offset, status in zip(events[3:7], (0, 0.001, 1, 1),
                                     ("OUT_FOR_DELIVERY", "OUT_FOR_DELIVERY", "DELIVERED", "RETURNED")):
        deadline = datetime.fromisoformat(dimensions[event["parcel_id"]]["promised_by"])
        event["event_ts"] = (deadline + timedelta(seconds=offset)).isoformat()
        event["event_type"] = event["status"] = status
    snapshot = next(iter(weather["snapshots"].values()))
    for obs in snapshot["observations"].values():
        obs["bad"], obs["source"] = None, "unknown"
    snapshot["observations"]["UNKNOWN"] = {"hub_id": "UNKNOWN", "bad": None, "source": "unknown"}
    snapshot_id = digest(json_bytes(snapshot))
    weather = {"snapshots": {snapshot_id: snapshot}, "event_context": {e["event_id"]: snapshot_id for e in events}}
    root = tmp_path / "batches"
    path = publish_batch(root, "paired", events, parcels, weather, 37, {"kind": "replay"})
    monkeypatch.setattr(databricks_batches, "batch_path", lambda catalog, batch_id: str(root / batch_id))
    args = ("--source", "volume", "--batch-id", "paired")
    # Deliberately contradictory live observations must never relabel the replay.
    spark.createDataFrame([(hub, True) for hub in snapshot["observations"]], "hub_id string, weather_bad boolean").write.format(
        "delta"
    ).saveAsTable("bronze.weather_latest")
    run_job("parcels_ingest", monkeypatch, repo, *args)
    run_job("weather_ingest", monkeypatch, repo, *args)
    # A failure after Bronze append must recover the same staged validation on retry.
    append = databricks_batches.append_once

    def fail_dlq(frame, table, *args, **kwargs):
        if table.endswith(".dlq_parcel_events"):
            raise RuntimeError("injected DLQ failure")
        return append(frame, table, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(databricks_batches, "append_once", fail_dlq)
        with pytest.raises(RuntimeError, match="injected DLQ"):
            run_job("bronze_stream", patch, repo, *args)
    for _ in range(2):
        for name in ("parcels_ingest", "weather_ingest", "bronze_stream"):
            run_job(name, monkeypatch, repo, *args)
        for step in ("events", "parcels", "optimize"):
            run_job("silver", monkeypatch, repo, "--step", step, "--enable-uniform", "false")
    assert spark.table("bronze.parcels").count() == len(parcels)
    assert spark.table("bronze.dlq_parcel_events").count() == 3
    assert spark.table("bronze.parcel_events").count() == len(events) - 3
    assert spark.table("silver.parcel_events").count() == len(events) - 3
    assert spark.table("silver.parcel_events").select("event_id").distinct().count() == len(events) - 3
    assert spark.table("silver.parcels").count() == len(parcels)
    assert spark.sql("SELECT count(*) n FROM silver.parcel_events e LEFT ANTI JOIN silver.parcels p USING (parcel_id)").first().n == 0
    assert spark.table("bronze.parcel_events").filter("weather_source != 'unknown' OR weather_bad IS NOT NULL").count() == 0
    assert spark.table("silver.parcel_events").filter("weather_bad IS NOT NULL").count() == 0
    assert spark.table("ops.ingested_batches").count() == 1
    assert spark.table("ops.ingested_batches").first().time_acceleration == 37
    assert "delta.universalFormat.enabledFormats" not in dict(spark.sql("SHOW TBLPROPERTIES silver.parcel_events").collect())
    from streaming.rules import load_rules

    spec = spec_from_file_location("verify_processing", REPO / "databricks/jobs/verify_processing.py")
    verifier = module_from_spec(spec)
    spec.loader.exec_module(verifier)
    batch = read_batch(path, 37)
    report = verifier.verify_batch(spark, "spark_catalog", batch, load_rules(repo))
    assert report["counts"]["silver.parcel_events"] == len(events) - 3
    assert report["counts"]["bronze.dlq_parcel_events"] == 3
    changed = copy.deepcopy(batch)
    changed["events"][3]["event_ts"] = (datetime.fromisoformat(events[3]["event_ts"]) - timedelta(seconds=1)).isoformat()
    with pytest.raises(RuntimeError, match="bronze.parcel_events: .* differs at event_ts"):
        verifier.verify_batch(spark, "spark_catalog", changed, load_rules(repo))
    from streaming import silver_snapshot
    from streaming.silver_exports import read_export

    # Serverless rejects the global Parquet timestamp setting; keep the classic default to expose INT96 writes.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "INT96")
    config_type = type(spark.conf)
    configure = config_type.set

    def serverless_config(self, key, value):
        if key != "spark.sql.session.timeZone":
            raise RuntimeError(f"CONFIG_NOT_AVAILABLE: {key}")
        return configure(self, key, value)

    monkeypatch.setattr(config_type, "set", serverless_config)
    export = tmp_path / "exports" / "fixed"
    metadata = silver_snapshot.file_metadata

    def fail_second_dataset(file):
        if file.parent.name == "parcels":
            raise OSError("injected export failure between datasets")
        return metadata(file)

    with monkeypatch.context() as patch:
        patch.setattr(silver_snapshot, "file_metadata", fail_second_dataset)
        with pytest.raises(OSError, match="between datasets"):
            silver_snapshot.export_silver(spark, "spark_catalog", "fixed", "run1", "paired", 37,
                                          destination=export, inputs=root)
    assert not (export / "COMPLETE").exists()
    versions = json.loads((export / "snapshot.json").read_bytes())["versions"]
    # A later concurrent write must not change a partially exported snapshot's fixed source versions.
    spark.sql("UPDATE silver.parcels SET carrier='changed-after-capture'")
    result = silver_snapshot.export_silver(spark, "spark_catalog", "fixed", "run1", "paired", 37,
                                           destination=export, inputs=root)
    assert result == read_export(export, 37)
    import pyarrow.parquet as pq

    for kind, fields in (("events", ("event_ts",)), ("parcels", ("created_at", "promised_by"))):
        for file in result["datasets"][kind]["files"]:
            schema = pq.ParquetFile(export / file).schema_arrow
            assert all(schema.field(name).type.unit == "us" and schema.field(name).type.tz is None for name in fields)
    assert result["datasets"]["parcels"]["delta_version"] == versions["parcels"]
    assert result["datasets"]["events"]["rows"] == len(events) - 3
    assert silver_snapshot.export_silver(spark, "spark_catalog", "fixed", "run1", "paired", 37,
                                         destination=export, inputs=root) == result
    rejected = export.with_name("inconsistent")
    with pytest.raises(ValueError, match="parcels differs"):
        silver_snapshot.export_silver(spark, "spark_catalog", "inconsistent", "run2", "paired", 37,
                                      destination=rejected, inputs=root)
    assert not (rejected / "COMPLETE").exists()
    # Retain originals and reject a different, unfinished selection without using prior inputs.
    incomplete = root / "incomplete"
    incomplete.mkdir()
    manifest = json.loads((path / "manifest.json").read_bytes())
    manifest["batch_id"] = "incomplete"
    (incomplete / "manifest.json").write_bytes(json_bytes(manifest))
    with pytest.raises(FileNotFoundError):
        run_job("parcels_ingest", monkeypatch, repo, "--source", "volume", "--batch-id", "incomplete")
    assert read_batch(path, 37)["events"] == events


def test_validation_snapshot_survives_repeated_actions_and_changed_retry(spark, paired_records, tmp_path):
    from pyspark.sql import functions as F

    from streaming.databricks_batches import event_frame, materialize_validation
    from streaming.rules import load_rules

    path = publish_batch(tmp_path, "snapshot", *paired_records, 37, {"kind": "replay"})
    batch = read_batch(path, 37)
    frame = event_frame(spark, batch, load_rules(REPO).avro_schema_json)
    table = "spark_catalog.ops.frozen_test"
    first = materialize_validation(spark, frame, table, "freeze-test", 0, F.lit(False))
    assert first.filter("_valid").count() == 0
    assert first.filter("NOT _valid").count() == len(batch["events"])
    retry = materialize_validation(spark, frame, table, "freeze-test", 0, F.lit(True))
    assert retry.filter("_valid").count() == 0
    assert retry.filter("NOT _valid").count() == len(batch["events"])
    assert spark.table(table).count() == len(batch["events"])
