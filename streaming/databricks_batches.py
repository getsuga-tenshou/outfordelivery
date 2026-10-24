from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from streaming.batches import digest, enrich_events, json_bytes, read_batch
from streaming.databricks_helpers import batch_path, ddl_from_avro


def selected_batch(catalog: str, batch_id: str, acceleration: float) -> dict:
    return read_batch(Path(batch_path(catalog, batch_id)), acceleration)


def transaction_id(batch: dict, task: str) -> str:
    manifest = batch["manifest"]
    return f"ofd-{task}-{manifest['batch_id']}"


def append_once(frame, table: str, app_id: str, version: int = 0, partition: str | None = None) -> None:
    writer = (frame.write.format("delta").mode("append")
              .option("txnAppId", app_id).option("txnVersion", version).option("mergeSchema", "true"))
    if partition:
        writer = writer.partitionBy(partition)
    writer.saveAsTable(table)


def parcel_frame(spark, batch: dict, repo: Path):
    from pyspark.sql import functions as F

    schema = ddl_from_avro((repo / "schemas" / "parcel.avsc").read_text())
    frame = spark.createDataFrame(batch["parcels"], schema)
    for name in ("created_at", "promised_by"):
        frame = frame.withColumn(name, F.col(name).cast("timestamp"))
    return frame.withColumn("_batch_id", F.lit(batch["manifest"]["batch_id"]))


def event_frame(spark, batch: dict, schema_json: str):
    from pyspark.sql import functions as F

    schema = ddl_from_avro(schema_json) + ", weather_bad boolean, weather_source string"
    frame = spark.createDataFrame(enrich_events(batch), schema)
    return frame.withColumn("event_ts", F.col("event_ts").cast("timestamp"))


def captured_weather_rows(batch: dict) -> list[tuple]:
    rows = []
    for snapshot_id, snapshot in batch["weather"]["snapshots"].items():
        for hub_id, obs in snapshot["observations"].items():
            rows.append((batch["manifest"]["batch_id"], snapshot_id, hub_id, obs["bad"], obs["source"], json.dumps(obs)))
    return rows


def materialize_validation(spark, frame, table: str, app_id: str, version: int, condition):
    """Freeze validation once in Delta; every later action reads that committed snapshot, without cache preview."""
    from pyspark.sql import functions as F

    staged = (frame.withColumn("_valid", F.coalesce(condition, F.lit(False)))
              .withColumn("_app_id", F.lit(app_id)).withColumn("_version", F.lit(version)))
    append_once(staged, table, app_id, version)
    return spark.table(table).filter((F.col("_app_id") == app_id) & (F.col("_version") == version)).drop("_app_id", "_version")


def record_ingest(spark, batch: dict, catalog: str) -> None:
    from pyspark.sql import functions as F

    row = [(batch["manifest"]["batch_id"], digest(json_bytes(batch["manifest"])),
            datetime.fromisoformat(batch["manifest"]["created_at"]), batch["manifest"]["time_acceleration"])]
    frame = spark.createDataFrame(row, "batch_id string, manifest_hash string, created_at timestamp, time_acceleration double")
    append_once(frame.withColumn("ingested_at", F.current_timestamp()), f"{catalog}.ops.ingested_batches",
                transaction_id(batch, "ingested"))
