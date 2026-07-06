#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

REPO = Path(os.environ.get("OFD_HOME", "/opt/outfordelivery"))
LAKE = os.environ.get("OFD_LAKE", "s3a://outfordelivery-lake")
KAFKA = os.environ.get("OFD_KAFKA", "redpanda:29092")
TOPIC = os.environ.get("OFD_TOPIC", "parcel.events")
BRONZE_PATH = f"{LAKE}/bronze/parcel_events"
DLQ_PATH = f"{LAKE}/dlq/parcel_events"
CHECKPOINT = f"{LAKE}/_checkpoints/bronze_parcel_events"


STATE_FIELDS = ["version", "event_type", "status", "event_ts", "hub_id", "lat", "lon", "weather_bad"]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("outfordelivery-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def load_network():
    network = yaml.safe_load((REPO / "config" / "network.yaml").read_text())
    return network["nl_bounding_box"], [h["id"] for h in network["hubs"]]


def load_weather() -> dict:
    cache = REPO / "data" / "weather_cache.json"
    if not cache.exists():
        return {}
    obs = json.loads(cache.read_text()).get("observations", {})
    return {hub: bool(o.get("bad")) for hub, o in obs.items()}


def main() -> int:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from dynamo import ParcelStateWriter

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bbox, hub_ids = load_network()
    schema_json = (REPO / "schemas" / "parcel_event.avsc").read_text()
    weather = load_weather()
    weather_df = (
        spark.createDataFrame([(h, b) for h, b in weather.items()], "hub_id string, weather_bad boolean")
        if weather
        else None
    )

    state_writer = ParcelStateWriter(
        table=os.environ.get("OFD_DDB_TABLE", "parcel_state"),
        endpoint=os.environ.get("OFD_DDB_ENDPOINT", "http://dynamodb:8000"),
        region=os.environ.get("AWS_REGION", "eu-west-1"),
    )
    state_writer.ensure_table()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


    payload = F.expr("substring(value, 6, length(value) - 5)")
    decoded = raw.select(from_avro(payload, schema_json).alias("e")).select("e.*")


    is_valid = (
        F.col("hub_id").isin(hub_ids)
        & F.col("lat").between(bbox["lat_min"], bbox["lat_max"])
        & F.col("lon").between(bbox["lon_min"], bbox["lon_max"])
        & (F.col("event_ts") <= F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"))
    )
    decoded = decoded.withColumn("_valid", is_valid)


    deduped = decoded.withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"])

    def process_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            valid = batch_df.filter("_valid").drop("_valid")
            invalid = batch_df.filter("NOT _valid").drop("_valid")

            if weather_df is not None:
                valid = valid.join(F.broadcast(weather_df), "hub_id", "left")
            else:
                valid = valid.withColumn("weather_bad", F.lit(None).cast("boolean"))

            valid = valid.withColumn("event_date", F.to_date("event_ts"))
            valid.write.format("delta").mode("append").partitionBy("event_date").save(BRONZE_PATH)

            if not invalid.isEmpty():
                invalid.write.format("delta").mode("append").save(DLQ_PATH)

            latest = valid.groupBy("parcel_id").agg(F.max(F.struct(*STATE_FIELDS)).alias("m"))
            rows = [{"parcel_id": r["parcel_id"], **r["m"].asDict()} for r in latest.collect()]
            written = state_writer.upsert_many(rows)
            print(f"# batch {batch_id}: bronze+={valid.count()} dynamo_upserts={written}")
        finally:
            batch_df.unpersist()

    query = (
        deduped.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
