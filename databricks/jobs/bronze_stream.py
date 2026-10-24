from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--secret-scope", default="")
    parser.add_argument("--source", choices=["kafka", "volume"], default="volume")
    parser.add_argument("--topic", default="parcel.events")
    parser.add_argument("--subject", default="parcel.events-value")
    parser.add_argument("--decode", choices=["registry", "substring"], default="registry")
    parser.add_argument("--ddb-table", default="")
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--trigger", default="availableNow")
    return parser.parse_args(argv)


def get_secret(scope: str, key: str, required: bool = True) -> str | None:
    from databricks.sdk.runtime import dbutils

    try:
        return dbutils.secrets.get(scope, key)
    except Exception:
        if required:
            raise
        return None


def optional_state_writer(args):
    if not args.ddb_table:
        return None
    from streaming.dynamo import ParcelStateWriter

    for env_name, key in (("AWS_ACCESS_KEY_ID", "aws_access_key_id"), ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")):
        value = get_secret(args.secret_scope, key, required=False) if args.secret_scope else None
        if value:
            os.environ[env_name] = value
    return ParcelStateWriter(table=args.ddb_table, endpoint=None, region=args.region)


def main() -> int:
    args = parse_args()
    repo = Path(args.repo)
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "streaming"))

    from databricks_helpers import checkpoint_path, parse_trigger, table_names
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from rules import STATE_FIELDS, add_breached, load_rules, load_time_acceleration, valid_event_condition

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rules = load_rules(repo)
    accel = load_time_acceleration(repo)
    tables = table_names(args.catalog)
    trigger = parse_trigger(args.trigger)

    state_writer = optional_state_writer(args)
    from streaming.databricks_batches import append_once, materialize_validation

    if args.source == "kafka":
        from pyspark.sql.avro.functions import from_avro

        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", get_secret(args.secret_scope, "kafka_bootstrap"))
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        if args.decode == "registry":
            decoded = raw.select(
                from_avro(
                    data=F.col("value"),
                    options={},
                    subject=args.subject,
                    schemaRegistryAddress=get_secret(args.secret_scope, "schema_registry_url"),
                ).alias("e")
            ).select("e.*")
        else:
            payload = F.expr("substring(value, 6, length(value) - 5)")
            decoded = raw.select(from_avro(payload, rules.avro_schema_json).alias("e")).select("e.*")
    def load_weather():
        if not spark.catalog.tableExists(tables["weather"]):
            return None
        return spark.table(tables["weather"]).select("hub_id", "weather_bad")

    def load_dimension(parcel_ids):
        """parcel_id -> (created_at, sla_hours) from silver.parcels; empty until ticket 07's job has run."""
        if not spark.catalog.tableExists(tables["parcels"]):
            return {}
        parcels = spark.table(tables["parcels"]).filter(F.col("parcel_id").isin(parcel_ids))
        return {r["parcel_id"]: (r["created_at"], r["sla_hours"]) for r in parcels.collect()}

    def process_batch(batch_df, batch_id, app_id, captured=False):
        if not captured:
            weather_df = load_weather()
            if weather_df is not None:
                batch_df = batch_df.join(F.broadcast(weather_df), "hub_id", "left")
            else:
                batch_df = batch_df.withColumn("weather_bad", F.lit(None).cast("boolean"))
        staged = materialize_validation(
            spark, batch_df, f"{args.catalog}.ops.bronze_validation", app_id, batch_id, valid_event_condition(rules)
        )
        valid = staged.filter("_valid").drop("_valid").withColumn("event_date", F.to_date("event_ts"))
        invalid = staged.filter("NOT _valid").drop("_valid")
        bronze_app = app_id + "-bronze" if captured else "bronze_parcel_events"
        dlq_app = app_id + "-dlq" if captured else "dlq_parcel_events"
        append_once(valid, tables["bronze"], bronze_app, batch_id, "event_date")
        append_once(invalid, tables["dlq"], dlq_app, batch_id)
        if state_writer is not None:
            latest = valid.groupBy("parcel_id").agg(F.max(F.struct(*STATE_FIELDS)).alias("m"))
            rows = [{"parcel_id": r["parcel_id"], **r["m"].asDict()} for r in latest.collect()]
            add_breached(rows, load_dimension([row["parcel_id"] for row in rows]), accel)
            print(f"# batch {batch_id}: dynamo_upserts={state_writer.upsert_many(rows)}")
        print(f"# batch {batch_id}: bronze={valid.count()} dlq={invalid.count()}")

    if args.source == "volume":
        from streaming.databricks_batches import event_frame, record_ingest, selected_batch, transaction_id

        batch = selected_batch(args.catalog, args.batch_id, accel)
        decoded = event_frame(spark, batch, rules.avro_schema_json)
        process_batch(decoded, 0, transaction_id(batch, "events"), captured=True)
        record_ingest(spark, batch, args.catalog)
        return 0

    deduped = decoded.withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"])
    checkpoint_name = "bronze_parcel_events"
    def process_kafka(batch_df, batch_id):
        process_batch(batch_df, batch_id, checkpoint_name)

    query = (
        deduped.writeStream.foreachBatch(process_kafka)
        .option("checkpointLocation", checkpoint_path(args.catalog, checkpoint_name))
        .trigger(**trigger)
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
