from __future__ import annotations

import argparse
import importlib
import json
import platform
import sys
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path


def runtime_info(spark) -> dict:
    packages = {}
    for distribution, module in (("pyyaml", "yaml"), ("fastavro", "fastavro"), ("boto3", "boto3"), ("requests", "requests")):
        importlib.import_module(module)
        packages[distribution] = version(distribution)
    return {
        "python": platform.python_version(),
        "spark": spark.version,
        "timezone": spark.conf.get("spark.sql.session.timeZone"),
        "packages": packages,
    }


def verify_batch(spark, catalog: str, batch: dict, rules) -> dict:
    from pyspark.sql import functions as F

    from streaming.batches import digest, enrich_events, json_bytes
    from streaming.databricks_batches import captured_weather_rows, transaction_id
    from streaming.databricks_helpers import SILVER_EVENT_COLUMNS, SILVER_PARCEL_COLUMNS
    from streaming.rules import TERMINAL_STATUSES, as_utc, is_breached
    from streaming.validation import is_valid_event

    def require(condition, message):
        if not condition:
            raise RuntimeError(message)

    def utc(value):
        return as_utc(datetime.fromisoformat(value) if isinstance(value, str) else value)

    def check_rows(table, key, expected, fields, timestamps=()):
        frame = spark.table(f"{catalog}.{table}").filter(F.col(key).isin(list(expected)))
        rows = [row.asDict() for row in frame.select(*fields).collect()]
        require(len(rows) == len(expected), f"{table}: expected {len(expected)} rows, found {len(rows)}")
        require(len({row[key] for row in rows}) == len(rows), f"{table}: duplicate {key}")
        for row in rows:
            source = expected[row[key]]
            for field in fields:
                actual, wanted = row[field], source[field]
                if field in timestamps:
                    actual, wanted = utc(actual), utc(wanted)
                require(actual == wanted, f"{table}: {row[key]} differs at {field}")
        return len(rows)

    manifest = batch["manifest"]
    # This job is for bounded verification fixtures, not an unbounded table export.
    require(0 < len(batch["events"]) <= 5000, "Verification requires 1-5000 fixture events")
    now = datetime.now(timezone.utc)
    events = {row["event_id"]: row for row in enrich_events(batch)}
    valid = {}
    invalid = {}
    for event_id, row in events.items():
        target = valid if is_valid_event({**row, "event_ts": utc(row["event_ts"])}, rules.bbox, rules.hub_ids, now) else invalid
        target[event_id] = row
    fields = list(batch["events"][0]) + ["weather_bad", "weather_source"]
    counts = {}
    for table, expected in (("bronze.parcel_events", valid), ("bronze.dlq_parcel_events", invalid)):
        counts[table] = check_rows(table, "event_id", expected, fields, ("event_ts",))
        other = invalid if expected is valid else valid
        require(spark.table(f"{catalog}.{table}").filter(F.col("event_id").isin(list(other))).count() == 0,
                f"{table}: event routed to both Bronze and DLQ")

    for table, columns in (("silver.parcel_events", SILVER_EVENT_COLUMNS), ("silver.parcels", SILVER_PARCEL_COLUMNS)):
        actual_types = {field.name: field.dataType.simpleString() for field in spark.table(f"{catalog}.{table}").schema}
        require(actual_types == {name: kind for name, kind, _ in columns}, f"{table}: unexpected schema {actual_types}")
    silver_events = {key: {**row, "event_date": utc(row["event_ts"]).date()} for key, row in valid.items()}
    counts["silver.parcel_events"] = check_rows(
        "silver.parcel_events", "event_id", silver_events, [name for name, _, _ in SILVER_EVENT_COLUMNS], ("event_ts",)
    )
    require(spark.table(f"{catalog}.silver.parcel_events").filter(F.col("event_id").isin(list(invalid))).count() == 0,
            "silver.parcel_events: quarantined event published")
    parcels = {row["parcel_id"]: row for row in batch["parcels"]}
    for table in ("bronze.parcels", "silver.parcels"):
        counts[table] = check_rows(table, "parcel_id", parcels, [name for name, _, _ in SILVER_PARCEL_COLUMNS],
                                   ("created_at", "promised_by"))

    elapsed = (F.unix_micros("event_ts") - F.unix_micros("created_at")) / F.lit(1_000_000)
    sla = (spark.table(f"{catalog}.silver.parcel_events").filter(F.col("event_id").isin(list(valid)))
           .join(spark.table(f"{catalog}.silver.parcels"), "parcel_id")
           .select("event_id", ((~F.col("status").isin(list(TERMINAL_STATUSES)))
                                & (elapsed * manifest["time_acceleration"] > F.col("sla_hours") * 3600))
                   .alias("breached")).collect())
    for row in sla:
        event = valid[row.event_id]
        parcel = parcels[event["parcel_id"]]
        require(row.breached == is_breached(event["status"], utc(parcel["created_at"]), parcel["sla_hours"],
                                           utc(event["event_ts"]), manifest["time_acceleration"]),
                f"silver: SLA clock differs for {row.event_id}")

    weather = spark.table(f"{catalog}.bronze.batch_weather").filter(F.col("batch_id") == manifest["batch_id"])
    weather_fields = ["batch_id", "snapshot_id", "hub_id", "weather_bad", "source", "observation_json"]
    actual_weather = [tuple(row) for row in weather.select(*weather_fields).collect()]
    expected_weather = captured_weather_rows(batch)
    require(len(actual_weather) == len(expected_weather) and set(actual_weather) == set(expected_weather),
            "bronze.batch_weather: captured observations differ")
    counts["bronze.batch_weather"] = len(actual_weather)

    staged = spark.table(f"{catalog}.ops.bronze_validation").filter(
        (F.col("_app_id") == transaction_id(batch, "events")) & (F.col("_version") == 0)
    ).select("event_id", "_valid").collect()
    require(len(staged) == len(events) and {row.event_id: row._valid for row in staged} == {
        event_id: event_id in valid for event_id in events
    }, "ops.bronze_validation: frozen routing differs")
    ledger = spark.table(f"{catalog}.ops.ingested_batches").filter(F.col("batch_id") == manifest["batch_id"]).collect()
    require(len(ledger) == 1 and ledger[0].manifest_hash == digest(json_bytes(manifest))
            and ledger[0].time_acceleration == manifest["time_acceleration"], "ops.ingested_batches: manifest differs")
    return {"batch_id": manifest["batch_id"], "source": manifest["source"],
            "manifest_hash": digest(json_bytes(manifest)), "counts": counts}


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only checks against a bounded completed processing fixture")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--batch-id", required=True, help="One fixture ID, or comma-separated IDs to verify in one run")
    args = parser.parse_args()
    repo = Path(args.repo)
    sys.path.insert(0, str(repo))

    from pyspark.sql import SparkSession

    from streaming.databricks_batches import selected_batch
    from streaming.rules import load_rules, load_time_acceleration

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print("# runtime=" + json.dumps(runtime_info(spark), sort_keys=True))
    batch_ids = args.batch_id.split(",")
    if len(batch_ids) > 20 or len(set(batch_ids)) != len(batch_ids):
        raise ValueError("Verification requires at most 20 distinct fixture IDs")
    for batch_id in batch_ids:
        batch = selected_batch(args.catalog, batch_id, load_time_acceleration(repo))
        print("# verification=" + json.dumps(verify_batch(spark, args.catalog, batch, load_rules(repo)), sort_keys=True))
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
