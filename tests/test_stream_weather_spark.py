"""Run with OFD_TEST_SPARK_WEATHER=1 under spark-submit (no Kafka or cloud dependencies)."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from streaming.rules import enrich_weather, load_rules, prepare_glue_stream, valid_event_condition

pytestmark = pytest.mark.skipif(os.getenv("OFD_TEST_SPARK_WEATHER") != "1", reason="opt-in real Spark weather restart")
REPO = Path(__file__).resolve().parents[1]


def test_weather_enrichment_preserves_glue_checkpoint_and_refreshes(tmp_path):
    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.master("local[2]").appName("ofd-weather-restart")
             .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    rules = load_rules(REPO)
    source = tmp_path / "events"
    source.mkdir()
    checkpoint = str(tmp_path / "checkpoint")
    schema = ("event_id string, parcel_id string, event_type string, hub_id string, lat double, lon double, "
              "event_ts timestamp, status string, version int")
    stream = spark.readStream.schema(schema).json(str(source))
    legacy = (stream.withColumn("_valid", valid_event_condition(rules))
              .withColumn("weather_bad", F.lit(None).cast("boolean"))
              .withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"]))
    current = prepare_glue_stream(stream, rules)
    query = None
    try:
        # The stateful operator stores these rows on Glue 4 (Spark 3.3); schema changes break existing checkpoints.
        assert current.schema == legacy.schema
        event = {"event_id": "before", "parcel_id": "P1", "event_type": "IN_TRANSIT", "hub_id": "AMS",
                 "lat": 52.3676, "lon": 4.9041,
                 "event_ts": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
                 "status": "IN_TRANSIT", "version": 1}
        (source / "first.json").write_text(json.dumps(event))
        seen = []
        query = legacy.writeStream.foreachBatch(lambda df, _: seen.extend(df.collect())).option(
            "checkpointLocation", checkpoint).start()
        query.processAllAvailable()
        query.stop()
        assert [r["event_id"] for r in seen] == ["before"]

        weather = {"AMS": True, "RTM": False, "UTR": None}
        received = []

        def consume(df, _):
            enriched = enrich_weather(df.filter("_valid").drop("_valid"), weather)
            received.extend(r.asDict() for r in enriched.collect())

        query = current.writeStream.foreachBatch(consume).option("checkpointLocation", checkpoint).start()
        rows = [event] + [{**event, "event_id": hub, "hub_id": hub} for hub in ("AMS", "RTM", "UTR", "EIN")]
        (source / "second.json").write_text("\n".join(json.dumps(r) for r in rows))
        query.processAllAvailable()
        assert {r["event_id"]: r["weather_bad"] for r in received} == {
            "AMS": True, "RTM": False, "UTR": None, "EIN": None,
        }
        received.clear()
        weather.update({"AMS": False, "RTM": True})
        (source / "third.json").write_text(json.dumps({**event, "event_id": "refresh"}))
        query.processAllAvailable()
        assert [(r["event_id"], r["weather_bad"]) for r in received] == [("refresh", False)]
        received.clear()
        weather.clear()
        (source / "fourth.json").write_text(json.dumps({**event, "event_id": "no-cache"}))
        query.processAllAvailable()
        assert [(r["event_id"], r["weather_bad"]) for r in received] == [("no-cache", None)]
    finally:
        if query is not None:
            query.stop()
        spark.stop()
