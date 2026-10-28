import shutil
import sys
import tempfile
from pathlib import Path

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from dynamo import ParcelStateWriter
from rules import (
    STATE_FIELDS,
    add_breached,
    enrich_weather,
    load_rules,
    load_s3_parcel_dimension,
    load_s3_weather,
    load_time_acceleration,
    prepare_glue_stream,
)

args = getResolvedOptions(sys.argv, ["OFD_LAKE", "OFD_KAFKA", "OFD_TOPIC", "OFD_DDB_TABLE", "OFD_REGION", "OFD_PARCELS", "OFD_WEATHER"])
LAKE, KAFKA, TOPIC, TABLE, REGION = args["OFD_LAKE"], args["OFD_KAFKA"], args["OFD_TOPIC"], args["OFD_DDB_TABLE"], args["OFD_REGION"]
PARCELS = args["OFD_PARCELS"]
WEATHER = args["OFD_WEATHER"]
BRONZE, DLQ, CKPT = f"{LAKE}/bronze/parcel_events", f"{LAKE}/dlq/parcel_events", f"{LAKE}/_checkpoints/bronze"


def resolve_repo() -> Path:
    """Glue copies --extra-files flat into the working directory; rebuild the layout load_rules expects."""
    repo = Path(tempfile.mkdtemp())
    for folder, name in (("config", "network.yaml"), ("config", "simulation.yaml"), ("schemas", "parcel_event.avsc")):
        (repo / folder).mkdir(exist_ok=True)
        shutil.copy(Path.cwd() / name, repo / folder / name)
    return repo


repo = resolve_repo()
rules = load_rules(repo)
accel = load_time_acceleration(repo)
s3 = boto3.client("s3", region_name=REGION)

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


payload = F.expr("substring(value, 6, length(value) - 5)")
decoded = raw.select(from_avro(payload, rules.avro_schema_json).alias("e")).select("e.*")

deduped = prepare_glue_stream(decoded, rules)

state_writer = ParcelStateWriter(table=TABLE, endpoint=None, region=REGION)


def process_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        # One snapshot per micro-batch, shared by Bronze and the live state; updates need no job restart.
        weather = load_s3_weather(s3, WEATHER)
        valid = enrich_weather(batch_df.filter("_valid").drop("_valid"), weather)
        valid = valid.withColumn("event_date", F.to_date("event_ts"))
        valid.write.format("delta").mode("append").partitionBy("event_date").save(BRONZE)
        # Keep the existing DLQ schema (weather is not assigned to invalid events).
        invalid = batch_df.filter("NOT _valid").drop("_valid").withColumn("weather_bad", F.lit(None).cast("boolean"))
        if not invalid.isEmpty():
            invalid.write.format("delta").mode("append").save(DLQ)
        latest = valid.groupBy("parcel_id").agg(F.max(F.struct(*STATE_FIELDS)).alias("m"))
        rows = [{"parcel_id": r["parcel_id"], **r["m"].asDict()} for r in latest.collect()]
        # Reloaded per batch so dimensions uploaded by a new producer run apply without restarting the job.
        add_breached(rows, load_s3_parcel_dimension(s3, PARCELS), accel)
        written = state_writer.upsert_many(rows)
        print(f"# batch {batch_id}: dynamo_rows={len(rows)} dynamo_upserts={written}")
    finally:
        batch_df.unpersist()


query = deduped.writeStream.foreachBatch(process_batch).option("checkpointLocation", CKPT).start()
query.awaitTermination()
