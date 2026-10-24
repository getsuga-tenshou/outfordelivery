import shutil
import sys
import tempfile
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from dynamo import ParcelStateWriter
from rules import STATE_FIELDS, load_rules, valid_event_condition

args = getResolvedOptions(sys.argv, ["OFD_LAKE", "OFD_KAFKA", "OFD_TOPIC", "OFD_DDB_TABLE", "OFD_REGION"])
LAKE, KAFKA, TOPIC, TABLE, REGION = args["OFD_LAKE"], args["OFD_KAFKA"], args["OFD_TOPIC"], args["OFD_DDB_TABLE"], args["OFD_REGION"]
BRONZE, DLQ, CKPT = f"{LAKE}/bronze/parcel_events", f"{LAKE}/dlq/parcel_events", f"{LAKE}/_checkpoints/bronze"


def resolve_repo() -> Path:
    """Glue copies --extra-files flat into the working directory; rebuild the layout load_rules expects."""
    repo = Path(tempfile.mkdtemp())
    for folder, name in (("config", "network.yaml"), ("schemas", "parcel_event.avsc")):
        (repo / folder).mkdir()
        shutil.copy(Path.cwd() / name, repo / folder / name)
    return repo


rules = load_rules(resolve_repo())

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

decoded = decoded.withColumn("_valid", valid_event_condition(rules)).withColumn("weather_bad", F.lit(None).cast("boolean"))
deduped = decoded.withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"])

state_writer = ParcelStateWriter(table=TABLE, endpoint=None, region=REGION)


def process_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        valid = batch_df.filter("_valid").drop("_valid").withColumn("event_date", F.to_date("event_ts"))
        valid.write.format("delta").mode("append").partitionBy("event_date").save(BRONZE)
        invalid = batch_df.filter("NOT _valid").drop("_valid")
        if not invalid.isEmpty():
            invalid.write.format("delta").mode("append").save(DLQ)
        latest = valid.groupBy("parcel_id").agg(F.max(F.struct(*STATE_FIELDS)).alias("m"))
        # TODO: write "breached" once the AWS path has a parcel dimension (streaming.rules.add_breached); without
        # one the Lambda in infra/alerts.tf can only alert on RETURNED.
        state_writer.upsert_many([{"parcel_id": r["parcel_id"], **r["m"].asDict()} for r in latest.collect()])
    finally:
        batch_df.unpersist()


query = deduped.writeStream.foreachBatch(process_batch).option("checkpointLocation", CKPT).start()
query.awaitTermination()
