import decimal
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

args = getResolvedOptions(sys.argv, ["OFD_LAKE", "OFD_KAFKA", "OFD_TOPIC", "OFD_DDB_TABLE", "OFD_REGION"])
LAKE, KAFKA, TOPIC, TABLE, REGION = args["OFD_LAKE"], args["OFD_KAFKA"], args["OFD_TOPIC"], args["OFD_DDB_TABLE"], args["OFD_REGION"]
BRONZE, DLQ, CKPT = f"{LAKE}/bronze/parcel_events", f"{LAKE}/dlq/parcel_events", f"{LAKE}/_checkpoints/bronze"

SCHEMA = """
{"type":"record","name":"ParcelEvent","namespace":"com.outfordelivery.events","fields":[
 {"name":"event_id","type":"string"},{"name":"parcel_id","type":"string"},
 {"name":"event_type","type":{"type":"enum","name":"EventType","symbols":["CREATED","LABEL_PRINTED","COLLECTED","AT_SORTING_HUB","IN_TRANSIT","OUT_FOR_DELIVERY","DELIVERED","DELIVERY_FAILED","RESCHEDULED","RETURNED"]}},
 {"name":"hub_id","type":"string"},{"name":"lat","type":"double"},{"name":"lon","type":"double"},
 {"name":"event_ts","type":{"type":"long","logicalType":"timestamp-millis"}},
 {"name":"status","type":"string"},{"name":"version","type":"int"}]}
"""

NL = {"lat_min": 50.75, "lat_max": 53.55, "lon_min": 3.36, "lon_max": 7.23}
HUBS = ["AMS", "RTM", "UTR", "EIN", "DHG", "GRO", "ZWO", "MST"]

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
decoded = raw.select(from_avro(payload, SCHEMA).alias("e")).select("e.*")

valid_cond = (
    F.col("hub_id").isin(HUBS)
    & F.col("lat").between(NL["lat_min"], NL["lat_max"])
    & F.col("lon").between(NL["lon_min"], NL["lon_max"])
    & (F.col("event_ts") <= F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"))
)
decoded = decoded.withColumn("_valid", valid_cond).withColumn("weather_bad", F.lit(None).cast("boolean"))
deduped = decoded.withWatermark("event_ts", "30 minutes").dropDuplicates(["event_id"])

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)


def _dec(value):
    return None if value is None else decimal.Decimal(str(value))


def _upsert(rows):
    for row in rows:
        item = {
            "parcel_id": row["parcel_id"],
            "version": int(row["version"]),
            "status": row.get("status"),
            "event_type": row.get("event_type"),
            "hub_id": row.get("hub_id"),
            "lat": _dec(row.get("lat")),
            "lon": _dec(row.get("lon")),
            "weather_bad": row.get("weather_bad"),
            "event_ts": row["event_ts"].isoformat() if row.get("event_ts") else None,
        }
        item = {k: v for k, v in item.items() if v is not None}
        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(parcel_id) OR #ver < :v",
                ExpressionAttributeNames={"#ver": "version"},
                ExpressionAttributeValues={":v": int(row["version"])},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise


def process_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        valid = batch_df.filter("_valid").drop("_valid").withColumn("event_date", F.to_date("event_ts"))
        valid.write.format("delta").mode("append").partitionBy("event_date").save(BRONZE)
        invalid = batch_df.filter("NOT _valid").drop("_valid")
        if not invalid.isEmpty():
            invalid.write.format("delta").mode("append").save(DLQ)
        latest = valid.groupBy("parcel_id").agg(
            F.max(F.struct("version", "event_type", "status", "event_ts", "hub_id", "lat", "lon", "weather_bad")).alias("m")
        )
        _upsert([{"parcel_id": r["parcel_id"], **r["m"].asDict()} for r in latest.collect()])
    finally:
        batch_df.unpersist()


query = deduped.writeStream.foreachBatch(process_batch).option("checkpointLocation", CKPT).start()
query.awaitTermination()
