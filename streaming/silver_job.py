#!/usr/bin/env python3

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.environ.get("OFD_LAKE", "s3a://outfordelivery-lake")
BRONZE_PATH = f"{LAKE}/bronze/parcel_events"
SILVER_PATH = f"{LAKE}/silver/parcel_events"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("outfordelivery-silver")
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


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(BRONZE_PATH)

    silver = (
        bronze.withColumn("parcel_id", F.col("parcel_id").cast("string"))
        .withColumn("version", F.col("version").cast("int"))
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
        .withColumn("event_date", F.to_date("event_ts"))
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "parcel_id",
            "event_type",
            "status",
            "hub_id",
            "lat",
            "lon",
            "event_ts",
            "version",
            "weather_bad",
            "event_date",
        )
    )

    (
        silver.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(SILVER_PATH)
    )

    print(f"# silver: wrote {silver.count()} deduplicated rows (parquet) to {SILVER_PATH}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
