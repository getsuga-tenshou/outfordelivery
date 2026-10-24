from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source", choices=["volume", "kafka"], default="volume")
    parser.add_argument("--batch-id", default="")
    args = parser.parse_args()

    repo = Path(args.repo)
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "streaming"))

    from databricks_helpers import PARCEL_SCHEMA_HINTS, checkpoint_path, parcels_dir
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.source == "volume":
        from streaming.databricks_batches import append_once, parcel_frame, selected_batch, transaction_id
        from streaming.rules import load_time_acceleration

        batch = selected_batch(args.catalog, args.batch_id, load_time_acceleration(repo))
        append_once(parcel_frame(spark, batch, repo), f"{args.catalog}.bronze.parcels", transaction_id(batch, "parcels"))
        return 0
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path(args.catalog, "parcels_schema"))
        .option("cloudFiles.schemaHints", PARCEL_SCHEMA_HINTS)
        .load(parcels_dir(args.catalog))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    query = (
        stream.writeStream.option("checkpointLocation", checkpoint_path(args.catalog, "parcels_ingest"))
        .trigger(availableNow=True)
        .toTable(f"{args.catalog}.bronze.parcels")
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
