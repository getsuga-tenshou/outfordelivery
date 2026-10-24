from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--step", choices=["events", "parcels", "optimize"], required=True)
    parser.add_argument("--enable-uniform", choices=["true", "false"], default="false")
    args = parser.parse_args()

    sys.path.insert(0, str(Path(args.repo) / "streaming"))

    from databricks_helpers import (
        checkpoint_path,
        parcels_merge_sql,
        silver_event_select,
        silver_events_ddl,
        silver_parcels_ddl,
        table_names,
    )
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    catalog = args.catalog
    events_table = f"{catalog}.silver.parcel_events"
    parcels_table = f"{catalog}.silver.parcels"

    if args.step == "events":
        spark.sql(silver_events_ddl(catalog, args.enable_uniform == "true"))

        def merge_batch(batch_df, batch_id):
            batch = batch_df.selectExpr(*silver_event_select()).dropDuplicates(["event_id"])
            batch.createOrReplaceTempView("ofd_silver_events_batch")
            batch.sparkSession.sql(
                f"MERGE INTO {events_table} t USING ofd_silver_events_batch s "
                "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT *"
            )

        query = (
            spark.readStream.table(table_names(catalog)["bronze"])
            .writeStream.foreachBatch(merge_batch)
            .option("checkpointLocation", checkpoint_path(catalog, "silver_parcel_events"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        row = spark.sql(f"SELECT count(*) AS n, count(DISTINCT event_id) AS d FROM {events_table}").first()
        print(f"# silver.parcel_events: rows={row['n']} distinct_event_ids={row['d']}")

    elif args.step == "parcels":
        spark.sql(silver_parcels_ddl(catalog, args.enable_uniform == "true"))
        if not spark.catalog.tableExists(f"{catalog}.bronze.parcels"):
            raise RuntimeError("bronze.parcels does not exist; run parcels_ingest first")
        spark.sql(parcels_merge_sql(catalog))
        print(f"# silver.parcels: rows={spark.table(parcels_table).count()}")

    else:
        for table in (events_table, parcels_table):
            spark.sql(f"OPTIMIZE {table}")
            print(f"# optimized {table}")
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
