from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
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

    from databricks_helpers import WEATHER_DDL, weather_rows, weather_table
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.source == "volume":
        from streaming.databricks_batches import append_once, captured_weather_rows, selected_batch, transaction_id
        from streaming.rules import load_time_acceleration

        batch = selected_batch(args.catalog, args.batch_id, load_time_acceleration(repo))
        frame = spark.createDataFrame(
            captured_weather_rows(batch),
            "batch_id string, snapshot_id string, hub_id string, weather_bad boolean, source string, observation_json string",
        )
        append_once(frame, f"{args.catalog}.bronze.batch_weather", transaction_id(batch, "weather"))
        return 0

    from simulator.network import load_network
    from simulator.weather import WeatherProvider, load_weather_config

    network = load_network(repo / "config" / "network.yaml")
    provider = WeatherProvider(load_weather_config(repo / "config" / "sources.yaml"), network)
    observations = provider.fetch()
    sources = {obs.source for obs in observations.values()}
    bad_hubs = [hub for hub, obs in observations.items() if obs.bad]
    print(f"# weather: source={sources} hubs={len(observations)} bad_weather_hubs={bad_hubs or 'none'}")

    rows = weather_rows(observations, datetime.now(timezone.utc))
    spark.createDataFrame(rows, WEATHER_DDL).write.mode("append").saveAsTable(weather_table(args.catalog))

    table = weather_table(args.catalog)
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {args.catalog}.bronze.weather_latest AS
        SELECT hub_id, observed_at, wind, precip, weather_bad, source
        FROM (
            SELECT *, row_number() OVER (PARTITION BY hub_id ORDER BY observed_at DESC) AS rn
            FROM {table}
        )
        WHERE rn = 1
        """
    )
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
