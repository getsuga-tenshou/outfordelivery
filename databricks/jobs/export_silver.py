from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source", choices=("volume", "kafka"), default="volume")
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--export-id", default="")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if args.source != "volume":
        print("# Compatible Kafka path uses its existing optional Iceberg/dbt integration")
        return 0
    sys.path.insert(0, args.repo)
    from pyspark.sql import SparkSession

    from streaming.rules import load_time_acceleration
    from streaming.silver_snapshot import export_silver

    manifest = export_silver(SparkSession.builder.getOrCreate(), args.catalog, args.export_id or args.run_id,
                             args.run_id, args.batch_id, load_time_acceleration(Path(args.repo)))
    print(f"# Completed export {manifest['export_id']}: "
          f"{manifest['datasets']['events']['rows']} events, {manifest['datasets']['parcels']['rows']} parcels")
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks export failed")
