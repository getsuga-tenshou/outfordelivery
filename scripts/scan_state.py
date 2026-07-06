#!/usr/bin/env python3

from __future__ import annotations

import os
from collections import Counter

import boto3

ENDPOINT = os.environ.get("OFD_DDB_ENDPOINT", "http://localhost:8000")
TABLE = os.environ.get("OFD_DDB_TABLE", "parcel_state")


def main() -> int:
    ddb = boto3.resource(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name=os.environ.get("AWS_REGION", "eu-west-1"),
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    table = ddb.Table(TABLE)

    items: list[dict] = []
    scan = table.scan()
    items.extend(scan.get("Items", []))
    while "LastEvaluatedKey" in scan:
        scan = table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        items.extend(scan.get("Items", []))

    print(f"parcels in {TABLE}: {len(items)}")
    print("by status:", dict(Counter(i.get("status") for i in items)))
    for item in items[:5]:
        print(
            f"  {item.get('parcel_id')}  v{item.get('version')}  {item.get('status')}  "
            f"hub={item.get('hub_id')}  bad_weather={item.get('weather_bad')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
