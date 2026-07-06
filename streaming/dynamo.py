from __future__ import annotations

import decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def _to_decimal(value):
    return None if value is None else decimal.Decimal(str(value))


class ParcelStateWriter:
    def __init__(self, table: str, endpoint: str, region: str):
        self.table_name = table
        self._ddb = boto3.resource(
            "dynamodb",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id="local",
            aws_secret_access_key="local",
            config=Config(retries={"max_attempts": 3}),
        )
        self._client = self._ddb.meta.client
        self._table = self._ddb.Table(table)

    def ensure_table(self) -> None:
        if self.table_name in self._client.list_tables()["TableNames"]:
            return
        self._client.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "parcel_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "parcel_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self._client.get_waiter("table_exists").wait(TableName=self.table_name)

    def upsert_many(self, rows) -> int:
        written = 0
        for row in rows:
            written += self._upsert(row)
        return written

    def _upsert(self, row: dict) -> int:
        version = int(row["version"])
        event_ts = row.get("event_ts")
        item = {
            "parcel_id": row["parcel_id"],
            "version": version,
            "status": row.get("status"),
            "event_type": row.get("event_type"),
            "hub_id": row.get("hub_id"),
            "lat": _to_decimal(row.get("lat")),
            "lon": _to_decimal(row.get("lon")),
            "weather_bad": row.get("weather_bad"),
            "event_ts": event_ts.isoformat() if event_ts is not None else None,
        }
        item = {key: value for key, value in item.items() if value is not None}
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(parcel_id) OR #ver < :v",
                ExpressionAttributeNames={"#ver": "version"},
                ExpressionAttributeValues={":v": version},
            )
            return 1
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return 0
            raise
