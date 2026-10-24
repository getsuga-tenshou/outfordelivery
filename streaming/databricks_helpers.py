from __future__ import annotations

import json
import re
from datetime import datetime, timezone

AVRO_TO_DDL = {"string": "string", "double": "double", "int": "int", "long": "bigint", "boolean": "boolean"}


def batch_path(catalog: str, batch_id: str) -> str:
    from streaming.batches import safe_batch_id

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("catalog must be a simple Unity Catalog identifier")
    return f"/Volumes/{catalog}/ops/raw/batches/{safe_batch_id(batch_id)}"


def parse_trigger(spec: str) -> dict:
    """Keyword arguments for DataStreamWriter.trigger. Serverless compute only allows availableNow."""
    if spec == "availableNow":
        return {"availableNow": True}
    if spec.startswith("processingTime="):
        return {"processingTime": spec.split("=", 1)[1]}
    raise ValueError(f"unsupported trigger {spec!r}: use 'availableNow' or 'processingTime=<interval>'")


def table_names(catalog: str) -> dict:
    return {
        "bronze": f"{catalog}.bronze.parcel_events",
        "dlq": f"{catalog}.bronze.dlq_parcel_events",
        "weather": f"{catalog}.bronze.weather_latest",
        "parcels": f"{catalog}.silver.parcels",
    }


def checkpoint_path(catalog: str, name: str) -> str:
    return f"/Volumes/{catalog}/ops/checkpoints/{name}"


def weather_table(catalog: str) -> str:
    return f"{catalog}.bronze.weather"


def parcels_dir(catalog: str) -> str:
    return f"/Volumes/{catalog}/ops/raw/parcels/"


WEATHER_DDL = "hub_id string, observed_at timestamp, wind double, precip double, weather_bad boolean, source string"
PARCEL_SCHEMA_HINTS = "sla_hours INT, created_at TIMESTAMP, promised_by TIMESTAMP"


def weather_rows(observations: dict, now: datetime) -> list[tuple]:
    """One row per hub for bronze.weather. Synthetic observations have no time, so they take `now`."""
    rows = []
    for obs in observations.values():
        observed_at = datetime.fromisoformat(obs.observed_at) if obs.observed_at else now
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        rows.append((obs.hub_id, observed_at, obs.wind_speed, obs.precipitation, obs.bad, obs.source))
    return rows


def events_dir(catalog: str) -> str:
    return f"/Volumes/{catalog}/ops/raw/events/"


def ddl_from_avro(schema_json: str) -> str:
    """Spark DDL for JSON files of the same events. Timestamps and enums arrive as text."""
    columns = []
    for field in json.loads(schema_json)["fields"]:
        avro_type = field["type"]
        if isinstance(avro_type, dict):
            if avro_type.get("logicalType") or avro_type["type"] == "enum":
                spark_type = "string"
            else:
                spark_type = AVRO_TO_DDL[avro_type["type"]]
        else:
            spark_type = AVRO_TO_DDL[avro_type]
        columns.append(f"{field['name']} {spark_type}")
    return ", ".join(columns)


SILVER_EVENT_COLUMNS = [
    ("event_id", "string", "Globally unique event id; the merge key"),
    ("parcel_id", "string", "Parcel the scan belongs to"),
    ("event_type", "string", "Lifecycle transition"),
    ("status", "string", "Status carried on the event"),
    ("hub_id", "string", "Hub where the scan happened"),
    ("lat", "double", "Scan latitude"),
    ("lon", "double", "Scan longitude"),
    ("event_ts", "timestamp", "When the scan happened (UTC)"),
    ("version", "int", "Monotonic per-parcel version"),
    ("weather_bad", "boolean", "Captured weather for volume batches; latest observation for Kafka; null if unknown"),
    ("event_date", "date", "Partition column: date of event_ts"),
]

SILVER_PARCEL_COLUMNS = [
    ("parcel_id", "string", "Parcel id; the merge key"),
    ("origin_hub", "string", "Hub the parcel entered the network at"),
    ("destination_postcode", "string", "Delivery postcode"),
    ("destination_region", "string", "Delivery region"),
    ("carrier", "string", "Carrier"),
    ("service_level", "string", "Service level"),
    ("sla_hours", "int", "Delivery window in hours"),
    ("created_at", "timestamp", "When the parcel was created"),
    ("promised_by", "timestamp", "Promised delivery time"),
]

# Delta UniForm: Iceberg metadata is generated on every commit so Snowflake can read the table in place.
UNIFORM_PROPERTIES = {
    "delta.enableDeletionVectors": "false",
    "delta.columnMapping.mode": "id",
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
}


def _silver_ddl(table: str, columns: list[tuple], comment: str, partition: str | None, enable_uniform: bool) -> str:
    cols = [f"  {name} {ddl_type} COMMENT '{doc}'" for name, ddl_type, doc in columns]
    props = ", ".join(f"'{k}' = '{v}'" for k, v in UNIFORM_PROPERTIES.items())
    lines = [f"CREATE TABLE IF NOT EXISTS {table} (", ",\n".join(cols), ")", "USING DELTA"]
    if partition:
        lines.append(f"PARTITIONED BY ({partition})")
    lines.append(f"COMMENT '{comment}'")
    if enable_uniform:
        lines.append(f"TBLPROPERTIES ({props})")
    return "\n".join(lines)


def silver_events_ddl(catalog: str, enable_uniform: bool = True) -> str:
    return _silver_ddl(
        f"{catalog}.silver.parcel_events", SILVER_EVENT_COLUMNS, "Validated, deduplicated parcel events", "event_date",
        enable_uniform,
    )


def silver_parcels_ddl(catalog: str, enable_uniform: bool = True) -> str:
    return _silver_ddl(
        f"{catalog}.silver.parcels", SILVER_PARCEL_COLUMNS, "One row per parcel (latest created_at)", None, enable_uniform
    )


def silver_event_select() -> list[str]:
    """Same casts as streaming/silver_job.py, as SQL expressions in contract order."""
    casts = {
        "parcel_id": "CAST(parcel_id AS STRING)",
        "version": "CAST(version AS INT)",
        "event_ts": "CAST(event_ts AS TIMESTAMP)",
        "lat": "CAST(lat AS DOUBLE)",
        "lon": "CAST(lon AS DOUBLE)",
        "event_date": "TO_DATE(CAST(event_ts AS TIMESTAMP))",
    }
    return [f"{casts.get(name, name)} AS {name}" for name, _, _ in SILVER_EVENT_COLUMNS]


def parcels_merge_sql(catalog: str) -> str:
    """Latest row per parcel by created_at; an existing row is replaced only by a newer one."""
    cols = [name for name, _, _ in SILVER_PARCEL_COLUMNS]
    select = ", ".join(cols)
    return f"""
        MERGE INTO {catalog}.silver.parcels AS t
        USING (
            SELECT {select} FROM (
                SELECT *, row_number() OVER (PARTITION BY parcel_id ORDER BY created_at DESC) AS rn
                FROM {catalog}.bronze.parcels
            ) WHERE rn = 1
        ) AS s
        ON t.parcel_id = s.parcel_id
        WHEN MATCHED AND s.created_at > t.created_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """


def dbt_commands(has_packages: bool) -> list[list[str]]:
    """dbt invocations of the pipeline's Snowflake task, in order. Freshness errors stop the run before the build."""
    commands = [["deps"]] if has_packages else []
    return commands + [["source", "freshness"], ["build"]]
