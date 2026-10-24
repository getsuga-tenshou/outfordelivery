from __future__ import annotations

import json
from functools import reduce
from operator import and_
from pathlib import Path

from streaming.batches import atomic_json, digest, durable_write, json_bytes, read_batch, safe_batch_id
from streaming.databricks_helpers import batch_path, silver_event_select
from streaming.silver_exports import COLUMNS, export_path, file_metadata, parse_manifest, utc_now


def export_silver(spark, catalog: str, export_id: str, run_id: str, batch_id: str, acceleration: float,
                  *, destination: Path | None = None, inputs: Path | None = None) -> dict:
    """The success-only DAG invokes this after optimize. Fixed versions and exact paired-source checks gate publication."""
    from pyspark.sql import functions as F

    safe_batch_id(export_id)
    batch_path(catalog, batch_id)
    if not run_id:
        raise ValueError("Export requires an upstream processing run ID")
    root = destination or Path(export_path(catalog, export_id))
    tables = {"events": f"{catalog}.silver.parcel_events", "parcels": f"{catalog}.silver.parcels",
              "validation": f"{catalog}.ops.bronze_validation", "ledger": f"{catalog}.ops.ingested_batches"}
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def version(table):
        return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])

    def frozen(kind, versions):
        return spark.read.option("versionAsOf", versions[kind]).table(tables[kind])

    def require_same(actual, expected, label):
        key = actual.columns[0]
        for frame in (actual, expected):
            if frame.groupBy(key).count().filter(F.col(key).isNull() | (F.col("count") != 1)).take(1):
                raise ValueError(f"{label} has null or duplicate keys")
        equal = reduce(and_, [F.col(f"a.{name}").eqNullSafe(F.col(f"b.{name}")) for name in actual.columns])
        if (actual.alias("a").join(expected.alias("b"), equal, "left_anti").take(1)
                or expected.alias("a").join(actual.alias("b"), equal, "left_anti").take(1)):
            raise ValueError(f"{label} differs from the complete paired input/validation snapshot")

    root.mkdir(parents=True, exist_ok=True)
    if (root / "COMPLETE").exists():
        raw = (root / "manifest.json").read_bytes()
        manifest = parse_manifest(raw, (root / "COMPLETE").read_bytes(), export_id, acceleration)
        if manifest["upstream"]["run_id"] != str(run_id) or manifest["upstream"]["selected_batch_id"] != batch_id:
            raise ValueError("Export ID belongs to another processing run")
        for dataset in manifest["datasets"].values():
            for name, metadata in dataset["files"].items():
                if file_metadata(root / name) != metadata:
                    raise ValueError("Completed remote export is corrupt")
        return manifest
    plan_file = root / "snapshot.json"
    if plan_file.exists():
        plan = json.loads(plan_file.read_bytes())
        if plan["run_id"] != str(run_id) or plan["selected_batch_id"] != batch_id or plan["catalog"] != catalog:
            raise ValueError("Partial export belongs to another run; resume that run")
    else:
        plan = {"run_id": str(run_id), "selected_batch_id": batch_id, "catalog": catalog,
                "versions": {kind: version(table) for kind, table in tables.items()}, "created_at": utc_now()}
        atomic_json(plan_file, plan)
    versions = plan["versions"]
    ledger = frozen("ledger", versions).collect()
    upstream, batches = [], []
    for row in sorted(ledger, key=lambda r: r.batch_id):
        if row.time_acceleration != acceleration:
            raise ValueError("Mixed time_acceleration in Silver history")
        path = inputs / row.batch_id if inputs else Path(batch_path(catalog, row.batch_id))
        batch = read_batch(path, acceleration)
        if digest((path / "manifest.json").read_bytes()) != row.manifest_hash:
            raise ValueError("Input manifest differs from recorded ingestion identity")
        batches.append(batch)
        upstream.append({"batch_id": row.batch_id, "manifest_hash": row.manifest_hash})
    if batch_id not in {row["batch_id"] for row in upstream} or len(upstream) != len({r["batch_id"] for r in upstream}):
        raise ValueError("Selected batch missing or duplicate in ingestion ledger")
    validation = frozen("validation", versions)
    apps = [f"ofd-events-{row['batch_id']}" for row in upstream]
    if validation.filter(~F.col("_app_id").isin(apps)).limit(1).count():
        raise ValueError("Unfinished or non-volume input exists in validation; finish processing before export")
    for batch in batches:
        staged = validation.filter(F.col("_app_id") == f"ofd-events-{batch['manifest']['batch_id']}")
        expected_keys = {row["event_id"] for row in batch["events"]}
        actual_keys = [row.event_id for row in staged.select("event_id").collect()]
        if set(actual_keys) != expected_keys or len(actual_keys) != len(expected_keys):
            raise ValueError("Frozen validation is missing or duplicates paired input events")
    expected_events = validation.filter("_valid").selectExpr(*silver_event_select()).dropDuplicates(["event_id"])
    dimensions = {}
    for batch in batches:
        for parcel in batch["parcels"]:
            previous = dimensions.setdefault(parcel["parcel_id"], parcel)
            if previous != parcel:
                raise ValueError("Conflicting parcel dimensions across input batches")
    parcel_columns = COLUMNS["parcels"]
    schema = ", ".join(f"{name} {'string' if typ == 'timestamp' else typ}" for name, typ, _ in parcel_columns)
    expected_parcels = spark.createDataFrame(list(dimensions.values()), schema)
    for name in ("created_at", "promised_by"):
        expected_parcels = expected_parcels.withColumn(name, F.col(name).cast("timestamp"))
    snapshots = {kind: frozen(kind, versions) for kind in COLUMNS}
    for kind, expected in (("events", expected_events), ("parcels", expected_parcels)):
        frame = snapshots[kind]
        if [(field.name, field.dataType.simpleString()) for field in frame.schema] != [
                (name, typ) for name, typ, _ in COLUMNS[kind]]:
            raise ValueError(f"Silver schema mismatch: {kind}")
        require_same(frame, expected, kind)
    if snapshots["events"].join(snapshots["parcels"], "parcel_id", "left_anti").limit(1).count():
        raise ValueError("Missing parcel coverage")
    datasets = {}
    for kind, frame in snapshots.items():
        # Only the unfinished export's derived files are rewritten; fixed source versions remain in snapshot.json.
        # UTC wall-time timestamps use standard Parquet microseconds without a restricted serverless setting.
        parquet = frame.select(*[
            F.col(name).cast("timestamp_ntz").alias(name) if typ == "timestamp" else F.col(name)
            for name, typ, _ in COLUMNS[kind]
        ])
        parquet.write.mode("overwrite").parquet(str(root / kind))
        files = {f"{kind}/{path.name}": file_metadata(path) for path in sorted((root / kind).glob("*.parquet"))}
        dataset = {"table": tables[kind], "delta_version": versions[kind], "rows": frame.count(),
                   "schema": [[name, typ] for name, typ, _ in COLUMNS[kind]], "files": files}
        if kind == "events":
            bounds = frame.agg(F.min("event_ts").alias("minimum"), F.max("event_ts").alias("maximum")).first()
            from streaming.rules import as_utc

            dataset["event_time"] = {"min": as_utc(bounds.minimum).isoformat() if bounds.minimum else None,
                                     "max": as_utc(bounds.maximum).isoformat() if bounds.maximum else None}
        datasets[kind] = dataset
    manifest = {"schema_version": 1, "mode": "full_snapshot", "export_id": export_id,
                "created_at": plan["created_at"], "time_acceleration": acceleration,
                "upstream": {"run_id": str(run_id), "selected_batch_id": batch_id, "batches": upstream,
                             "validation_version": versions["validation"], "ledger_version": versions["ledger"]},
                "datasets": datasets}
    raw = json_bytes(manifest)
    marker = (digest(raw) + "\n").encode()
    parse_manifest(raw, marker, export_id, acceleration)
    durable_write(root / "manifest.json", raw)
    durable_write(root / "COMPLETE", marker)
    return manifest
