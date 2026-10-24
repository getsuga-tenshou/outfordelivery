from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

from streaming.batch_upload import _download
from streaming.batches import digest, durable_write, file_lock, safe_batch_id
from streaming.databricks_helpers import SILVER_EVENT_COLUMNS, SILVER_PARCEL_COLUMNS, batch_path
from streaming.rules import as_utc, checked_acceleration

COLUMNS = {"events": SILVER_EVENT_COLUMNS, "parcels": SILVER_PARCEL_COLUMNS}


def export_path(catalog: str, export_id: str) -> str:
    batch_path(catalog, export_id)
    return f"/Volumes/{catalog}/ops/exports/{safe_batch_id(export_id)}"


def file_metadata(path: Path) -> dict:
    import hashlib

    checksum = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(block)
    return {"bytes": path.stat().st_size, "sha256": checksum.hexdigest()}


def parse_manifest(raw: bytes, marker: bytes, export_id: str, acceleration: float) -> dict:
    manifest = json.loads(raw)
    if marker.decode().strip() != digest(raw):
        raise ValueError("Export completion checksum mismatch")
    if (manifest["schema_version"] != 1 or manifest["mode"] != "full_snapshot"
            or manifest["export_id"] != safe_batch_id(export_id)):
        raise ValueError("Unsupported export schema, mode or identity")
    if manifest["time_acceleration"] != checked_acceleration(acceleration):
        raise ValueError("Export time_acceleration mismatch")
    if not manifest["upstream"]["run_id"] or not manifest["upstream"]["batches"]:
        raise ValueError("Missing successful processing identity")
    ids = []
    for batch in manifest["upstream"]["batches"]:
        ids.append(safe_batch_id(batch["batch_id"]))
        if not re.fullmatch(r"[0-9a-f]{64}", batch["manifest_hash"]):
            raise ValueError("Invalid upstream manifest checksum")
    if len(set(ids)) != len(ids) or manifest["upstream"]["selected_batch_id"] not in ids:
        raise ValueError("Upstream batch identity mismatch")
    if set(manifest["datasets"]) != set(COLUMNS):
        raise ValueError("Both Silver datasets are required")
    for kind, columns in COLUMNS.items():
        dataset = manifest["datasets"][kind]
        if dataset["schema"] != [[name, typ] for name, typ, _ in columns]:
            raise ValueError(f"Export schema mismatch: {kind}")
        if (type(dataset["delta_version"]) is not int or dataset["delta_version"] < 0
                or type(dataset["rows"]) is not int or dataset["rows"] < 0 or not dataset["files"]):
            raise ValueError("Invalid export version/count/files")
        for name, metadata in dataset["files"].items():
            path = PurePosixPath(name)
            if (len(path.parts) != 2 or path.parts[0] != kind or path.suffix != ".parquet"
                    or not re.fullmatch(r"[A-Za-z0-9_.-]+", path.name)):
                raise ValueError("Unsafe export file path")
            if (type(metadata["bytes"]) is not int or metadata["bytes"] <= 0
                    or not re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"])):
                raise ValueError("Invalid export file checksum/size")
    return manifest


def read_export(path: Path, acceleration: float, *, require_complete: bool = True) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    raw = (path / "manifest.json").read_bytes()
    marker = (path / "COMPLETE").read_bytes() if require_complete else (digest(raw) + "\n").encode()
    manifest = parse_manifest(raw, marker, path.name, acceleration)
    keys, parcel_ids = {}, set()
    for kind, columns in COLUMNS.items():
        dataset = manifest["datasets"][kind]
        rows = []
        for name, metadata in dataset["files"].items():
            local = path / name
            if local.is_symlink() or local.parent.is_symlink() or file_metadata(local) != metadata:
                raise ValueError(f"Export checksum/size mismatch: {name}")
            parquet = pq.ParquetFile(local)
            fields = parquet.schema_arrow
            if fields.names != [name for name, _, _ in columns]:
                raise ValueError(f"Parquet schema mismatch: {name}")
            for field, (_, typ, _) in zip(fields, columns):
                checks = {"string": pa.types.is_string, "double": pa.types.is_float64,
                          "int": pa.types.is_int32, "boolean": pa.types.is_boolean,
                          "date": pa.types.is_date32, "timestamp": pa.types.is_timestamp}
                if not checks[typ](field.type):
                    raise ValueError(f"Parquet type mismatch: {field.name}")
                if typ == "timestamp" and (field.type.unit not in ("us", "ms") or field.type.tz not in (None, "UTC")):
                    raise ValueError("Parquet timestamps must preserve UTC microseconds")
            rows.extend(parquet.read().to_pylist())
        key = "event_id" if kind == "events" else "parcel_id"
        keys[kind] = {row[key] for row in rows}
        if len(rows) != dataset["rows"] or len(keys[kind]) != len(rows) or None in keys[kind]:
            raise ValueError(f"Export count/null/duplicate key mismatch: {kind}")
        if kind == "events":
            parcel_ids = {row["parcel_id"] for row in rows}
            timestamps = [as_utc(row["event_ts"]) for row in rows]
            bounds = {"min": min(timestamps).isoformat() if timestamps else None,
                      "max": max(timestamps).isoformat() if timestamps else None}
            if bounds != dataset["event_time"]:
                raise ValueError("Export event-time bounds mismatch")
            if any(row["event_date"] != as_utc(row["event_ts"]).date() for row in rows):
                raise ValueError("Export event_date differs from UTC event_ts")
        else:
            for row in rows:
                if any(row[name] is None for name, _, _ in columns):
                    raise ValueError("Missing required parcel dimension")
                expected = as_utc(row["created_at"]) + timedelta(seconds=row["sla_hours"] * 3600 / acceleration)
                if abs((as_utc(row["promised_by"]) - expected).total_seconds()) > 0.000001:
                    raise ValueError("Parcel deadline disagrees with time_acceleration")
    if not parcel_ids <= keys["parcels"]:
        raise ValueError("Orphan event parcel keys")
    return manifest


def download_export(files, catalog: str, export_id: str, root: Path, acceleration: float) -> Path:
    remote = export_path(catalog, export_id)
    root = Path(root)
    with file_lock(root / ".locks" / f"{export_id}.lock"):
        marker = _download(files, remote + "/COMPLETE")
        raw = _download(files, remote + "/manifest.json")
        if marker is None or raw is None:
            raise ValueError("Remote export is incomplete; retain the run for recovery")
        manifest = parse_manifest(raw, marker, export_id, acceleration)
        local = root / export_id
        if (local / "COMPLETE").exists():
            read_export(local, acceleration)
            if (local / "manifest.json").read_bytes() != raw:
                raise ValueError("Export ID already belongs to different local bytes")
            return local
        if (local / "manifest.json").exists() and (local / "manifest.json").read_bytes() != raw:
            raise ValueError("Export ID conflicts with retained partial download")
        durable_write(local / "manifest.json", raw)
        for dataset in manifest["datasets"].values():
            for name, metadata in dataset["files"].items():
                target = local / name
                if target.exists():
                    if file_metadata(target) != metadata:
                        raise ValueError(f"Conflicting retained file: {name}")
                    continue
                data = _download(files, f"{remote}/{name}")
                if data is None or {"bytes": len(data), "sha256": digest(data)} != metadata:
                    raise ValueError(f"Download checksum/size mismatch: {name}")
                partial = target.with_suffix(".partial")
                durable_write(partial, data)
                partial.replace(target)
        read_export(local, acceleration, require_complete=False)
        durable_write(local / "COMPLETE", marker)
        return local


def cleanup_export(root: Path, export_id: str, acceleration: float) -> None:
    root = Path(root).resolve()
    target = root / safe_batch_id(export_id)
    with file_lock(root / ".sync.lock"), file_lock(root / ".locks" / f"{export_id}.lock"):
        if target.is_symlink() or target.resolve().parent != root:
            raise ValueError("Cleanup target must remain directly inside the export root")
        read_export(target, acceleration)
        others = []
        for path in root.iterdir():
            if path != target and path.is_dir() and (path / "COMPLETE").exists():
                try:
                    read_export(path, acceleration)
                    others.append(path)
                except (ValueError, OSError, KeyError):
                    continue
        if not others:
            raise ValueError("Refusing to delete the last verified export")
        # The user explicitly selects this ID; never prune failed/incomplete transfers automatically.
        shutil.rmtree(target)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
