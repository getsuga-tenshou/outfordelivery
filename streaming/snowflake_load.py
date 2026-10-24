from __future__ import annotations

import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from streaming.batches import digest
from streaming.rules import as_utc
from streaming.silver_exports import COLUMNS, file_metadata, read_export

LAKE = "OUTFORDELIVERY.LAKE"
TABLES = {"events": "SILVER_PARCEL_EVENTS", "parcels": "SILVER_PARCELS"}
SF_TYPES = {"string": "VARCHAR", "double": "DOUBLE", "int": "NUMBER(38,0)",
            "timestamp": "TIMESTAMP_NTZ(6)", "date": "DATE", "boolean": "BOOLEAN"}


def connect_loader():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.getenv("OFD_LOADER_USER", "OFD_LOADER"), role="OFD_LOADER",
        private_key_file=str(Path(os.getenv("OFD_LOADER_PRIVATE_KEY_PATH", "~/.snowflake/ofd_loader.p8")).expanduser()),
        private_key_file_pwd=os.getenv("OFD_LOADER_PRIVATE_KEY_PASSPHRASE") or None,
        warehouse="OFD_WH", database="OUTFORDELIVERY", schema="LAKE",
        session_parameters={"TIMEZONE": "UTC", "LOCK_TIMEOUT": 0,
                            "STATEMENT_TIMEOUT_IN_SECONDS": 600, "QUERY_TAG": "ofd-snapshot-loader"},
    )


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def stage_files(cursor, path: Path, manifest: dict, manifest_hash: str) -> None:
    """PUT exactly the saved files; GET verifies even skipped/reused stage objects before COPY."""
    for kind, dataset in manifest["datasets"].items():
        for name, metadata in dataset["files"].items():
            source = (path / name).resolve()
            prefix = f"@{LAKE}.SILVER_EXPORTS/{manifest['export_id']}/{manifest_hash}/{kind}"
            if file_metadata(source) != metadata:
                raise ValueError("Local export changed before PUT")
            cursor.execute(f"PUT {_quote('file://' + source.as_posix())} {prefix}/ AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
            results = cursor.fetchall()
            if not results or any(str(row[6]).upper() not in ("UPLOADED", "SKIPPED") for row in results):
                raise ValueError("Snowflake PUT did not upload or reuse the saved file")
            with tempfile.TemporaryDirectory(prefix="ofd-stage-") as temporary:
                cursor.execute(f"GET {prefix}/{source.name} {_quote('file://' + Path(temporary).as_posix() + '/')}")
                result = cursor.fetchall()
                if not result or any(str(row[2]).upper() != "DOWNLOADED" for row in result):
                    raise ValueError("Could not verify saved file on Snowflake stage")
                if file_metadata(Path(temporary) / source.name) != metadata:
                    raise ValueError("Snowflake staged checksum mismatch")


def validate_loading(cursor, path: Path, manifest: dict, kind: str) -> None:
    import pyarrow.parquet as pq

    columns = COLUMNS[kind]
    names = ", ".join(name for name, _, _ in columns)
    cursor.execute(f"SELECT {names} FROM {LAKE}.LOADING_{TABLES[kind]}")

    def normalize(row):
        return tuple(as_utc(value) if isinstance(value, datetime) else value for value in row)

    actual = [normalize(row) for row in cursor.fetchall()]
    expected = []
    for name in manifest["datasets"][kind]["files"]:
        for row in pq.ParquetFile(path / name).read().to_pylist():
            expected.append(normalize(tuple(row[name] for name, _, _ in columns)))
    if sorted(actual, key=lambda row: row[0]) != sorted(expected, key=lambda row: row[0]):
        raise ValueError(f"Snowflake loading rows/types/timestamps differ from saved Parquet: {kind}")


def load_export(connection, path: Path, acceleration: float, *, upload=stage_files, validate=validate_loading) -> bool:
    """Return False for a verified repeat. One transaction owns loading, both targets and successful-load audit."""
    path = Path(path)
    manifest = read_export(path, acceleration)
    manifest_hash = digest((path / "manifest.json").read_bytes())
    export_id = manifest["export_id"]
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CURRENT_ROLE()")
        if cursor.fetchone()[0] != "OFD_LOADER":
            raise ValueError("Transfers require the narrowly scoped OFD_LOADER role")
        cursor.execute("ALTER SESSION SET TIMEZONE='UTC', LOCK_TIMEOUT=0")
        # Avoid PUT for a completed ID; the authoritative repeat check is repeated under the DML lock.
        cursor.execute(f"SELECT MANIFEST_HASH FROM {LAKE}.LOAD_AUDIT WHERE EXPORT_ID=%s", (export_id,))
        previous = cursor.fetchall()
        if previous and (len(previous) != 1 or previous[0][0] != manifest_hash):
            raise ValueError("Export ID already published with a different manifest checksum")
        if not previous:
            upload(cursor, path, manifest, manifest_hash)
        cursor.execute("BEGIN TRANSACTION")
        try:
            # This pre-seeded singleton UPDATE takes a Snowflake DML lock until COMMIT/ROLLBACK.
            # LOCK_TIMEOUT=0 makes another device fail promptly, without stale lease files or expiry races.
            cursor.execute(f"UPDATE {LAKE}.LOAD_LOCK SET OWNER=%s WHERE ID=1", (uuid.uuid4().hex,))
            if cursor.rowcount != 1:
                raise ValueError("Loader lock must contain exactly one pre-provisioned row")
            cursor.execute(f"SELECT MANIFEST_HASH FROM {LAKE}.LOAD_AUDIT WHERE EXPORT_ID=%s", (export_id,))
            previous = cursor.fetchall()
            if previous:
                if len(previous) != 1 or previous[0][0] != manifest_hash:
                    raise ValueError("Export ID already published with a different manifest checksum")
                cursor.execute("ROLLBACK")
                return False
            # Reject an older/incomparable snapshot rather than silently replacing a newer publication.
            cursor.execute(f"SELECT EVENTS_VERSION, PARCELS_VERSION FROM {LAKE}.LOAD_AUDIT ORDER BY LOADED_AT DESC LIMIT 1")
            latest = cursor.fetchone()
            if latest and any(manifest["datasets"][kind]["delta_version"] < prior
                              for kind, prior in zip(("events", "parcels"), latest)):
                raise ValueError("Export would regress the published source versions")
            for kind, columns in COLUMNS.items():
                table = f"{LAKE}.LOADING_{TABLES[kind]}"
                cursor.execute(f"DELETE FROM {table}")
                names = ", ".join(name for name, _, _ in columns)
                expressions = ", ".join(f"$1:{name}::{SF_TYPES[typ]}" for name, typ, _ in columns)
                prefix = f"@{LAKE}.SILVER_EXPORTS/{export_id}/{manifest_hash}/{kind}"
                for name in manifest["datasets"][kind]["files"]:
                    cursor.execute(
                        f"COPY INTO {table} ({names}) FROM (SELECT {expressions} FROM {prefix}/) "
                        f"FILES=({_quote(Path(name).name)}) FILE_FORMAT=(FORMAT_NAME='{LAKE}.SILVER_PARQUET') "
                        "ON_ERROR='ABORT_STATEMENT' FORCE=TRUE")
                    result = cursor.fetchall()
                    if not result or any(str(row[1]).upper() != "LOADED" or int(row[5] or 0) for row in result):
                        raise ValueError(f"Partial/failed COPY into {kind}")
                validate(cursor, path, manifest, kind)
            cursor.execute(f"SELECT COUNT(*) FROM {LAKE}.LOADING_SILVER_PARCEL_EVENTS e "
                           f"LEFT JOIN {LAKE}.LOADING_SILVER_PARCELS p ON e.PARCEL_ID=p.PARCEL_ID WHERE p.PARCEL_ID IS NULL")
            if cursor.fetchone()[0]:
                raise ValueError("Snowflake loading event keys lack parcel coverage")
            # No DDL, TRUNCATE or swaps: readers retain the prior complete pair on any failure.
            for kind, columns in COLUMNS.items():
                names = ", ".join(name for name, _, _ in columns)
                target = f"{LAKE}.{TABLES[kind]}"
                cursor.execute(f"DELETE FROM {target}")
                cursor.execute(f"INSERT INTO {target} ({names}) SELECT {names} FROM {LAKE}.LOADING_{TABLES[kind]}")
            cursor.execute(
                f"INSERT INTO {LAKE}.LOAD_AUDIT "
                "(EXPORT_ID, MANIFEST_HASH, LOADED_AT, TIME_ACCELERATION, EVENTS_VERSION, PARCELS_VERSION, "
                "EVENTS_COUNT, PARCELS_COUNT, EVENT_MIN, EVENT_MAX, MANIFEST_JSON) "
                "VALUES (%s,%s,CURRENT_TIMESTAMP(),%s,%s,%s,%s,%s,%s,%s,%s)",
                (export_id, manifest_hash, acceleration, manifest["datasets"]["events"]["delta_version"],
                 manifest["datasets"]["parcels"]["delta_version"], manifest["datasets"]["events"]["rows"],
                 manifest["datasets"]["parcels"]["rows"], manifest["datasets"]["events"]["event_time"]["min"],
                 manifest["datasets"]["events"]["event_time"]["max"], (path / "manifest.json").read_text(encoding="utf-8")),
            )
            cursor.execute("COMMIT")
            return True
        except BaseException:
            cursor.execute("ROLLBACK")
            raise
    finally:
        cursor.close()
