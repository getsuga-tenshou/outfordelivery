from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import boto3
import duckdb
from dotenv import dotenv_values
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from streaming.rules import TERMINAL_STATUSES, elapsed_simulated_seconds, is_breached, load_time_acceleration

REPO = Path(__file__).resolve().parents[1]
DDB_ENDPOINT = os.environ.get("OFD_DDB_ENDPOINT", "http://localhost:8000") or None  # empty = real AWS DynamoDB
DDB_TABLE = os.environ.get("OFD_DDB_TABLE", "parcel_state")
DUCKDB_PATH = os.environ.get("OFD_DUCKDB", str(REPO / "dbt" / "outfordelivery.duckdb"))
PARCELS_PATH = Path(os.environ.get("OFD_PARCELS_PATH") or REPO / "data" / "parcels.jsonl")
TERMINAL = TERMINAL_STATUSES
MARTS = {"gold_delivery_funnel", "gold_on_time_by_region", "gold_failure_analysis", "gold_weather_impact"}


def _time_acceleration() -> float:
    return load_time_acceleration(REPO)


def _load_dimension() -> dict:
    dim: dict[str, dict] = {}
    if PARCELS_PATH.exists():
        for line in PARCELS_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                parcel = json.loads(line)
                dim[parcel["parcel_id"]] = parcel
    return dim


def _table():
    # DynamoDB Local needs dummy keys; real AWS uses the default credential chain (profile, env or role).
    local = {"aws_access_key_id": "local", "aws_secret_access_key": "local"} if DDB_ENDPOINT else {}
    return boto3.resource(
        "dynamodb",
        endpoint_url=DDB_ENDPOINT,
        region_name=os.environ.get("AWS_REGION", "eu-west-1"),
        **local,
    ).Table(DDB_TABLE)


def _scan_state() -> list[dict]:
    table = _table()
    items: list[dict] = []
    scan = table.scan()
    items.extend(scan.get("Items", []))
    while "LastEvaluatedKey" in scan:
        scan = table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        items.extend(scan.get("Items", []))
    return items


def _get_item(parcel_id: str):
    return _table().get_item(Key={"parcel_id": parcel_id}).get("Item")


def _sla_state(item: dict, dim: dict, accel: float, now: datetime) -> str:
    if item.get("status") in TERMINAL:
        return "delivered"
    parcel = dim.get(item.get("parcel_id"))
    if not parcel:
        return "unknown"
    created = datetime.fromisoformat(parcel["created_at"])
    sla_seconds = int(parcel["sla_hours"]) * 3600
    if is_breached(item.get("status"), created, parcel["sla_hours"], now, accel):
        return "breached"
    elapsed_sim = elapsed_simulated_seconds(created, now, accel)
    if elapsed_sim > sla_seconds * 0.8:
        return "at_risk"
    return "on_time"


def _parcel_view(item: dict, dim: dict, accel: float, now: datetime) -> dict:
    parcel = dim.get(item.get("parcel_id"), {})
    remaining = None
    created = parcel.get("created_at")
    sla_hours = parcel.get("sla_hours")
    if created and sla_hours is not None:
        elapsed_sim = elapsed_simulated_seconds(datetime.fromisoformat(created), now, accel)
        remaining = round((int(sla_hours) * 3600 - elapsed_sim) / 3600, 1)
    return {
        "parcel_id": item.get("parcel_id"),
        "status": item.get("status"),
        "sla": _sla_state(item, dim, accel, now),
        "service_level": parcel.get("service_level"),
        "region": parcel.get("destination_region"),
        "destination_postcode": parcel.get("destination_postcode"),
        "carrier": parcel.get("carrier"),
        "origin_hub": parcel.get("origin_hub"),
        "hub_id": item.get("hub_id"),
        "version": int(item["version"]) if item.get("version") is not None else None,
        "lat": float(item["lat"]) if item.get("lat") is not None else None,
        "lon": float(item["lon"]) if item.get("lon") is not None else None,
        "weather_bad": bool(item.get("weather_bad")) if item.get("weather_bad") is not None else None,
        "last_scan": item.get("event_ts"),
        "created_at": created,
        "promised_by": parcel.get("promised_by"),
        "sla_hours": sla_hours,
        "sim_hours_remaining": remaining,
    }


app = FastAPI(title="outfordelivery dashboard")


@app.get("/api/map/style")
def map_style() -> JSONResponse:
    key = os.environ.get("CARTO_MAP_API_KEY")
    if key is None:
        key = dotenv_values(REPO / ".env", interpolate=False).get("CARTO_MAP_API_KEY")
    key = (key or "").strip()
    style = {
        "version": 8,
        "sources": {},
        "layers": [{"id": "background", "type": "background", "paint": {"background-color": "#eaeff5"}}],
    }
    if key:
        style["sources"]["base"] = {
            "type": "raster",
            "tileSize": 256,
            "tiles": ["https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png?" + urlencode({"key": key})],
            "attribution": '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>, '
                           '© <a href="https://carto.com/attributions">CARTO</a>',
        }
        style["layers"].append({"id": "base", "type": "raster", "source": "base"})
    return JSONResponse(style, headers={"Cache-Control": "no-store"})


@app.get("/api/live/summary")
def live_summary() -> dict:
    items = _scan_state()
    dim = _load_dimension()
    accel = _time_acceleration()
    now = datetime.now(timezone.utc)
    by_status: dict[str, int] = {}
    by_hub: dict[str, int] = {}
    sla = {"on_time": 0, "at_risk": 0, "breached": 0, "delivered": 0, "unknown": 0}
    for item in items:
        by_status[item.get("status", "UNKNOWN")] = by_status.get(item.get("status", "UNKNOWN"), 0) + 1
        by_hub[item.get("hub_id", "?")] = by_hub.get(item.get("hub_id", "?"), 0) + 1
        sla[_sla_state(item, dim, accel, now)] += 1
    return {
        "total": len(items),
        "in_flight": len(items) - sla["delivered"],
        "by_status": by_status,
        "by_hub": by_hub,
        "sla": sla,
    }


@app.get("/api/live/map")
def live_map() -> dict:
    items = _scan_state()
    dim = _load_dimension()
    accel = _time_acceleration()
    now = datetime.now(timezone.utc)
    points = []
    for item in items:
        lat, lon = item.get("lat"), item.get("lon")
        if lat is None or lon is None:
            continue
        points.append(
            {
                "parcel_id": item.get("parcel_id"),
                "lat": float(lat),
                "lon": float(lon),
                "status": item.get("status"),
                "sla": _sla_state(item, dim, accel, now),
            }
        )
    return {"points": points}


@app.get("/api/live/parcels")
def live_parcels(sla: str | None = None, limit: int = 200) -> dict:
    items = _scan_state()
    dim = _load_dimension()
    accel = _time_acceleration()
    now = datetime.now(timezone.utc)
    views = [_parcel_view(item, dim, accel, now) for item in items]
    if sla:
        views = [v for v in views if v["sla"] == sla]
    rank = {"breached": 0, "at_risk": 1, "on_time": 2, "delivered": 3, "unknown": 4}
    views.sort(key=lambda v: (rank.get(v["sla"], 9), v["sim_hours_remaining"] if v["sim_hours_remaining"] is not None else 1e9))
    return {"sla": sla, "total": len(views), "shown": min(len(views), limit), "parcels": views[:limit]}


@app.get("/api/live/parcel/{parcel_id}")
def parcel_detail(parcel_id: str):
    item = _get_item(parcel_id)
    if not item:
        return JSONResponse({"error": "not found"}, status_code=404)
    return _parcel_view(item, _load_dimension(), _time_acceleration(), datetime.now(timezone.utc))


_sf_lock = threading.Lock()
_sf_conn = None


def _snowflake_connection():
    global _sf_conn
    with _sf_lock:
        if _sf_conn is None or _sf_conn.is_closed():
            import snowflake.connector

            _sf_conn = snowflake.connector.connect(
                account=os.environ["OFD_SF_ACCOUNT"],
                user=os.environ["OFD_SF_USER"],
                private_key_file=os.environ["OFD_SF_PRIVATE_KEY_PATH"],
                private_key_file_pwd=os.environ.get("OFD_SF_PRIVATE_KEY_PASSPHRASE") or None,
                role=os.environ.get("OFD_SF_ROLE", "OFD_READER"),
                warehouse=os.environ.get("OFD_SF_WAREHOUSE", "OFD_WH"),
                database=os.environ.get("OFD_SF_DATABASE", "OUTFORDELIVERY"),
                schema=os.environ.get("OFD_SF_SCHEMA", "MARTS"),
            )
        return _sf_conn


def _mart_duckdb(name: str) -> list[dict]:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        cur = con.execute(f"select * from {name}")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()


def _mart_snowflake(name: str) -> list[dict]:
    cur = _snowflake_connection().cursor()
    try:
        cur.execute(f"select * from {name}")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close()


def _normalise_row(row: dict) -> dict:
    return {key.lower(): float(value) if isinstance(value, Decimal) else value for key, value in row.items()}


def _mart(name: str):
    try:
        if name not in MARTS:
            raise ValueError(f"unknown mart {name!r}")
        if os.environ.get("OFD_WAREHOUSE", "duckdb").lower() == "snowflake":
            return [_normalise_row(row) for row in _mart_snowflake(name)]
        return _mart_duckdb(name)
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/history/funnel")
def history_funnel() -> dict:
    return {"rows": _mart("gold_delivery_funnel")}


@app.get("/api/history/on_time")
def history_on_time() -> dict:
    return {"rows": _mart("gold_on_time_by_region")}


@app.get("/api/history/failures")
def history_failures() -> dict:
    return {"rows": _mart("gold_failure_analysis")}


@app.get("/api/history/weather")
def history_weather() -> dict:
    return {"rows": _mart("gold_weather_impact")}


_static_dir = Path(__file__).resolve().parent / "static"
if _static_dir.exists():
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
