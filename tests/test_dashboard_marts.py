from decimal import Decimal

import duckdb
import pytest
from fastapi.testclient import TestClient

from dashboard import app as dashboard

client = TestClient(dashboard.app)

SNOWFLAKE_ROWS = {
    "gold_delivery_funnel": [{"EVENT_TYPE": "DELIVERED", "EVENTS": 12, "PARCELS": 10}],
    "gold_on_time_by_region": [
        {"REGION": "Utrecht", "PARCELS": 4, "DELIVERED": 3, "ON_TIME": 2, "ON_TIME_RATE": Decimal("0.6667")}
    ],
    "gold_failure_analysis": [
        {
            "REGION": "Utrecht",
            "PARCELS": 4,
            "PARCELS_WITH_FAILURE": 1,
            "RETURNED": 0,
            "FAILURE_RATE": Decimal("0.2500"),
            "RETURN_RATE": Decimal("0.0000"),
        }
    ],
    "gold_weather_impact": [
        {"BAD_WEATHER": True, "DELIVERY_ATTEMPTS": 5, "FAILURES": 1, "FAILURE_RATE": Decimal("0.2000")}
    ],
}
ENDPOINTS = [
    "/api/history/funnel",
    "/api/history/on_time",
    "/api/history/failures",
    "/api/history/weather",
]


@pytest.fixture
def duckdb_path(tmp_path, monkeypatch):
    path = str(tmp_path / "marts.duckdb")
    con = duckdb.connect(path)
    con.execute("create table gold_delivery_funnel as select 'DELIVERED' as event_type, 12 as events, 10 as parcels")
    con.execute(
        "create table gold_on_time_by_region as select 'Utrecht' as region, 4 as parcels, 3 as delivered, "
        "2 as on_time, round(2 * 1.0 / 3, 4) as on_time_rate"
    )
    con.execute(
        "create table gold_failure_analysis as select 'Utrecht' as region, 4 as parcels, "
        "1 as parcels_with_failure, 0 as returned, round(1 * 1.0 / 4, 4) as failure_rate, "
        "round(0 * 1.0 / 4, 4) as return_rate"
    )
    con.execute(
        "create table gold_weather_impact as select true as bad_weather, 5 as delivery_attempts, "
        "1 as failures, round(1 * 1.0 / 5, 4) as failure_rate"
    )
    con.close()
    monkeypatch.setattr(dashboard, "DUCKDB_PATH", path)
    return path


def test_default_backend_is_duckdb(duckdb_path, monkeypatch):
    monkeypatch.delenv("OFD_WAREHOUSE", raising=False)
    monkeypatch.setattr(dashboard, "_mart_snowflake", lambda name: pytest.fail("snowflake must not be used"))
    body = client.get("/api/history/on_time").json()
    assert body == {"rows": [{"region": "Utrecht", "parcels": 4, "delivered": 3, "on_time": 2, "on_time_rate": 0.6667}]}


def test_snowflake_rows_match_the_duckdb_shape(duckdb_path, monkeypatch):
    monkeypatch.delenv("OFD_WAREHOUSE", raising=False)
    expected = {url: client.get(url).json() for url in ENDPOINTS}
    monkeypatch.setenv("OFD_WAREHOUSE", "snowflake")
    monkeypatch.setattr(dashboard, "_mart_snowflake", lambda name: SNOWFLAKE_ROWS[name])
    for url in ENDPOINTS:
        assert client.get(url).json() == expected[url]


def test_unknown_mart_is_rejected(monkeypatch):
    monkeypatch.setattr(dashboard, "_mart_duckdb", lambda name: pytest.fail("must not run"))
    monkeypatch.setattr(dashboard, "_mart_snowflake", lambda name: pytest.fail("must not run"))
    for warehouse in ("duckdb", "snowflake"):
        monkeypatch.setenv("OFD_WAREHOUSE", warehouse)
        assert "error" in dashboard._mart("gold_delivery_funnel; drop table x")
        assert "error" in dashboard._mart("stg_parcels")


def test_backend_errors_become_an_error_payload(monkeypatch):
    monkeypatch.setenv("OFD_WAREHOUSE", "snowflake")

    def boom(name):
        raise RuntimeError("no connection")

    monkeypatch.setattr(dashboard, "_mart_snowflake", boom)
    assert dashboard._mart("gold_delivery_funnel") == {"error": "no connection"}


def test_normalise_row():
    assert dashboard._normalise_row({"A_B": Decimal("1.5"), "C": 3, "D": None}) == {"a_b": 1.5, "c": 3, "d": None}


@pytest.fixture
def reloaded(monkeypatch):
    """Reload the module under test-specific environment, then restore the defaults for other tests."""
    import importlib

    yield lambda: importlib.reload(dashboard)
    for name in ("OFD_DDB_ENDPOINT", "OFD_PARCELS_PATH"):
        monkeypatch.delenv(name, raising=False)
    importlib.reload(dashboard)


def _captured_resource(monkeypatch):
    calls = []

    class Resource:
        def Table(self, name):
            return name

    def resource(service, **kwargs):
        calls.append(kwargs)
        return Resource()

    monkeypatch.setattr(dashboard.boto3, "resource", resource)
    return calls


def test_live_backend_defaults_to_dynamodb_local(monkeypatch, reloaded):
    monkeypatch.delenv("OFD_DDB_ENDPOINT", raising=False)
    reloaded()
    calls = _captured_resource(monkeypatch)
    dashboard._table()
    assert calls[0]["endpoint_url"] == "http://localhost:8000"
    assert calls[0]["aws_access_key_id"] == "local"


def test_empty_endpoint_uses_the_default_aws_credential_chain(monkeypatch, reloaded):
    monkeypatch.setenv("OFD_DDB_ENDPOINT", "")
    reloaded()
    calls = _captured_resource(monkeypatch)
    dashboard._table()
    assert calls[0]["endpoint_url"] is None
    assert "aws_access_key_id" not in calls[0] and "aws_secret_access_key" not in calls[0]


def test_parcel_dimension_path_is_configurable(tmp_path, monkeypatch, reloaded):
    dim = tmp_path / "parcels.jsonl"
    dim.write_text('{"parcel_id": "P1", "sla_hours": 24}\n', encoding="utf-8")
    monkeypatch.setenv("OFD_PARCELS_PATH", str(dim))
    reloaded()
    assert dashboard._load_dimension() == {"P1": {"parcel_id": "P1", "sla_hours": 24}}
