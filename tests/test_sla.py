import json
from datetime import datetime, timedelta, timezone

from dashboard.app import _parcel_view, _sla_state
from streaming.dynamo import ParcelStateWriter
import io

import pytest
from botocore.exceptions import ClientError

from streaming.rules import (
    add_breached,
    is_breached,
    load_parcel_dimension,
    load_s3_parcel_dimension,
    load_time_acceleration,
)

CREATED = datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)
ACCEL = 120.0
SLA_HOURS = 24
# 24 h of SLA elapse in 12 wall minutes at 120x


def _at(minutes: float) -> datetime:
    return CREATED + timedelta(minutes=minutes)


def test_on_time():
    assert not is_breached("IN_TRANSIT", CREATED, SLA_HOURS, _at(5), ACCEL)


def test_at_80_percent_is_not_yet_breached():
    assert not is_breached("IN_TRANSIT", CREATED, SLA_HOURS, _at(12 * 0.8), ACCEL)


def test_breached_after_the_sla():
    assert not is_breached("OUT_FOR_DELIVERY", CREATED, SLA_HOURS, _at(12), ACCEL)
    assert is_breached("OUT_FOR_DELIVERY", CREATED, SLA_HOURS, _at(12.1), ACCEL)


def test_terminal_statuses_are_never_breached():
    for status in ("DELIVERED", "RETURNED"):
        assert not is_breached(status, CREATED, SLA_HOURS, _at(600), ACCEL)


def test_missing_dimension_is_not_breached():
    assert not is_breached("IN_TRANSIT", None, None, _at(600), ACCEL)
    assert not is_breached("IN_TRANSIT", CREATED, None, _at(600), ACCEL)
    assert not is_breached("IN_TRANSIT", CREATED, SLA_HOURS, None, ACCEL)


def test_naive_event_time_is_read_as_utc():
    naive = _at(30).replace(tzinfo=None)
    assert is_breached("IN_TRANSIT", CREATED, SLA_HOURS, naive, ACCEL)


def test_add_breached_marks_each_row(tmp_path):
    path = tmp_path / "parcels.jsonl"
    lines = [
        {"parcel_id": "P1", "created_at": CREATED.isoformat(), "sla_hours": 24},
        {"parcel_id": "P1", "created_at": (CREATED + timedelta(days=1)).isoformat(), "sla_hours": 24},
        {"parcel_id": "P2", "created_at": CREATED.isoformat(), "sla_hours": 24},
    ]
    path.write_text("".join(json.dumps(line) + "\n" for line in lines))
    dimension = load_parcel_dimension(path)
    assert dimension["P1"][0] == CREATED + timedelta(days=1)

    rows = [
        {"parcel_id": "P1", "status": "IN_TRANSIT", "event_ts": _at(30)},
        {"parcel_id": "P2", "status": "IN_TRANSIT", "event_ts": _at(30)},
        {"parcel_id": "P3", "status": "IN_TRANSIT", "event_ts": _at(30)},
    ]
    result = add_breached(rows, dimension, ACCEL)
    assert [row["breached"] for row in result] == [False, True, False]


def test_missing_dimension_file_gives_an_empty_dimension(tmp_path):
    assert load_parcel_dimension(tmp_path / "nope.jsonl") == {}


class _S3:
    def __init__(self, objects, error=None):
        self.objects, self.error, self.requested = objects, error, []

    def get_object(self, Bucket, Key):
        self.requested.append((Bucket, Key))
        if self.error:
            raise ClientError({"Error": {"Code": self.error}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def test_s3_dimension_matches_the_file_loader(tmp_path):
    lines = [
        {"parcel_id": "P1", "created_at": CREATED.isoformat(), "sla_hours": 6},
        {"parcel_id": "P1", "created_at": (CREATED + timedelta(hours=1)).isoformat(), "sla_hours": 6},
        {"parcel_id": "P2", "created_at": CREATED.isoformat(), "sla_hours": 48},
    ]
    text = "".join(json.dumps(line) + "\n" for line in lines)
    (tmp_path / "parcels.jsonl").write_text(text)
    s3 = _S3({("lake", "dimensions/parcels.jsonl"): text.encode()})
    dimension = load_s3_parcel_dimension(s3, "s3://lake/dimensions/parcels.jsonl")
    assert s3.requested == [("lake", "dimensions/parcels.jsonl")]
    assert dimension == load_parcel_dimension(tmp_path / "parcels.jsonl")


def test_s3_dimension_missing_object_is_empty_and_other_errors_raise():
    assert load_s3_parcel_dimension(_S3({}, error="NoSuchKey"), "s3://lake/dimensions/parcels.jsonl") == {}
    with pytest.raises(ClientError):
        load_s3_parcel_dimension(_S3({}, error="AccessDenied"), "s3://lake/dimensions/parcels.jsonl")


def test_time_acceleration_comes_from_the_simulation_config():
    from pathlib import Path

    assert load_time_acceleration(Path(__file__).resolve().parent.parent) == 120.0


def test_writer_stores_breached_even_when_false():
    writer = ParcelStateWriter("t", "http://localhost:1", "eu-west-1")
    captured = {}

    class Table:
        def put_item(self, **kwargs):
            captured.update(kwargs["Item"])

    writer._table = Table()
    writer.upsert_many([{"parcel_id": "P1", "version": 1, "status": "IN_TRANSIT", "breached": False}])
    assert captured["breached"] is False


def test_dashboard_sla_states_are_unchanged():
    dim = {"P1": {"created_at": CREATED.isoformat(), "sla_hours": 24}}
    item = {"parcel_id": "P1", "status": "IN_TRANSIT"}
    assert _sla_state(item, dim, ACCEL, _at(5)) == "on_time"
    assert _sla_state(item, dim, ACCEL, _at(10)) == "at_risk"
    assert _sla_state(item, dim, ACCEL, _at(13)) == "breached"
    assert _sla_state({"parcel_id": "P1", "status": "DELIVERED"}, dim, ACCEL, _at(600)) == "delivered"
    assert _sla_state({"parcel_id": "PX", "status": "IN_TRANSIT"}, dim, ACCEL, _at(5)) == "unknown"


def test_dashboard_normalizes_naive_and_offset_timestamps():
    dim = {"P1": {"created_at": CREATED.replace(tzinfo=None).isoformat(), "sla_hours": 24}}
    item = {"parcel_id": "P1", "status": "IN_TRANSIT"}
    assert _parcel_view(item, dim, 40, _at(30))["sim_hours_remaining"] == 4
    assert _sla_state(item, dim, 40, _at(37).astimezone(timezone(timedelta(hours=5, minutes=30)))) == "breached"
