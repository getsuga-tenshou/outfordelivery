from datetime import datetime, timedelta, timezone

from simulator.network import load_network
from streaming.validation import coord_in_bbox, is_known_hub, is_valid_event, not_in_future

NET = load_network()
HUBS = [hub.id for hub in NET.hubs]
BBOX = NET.bbox


def _event(hub="AMS", lat=52.3, lon=4.9, ts=None):
    return {"hub_id": hub, "lat": lat, "lon": lon, "event_ts": ts or datetime.now(timezone.utc)}


def test_coord_in_bbox():
    assert coord_in_bbox(52.3, 4.9, BBOX)
    assert not coord_in_bbox(0.0, 0.0, BBOX)
    assert not coord_in_bbox(48.85, 2.35, BBOX)


def test_is_known_hub():
    assert is_known_hub("AMS", HUBS)
    assert not is_known_hub("XXX", HUBS)


def test_not_in_future():
    now = datetime.now(timezone.utc)
    assert not_in_future(now - timedelta(hours=1), now)
    assert not_in_future(now + timedelta(minutes=2), now)
    assert not not_in_future(now + timedelta(hours=1), now)


def test_is_valid_event():
    now = datetime.now(timezone.utc)
    assert is_valid_event(_event(), BBOX, HUBS, now)
    assert not is_valid_event(_event(hub="XXX"), BBOX, HUBS, now)
    assert not is_valid_event(_event(lat=0.0, lon=0.0), BBOX, HUBS, now)
    assert not is_valid_event(_event(ts=now + timedelta(hours=2)), BBOX, HUBS, now)
