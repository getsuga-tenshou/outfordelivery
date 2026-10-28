import io
import json

import pytest
from botocore.exceptions import ClientError

from streaming.rules import load_s3_weather, parse_weather_cache


class S3:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.requests = []
        self.bodies = []

    def get_object(self, **kwargs):
        self.requests.append(kwargs)
        if self.error:
            raise ClientError({"Error": {"Code": self.error}}, "GetObject")
        body = io.BytesIO(self.payload)
        self.bodies.append(body)
        return {"Body": body}


def test_weather_preserves_bad_clear_and_unknown():
    payload = {"observations": {
        "AMS": {"bad": True, "source": "open-meteo"},
        "RTM": {"bad": False, "source": "synthetic"},
        "UTR": {"bad": None, "source": "unknown"},
        "EIN": {"bad": False, "source": "unknown"},
        "GRO": {},
    }}
    assert parse_weather_cache(json.dumps(payload)) == {
        "AMS": True, "RTM": False, "UTR": None, "EIN": None, "GRO": None,
    }
    assert parse_weather_cache("{}") == {}


@pytest.mark.parametrize("bad", ["false", 0, 1, [], {}])
def test_invalid_weather_flags_fail_instead_of_becoming_clear(bad):
    with pytest.raises(ValueError, match="Invalid weather state"):
        parse_weather_cache(json.dumps({"observations": {"AMS": {"bad": bad}}}))


def test_weather_reloads_s3_instead_of_reusing_a_snapshot():
    s3 = S3(b'{"observations":{"AMS":{"bad":true}}}')
    uri = "s3://lake/dimensions/weather_cache.json"
    assert load_s3_weather(s3, uri) == {"AMS": True}
    s3.payload = b'{"observations":{"AMS":{"bad":false}}}'
    assert load_s3_weather(s3, uri) == {"AMS": False}
    assert s3.requests == [{"Bucket": "lake", "Key": "dimensions/weather_cache.json"}] * 2
    assert all(body.closed for body in s3.bodies)


@pytest.mark.parametrize("code", ["NoSuchKey", "404"])
def test_missing_weather_object_is_unknown(code):
    assert load_s3_weather(S3(error=code), "s3://lake/weather.json") == {}


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchBucket", "SlowDown"])
def test_weather_service_failures_are_not_silently_unknown(code):
    with pytest.raises(ClientError):
        load_s3_weather(S3(error=code), "s3://lake/weather.json")


def test_corrupt_weather_fails_and_closes_the_s3_body():
    s3 = S3(b"not json")
    with pytest.raises(json.JSONDecodeError):
        load_s3_weather(s3, "s3://lake/weather.json")
    assert s3.bodies[0].closed
