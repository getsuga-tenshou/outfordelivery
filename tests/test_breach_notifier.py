import importlib.util
from pathlib import Path

import pytest

LAMBDA = Path(__file__).resolve().parent.parent / "infra" / "lambda" / "breach_notifier.py"


@pytest.fixture
def notifier(monkeypatch):
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-central-1:123456789012:topic")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    spec = importlib.util.spec_from_file_location("breach_notifier", LAMBDA)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    published = []

    class FakeSns:
        def publish(self, **kwargs):
            published.append(kwargs)

    module.sns = FakeSns()
    module.published = published
    return module


def _image(parcel_id="P1", status="IN_TRANSIT", breached=None):
    image = {"parcel_id": {"S": parcel_id}, "status": {"S": status}}
    if breached is not None:
        image["breached"] = {"BOOL": breached}
    return image


def _record(new, old=None):
    body = {"NewImage": new}
    if old is not None:
        body["OldImage"] = old
    return {"dynamodb": body}


def test_alerts_when_breached_flips_to_true(notifier):
    for old in (None, _image(breached=False), _image()):
        notifier.published.clear()
        result = notifier.handler({"Records": [_record(_image(breached=True), old)]}, None)
        assert result == {"alerts": 1}
        assert len(notifier.published) == 1


def test_no_alert_on_a_repeat_true(notifier):
    event = {"Records": [_record(_image(breached=True), _image(breached=True))]}
    assert notifier.handler(event, None) == {"alerts": 0}
    assert notifier.published == []


def test_no_alert_when_not_breached(notifier):
    event = {"Records": [_record(_image(breached=False), _image(breached=False)), _record(_image())]}
    assert notifier.handler(event, None) == {"alerts": 0}


def test_still_alerts_on_returned(notifier):
    event = {"Records": [_record(_image(status="RETURNED", breached=False), _image(breached=False))]}
    assert notifier.handler(event, None) == {"alerts": 1}
    assert "P1" in notifier.published[0]["Subject"]


@pytest.mark.parametrize("breached", [False, True])
def test_repeat_returned_update_still_alerts(notifier, breached):
    image = _image(status="RETURNED", breached=breached)
    assert notifier.handler({"Records": [_record(image, image)]}, None) == {"alerts": 1}
    assert len(notifier.published) == 1


def test_return_and_breach_in_same_update_publish_one_alert(notifier):
    event = {"Records": [_record(_image(status="RETURNED", breached=True), _image(breached=False))]}
    assert notifier.handler(event, None) == {"alerts": 1}
    assert len(notifier.published) == 1


def test_breach_can_alert_again_after_it_clears(notifier):
    event = {"Records": [
        _record(_image(breached=True), _image(breached=False)),
        _record(_image(breached=True), _image(breached=True)),
        _record(_image(breached=False), _image(breached=True)),
        _record(_image(breached=True), _image(breached=False)),
    ]}
    assert notifier.handler(event, None) == {"alerts": 2}
    assert len(notifier.published) == 2


def test_scheduled_tick_does_nothing(notifier):
    assert notifier.handler({"source": "aws.events"}, None) == {"ok": True}
