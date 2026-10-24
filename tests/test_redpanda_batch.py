import io
import json
import struct
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer
from fastavro import schemaless_writer

from scripts.export_redpanda_batch import EXPORT_GROUP, consumer_config, export_batch
from streaming.batches import ProducerContext, read_batch

SCHEMA = (Path(__file__).resolve().parents[1] / "schemas" / "parcel_event.avsc").read_text(encoding="utf-8")


class Registry:
    def get_associations_by_resource_name(self, **kwargs):
        return []

    def get_schema(self, schema_id, subject_name=None, fmt=None):
        assert schema_id == 1
        return Schema(SCHEMA, "AVRO")


class Message:
    def __init__(self, event, partition, offset):
        self.event, self._partition, self._offset = event, partition, offset

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None

    def value(self):
        buffer = io.BytesIO()
        buffer.write(struct.pack(">bI", 0, 1))
        event = {**self.event, "event_ts": datetime.fromisoformat(self.event["event_ts"])}
        schemaless_writer(buffer, json.loads(SCHEMA), event)
        return buffer.getvalue()


class Broker:
    """Injectable broker transport; the exporter still uses the real Avro decoder."""

    def __init__(self, events):
        self.messages = []
        for partition in (0, 1):
            self.messages.extend(Message(event, partition, offset)
                                 for offset, event in enumerate(events[partition::2]))
        self.offsets = {0: -1001, 1: -1001}
        self.commits, self.queue = [], []
        self.polls = 0
        self.fail_commit = False
        self.after_assign = None
        self.low = 0

    def list_topics(self, **kwargs):
        return SimpleNamespace(cluster_id="cluster", topics={"parcel.events": SimpleNamespace(error=None, partitions={0: {}, 1: {}})})

    def committed(self, partitions, **kwargs):
        return [SimpleNamespace(partition=p.partition, offset=self.offsets[p.partition], error=None) for p in partitions]

    def get_watermark_offsets(self, partition, **kwargs):
        return self.low, sum(message.partition() == partition.partition for message in self.messages)

    def assign(self, partitions):
        if self.after_assign:
            self.after_assign(self)
        starts = {p.partition: p.offset for p in partitions}
        self.queue = [message for message in self.messages
                      if message.partition() in starts and message.offset() >= starts[message.partition()]]

    def poll(self, timeout):
        self.polls += 1
        return self.queue.pop(0) if self.queue else None

    def pause(self, partitions):
        self.queue = [message for message in self.queue if message.partition() not in {p.partition for p in partitions}]

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append({p.partition: p.offset for p in offsets})
        if self.fail_commit:
            # Simulate a commit succeeding on only one partition before the connection fails.
            self.offsets[offsets[0].partition] = offsets[0].offset
            raise OSError("injected commit failure")
        self.offsets.update({p.partition: p.offset for p in offsets})
        return [SimpleNamespace(error=None) for _ in offsets]


@pytest.fixture
def export_setup(tmp_path, paired_records):
    events, parcels, weather = paired_records
    store = ProducerContext(tmp_path / "context")
    snapshot_id = store.save_weather(next(iter(weather["snapshots"].values())))
    dimension = {p["parcel_id"]: p for p in parcels}
    for event in events:
        store.save_event(event, dimension[event["parcel_id"]], snapshot_id, 37)
    broker = Broker(events)
    decode = AvroDeserializer(Registry(), SCHEMA)

    def run(batch_id="first", **kwargs):
        return export_batch(broker, decode, store, tmp_path / "batches", batch_id,
                            "localhost:9092", "parcel.events", 37, **kwargs)

    return broker, store, run


def test_bounded_export_preserves_wire_events_and_ignores_later_production(export_setup, paired_records):
    broker, _, run = export_setup
    initial = list(broker.messages)
    broker.after_assign = lambda b: b.messages.append(Message(paired_records[0][0], 0, len(initial[::2])))
    path = run()
    batch = read_batch(path, 37)
    expected = {}
    decoder = AvroDeserializer(Registry(), SCHEMA)
    for message in initial:
        decoded = decoder(message.value(), None)
        decoded["event_ts"] = decoded["event_ts"].isoformat()
        expected[decoded["event_id"]] = decoded
    assert {event["event_id"]: event for event in batch["events"]} == expected
    assert batch["manifest"]["source"]["partitions"] == [
        {"partition": partition, "start": 0, "end": sum(m.partition() == partition for m in initial)}
        for partition in (0, 1)]
    assert broker.offsets[0] < len([m for m in broker.messages if m.partition() == 0])
    assert consumer_config("broker")["group.id"] == EXPORT_GROUP
    assert consumer_config("broker")["enable.auto.commit"] is False
    assert consumer_config("broker")["enable.auto.offset.store"] is False


def test_commit_failure_recovers_the_same_published_batch_without_reading_again(export_setup):
    broker, _, run = export_setup
    broker.fail_commit = True
    with pytest.raises(OSError, match="commit failure"):
        run()
    polls = broker.polls
    broker.fail_commit = False
    path = run()
    assert broker.polls == polls
    assert read_batch(path, 37)["manifest"]["counts"]["events"] == len(broker.messages)
    assert broker.offsets == {p: sum(m.partition() == p for m in broker.messages) for p in (0, 1)}


def test_missing_context_never_commits_and_retry_keeps_original_end_offsets(export_setup):
    broker, store, run = export_setup
    first = broker.messages[0].event
    from streaming.batches import digest

    context_path = store.root / "events" / f"{digest(first['event_id'].encode())}.json"
    saved = context_path.read_bytes()
    context_path.unlink()
    with pytest.raises(ValueError, match="Missing producer context"):
        run()
    assert not broker.commits
    with pytest.raises(ValueError, match="Unfinished export first"):
        run("different")
    broker.messages.append(Message(first, 0, sum(m.partition() == 0 for m in broker.messages)))
    context_path.write_bytes(saved)
    batch = read_batch(run(), 37)
    assert batch["manifest"]["counts"]["events"] == len(broker.messages) - 1


def test_timeout_never_commits(export_setup, monkeypatch):
    broker, _, run = export_setup
    with pytest.raises(TimeoutError):
        run(timeout=0)
    assert not broker.commits


def test_new_export_recovers_a_prior_published_batch_before_snapshotting(export_setup):
    broker, _, run = export_setup
    broker.fail_commit = True
    with pytest.raises(OSError):
        run("z-earlier")
    broker.fail_commit = False
    later = read_batch(run("a-later"), 37)
    assert later["manifest"]["counts"]["events"] == 0
    assert all(row["start"] == row["end"] for row in later["manifest"]["source"]["partitions"])


def test_duplicate_wire_event_is_deduplicated_without_losing_its_offset(export_setup):
    broker, _, run = export_setup
    message = broker.messages[0]
    offset = sum(m.partition() == 0 for m in broker.messages)
    broker.messages.append(Message(message.event, 0, offset))
    batch = read_batch(run(), 37)
    assert len(batch["events"]) == len(broker.messages) - 1
    assert broker.offsets[0] == offset + 1


def test_conflicting_wire_event_identity_cannot_publish_or_commit(export_setup):
    broker, _, run = export_setup
    message = broker.messages[0]
    broker.messages.append(Message({**message.event, "version": 999}, 0, sum(m.partition() == 0 for m in broker.messages)))
    with pytest.raises(ValueError, match="conflicting payloads"):
        run()
    assert not broker.commits


def test_recovery_refuses_lost_retention(export_setup):
    broker, _, run = export_setup
    with pytest.raises(TimeoutError):
        run(timeout=0)
    broker.low = 1
    with pytest.raises(ValueError, match="no longer retained"):
        run()
    assert not broker.commits


def test_retry_old_batch_never_rewinds_offsets(export_setup):
    broker, _, run = export_setup
    run("z-first")
    # Duplicated wire records have the same IDs and payloads; they are deduplicated within the next batch.
    for message in list(broker.messages):
        partition = message.partition()
        broker.messages.append(Message(message.event, partition, sum(m.partition() == partition for m in broker.messages)))
    run("a-second")
    latest = dict(broker.offsets)
    run("z-first")
    assert broker.offsets == latest


def test_corrupt_published_batch_blocks_commits_and_new_exports(export_setup):
    broker, _, run = export_setup
    broker.fail_commit = True
    with pytest.raises(OSError):
        run()
    # The context store and batches share the test's parent directory.
    _, store, _ = export_setup
    (store.root.parent / "batches" / "first" / "parcels.jsonl").write_bytes(b"broken")
    broker.fail_commit = False
    count = len(broker.commits)
    with pytest.raises(ValueError, match="checksum"):
        run("new")
    assert len(broker.commits) == count


def test_crash_after_hidden_completion_marker_does_not_publish_or_skip(export_setup, monkeypatch):
    from streaming import batches

    broker, store, run = export_setup
    original_replace = batches.os.replace

    def crash_on_publication(source, target):
        if source.is_dir():
            raise OSError("crash before directory publication")
        original_replace(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(batches.os, "replace", crash_on_publication)
        with pytest.raises(OSError, match="crash before"):
            run()
    assert not broker.commits
    assert not (store.root.parent / "batches" / "first").exists()
    assert read_batch(run(), 37)["manifest"]["counts"]["events"] == len(broker.messages)
