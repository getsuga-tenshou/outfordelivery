import json
import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("OFD_TEST_REDPANDA") != "1",
                                reason="set OFD_TEST_REDPANDA=1 with local Redpanda and Schema Registry running")


def test_actual_producer_bounded_export_retry_and_independent_consumers(tmp_path, monkeypatch):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    from scripts.export_redpanda_batch import consumer_config, export_batch, main as export_main
    from simulator import simulate
    from simulator.engine import SimulationEngine
    from simulator.producer import EventProducer
    from simulator.weather import WeatherProvider
    from streaming.batches import ProducerContext, read_batch

    bootstrap = os.environ.get("OFD_TEST_BOOTSTRAP", "localhost:9092")
    registry_url = os.environ.get("OFD_TEST_SCHEMA_REGISTRY", "http://localhost:8081")
    topic = f"ofd.ticket19.{uuid.uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[topic].result(timeout=20)
    registry = SchemaRegistryClient({"url": registry_url})
    sentinel = Consumer({**consumer_config(bootstrap), "group.id": f"ofd-ticket19-existing-{uuid.uuid4().hex}"})
    consumer = Consumer(consumer_config(bootstrap))
    try:
        sentinel.commit(offsets=[TopicPartition(topic, partition, 0) for partition in (0, 1)], asynchronous=False)
        config = simulate.load_sim_config()
        monkeypatch.setattr(simulate, "REPO_ROOT", tmp_path)
        monkeypatch.setattr(simulate, "load_sim_config", lambda: config)
        monkeypatch.setattr(WeatherProvider, "fetch", lambda self: self._synthetic())
        monkeypatch.setattr(WeatherProvider, "write_cache", lambda self: tmp_path / "weather_cache.json")
        # Shorten the live test schedule, keeping the configured acceleration and valid state sequence.
        states = ["CREATED", "LABEL_PRINTED", "COLLECTED", "AT_SORTING_HUB", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]
        monkeypatch.setattr(SimulationEngine, "plan_events", lambda self, parcel: [(i / 1000, state) for i, state in enumerate(states)])
        context_dir = tmp_path / "context"
        assert simulate.main(["--produce", "--max-parcels", "2", "--context-dir", str(context_dir),
                              "--bootstrap", bootstrap, "--schema-registry", registry_url, "--topic", topic]) == 0
        context = ProducerContext(context_dir)
        original_context = [json.loads(path.read_bytes()) for path in (context_dir / "events").glob("*.json")]
        assert len(original_context) == 14
        schema = (Path(__file__).resolve().parents[1] / "schemas" / "parcel_event.avsc").read_text(encoding="utf-8")
        decode = AvroDeserializer(registry, schema)
        extra_id = str(uuid.uuid4())

        class ContinuingProducer:
            def __getattr__(self, name):
                return getattr(consumer, name)

            def assign(self, partitions):
                record = original_context[0]
                event = {"event_id": extra_id, "parcel_id": record["parcel"]["parcel_id"],
                         "event_type": "CREATED", "status": "CREATED", "version": 1,
                         "hub_id": record["parcel"]["origin_hub"], "lat": 52.3, "lon": 4.9,
                         "event_ts": datetime.fromisoformat(record["parcel"]["created_at"])}
                context.save_event(event, record["parcel"], record["weather_id"], 120)
                producer = EventProducer(bootstrap, registry_url, topic)
                producer.produce(event["parcel_id"], event)
                assert producer.flush() == 0 and producer.failed == 0
                consumer.assign(partitions)

        root = tmp_path / "batches"
        first = export_batch(ContinuingProducer(), decode, context, root, "live-first", bootstrap, topic, 120)
        batch = read_batch(first, 120)
        assert {event["event_id"] for event in batch["events"]} == {record["event_id"] for record in original_context}
        assert extra_id not in {event["event_id"] for event in batch["events"]}
        assert batch["manifest"]["counts"]["parcels"] == 2
        # Exercise the real CLI and a second client: only the post-snapshot event remains.
        args = ["--batch-id", "live-second", "--out", str(root), "--context-dir", str(context_dir),
                "--bootstrap", bootstrap, "--schema-registry", registry_url, "--topic", topic]
        assert export_main(args) == 0
        assert [event["event_id"] for event in read_batch(root / "live-second", 120)["events"]] == [extra_id]
        before = (first / "manifest.json").read_bytes()
        assert export_main([*args[:1], "live-first", *args[2:]]) == 0
        assert (first / "manifest.json").read_bytes() == before
        sentinel_offsets = sentinel.committed([TopicPartition(topic, partition) for partition in (0, 1)], timeout=10)
        assert [offset.offset for offset in sentinel_offsets] == [0, 0]
    finally:
        consumer.close()
        sentinel.close()
        # Only the isolated topic/subject created by this test are removed.
        admin.delete_topics([topic])[topic].result(timeout=20)
        registry.delete_subject(f"{topic}-value")
