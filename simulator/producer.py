from __future__ import annotations

import logging
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]


class EventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str = "parcel.events",
        schema_path: str | Path | None = None,
    ):
        self.topic = topic
        schema_path = Path(schema_path) if schema_path else REPO_ROOT / "schemas" / "parcel_event.avsc"
        schema_str = schema_path.read_text(encoding="utf-8")

        registry = SchemaRegistryClient({"url": schema_registry_url})
        self._value_serializer = AvroSerializer(registry, schema_str, lambda obj, ctx: obj)
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            logger.warning("delivery failed for key %s: %s", msg.key(), err)
        else:
            self.delivered += 1

    def produce(self, key: str, event: dict) -> None:
        value = self._value_serializer(event, SerializationContext(self.topic, MessageField.VALUE))
        self._producer.produce(
            topic=self.topic,
            key=self._key_serializer(key),
            value=value,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
