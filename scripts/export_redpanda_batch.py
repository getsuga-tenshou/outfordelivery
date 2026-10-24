from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simulator.envfile import load_env
from streaming.batches import ProducerContext, atomic_json, file_lock, publish_batch, read_batch, safe_batch_id
from streaming.rules import load_time_acceleration

REPO = Path(__file__).resolve().parents[1]
EXPORT_GROUP = "ofd-batch-export-v1"


def consumer_config(bootstrap: str) -> dict:
    return {"bootstrap.servers": bootstrap, "group.id": EXPORT_GROUP,
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "enable.partition.eof": True, "auto.offset.reset": "error"}


def _committed(consumer, topic: str, partitions: list[int]) -> dict[int, int]:
    offsets = consumer.committed([TopicPartition(topic, partition) for partition in partitions], timeout=10)
    for offset in offsets:
        if offset.error:
            raise KafkaException(offset.error)
    return {offset.partition: offset.offset for offset in offsets}


def _commit_published(consumer, source: dict) -> None:
    topic = source["topic"]
    current = _committed(consumer, topic, [row["partition"] for row in source["partitions"]])
    offsets = []
    for row in source["partitions"]:
        committed = current[row["partition"]]
        if committed < 0:
            low, _ = consumer.get_watermark_offsets(TopicPartition(topic, row["partition"]), timeout=10, cached=False)
            if row["start"] != low:
                raise ValueError("Export group has lost its offsets; refusing to skip an earlier unsaved range")
        if committed >= 0 and committed < row["start"]:
            raise ValueError("Consumer offsets precede this batch; finish the earlier batch first")
        if committed < row["end"]:
            offsets.append(TopicPartition(topic, row["partition"], row["end"]))
    if offsets:
        results = consumer.commit(offsets=offsets, asynchronous=False)
        for result in results:
            if result.error:
                raise KafkaException(result.error)


def _consume(consumer, decode, source: dict, timeout: float) -> list[dict]:
    topic = source["topic"]
    ranges = {row["partition"]: row for row in source["partitions"]}
    active = {partition for partition, row in ranges.items() if row["start"] < row["end"]}
    if not active:
        return []
    consumer.assign([TopicPartition(topic, partition, ranges[partition]["start"]) for partition in active])
    events = {}
    deadline = time.monotonic() + timeout
    while active:
        if time.monotonic() >= deadline:
            raise TimeoutError("Export timed out; retry the same --batch-id to resume its fixed offset ranges")
        message = consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
        if message is None:
            continue
        partition = message.partition()
        if partition not in active:
            continue
        row = ranges[partition]
        error = message.error()
        if error:
            if error.code() != KafkaError._PARTITION_EOF:
                raise KafkaException(error)
            finished = message.offset() >= row["end"]
        else:
            offset = message.offset()
            if row["start"] <= offset < row["end"]:
                event = decode(message.value(), SerializationContext(topic, MessageField.VALUE))
                if event is None:
                    raise ValueError("Tombstone encountered; no offsets committed")
                known = events.setdefault(event["event_id"], event)
                if known != event:
                    raise ValueError("One event ID has conflicting payloads")
            finished = offset + 1 >= row["end"]
        if finished:
            active.remove(partition)
            consumer.pause([TopicPartition(topic, partition)])
    return list(events.values())


def export_batch(consumer, decode, context: ProducerContext, root: Path, batch_id: str,
                 bootstrap: str, topic: str, acceleration: float, timeout: float = 60) -> Path:
    """Snapshot, publish durably, then commit only the reserved export group's offsets."""
    batch_id = safe_batch_id(batch_id)
    root = Path(root)
    # One export on this device/root at a time, including recovery after partial broker commits.
    with file_lock(root / ".locks" / "redpanda-export.lock"):
        metadata = consumer.list_topics(topic=topic, timeout=10)
        info = metadata.topics.get(topic)
        if info is None or info.error:
            raise ValueError(f"Cannot read topic metadata for {topic}")
        identity = {"kind": "redpanda", "bootstrap": bootstrap, "cluster_id": metadata.cluster_id,
                    "topic": topic, "group_id": EXPORT_GROUP}
        # Recover all published batches before making a new range; a corrupt completed batch blocks progress.
        completed = [(marker.parent, json.loads((marker.parent / "manifest.json").read_bytes()))
                     for marker in root.glob("*/COMPLETE") if not marker.parent.name.startswith(".")]
        for path, manifest in sorted(completed, key=lambda item: item[1]["created_at"]):
            source = manifest["source"]
            if all(source.get(key) == value for key, value in identity.items()):
                published = read_batch(path, acceleration)
                _commit_published(consumer, published["manifest"]["source"])
        target = root / batch_id
        if target.exists():
            batch = read_batch(target, acceleration)
            source = batch["manifest"]["source"]
            if not all(source.get(key) == value for key, value in identity.items()):
                raise ValueError("Batch ID belongs to another source")
            _commit_published(consumer, source)
            return target
        plan_path = root / ".plans" / f"{batch_id}.json"
        # An unfinished export must be retried with its original ID, never bypassed.
        for pending in (root / ".plans").glob("*.json"):
            if pending != plan_path and not (root / pending.stem / "COMPLETE").exists():
                raise ValueError(f"Unfinished export {pending.stem}; retry that --batch-id first")
        partitions = sorted(info.partitions)
        if plan_path.exists():
            plan = json.loads(plan_path.read_bytes())
            source = plan["source"]
            if (plan["time_acceleration"] != acceleration
                    or not all(source.get(key) == value for key, value in identity.items())):
                raise ValueError("Pending batch source/acceleration mismatch")
        else:
            committed = _committed(consumer, topic, partitions)
            ranges = []
            for partition in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10, cached=False)
                start = committed[partition] if committed[partition] >= 0 else low
                if not low <= start <= high:
                    raise ValueError("Committed offsets are outside retained data; refusing to skip source events")
                ranges.append({"partition": partition, "start": start, "end": high})
            source = {**identity, "partitions": ranges}
            atomic_json(plan_path, {"source": source, "time_acceleration": acceleration})
        for row in source["partitions"]:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, row["partition"]), timeout=10, cached=False)
            if low > row["start"] or high < row["end"]:
                raise ValueError("Pending batch offsets no longer retained; no offsets committed")
        events = _consume(consumer, decode, source, timeout)
        parcels, weather = context.pair(events, acceleration)
        target = publish_batch(root, batch_id, events, parcels, weather, acceleration, source)
        read_batch(target, acceleration)
        _commit_published(consumer, source)
        return target


def main(argv: list[str] | None = None) -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Export a bounded Redpanda batch with the producer's saved context")
    parser.add_argument("--batch-id", required=True, help="stable ID; reuse on every retry")
    parser.add_argument("--out", type=Path, default=REPO / "data" / "batches")
    parser.add_argument("--context-dir", type=Path, default=REPO / "data" / "producer_context")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--schema-registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    parser.add_argument("--topic", default="parcel.events")
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args(argv)
    registry = SchemaRegistryClient({"url": args.schema_registry})
    decode = AvroDeserializer(registry, (REPO / "schemas" / "parcel_event.avsc").read_text(encoding="utf-8"))
    consumer = Consumer(consumer_config(args.bootstrap))
    try:
        path = export_batch(consumer, decode, ProducerContext(args.context_dir), args.out,
                            args.batch_id, args.bootstrap, args.topic, load_time_acceleration(REPO), args.timeout)
        print(path / "manifest.json")
        return 0
    except (ValueError, OSError, KafkaException, TimeoutError) as exc:
        print(f"Export {args.batch_id} failed: {exc}. Keep its files and retry the same --batch-id.", file=sys.stderr)
        return 1
    finally:
        consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
