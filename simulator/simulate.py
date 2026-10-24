#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .engine import SimulationEngine
from .envfile import load_env
from .network import load_network
from streaming.batches import ProducerContext, digest, json_bytes, publish_batch, read_batch
from streaming.rules import TERMINAL_STATUSES, load_time_acceleration

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_sim_config() -> dict:
    with open(REPO_ROOT / "config" / "simulation.yaml", "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _json_safe(event: dict) -> dict:
    out = dict(event)
    ts = out.get("event_ts")
    if hasattr(ts, "isoformat"):
        out["event_ts"] = ts.isoformat()
    return out


def _parcel_record(parcel) -> dict:
    return {
        "parcel_id": parcel.parcel_id,
        "origin_hub": parcel.origin_hub.id,
        "destination_postcode": parcel.destination_postcode,
        "destination_region": parcel.destination_region,
        "carrier": parcel.carrier,
        "service_level": parcel.service_level,
        "sla_hours": parcel.sla_hours,
        "created_at": parcel.created_at.isoformat(),
        "promised_by": parcel.promised_by.isoformat(),
    }


def run_dry(args: argparse.Namespace) -> int:
    engine = SimulationEngine(load_sim_config(), load_network(), seed=args.seed)
    events = engine.generate(args.count)
    for event in events:
        print(json.dumps(_json_safe(event)))
    print(
        f"\n# generated {len(events)} events for {args.count} parcels (dry run, no Kafka). "
        "Timestamps follow the simulated schedule.",
        file=sys.stderr,
    )
    return 0


def run_files(args: argparse.Namespace) -> int:
    from .weather import WeatherProvider, load_weather_config

    config, network = load_sim_config(), load_network()
    seed = config["seed"] if args.seed is None else args.seed
    reference = datetime.fromisoformat(args.reference_time) if args.reference_time else datetime.now(timezone.utc)
    weather = WeatherProvider(load_weather_config(), network, rng=random.Random(seed), offline=True)
    weather.fetch()
    snapshot = weather.snapshot(config["time_acceleration"], reference)
    snapshot_id = digest(json_bytes(snapshot))
    engine = SimulationEngine(config, network, weather_is_bad=weather.is_bad, seed=seed)
    parcels = []
    events = engine.generate(args.count, reference_time=reference, on_parcel=lambda parcel: parcels.append(_parcel_record(parcel)))
    batch_id = args.batch_id or f"replay-{uuid.uuid4().hex}"
    root = Path(args.out) if args.out else REPO_ROOT / "data" / "batches"
    path = publish_batch(root, batch_id, events, parcels,
                         {"snapshots": {snapshot_id: snapshot},
                          "event_context": {event["event_id"]: snapshot_id for event in events}},
                         engine.time_acceleration, {"kind": "replay", "seed": seed})
    print(f"# completed {path / 'manifest.json'} ({len(events)} events, {len(parcels)} parcels)", file=sys.stderr)
    return 0


def run_produce(args: argparse.Namespace) -> int:
    network = load_network()
    config = load_sim_config()

    from .weather import WeatherProvider, load_weather_config

    weather = WeatherProvider(load_weather_config(), network)
    observations = weather.fetch()
    cache = weather.write_cache()
    source = next(iter(observations.values())).source if observations else "n/a"
    bad_hubs = [hub for hub, obs in observations.items() if obs.bad]
    print(
        f"# weather: source={source}, bad_weather_hubs={bad_hubs or 'none'}, cache={cache}",
        file=sys.stderr,
    )

    from .producer import EventProducer

    bootstrap = args.bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    registry = args.schema_registry or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    producer = EventProducer(bootstrap, registry, topic=args.topic)
    engine = SimulationEngine(config, network, weather_is_bad=weather.is_bad, seed=args.seed)
    context = ProducerContext(Path(args.context_dir))
    snapshot_id = context.save_weather(weather.snapshot(engine.time_acceleration))
    dimensions = {}

    print(
        f"# producing to topic '{args.topic}' at {bootstrap} (Ctrl-C to stop)",
        file=sys.stderr,
    )
    parcels_path = REPO_ROOT / "data" / "parcels.jsonl"
    parcels_path.parent.mkdir(parents=True, exist_ok=True)
    parcels_file = parcels_path.open("a", encoding="utf-8")

    progress = {"count": 0, "last": time.monotonic()}

    def emit(key, event):
        context.save_event(event, dimensions[key], snapshot_id, engine.time_acceleration)
        producer.produce(key, event)
        if event["status"] in TERMINAL_STATUSES:
            dimensions.pop(key)
        progress["count"] += 1
        now = time.monotonic()
        if now - progress["last"] >= 5.0:
            print(
                f"# {progress['count']} events produced ({producer.delivered} delivered so far)...",
                file=sys.stderr,
            )
            progress["last"] = now

    def on_parcel(parcel):
        record = _parcel_record(parcel)
        dimensions[parcel.parcel_id] = record
        parcels_file.write(json.dumps(record) + "\n")
        parcels_file.flush()
        os.fsync(parcels_file.fileno())

    try:
        engine.run(emit, on_parcel=on_parcel, max_parcels=args.max_parcels, max_runtime_s=args.duration)
    except KeyboardInterrupt:
        print("\n# interrupted, flushing...", file=sys.stderr)
    finally:
        parcels_file.close()
        still_queued = producer.flush()

    print(
        f"# done: delivered={producer.delivered} failed={producer.failed} still_queued={still_queued}",
        file=sys.stderr,
    )
    return 0 if producer.failed == 0 and still_queued == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="outfordelivery event simulator")
    parser.add_argument("--produce", action="store_true", help="stream events to Kafka (default is a dry-run print)")
    parser.add_argument(
        "--sink", choices=["kafka", "files"], default="kafka", help="files: write a completed paired batch (no Kafka)"
    )
    parser.add_argument("--out", default=None, help="files sink: batches parent directory (default data/batches)")
    parser.add_argument("--batch-id", help="files sink: stable batch directory name; an existing batch is never overwritten")
    parser.add_argument("--reference-time", help="files sink: latest event UTC reference time (ISO 8601; naive means UTC)")
    parser.add_argument("--context-dir", default=str(REPO_ROOT / "data" / "producer_context"),
                        help="produce: retain local per-event parcel and weather context for the exporter")
    parser.add_argument("--verify-batch", help="validate an existing completed batch without generating or uploading data")
    parser.add_argument("--count", type=int, default=10, help="dry run: number of parcels to generate")
    parser.add_argument("--max-parcels", type=int, default=None, help="produce: stop after creating this many parcels")
    parser.add_argument("--duration", type=float, default=None, help="produce: stop after this many seconds")
    parser.add_argument("--seed", type=int, default=None, help="override the config seed")
    parser.add_argument("--topic", default="parcel.events", help="Kafka topic to produce to")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers (default env KAFKA_BOOTSTRAP)")
    parser.add_argument(
        "--schema-registry", default=None, help="Schema Registry URL (default env SCHEMA_REGISTRY_URL)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = build_parser().parse_args(argv)
    if args.verify_batch:
        batch = read_batch(Path(args.verify_batch), load_time_acceleration(REPO_ROOT))
        print(json.dumps(batch["manifest"]))
        return 0
    if args.sink == "files":
        return run_files(args)
    return run_produce(args) if args.produce else run_dry(args)


if __name__ == "__main__":
    raise SystemExit(main())
