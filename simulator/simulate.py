#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import yaml

from .engine import SimulationEngine
from .envfile import load_env
from .network import load_network

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

    print(
        f"# producing to topic '{args.topic}' at {bootstrap} (Ctrl-C to stop)",
        file=sys.stderr,
    )
    parcels_path = REPO_ROOT / "data" / "parcels.jsonl"
    parcels_path.parent.mkdir(parents=True, exist_ok=True)
    parcels_file = parcels_path.open("a", encoding="utf-8")

    progress = {"count": 0, "last": time.monotonic()}

    def emit(key, event):
        producer.produce(key, event)
        progress["count"] += 1
        now = time.monotonic()
        if now - progress["last"] >= 5.0:
            print(
                f"# {progress['count']} events produced ({producer.delivered} delivered so far)...",
                file=sys.stderr,
            )
            progress["last"] = now

    def on_parcel(parcel):
        parcels_file.write(json.dumps(_parcel_record(parcel)) + "\n")
        parcels_file.flush()

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
    return run_produce(args) if args.produce else run_dry(args)


if __name__ == "__main__":
    raise SystemExit(main())
