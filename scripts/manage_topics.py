#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from pathlib import Path

import requests
from confluent_kafka.admin import AdminClient, NewTopic

REPO_ROOT = Path(__file__).resolve().parents[1]


TOPICS = {
    "parcel.events": 6,
    "weather": 1,
    "parcel.events.dlq": 3,
}
SUBJECT = "parcel.events-value"
COMPATIBILITY = "BACKWARD"


def load_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def create_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new_topics = [NewTopic(name, num_partitions=parts, replication_factor=1) for name, parts in TOPICS.items()]
    for name, future in admin.create_topics(new_topics).items():
        try:
            future.result()
            print(f"created topic: {name} ({TOPICS[name]} partitions)")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                print(f"topic exists: {name}")
            else:
                print(f"ERROR creating {name}: {exc}", file=sys.stderr)


def set_compatibility(registry: str) -> None:
    url = f"{registry.rstrip('/')}/config/{SUBJECT}"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    try:
        response = requests.put(url, json={"compatibility": COMPATIBILITY}, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"compatibility for {SUBJECT}: {COMPATIBILITY}")
    except Exception as exc:
        print(
            f"NOTE: could not set compatibility for {SUBJECT} ({exc}). "
            "Produce at least once so the schema is registered, then re-run this script.",
            file=sys.stderr,
        )


def main() -> int:
    load_env()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    print(f"bootstrap={bootstrap}  registry={registry}")
    create_topics(bootstrap)
    set_compatibility(registry)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
