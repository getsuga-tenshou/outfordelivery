#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastavro import parse_schema

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = REPO_ROOT / "schemas"

EXPECTED_FIELDS = {
    "parcel_event.avsc": {
        "event_id",
        "parcel_id",
        "event_type",
        "hub_id",
        "lat",
        "lon",
        "event_ts",
        "status",
        "version",
    },
    "parcel.avsc": {
        "parcel_id",
        "origin_hub",
        "destination_postcode",
        "destination_region",
        "carrier",
        "service_level",
        "sla_hours",
        "created_at",
        "promised_by",
    },
}


def main() -> int:
    failures: list[str] = []

    for filename, expected in EXPECTED_FIELDS.items():
        path = SCHEMA_DIR / filename
        if not path.exists():
            failures.append(f"missing schema file: {filename}")
            continue

        raw = json.loads(path.read_text(encoding="utf-8"))
        parse_schema(raw)

        present = {field["name"] for field in raw["fields"]}
        missing = expected - present
        if missing:
            failures.append(f"{filename}: missing fields {sorted(missing)}")
        else:
            print(f"ok: {filename} parses, {len(present)} fields")

    if failures:
        print("SCHEMA VALIDATION FAILED:", file=sys.stderr)
        for item in failures:
            print(f"  - {item}", file=sys.stderr)
        return 1

    print("all schemas valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
