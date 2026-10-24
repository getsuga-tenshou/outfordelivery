from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simulator.envfile import load_env
from streaming.batches import read_batch
from streaming.rules import checked_acceleration, load_time_acceleration

REPO = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Run dbt with the shared simulation clock; optionally verify an input batch")
    parser.add_argument("--batch", type=Path, help="completed batch directory or manifest; verifies its clock, not a Silver load")
    parser.add_argument("--vars", default="{}", help="dbt YAML/JSON variables; acceleration must match config/simulation.yaml")
    parser.add_argument("--project-dir", type=Path, default=REPO / "dbt")
    parser.add_argument("--profiles-dir", type=Path, default=REPO / "dbt")
    args, dbt_args = parser.parse_known_args(argv)
    try:
        acceleration = load_time_acceleration(REPO)
        if args.batch:
            path = args.batch.parent if args.batch.name == "manifest.json" else args.batch
            read_batch(path, acceleration)
        variables = yaml.safe_load(args.vars)
        if not isinstance(variables, dict):
            raise ValueError("--vars must be a mapping")
        if "time_acceleration" in variables and checked_acceleration(variables["time_acceleration"]) != acceleration:
            raise ValueError("dbt time_acceleration differs from the configured producer/dashboard clock")
        variables["time_acceleration"] = acceleration
    except (ValueError, OSError, yaml.YAMLError) as exc:
        print(f"Cannot run dbt: {exc}", file=sys.stderr)
        return 2
    command = [sys.executable, "-m", "dbt.cli.main", *(dbt_args or ["build"]),
               "--project-dir", str(args.project_dir.resolve()), "--profiles-dir", str(args.profiles_dir.resolve()),
               "--vars", json.dumps(variables)]
    return subprocess.run(command, cwd=args.project_dir).returncode


if __name__ == "__main__":
    raise SystemExit(main())
