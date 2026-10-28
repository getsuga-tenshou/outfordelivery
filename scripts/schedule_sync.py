from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simulator.envfile import load_env
from streaming.batches import atomic_json, digest, file_lock, read_batch, safe_batch_id
from streaming.rules import as_utc, load_time_acceleration

REPO = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO / "data" / "scheduler" / "config.json"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_config(path: Path) -> dict:
    config = json.loads(path.read_bytes())
    if config["schema_version"] != 1 or not isinstance(config["job_id"], int) or config["job_id"] <= 0:
        raise ValueError("Invalid scheduler configuration")
    for name in ("batches_dir", "exports_dir"):
        if not Path(config[name]).is_absolute():
            raise ValueError(f"Scheduler {name} must be absolute")
    if config["time_acceleration"] != load_time_acceleration(REPO):
        raise ValueError("Scheduler clock differs from project configuration")
    return config


def initialize(path: Path, batches: Path, exports: Path, job_id: int, profile: str, catalog: str) -> dict:
    path = path.resolve()
    batches, exports = batches.resolve(), exports.resolve()
    if not batches.is_dir() or job_id <= 0:
        raise ValueError("Initialization needs an existing batch directory and a positive pipeline job ID")
    with file_lock(path.parent / ".scheduler.lock"):
        state_path = path.parent / "state.json"
        if path.exists() or state_path.exists():
            raise ValueError("Scheduler already initialized; preserve its configuration and state")
        config = {"schema_version": 1, "batches_dir": str(batches), "exports_dir": str(exports),
                  "job_id": job_id, "profile": profile, "catalog": catalog,
                  "time_acceleration": load_time_acceleration(REPO), "initialized_at": now()}
        # Baseline every existing ID, including unfinished directories, without changing retained inputs.
        excluded = sorted(entry.name for entry in batches.iterdir() if not entry.name.startswith("."))
        atomic_json(state_path, {"schema_version": 1, "config_hash": digest_config(config),
                                 "excluded_batch_ids": excluded, "runs": {}, "status": "initialized"})
        atomic_json(path, config)
        return config


def digest_config(config: dict) -> str:
    return digest(json.dumps(config, sort_keys=True).encode())


def read_state(path: Path, config: dict) -> dict:
    state = json.loads((path.resolve().parent / "state.json").read_bytes())
    if state["schema_version"] != 1 or state["config_hash"] != digest_config(config):
        raise ValueError("Scheduler configuration/state changed; restore the original pair")
    return state


def run_coordinator(command: list[str], log: Path) -> int:
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with log.open("xb") as output:
        return subprocess.run(command, cwd=REPO, stdout=output, stderr=subprocess.STDOUT,
                              creationflags=flags).returncode


def tick(path: Path, runner=run_coordinator) -> dict:
    path = path.resolve()
    with file_lock(path.parent / ".scheduler.lock"):
        config = read_config(path)
        state_path = path.parent / "state.json"
        state = read_state(path, config)

        def save(status: str, **changes):
            state.update(status=status, checked_at=now(), **changes)
            atomic_json(state_path, state)
            return state

        try:
            batches = Path(config["batches_dir"])
            if not batches.is_dir():
                raise ValueError("Configured batch directory is unavailable")
            pending = [name for name, run in state["runs"].items()
                       if run["status"] not in {"complete", "empty"}]
            if len(pending) > 1:
                raise ValueError("Multiple unfinished scheduler runs; inspect retained state")
            candidates = []
            for entry in sorted(batches.iterdir()):
                if entry.name.startswith(".") or entry.name in state["excluded_batch_ids"]:
                    continue
                if pending and entry.name != pending[0]:
                    continue
                if not entry.is_dir() or not (entry / "COMPLETE").exists():
                    continue
                safe_batch_id(entry.name)
                if entry.resolve().parent != batches:
                    raise ValueError("Batch directory escapes the configured input root")
                manifest_path = entry / "manifest.json"
                manifest_hash = digest(manifest_path.read_bytes())
                known = state["runs"].get(entry.name)
                if known and known["input_hash"] != manifest_hash:
                    raise ValueError("A scheduled batch identity changed; restore its original files")
                if known and known["status"] in {"complete", "empty"}:
                    continue
                manifest = json.loads(manifest_path.read_bytes())
                if manifest["source"]["kind"] != "redpanda":
                    continue
                batch = read_batch(entry, config["time_acceleration"])
                created = as_utc(datetime.fromisoformat(batch["manifest"]["created_at"]))
                candidates.append((created, entry.name, manifest_path, manifest_hash, batch))

            # An interrupted/failed attempt always precedes newly arrived work.
            if pending:
                candidates = [candidate for candidate in candidates if candidate[1] == pending[0]]
                if not candidates:
                    raise ValueError("Pending scheduled batch is missing or incomplete; restore it before retrying")
            if not candidates:
                return save("idle", last_error=None)
            _, batch_id, manifest_path, manifest_hash, batch = min(candidates)
            identity = f"{config['catalog']}/{config['job_id']}/{batch_id}/{manifest_hash}"
            record = state["runs"].setdefault(batch_id, {"input_hash": manifest_hash,
                        "export_id": "auto-" + digest(identity.encode()), "attempts": 0})
            if not batch["events"]:
                record.update(status="empty", completed_at=now())
                return save("empty", last_error=None)
            record.update(status="pending", attempts=record["attempts"] + 1, attempted_at=now())
            log = path.parent / "logs" / f"{record['export_id']}-{uuid.uuid4().hex}.log"
            log.parent.mkdir(exist_ok=True)
            record["log"] = str(log)
            save("running", active_batch_id=batch_id, last_error=None)

            exports = Path(config["exports_dir"])
            run_path = exports / ".runs" / f"{record['export_id']}.json"
            python = Path(sys.executable)
            if python.name.lower() == "pythonw.exe":
                python = python.with_name("python.exe")
            action = "resume" if run_path.exists() else "sync"
            command = [str(python), str(REPO / "scripts" / "sync_snowflake.py"), action,
                       "--export-id", record["export_id"], "--exports-dir", str(exports),
                       "--profile", config["profile"], "--catalog", config["catalog"],
                       "--job-id", str(config["job_id"])]
            if action == "sync":
                command.extend(["--manifest", str(manifest_path)])
            code = runner(command, log)
            record["last_exit_code"] = code
            if code:
                record["status"] = "failed"
                return save("failed", last_error="CoordinatorFailed")
            completed = json.loads(run_path.read_bytes())
            if (completed.get("stage") != "complete" or completed.get("dbt_status") != "success"
                    or completed.get("input_hash") != manifest_hash
                    or completed.get("export_id") != record["export_id"]):
                raise ValueError("Coordinator did not record successful completion for the selected input")
            record.update(status="complete", completed_at=now())
            return save("complete", active_batch_id=None, last_error=None)
        except Exception as exc:
            # Connector messages and credentials belong neither in task status nor terminal output.
            save("failed", last_error=type(exc).__name__)
            raise


def main(argv: list[str] | None = None) -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Schedule existing completed actual-stream batches through this device")
    parser.add_argument("action", choices=("init", "tick", "status"))
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--batches-dir", type=Path, default=REPO / "data" / "batches")
    parser.add_argument("--exports-dir", type=Path, default=REPO / "data" / "exports")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--profile", default="ofd")
    parser.add_argument("--catalog", default="outfordelivery")
    args = parser.parse_args(argv)
    try:
        if args.action == "init":
            if args.job_id is None:
                raise ValueError("init requires --job-id")
            initialize(args.config, args.batches_dir, args.exports_dir, args.job_id, args.profile, args.catalog)
            result = {"status": "initialized", "config": str(args.config.resolve())}
        elif args.action == "tick":
            result = tick(args.config)
        else:
            result = read_state(args.config, read_config(args.config))
        if sys.stdout is not None:
            print(json.dumps(result, sort_keys=True))
        return 2 if result["status"] == "failed" else 0
    except Exception as exc:
        if sys.stderr is not None:
            print(f"Scheduler stopped ({type(exc).__name__}); retained files/state unchanged except attempt status.",
                  file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
