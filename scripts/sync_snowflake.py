from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_dbt
from simulator.envfile import load_env
from streaming.batch_upload import upload_batch
from streaming.batches import atomic_json, digest, file_lock, read_batch, safe_batch_id
from streaming.rules import load_time_acceleration
from streaming.silver_exports import cleanup_export, download_export, read_export, utc_now
from streaming.snowflake_load import connect_loader, load_export

REPO = Path(__file__).resolve().parents[1]


@contextmanager
def required_files(kind: str, recovery: str):
    """Add local recovery context without relabeling connector or validation failures."""
    try:
        yield
    except FileNotFoundError as exc:
        path = str(exc.filename) if exc.filename is not None else "(path unavailable)"
        raise ValueError(f"Missing {kind} file: {path}. {recovery}; local files and run state retained.") from exc


def read_input_manifest(path: Path, acceleration: float) -> tuple[dict, str]:
    if path.name != "manifest.json":
        raise ValueError("--manifest must name the existing completed input manifest")
    with required_files("input batch", "Restore the original completed input batch at its retained path and retry"):
        batch = read_batch(path.parent, acceleration)
        return batch, digest(path.read_bytes())


def read_local_export(path: Path, acceleration: float) -> dict:
    with required_files("local export", "Restore the original files; for an unfinished download, use resume with the same export ID"):
        return read_export(path, acceleration)


def coordinate(root: Path, export_id: str, acceleration: float, *, manifest_path: Path | None = None,
               client=None, catalog: str = "outfordelivery", job_id: int | None = None,
               connection_factory=connect_loader, dbt_runner=run_dbt.main) -> Path:
    """A saved run and SDK idempotency token prevent source regeneration or duplicate processing on resume."""
    root = Path(root).resolve()
    safe_batch_id(export_id)
    state_file = root / ".runs" / f"{export_id}.json"
    with file_lock(root / ".sync.lock"):
        state = json.loads(state_file.read_bytes()) if state_file.exists() else {"export_id": export_id}
        if manifest_path is None:
            catalog = state.get("catalog", catalog)
        job_id = job_id or state.get("job_id")
        local = root / export_id

        def save(**changes):
            state.update(changes)
            state["updated_at"] = utc_now()
            atomic_json(state_file, state)

        try:
            if manifest_path is not None:
                manifest_path = Path(manifest_path).resolve()
                batch, identity = read_input_manifest(manifest_path, acceleration)
                if state.get("input_hash", identity) != identity or state.get("catalog", catalog) != catalog:
                    raise ValueError("Export ID already coordinates another input or catalog")
                save(input_manifest=str(manifest_path), input_hash=identity, catalog=catalog,
                     batch_id=batch["manifest"]["batch_id"])
            if not (local / "COMPLETE").exists():
                if client is None:
                    raise ValueError("Incomplete local export requires Databricks authentication to resume download")
                if not state.get("run_id") and state.get("input_manifest"):
                    if job_id is None:
                        raise ValueError("--job-id is required to start processing")
                    batch_manifest = Path(state["input_manifest"])
                    with required_files("input batch", "Restore the original input at its retained path and resume"):
                        if digest(batch_manifest.read_bytes()) != state["input_hash"]:
                            raise ValueError("Retained input manifest changed")
                        upload_batch(client.files, batch_manifest, catalog, acceleration)
                    save(stage="input_uploaded", job_id=job_id)
                    token = digest(f"{catalog}/{job_id}/{export_id}/{state['input_hash']}".encode())
                    request = client.jobs.run_now(job_id=job_id, idempotency_token=token,
                                                  job_parameters={"source": "volume", "batch_id": state["batch_id"],
                                                                  "export_id": export_id, "ddb_table": ""})
                    save(run_id=request.response.run_id, stage="processing")
                if state.get("run_id"):
                    from datetime import timedelta

                    run = client.jobs.wait_get_run_job_terminated_or_skipped(state["run_id"], timeout=timedelta(hours=2))
                    if run.state.result_state.value != "SUCCESS":
                        raise RuntimeError(f"Processing run {state['run_id']} did not succeed; inspect/repair that run")
                    save(stage="processed")
                local = download_export(client.files, catalog, export_id, root, acceleration)
            export = read_local_export(local, acceleration)
            if state.get("input_hash"):
                selected = {row["batch_id"]: row["manifest_hash"] for row in export["upstream"]["batches"]}
                if selected.get(state["batch_id"]) != state["input_hash"]:
                    raise ValueError("Downloaded snapshot does not contain this paired input identity")
                if str(state.get("run_id")) != export["upstream"]["run_id"]:
                    raise ValueError("Downloaded export belongs to another processing run")
            save(stage="downloaded", local_export=str(local))
            connection = connection_factory()
            try:
                published = load_export(connection, local, acceleration)
            finally:
                connection.close()
            # Data publication is authoritative in Snowflake; dbt status is deliberately independent.
            save(stage="loaded", published_this_attempt=published, dbt_status="pending")
            variables = json.dumps({"time_acceleration": export["time_acceleration"], "source_mode": "native"})
            for command in (["source", "freshness"], ["build"]):
                if dbt_runner([*command, "--target", "snowflake", "--vars", variables]):
                    save(stage="loaded", dbt_status="failed", dbt_failed_command=command)
                    raise RuntimeError("Snowflake data loaded successfully; dbt failed. Resume this export to rebuild marts.")
            save(stage="complete", dbt_status="success", dbt_failed_command=None, last_error=None)
            return local
        except Exception as exc:
            # Preserve the last successful stage. Never record raw connector errors, which may contain account details.
            save(last_error=type(exc).__name__)
            raise


def account_error(exc) -> str:
    code = str(getattr(exc, "errno", ""))
    message = str(exc).lower()
    if any(word in message for word in ("suspended", "trial expired", "trial has ended", "credit quota")):
        return "Snowflake account/credit suspension; credential renewal does not extend the trial. Local export retained."
    if code in {"250001", "250003", "390100", "390144", "390303"} or any(
            word in message for word in ("authentication", "jwt token", "private key", "password")):
        return "Snowflake authentication failed. Check the loader credentials; local export retained."
    return f"Transfer stopped ({type(exc).__name__}); local files and run state retained."


def main(argv: list[str] | None = None) -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Copy paired Silver snapshots through this device into native Snowflake")
    parser.add_argument("action", choices=("sync", "resume", "load", "cleanup"))
    parser.add_argument("--export-id", required=True)
    parser.add_argument("--exports-dir", type=Path, default=Path(os.getenv("OFD_EXPORTS_DIR", "data/exports")))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE", "ofd"))
    parser.add_argument("--catalog", default="outfordelivery")
    parser.add_argument("--job-id", type=int)
    args = parser.parse_args(argv)
    try:
        acceleration = load_time_acceleration(REPO)
        safe_batch_id(args.export_id)
        if args.action == "cleanup":
            cleanup_export(args.exports_dir, args.export_id, acceleration)
            print(f"Removed selected export {args.export_id}; another verified copy remains")
            return 0
        if args.action == "sync" and not args.manifest:
            raise ValueError("sync requires --manifest for an existing completed input batch")
        if args.action != "sync" and args.manifest:
            raise ValueError("Only sync accepts --manifest; retries use the retained run or local export")
        # Report invalid local input before SDK authentication can obscure the actual problem.
        # coordinate/upload_batch still validate again at the point of use.
        if args.manifest:
            read_input_manifest(args.manifest.resolve(), acceleration)
        local = args.exports_dir / args.export_id
        if args.action == "load":
            read_local_export(local, acceleration)
        client = None
        if not (local / "COMPLETE").exists():
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient(profile=args.profile)
        result = coordinate(args.exports_dir, args.export_id, acceleration, manifest_path=args.manifest,
                            client=client, catalog=args.catalog, job_id=args.job_id)
        print(f"Snapshot and dbt complete; retained export: {result}")
        return 0
    except FileNotFoundError as exc:
        path = str(exc.filename) if exc.filename is not None else "(path unavailable)"
        print(f"Missing required local file: {path}. Restore the file and retry; local files and run state retained.",
              file=sys.stderr)
        return 2
    except (ValueError, OSError, KeyError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except subprocess.SubprocessError:
        print("dbt process failed; retained exports and successful Snowflake load can be resumed", file=sys.stderr)
        return 2
    except Exception as exc:
        print(account_error(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
