from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from streaming.batch_upload import upload_batch  # noqa: E402
from streaming.rules import load_time_acceleration  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload an existing completed batch; never generates input")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog", default="outfordelivery")
    parser.add_argument("--profile", default=None)
    args = parser.parse_args(argv)
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=args.profile)
    path = upload_batch(client.files, args.manifest, args.catalog, load_time_acceleration(REPO))
    print(f"# uploaded and verified {path}")
    print(f"# run outfordelivery_pipeline with batch_id={path.rsplit('/', 1)[-1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
