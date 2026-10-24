from __future__ import annotations

import io
import json
from pathlib import Path

from streaming.batches import DATA_FILES, digest, file_lock, read_batch
from streaming.databricks_helpers import batch_path


def _download(files, path: str) -> bytes | None:
    from databricks.sdk.errors import NotFound

    try:
        response = files.download(path)
    except NotFound:
        return None
    with response.contents as content:
        return content.read()


def upload_batch(files, manifest: Path, catalog: str, acceleration: float) -> str:
    """Immutable Files API publication. COMPLETE is created only after remote byte verification."""
    manifest = Path(manifest).resolve()
    if manifest.name != "manifest.json":
        raise ValueError("--manifest must name an existing manifest.json")
    root = manifest.parent
    with file_lock(root.parent / ".locks" / f"{root.name}.lock"):
        batch = read_batch(root, acceleration)
        target = batch_path(catalog, batch["manifest"]["batch_id"])
        payload = {name: (root / name).read_bytes() for name in (*DATA_FILES, "manifest.json", "COMPLETE")}
        # Check again after capturing bytes so local edits cannot bypass the validator.
        if digest(payload["manifest.json"]) != payload["COMPLETE"].decode().strip():
            raise ValueError("Batch changed during upload")
        if json.loads(payload["manifest.json"]) != batch["manifest"]:
            raise ValueError("Manifest changed during upload")
        for name in DATA_FILES:
            if digest(payload[name]) != batch["manifest"]["files"][name]["sha256"]:
                raise ValueError("Batch changed during upload")
        remote_complete = _download(files, f"{target}/COMPLETE")
        if remote_complete is not None and remote_complete != payload["COMPLETE"]:
            raise ValueError("Remote batch ID already belongs to a different manifest")
        files.create_directory(target)
        for name, data in payload.items():
            remote = _download(files, f"{target}/{name}")
            if remote is not None:
                if remote != data:
                    raise ValueError(f"Conflicting remote file {name}; retain this batch ID for recovery")
            elif remote_complete is not None:
                raise ValueError(f"Completed remote batch is corrupt: missing {name}")
            else:
                # Never overwrite: concurrent retries can only publish the same immutable bytes.
                files.upload(f"{target}/{name}", io.BytesIO(data), overwrite=False)
            if _download(files, f"{target}/{name}") != data:
                raise OSError(f"Remote verification failed: {name}; retry the same manifest")
    return target
