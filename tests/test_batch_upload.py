from __future__ import annotations

import io
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound

from streaming.batch_upload import upload_batch
from streaming.batches import DATA_FILES, publish_batch, read_batch
from streaming.databricks_helpers import batch_path, silver_events_ddl, silver_parcels_ddl


class Files:
    def __init__(self):
        self.data = {}
        self.uploads = []
        self.fail = None

    def create_directory(self, path):
        pass

    def download(self, path):
        if path not in self.data:
            raise NotFound("missing")
        return SimpleNamespace(contents=io.BytesIO(self.data[path]))

    def upload(self, path, contents, overwrite=False):
        assert not overwrite
        if path.rsplit("/", 1)[-1] == self.fail:
            raise OSError("injected transfer failure")
        assert path not in self.data
        self.data[path] = contents.read()
        self.uploads.append(path)


@pytest.mark.parametrize("failure", [*DATA_FILES, "manifest.json", "COMPLETE"])
def test_partial_upload_is_not_complete_and_retry_reuses_identity(tmp_path, paired_records, failure):
    path = publish_batch(tmp_path, "fixture", *paired_records, 37, {"kind": "replay"})
    files = Files()
    files.fail = failure
    with pytest.raises(OSError, match="injected"):
        upload_batch(files, path / "manifest.json", "c", 37)
    assert batch_path("c", "fixture") + "/COMPLETE" not in files.data
    files.fail = None
    target = upload_batch(files, path / "manifest.json", "c", 37)
    assert files.uploads[-1].endswith("/COMPLETE")
    uploads = files.uploads[:]
    assert upload_batch(files, path / "manifest.json", "c", 37) == target
    assert files.uploads == uploads
    # Independently run the same reader as the remote jobs on the downloaded bytes.
    remote = tmp_path / "download" / "fixture"
    remote.mkdir(parents=True)
    for name in (*DATA_FILES, "manifest.json", "COMPLETE"):
        (remote / name).write_bytes(files.data[f"{target}/{name}"])
    assert read_batch(remote, 37) == read_batch(path, 37)


def test_bad_local_batch_never_touches_remote(tmp_path, paired_records):
    path = publish_batch(tmp_path, "fixture", *paired_records, 37, {"kind": "replay"})
    files = Files()
    with pytest.raises(ValueError, match="time_acceleration"):
        upload_batch(files, path / "manifest.json", "c", 120)
    (path / "COMPLETE").unlink()
    with pytest.raises(FileNotFoundError):
        upload_batch(files, path / "manifest.json", "c", 37)
    assert not files.data


def test_remote_conflict_never_overwrites_or_publishes(tmp_path, paired_records):
    path = publish_batch(tmp_path, "fixture", *paired_records, 37, {"kind": "replay"})
    files = Files()
    files.data[batch_path("c", "fixture") + "/events.jsonl"] = b"conflicting input"
    with pytest.raises(ValueError, match="Conflicting"):
        upload_batch(files, path / "manifest.json", "c", 37)
    assert not files.uploads
    assert not any(name.endswith("/COMPLETE") for name in files.data)


def test_selected_batch_rejects_partial_and_only_reads_exact_path(tmp_path, paired_records, monkeypatch):
    from streaming import databricks_batches

    path = publish_batch(tmp_path, "fixture", *paired_records, 37, {"kind": "replay"})
    monkeypatch.setattr(databricks_batches, "batch_path", lambda catalog, batch_id: str(tmp_path / batch_id))
    assert databricks_batches.selected_batch("c", "fixture", 37)["events"] == paired_records[0]
    with pytest.raises(ValueError, match="time_acceleration"):
        databricks_batches.selected_batch("c", "fixture", 120)
    (tmp_path / "unfinished").mkdir()
    with pytest.raises(FileNotFoundError):
        databricks_batches.selected_batch("c", "unfinished", 37)
    (path / "COMPLETE").unlink()
    with pytest.raises(FileNotFoundError):
        databricks_batches.selected_batch("c", "fixture", 37)


def test_disabled_dynamo_does_not_read_secrets_or_construct_writer(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "databricks/jobs/bronze_stream.py"
    spec = spec_from_file_location("bronze_job", path)
    job = module_from_spec(spec)
    spec.loader.exec_module(job)
    monkeypatch.setattr(job, "get_secret", lambda *args, **kwargs: pytest.fail("unexpected secret lookup"))
    args = job.parse_args(["--repo", ".", "--catalog", "c", "--batch-id", "fixture"])
    assert args.source == "volume"
    assert job.optional_state_writer(args) is None


def test_required_delta_ddl_has_no_iceberg_properties():
    for ddl in (silver_events_ddl("c", False), silver_parcels_ddl("c", False)):
        assert "USING DELTA" in ddl
        assert "iceberg" not in ddl.lower()
        assert "TBLPROPERTIES" not in ddl


@pytest.mark.parametrize("catalog,batch_id", [("../c", "fixture"), ("c", "../fixture"), ("c", ""), ("c", "a/b")])
def test_batch_path_rejects_unsafe_selection(catalog, batch_id):
    with pytest.raises(ValueError):
        batch_path(catalog, batch_id)
