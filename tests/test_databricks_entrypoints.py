import runpy
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def test_hello_returns_to_the_hosted_interpreter(monkeypatch, capsys):
    job = REPO / "databricks/jobs/hello.py"
    monkeypatch.syspath_prepend(str(REPO))
    monkeypatch.setattr(sys, "argv", [str(job), "--repo", str(REPO), "--catalog", "outfordelivery",
                                     "--secret-scope", "unused"])
    runpy.run_path(str(job), run_name="__main__")
    assert "# hubs (8):" in capsys.readouterr().out
