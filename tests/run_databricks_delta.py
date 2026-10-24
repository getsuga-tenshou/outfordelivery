from __future__ import annotations

from pathlib import Path

import pytest

if __name__ == "__main__":
    raise SystemExit(pytest.main(["-q", "-p", "no:cacheprovider", str(Path(__file__).with_name("test_databricks_delta.py"))]))
