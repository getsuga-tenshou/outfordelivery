from pathlib import Path

import pytest

if __name__ == "__main__":
    raise SystemExit(pytest.main(["-q", "--noconftest", "-p", "no:cacheprovider",
                                 str(Path(__file__).with_name("test_stream_weather_spark.py"))]))
