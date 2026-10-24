from datetime import datetime, timezone

from simulator.engine import SimulationEngine
from simulator.network import load_network
from simulator.simulate import _parcel_record, load_sim_config
from simulator.weather import WeatherObs
from streaming.databricks_helpers import PARCEL_SCHEMA_HINTS, WEATHER_DDL, parcels_dir, weather_rows, weather_table

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def test_weather_rows_cover_the_table_columns():
    obs = {
        "AMS": WeatherObs("AMS", 12.0, 4.5, 0.2, False, "open-meteo", "2026-10-23T11:45"),
        "RTM": WeatherObs("RTM", 11.0, 12.0, 0.0, True, "synthetic"),
    }
    rows = weather_rows(obs, NOW)
    assert rows[0] == ("AMS", datetime(2026, 10, 23, 11, 45, tzinfo=timezone.utc), 4.5, 0.2, False, "open-meteo")
    assert rows[1] == ("RTM", NOW, 12.0, 0.0, True, "synthetic")
    assert len(WEATHER_DDL.split(",")) == len(rows[0])


def test_parcel_schema_hints_name_real_parcel_fields():
    engine = SimulationEngine(load_sim_config(), load_network(), seed=1)
    record = _parcel_record(engine.build_parcel(NOW))
    hinted = [hint.split()[0] for hint in PARCEL_SCHEMA_HINTS.split(",")]
    assert set(hinted) <= set(record)


def test_names():
    assert weather_table("c") == "c.bronze.weather"
    assert parcels_dir("c") == "/Volumes/c/ops/raw/parcels/"
