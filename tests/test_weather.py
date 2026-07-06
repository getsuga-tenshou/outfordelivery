from simulator.network import load_network
from simulator.weather import WeatherProvider, parse_open_meteo_current

CODES = {
    "temperature": "temperature_2m",
    "wind_speed": "wind_speed_10m",
    "precipitation": "precipitation",
}
BAD_CFG = {"wind_speed_mps": 10.0, "precipitation": 0.5}


def test_parse_open_meteo_current_reads_values():
    entry = {
        "latitude": 52.37,
        "longitude": 4.90,
        "current_units": {"temperature_2m": "°C", "wind_speed_10m": "m/s", "precipitation": "mm"},
        "current": {"time": "2026-06-24T10:00", "temperature_2m": 15.3, "wind_speed_10m": 3.2, "precipitation": 0.0},
    }
    parsed = parse_open_meteo_current(entry, CODES)
    assert parsed["temperature"] == 15.3
    assert parsed["wind_speed"] == 3.2
    assert parsed["precipitation"] == 0.0
    assert parsed["_observed_at"] == "2026-06-24T10:00"


def test_parse_open_meteo_current_handles_missing_values():
    entry = {"current": {"time": "2026-06-24T10:00", "temperature_2m": 9.0}}
    parsed = parse_open_meteo_current(entry, CODES)
    assert parsed["temperature"] == 9.0
    assert parsed["wind_speed"] is None
    assert parsed["precipitation"] is None


def test_synthetic_fallback_covers_all_hubs():
    net = load_network()
    provider = WeatherProvider({"parameters": CODES, "bad_weather": BAD_CFG}, net, offline=True)
    obs = provider.fetch()
    assert set(obs) == {hub.id for hub in net.hubs}
    for ob in obs.values():
        assert ob.source == "synthetic"
        assert ob.temperature is not None
        assert isinstance(ob.bad, bool)


def test_is_bad_thresholds():
    net = load_network()
    provider = WeatherProvider({"parameters": CODES, "bad_weather": BAD_CFG}, net, offline=True)
    assert provider._is_bad(12.0, 0.0) is True
    assert provider._is_bad(2.0, 1.0) is True
    assert provider._is_bad(2.0, 0.0) is False
    assert provider._is_bad(None, None) is False


def test_is_bad_matches_fetched_observations():
    net = load_network()
    provider = WeatherProvider({"parameters": CODES, "bad_weather": BAD_CFG}, net, offline=True)
    obs = provider.fetch()
    for hub_id, ob in obs.items():
        assert provider.is_bad(hub_id) == ob.bad
