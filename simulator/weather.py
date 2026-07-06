from __future__ import annotations

import json
import logging
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

from .envfile import load_env
from .network import Network, load_network

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class WeatherObs:
    hub_id: str
    temperature: float | None
    wind_speed: float | None
    precipitation: float | None
    bad: bool
    source: str
    observed_at: str | None = None


def load_weather_config(path: str | Path | None = None) -> dict:
    path = Path(path) if path else REPO_ROOT / "config" / "sources.yaml"
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["weather"]


def parse_open_meteo_current(entry: dict, codes: dict) -> dict:
    current = entry.get("current", {}) or {}
    result: dict = {}
    for name, code in codes.items():
        value = current.get(code)
        result[name] = float(value) if isinstance(value, (int, float)) else None
    result["_observed_at"] = current.get("time")
    return result


class WeatherProvider:
    def __init__(self, weather_config: dict, network: Network, rng=None, offline: bool = False):
        self.cfg = weather_config
        self.net = network
        self.rng = rng or random.Random()
        self.codes = weather_config.get("parameters", {})
        self.bad_cfg = weather_config.get("bad_weather", {})
        self.offline = offline
        self._obs: dict[str, WeatherObs] = {}


    def _fetch_open_meteo(self, timeout: float = 15.0) -> dict[str, WeatherObs]:
        hubs = self.net.hubs
        params = {
            "latitude": ",".join(str(hub.lat) for hub in hubs),
            "longitude": ",".join(str(hub.lon) for hub in hubs),
            "current": ",".join(self.codes.values()),
            "wind_speed_unit": self.cfg.get("wind_speed_unit", "ms"),
            "timezone": "UTC",
        }
        response = requests.get(self.cfg["base_url"], params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        entries = payload if isinstance(payload, list) else [payload]

        obs: dict[str, WeatherObs] = {}
        for hub, entry in zip(hubs, entries):
            values = parse_open_meteo_current(entry, self.codes)
            obs[hub.id] = WeatherObs(
                hub_id=hub.id,
                temperature=values["temperature"],
                wind_speed=values["wind_speed"],
                precipitation=values["precipitation"],
                bad=self._is_bad(values["wind_speed"], values["precipitation"]),
                source="open-meteo",
                observed_at=values["_observed_at"],
            )
        self._obs = obs
        return obs


    def fetch(self) -> dict[str, WeatherObs]:
        if self.offline:
            return self._synthetic()
        try:
            return self._fetch_open_meteo()
        except Exception as exc:
            logger.warning("Open-Meteo fetch failed (%s); using synthetic weather", exc)
            return self._synthetic()

    def _synthetic(self) -> dict[str, WeatherObs]:
        obs: dict[str, WeatherObs] = {}
        for hub in self.net.hubs:
            wind = round(self.rng.uniform(0.0, 14.0), 1)
            precip = round(max(0.0, self.rng.uniform(-2.5, 1.5)), 1)
            obs[hub.id] = WeatherObs(
                hub_id=hub.id,
                temperature=round(self.rng.uniform(2.0, 18.0), 1),
                wind_speed=wind,
                precipitation=precip,
                bad=self._is_bad(wind, precip),
                source="synthetic",
            )
        self._obs = obs
        return obs

    def _is_bad(self, wind, precip) -> bool:
        wind_threshold = self.bad_cfg.get("wind_speed_mps", float("inf"))
        precip_threshold = self.bad_cfg.get("precipitation", float("inf"))
        windy = wind is not None and wind >= wind_threshold
        wet = precip is not None and precip >= precip_threshold
        return bool(windy or wet)

    def is_bad(self, hub_id: str) -> bool:
        obs = self._obs.get(hub_id)
        return bool(obs and obs.bad)

    def write_cache(self, path: str | Path | None = None) -> Path:
        path = Path(path) if path else REPO_ROOT / "data" / "weather_cache.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "observations": {hub_id: asdict(obs) for hub_id, obs in self._obs.items()},
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path


def _main() -> int:
    import sys

    load_env()
    provider = WeatherProvider(load_weather_config(), load_network())
    observations = provider.fetch()
    for hub_id, obs in observations.items():
        print(
            f"{hub_id}: temp={obs.temperature} wind={obs.wind_speed} "
            f"precip={obs.precipitation} bad={obs.bad} ({obs.source})"
        )
    print(f"# cache written to {provider.write_cache()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
