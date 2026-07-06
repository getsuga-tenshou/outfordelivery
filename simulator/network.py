from __future__ import annotations

import math
import random
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
_POSTCODE_LETTERS = "ABCDEFGHJKLMNPRSTUVWXYZ"


@dataclass(frozen=True)
class Hub:
    id: str
    name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Town:
    name: str
    lat: float
    lon: float
    postcode: str
    region: str


@dataclass(frozen=True)
class Network:
    hubs: list[Hub]
    towns: list[Town]
    postcode_regions: dict[str, str]
    bbox: dict[str, float]


def load_network(path: str | Path | None = None) -> Network:
    path = Path(path) if path else REPO_ROOT / "config" / "network.yaml"
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    hubs = [Hub(h["id"], h["name"], float(h["lat"]), float(h["lon"])) for h in raw["hubs"]]
    towns = [
        Town(t["name"], float(t["lat"]), float(t["lon"]), str(t["postcode"]), t["region"])
        for t in raw.get("towns", [])
    ]
    regions = {str(k): v for k, v in raw["postcode_regions"].items()}
    return Network(hubs=hubs, towns=towns, postcode_regions=regions, bbox=raw["nl_bounding_box"])


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def nearest_hub(net: Network, lat: float, lon: float) -> Hub:
    return min(net.hubs, key=lambda h: haversine_km(lat, lon, h.lat, h.lon))


def jitter(net: Network, lat: float, lon: float, rng: random.Random, km: float = 8.0) -> tuple[float, float]:
    dlat = (rng.uniform(-1.0, 1.0) * km) / 111.0
    cos_lat = math.cos(math.radians(lat)) or 1.0
    dlon = (rng.uniform(-1.0, 1.0) * km) / (111.0 * cos_lat)
    box = net.bbox
    clamped_lat = min(max(lat + dlat, box["lat_min"]), box["lat_max"])
    clamped_lon = min(max(lon + dlon, box["lon_min"]), box["lon_max"])
    return round(clamped_lat, 5), round(clamped_lon, 5)


def pick_destination(net: Network, rng: random.Random) -> tuple[str, str, float, float]:
    letters = "".join(rng.choice(_POSTCODE_LETTERS) for _ in range(2))
    if net.towns:
        town = rng.choice(net.towns)
        lat, lon = jitter(net, town.lat, town.lon, rng, km=2.0)
        return f"{town.postcode} {letters}", town.region, lat, lon

    hub = rng.choice(net.hubs)
    lat, lon = jitter(net, hub.lat, hub.lon, rng, km=12.0)
    digits = rng.randint(1000, 9999)
    return f"{digits} {letters}", net.postcode_regions.get(str(digits)[0], "Unknown"), lat, lon
