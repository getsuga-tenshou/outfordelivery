from __future__ import annotations

import heapq
import itertools
import random
import time as wallclock
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np

from . import statemachine as sm
from .network import Hub, Network, jitter, nearest_hub, pick_destination


@dataclass
class Parcel:
    parcel_id: str
    origin_hub: Hub
    destination_postcode: str
    destination_region: str
    destination_lat: float
    destination_lon: float
    carrier: str
    service_level: str
    sla_hours: int
    created_at: datetime
    promised_by: datetime


def _weighted_choice(rng: random.Random, options: dict[str, float]) -> str:
    items = list(options.items())
    total = sum(weight for _, weight in items)
    threshold = rng.uniform(0, total)
    cumulative = 0.0
    for name, weight in items:
        cumulative += weight
        if threshold <= cumulative:
            return name
    return items[-1][0]


class SimulationEngine:
    def __init__(self, sim_config: dict, network: Network, weather_is_bad=None, seed: int | None = None):
        self.net = network
        seed = sim_config.get("seed", 42) if seed is None else seed
        self.rng = random.Random(seed)
        self.np_rng = np.random.default_rng(seed)

        levels = sim_config["service_levels"]
        self.service_weights = {name: spec["weight"] for name, spec in levels.items()}
        self.sla_hours = {name: spec["sla_hours"] for name, spec in levels.items()}
        self.carriers = list(sim_config["carriers"])

        failure = sim_config["failure"]
        self.base_failure_rate = float(failure["base_failure_rate"])
        self.bad_weather_multiplier = float(failure["bad_weather_multiplier"])
        self.returned_share = float(failure["returned_share"])
        self.max_attempts = int(failure["max_delivery_attempts"])

        self.transit_hours = sim_config["transit_hours"]
        self.reschedule_extra = sim_config["reschedule_extra_hours"]
        self.dwell_multiplier_bad = float(sim_config["weather_effect"]["bad_weather_dwell_multiplier"])
        self.arrival_rate = float(sim_config["arrivals"]["new_parcels_per_minute"])
        self.time_acceleration = float(sim_config["time_acceleration"])
        self.min_step = float(sim_config["min_step_wall_seconds"])

        self.weather_is_bad = weather_is_bad or (lambda hub_id: False)


    def build_parcel(self, created_at: datetime) -> Parcel:
        rng, net = self.rng, self.net
        service_level = _weighted_choice(rng, self.service_weights)
        sla_hours = self.sla_hours[service_level]
        origin = rng.choice(net.hubs)
        postcode, region, dest_lat, dest_lon = pick_destination(net, rng)
        return Parcel(
            parcel_id=f"P{rng.randint(10**9, 10**10 - 1)}",
            origin_hub=origin,
            destination_postcode=postcode,
            destination_region=region,
            destination_lat=dest_lat,
            destination_lon=dest_lon,
            carrier=rng.choice(self.carriers),
            service_level=service_level,
            sla_hours=sla_hours,
            created_at=created_at,
            promised_by=created_at + timedelta(hours=sla_hours),
        )

    def plan_events(self, parcel: Parcel) -> list[tuple[float, str]]:
        rng, np_rng = self.rng, self.np_rng
        bad = self.weather_is_bad(parcel.origin_hub.id)
        dwell_scale = self.dwell_multiplier_bad if bad else 1.0

        spec = self.transit_hours[parcel.service_level]
        total = max(0.5, float(np_rng.normal(spec["mean"], spec["sd"]))) * dwell_scale

        gaps = np_rng.dirichlet(np.ones(len(sm.PRE_DELIVERY) - 1))
        offsets = np.cumsum(gaps * total)
        plan: list[tuple[float, str]] = [(0.0, sm.CREATED)]
        for index, state in enumerate(sm.PRE_DELIVERY[1:]):
            plan.append((float(offsets[index]), state))

        elapsed = float(offsets[-1])
        fail_p = self.base_failure_rate * (self.bad_weather_multiplier if bad else 1.0)
        attempts = 0
        while True:
            attempts += 1
            failed = rng.random() < fail_p and attempts <= self.max_attempts
            elapsed += abs(float(np_rng.normal(0.2, 0.1)))
            if not failed:
                plan.append((elapsed, sm.DELIVERED))
                break
            plan.append((elapsed, sm.DELIVERY_FAILED))
            if attempts >= self.max_attempts or rng.random() < self.returned_share:
                plan.append((elapsed, sm.RETURNED))
                break
            extra = max(0.5, float(np_rng.normal(self.reschedule_extra["mean"], self.reschedule_extra["sd"])))
            elapsed += extra * dwell_scale
            plan.append((elapsed, sm.RESCHEDULED))
            plan.append((elapsed, sm.OUT_FOR_DELIVERY))
        return plan

    def to_event(self, parcel: Parcel, version: int, event_type: str, event_ts: datetime) -> dict:
        if event_type in sm.DELIVERY_AREA:
            lat, lon = jitter(self.net, parcel.destination_lat, parcel.destination_lon, self.rng, km=4.0)
            hub_id = nearest_hub(self.net, parcel.destination_lat, parcel.destination_lon).id
        else:
            lat, lon = jitter(self.net, parcel.origin_hub.lat, parcel.origin_hub.lon, self.rng, km=6.0)
            hub_id = parcel.origin_hub.id
        return {
            "event_id": str(uuid.uuid4()),
            "parcel_id": parcel.parcel_id,
            "event_type": event_type,
            "hub_id": hub_id,
            "lat": lat,
            "lon": lon,
            "event_ts": event_ts,
            "status": event_type,
            "version": version,
        }


    def generate(self, n_parcels: int) -> list[dict]:
        events: list[dict] = []
        base = datetime.now(timezone.utc)
        for _ in range(n_parcels):
            parcel = self.build_parcel(base)
            for version, (offset_hours, event_type) in enumerate(self.plan_events(parcel), start=1):
                event_ts = parcel.created_at + timedelta(hours=offset_hours)
                events.append(self.to_event(parcel, version, event_type, event_ts))
        return events


    def run(self, emit, on_parcel=None, max_parcels: int | None = None, max_runtime_s: float | None = None) -> int:
        start = wallclock.monotonic()
        heap: list[tuple[float, int, Parcel, int, str]] = []
        tie = itertools.count()
        created = 0
        emitted = 0
        next_arrival = start

        while True:
            now = wallclock.monotonic()
            if max_runtime_s is not None and now - start >= max_runtime_s and not heap:
                break

            while (max_parcels is None or created < max_parcels) and next_arrival <= now:
                created += 1
                parcel = self.build_parcel(datetime.now(timezone.utc))
                if on_parcel is not None:
                    on_parcel(parcel)
                base = next_arrival
                for version, (offset_hours, event_type) in enumerate(self.plan_events(parcel), start=1):
                    due = base + max(offset_hours * 3600.0 / self.time_acceleration, (version - 1) * self.min_step)
                    heapq.heappush(heap, (due, next(tie), parcel, version, event_type))
                next_arrival += float(self.np_rng.exponential(60.0 / self.arrival_rate))

            if not heap:
                if max_parcels is not None and created >= max_parcels:
                    break
                wallclock.sleep(min(0.05, max(0.0, next_arrival - wallclock.monotonic())))
                continue

            due = heap[0][0]
            now = wallclock.monotonic()
            if due > now:
                wallclock.sleep(min(due - now, 0.25))
                continue

            _, _, parcel, version, event_type = heapq.heappop(heap)
            emit(parcel.parcel_id, self.to_event(parcel, version, event_type, datetime.now(timezone.utc)))
            emitted += 1

        return emitted
