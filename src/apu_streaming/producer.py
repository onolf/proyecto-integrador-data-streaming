"""Replay historical MetroPT-3 sensor readings to Kafka with controllable scenarios."""

from __future__ import annotations

import argparse
import json
import random
import time
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

import pandas as pd
from confluent_kafka import Producer

from apu_streaming.config import Settings, processed_dataset_path, sample_dataset_path
from apu_streaming.contracts import (
    SensorReading,
    encode_event,
    reading_events,
)


@dataclass(frozen=True)
class Scenario:
    name: str
    duplicate_rate: float
    out_of_order_rate: float
    out_of_order_lag_s: int
    late_rate: float
    late_lag_s: int
    too_late_rate: float
    too_late_lag_s: int


SCENARIOS: dict[str, Scenario] = {
    "normal": Scenario("normal", 0.0, 0.0, 0, 0.0, 0, 0.0, 0),
    "adverse": Scenario("adverse", 0.02, 0.05, 60, 0.02, 300, 0.005, 1800),
}


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def load_readings(
    path: Path,
    *,
    max_assets: int | None = None,
    schema_version: int = 1,
) -> list[SensorReading]:
    """Load readings from parquet or sample JSONL, sorted by source_event_time."""
    if not path.exists():
        raise FileNotFoundError(f"dataset file not found at {path}")

    readings: list[SensorReading] = []

    if path.suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                sensor = data.get("sensor", {})
                reading = SensorReading(
                    schema_version=data.get("schema_version", schema_version),
                    event_id=data["event_id"],
                    asset_id=data["asset_id"],
                    asset_type=data.get("asset_type", "apu_compresor_motor_trifasico"),
                    stream=sensor.get("stream", data.get("stream", "")),
                    channel=sensor.get("channel", data.get("channel", "")),
                    unit=sensor.get("unit", data.get("unit", "")),
                    value=float(data["value"]),
                    event_time=data["event_time"],
                    ingestion_time=data.get("ingestion_time", data["event_time"]),
                    source_event_time=data.get("source_event_time", data["event_time"]),
                )
                readings.append(reading)
    else:
        frame = pd.read_parquet(path)
        assets = sorted(frame["asset_id"].unique())
        if max_assets is not None:
            allowed_assets = set(assets[:max_assets])
            frame = frame[frame["asset_id"].isin(allowed_assets)]

        for row_dict in frame.to_dict(orient="records"):
            readings.extend(reading_events(row_dict, schema_version=schema_version))

    if max_assets is not None:
        readings = [
            r
            for r in readings
            if r.asset_id in set(sorted({r.asset_id for r in readings})[:max_assets])
        ]

    return sorted(readings, key=lambda r: (r.source_event_time, r.asset_id, r.stream))


class ApuReplay:
    """Controllable, deterministic replay producer for APU readings."""

    def __init__(
        self,
        producer: Any,
        *,
        topic: str,
        scenario: Scenario = SCENARIOS["normal"],
        speedup: float = 120.0,
        seed: int = 7,
    ) -> None:
        if speedup <= 0:
            raise ValueError("speedup must be positive")
        self.producer = producer
        self.topic = topic
        self.scenario = scenario
        self.speedup = speedup
        self.random = random.Random(seed)
        self.stop_event = Event()
        self.sent = 0
        self.duplicates = 0
        self.out_of_order = 0
        self.late = 0
        self.too_late = 0

    def stop(self) -> None:
        self.stop_event.set()

    def _produce(self, event: SensorReading) -> None:
        timestamp_ms = int(_parse_iso(event.event_time).timestamp() * 1000)
        self.producer.produce(
            self.topic,
            key=event.asset_id.encode(),
            value=encode_event(event),
            timestamp=timestamp_ms,
        )
        self.producer.poll(0)
        self.sent += 1

    def replay(
        self,
        readings: Iterable[SensorReading],
        *,
        realtime: bool = True,
        target_start: datetime | None = None,
    ) -> dict[str, int]:
        """Replay readings across assets on a common timeline, injecting scenario faults."""
        materialized = list(readings)
        if not materialized:
            return {
                "events": 0,
                "duplicates": 0,
                "out_of_order": 0,
                "late": 0,
                "too_late": 0,
            }

        # Determine each asset's start and the fleet's maximum segment duration
        asset_starts: dict[str, datetime] = {}
        asset_ends: dict[str, datetime] = {}
        for r in materialized:
            t = _parse_iso(r.source_event_time)
            aid = r.asset_id
            if aid not in asset_starts or t < asset_starts[aid]:
                asset_starts[aid] = t
            if aid not in asset_ends or t > asset_ends[aid]:
                asset_ends[aid] = t

        max_duration = max(
            (asset_ends[aid] - asset_starts[aid]).total_seconds() for aid in asset_starts
        )
        if target_start is None:
            # Anchor the end of the common timeline at now, so max(event_time) <= now
            target_start = datetime.now(UTC) - timedelta(seconds=max_duration)

        # Plan the schedule: assign logical offsets, shifted event_times, and lags
        scheduled: list[tuple[float, SensorReading, bool]] = []
        for original in materialized:
            offset = (
                _parse_iso(original.source_event_time) - asset_starts[original.asset_id]
            ).total_seconds()
            event_time_dt = target_start + timedelta(seconds=offset)
            event_time_str = _iso(event_time_dt)

            # Determine fault category
            roll = self.random.random()
            lag = 0
            is_duplicate = False

            sc = self.scenario
            if roll < sc.too_late_rate:
                lag = sc.too_late_lag_s
                self.too_late += 1
            elif roll < sc.too_late_rate + sc.late_rate:
                lag = sc.late_lag_s
                self.late += 1
            elif roll < sc.too_late_rate + sc.late_rate + sc.out_of_order_rate:
                lag = sc.out_of_order_lag_s
                self.out_of_order += 1

            if self.random.random() < sc.duplicate_rate:
                is_duplicate = True

            ingestion_time_str = _iso(event_time_dt + timedelta(seconds=lag))
            shifted = replace(
                original,
                event_time=event_time_str,
                ingestion_time=ingestion_time_str,
            )

            # Execution wall offset is determined by logical event arrival: (offset + lag) / speedup
            delivery_offset_s = offset + lag
            scheduled.append((delivery_offset_s, shifted, is_duplicate))

        # Sort emissions chronologically by delivery arrival time
        scheduled.sort(key=lambda item: item[0])

        wall_start = time.monotonic()
        for delivery_offset_s, event, is_duplicate in scheduled:
            if self.stop_event.is_set():
                break

            due = wall_start + (delivery_offset_s / self.speedup)
            if realtime:
                while not self.stop_event.is_set() and (remaining := due - time.monotonic()) > 0:
                    time.sleep(min(remaining, 0.05))

            self._produce(event)
            if is_duplicate:
                self._produce(event)
                self.duplicates += 1

        if hasattr(self.producer, "flush"):
            self.producer.flush(15)

        return {
            "events": self.sent,
            "duplicates": self.duplicates,
            "out_of_order": self.out_of_order,
            "late": self.late,
            "too_late": self.too_late,
        }


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "apu-replay-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=["normal", "adverse"],
        default="adverse",
        help="traffic scenario to simulate",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=120.0,
        help="replay speedup multiplier (14400s in ~120s)",
    )
    parser.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="limit number of assets replayed",
    )
    parser.add_argument(
        "--source",
        choices=["processed", "sample"],
        default="processed",
        help="dataset source to replay",
    )
    parser.add_argument("--seed", type=int, default=7, help="random seed")
    parser.add_argument(
        "--no-realtime",
        action="store_true",
        help="emit without wall-clock pacing (for tests/smoke)",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    dataset_path = sample_dataset_path() if args.source == "sample" else processed_dataset_path()
    readings = load_readings(dataset_path, max_assets=args.max_assets)

    scenario = SCENARIOS[args.scenario]
    kafka_producer = build_producer(settings.kafka_bootstrap_servers)
    replay_runner = ApuReplay(
        kafka_producer,
        topic=settings.raw_topic,
        scenario=scenario,
        speedup=args.speedup,
        seed=args.seed,
    )

    result = replay_runner.replay(readings, realtime=not args.no_realtime)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
