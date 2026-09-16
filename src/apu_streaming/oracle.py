"""Pure-Python oracle: replicates the Beam aggregation logic without Beam.

Used as the ground truth in tests that assert the Beam pipeline's output
matches a hand-computed reference, and to simulate the E1-E9 sequence from
`../tarea2.pdf` without running a pipeline.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from apu_streaming.config import (
    MOTOR_RUNNING_THRESHOLD_A,
    OIL_TEMPERATURE_ALERT_C,
    RUNNING_RATIO_ALERT,
)
from apu_streaming.contracts import event_lag_seconds, normalize_event


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def align_window(epoch_seconds: float, size_seconds: int) -> tuple[int, int]:
    """Return the half-open `[start, end)` fixed-window boundary in epoch seconds."""
    start = int(epoch_seconds // size_seconds) * size_seconds
    return start, start + size_seconds


def summarize_readings(
    events: list[dict[str, Any]],
    *,
    window_seconds: int,
    allowed_lateness_seconds: int,
) -> dict[str, Any]:
    """Classify, deduplicate, and aggregate readings exactly like the Beam pipeline.

    Returns a dict with `accepted`, `too_late`, `duplicates`, `signal_stats`
    (keyed by `(asset_id, stream, window_start)`), and `health` (keyed by
    `(asset_id, window_start)`).
    """
    accepted: list[dict[str, Any]] = []
    too_late: list[dict[str, Any]] = []

    seen_ids: set[tuple[str, int, str]] = set()
    duplicates = 0

    for raw_event in events:
        event = normalize_event(raw_event)
        if event_lag_seconds(event) > allowed_lateness_seconds:
            too_late.append(event)
            continue
        event_time = _parse_iso(event["event_time"])
        window_start, _ = align_window(event_time.timestamp(), window_seconds)
        dedup_key = (event["asset_id"], window_start, event["event_id"])
        if dedup_key in seen_ids:
            duplicates += 1
            continue
        seen_ids.add(dedup_key)
        accepted.append(event)

    signal_groups: dict[tuple[str, str, int], list[float]] = {}
    health_groups: dict[tuple[str, int], dict[str, float]] = {}

    for event in accepted:
        event_time = _parse_iso(event["event_time"])
        window_start, window_end = align_window(event_time.timestamp(), window_seconds)
        signal_key = (event["asset_id"], event["stream"], window_start)
        signal_groups.setdefault(signal_key, []).append(event["value"])

        health_key = (event["asset_id"], window_start)
        health = health_groups.setdefault(
            health_key,
            {"motor_count": 0, "running_count": 0, "oil_count": 0, "oil_total": 0.0},
        )
        if event["stream"] == "motor_current":
            health["motor_count"] += 1
            if event["value"] >= MOTOR_RUNNING_THRESHOLD_A:
                health["running_count"] += 1
        elif event["stream"] == "oil_temperature":
            health["oil_count"] += 1
            health["oil_total"] += event["value"]

    signal_stats: dict[tuple[str, str, int], dict[str, Any]] = {}
    for (asset_id, stream, window_start), values in signal_groups.items():
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        signal_stats[(asset_id, stream, window_start)] = {
            "asset_id": asset_id,
            "stream": stream,
            "window_start": _iso(datetime.fromtimestamp(window_start, tz=UTC)),
            "window_end": _iso(datetime.fromtimestamp(window_start + window_seconds, tz=UTC)),
            "readings": n,
            "value_mean": round(mean, 4),
            "value_min": min(values),
            "value_max": max(values),
            "value_stddev": round(math.sqrt(variance), 4),
        }

    health: dict[tuple[str, int], dict[str, Any]] = {}
    for (asset_id, window_start), acc in health_groups.items():
        running_ratio = acc["running_count"] / acc["motor_count"] if acc["motor_count"] else 0.0
        oil_mean = acc["oil_total"] / acc["oil_count"] if acc["oil_count"] else 0.0
        air_leak_suspected = (
            running_ratio >= RUNNING_RATIO_ALERT and oil_mean >= OIL_TEMPERATURE_ALERT_C
        )
        health[(asset_id, window_start)] = {
            "asset_id": asset_id,
            "window_start": _iso(datetime.fromtimestamp(window_start, tz=UTC)),
            "window_end": _iso(datetime.fromtimestamp(window_start + window_seconds, tz=UTC)),
            "motor_readings": acc["motor_count"],
            "running_ratio": round(running_ratio, 4),
            "oil_temperature_mean": round(oil_mean, 4),
            "air_leak_suspected": air_leak_suspected,
        }

    return {
        "accepted": accepted,
        "too_late": too_late,
        "duplicates": duplicates,
        "signal_stats": signal_stats,
        "health": health,
    }
