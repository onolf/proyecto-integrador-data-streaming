"""Unit tests for ApuReplay producer and scenario injection (no Kafka required)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from apu_streaming.config import sample_dataset_path
from apu_streaming.contracts import decode_event, event_lag_seconds
from apu_streaming.producer import SCENARIOS, ApuReplay, load_readings


class MockProducer:
    """In-memory mock recording produce calls for assertions."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(
        self,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
        timestamp: int | None = None,
    ) -> None:
        self.calls.append(
            {
                "topic": topic,
                "key": key.decode() if key else None,
                "value": decode_event(value) if value else None,
                "timestamp": timestamp,
            }
        )

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float = 0) -> int:
        return 0


@pytest.fixture
def sample_readings():
    sample_path = sample_dataset_path()
    assert sample_path.exists(), f"sample file missing at {sample_path}"
    return load_readings(sample_path)


def test_load_readings_loads_and_sorts_sample(sample_readings):
    assert len(sample_readings) > 0
    assets = {r.asset_id for r in sample_readings}
    assert len(assets) == 6
    streams = {r.stream for r in sample_readings}
    assert streams == {"motor_current", "oil_temperature", "tp2_pressure", "dv_pressure"}


def test_load_readings_filters_max_assets(sample_readings):
    sample_path = sample_dataset_path()
    filtered = load_readings(sample_path, max_assets=2)
    assert len({r.asset_id for r in filtered}) == 2


def test_replay_normal_scenario_produces_no_faults(sample_readings):
    mock = MockProducer()
    replay = ApuReplay(
        mock,
        topic="test.topic",
        scenario=SCENARIOS["normal"],
        seed=42,
    )
    result = replay.replay(sample_readings, realtime=False)

    assert result["events"] == len(sample_readings)
    assert result["duplicates"] == 0
    assert result["out_of_order"] == 0
    assert result["late"] == 0
    assert result["too_late"] == 0
    assert len(mock.calls) == len(sample_readings)


def test_replay_is_deterministic_with_same_seed(sample_readings):
    mock1 = MockProducer()
    replay1 = ApuReplay(
        mock1,
        topic="test.topic",
        scenario=SCENARIOS["adverse"],
        seed=7,
    )
    res1 = replay1.replay(sample_readings, realtime=False)

    mock2 = MockProducer()
    replay2 = ApuReplay(
        mock2,
        topic="test.topic",
        scenario=SCENARIOS["adverse"],
        seed=7,
    )
    res2 = replay2.replay(sample_readings, realtime=False)

    assert res1 == res2
    ids1 = [c["value"]["event_id"] for c in mock1.calls]
    ids2 = [c["value"]["event_id"] for c in mock2.calls]
    assert ids1 == ids2


def test_duplicates_preserve_identical_event_id(sample_readings):
    mock = MockProducer()
    replay = ApuReplay(
        mock,
        topic="test.topic",
        scenario=SCENARIOS["adverse"],
        seed=7,
    )
    result = replay.replay(sample_readings, realtime=False)
    assert result["duplicates"] > 0

    seen_ids = set()
    found_duplicate = False
    for call in mock.calls:
        eid = call["value"]["event_id"]
        if eid in seen_ids:
            found_duplicate = True
            break
        seen_ids.add(eid)

    assert found_duplicate, "expected at least one duplicate event_id emitted"


def test_too_late_events_have_lag_exceeding_horizon(sample_readings):
    mock = MockProducer()
    replay = ApuReplay(
        mock,
        topic="test.topic",
        scenario=SCENARIOS["adverse"],
        seed=7,
    )
    result = replay.replay(sample_readings, realtime=False)
    assert result["too_late"] > 0

    too_late_lags = [
        event_lag_seconds(c["value"]) for c in mock.calls if event_lag_seconds(c["value"]) > 720
    ]
    assert len(too_late_lags) >= result["too_late"]
    assert all(lag == 1800 for lag in too_late_lags)


def test_max_event_time_is_in_the_past_or_equal_to_now(sample_readings):
    mock = MockProducer()
    target_start = datetime(2026, 9, 16, 8, 0, tzinfo=UTC)
    replay = ApuReplay(
        mock,
        topic="test.topic",
        scenario=SCENARIOS["adverse"],
        seed=7,
    )
    replay.replay(sample_readings, realtime=False, target_start=target_start)

    now = datetime.now(UTC)
    for call in mock.calls:
        ev_time = datetime.fromisoformat(call["value"]["event_time"].replace("Z", "+00:00"))
        assert ev_time <= now
