"""Tests for AggregateStore in-memory upsert semantics."""

from __future__ import annotations

from apu_streaming.consumer import AggregateStore


def aggregate(*, pane_index: int, running_ratio: float) -> dict:
    return {
        "aggregate_id": "apu_health_indicator|apu-01|2026-01-01T00:00:00Z",
        "metric_type": "apu_health_indicator",
        "asset_id": "apu-01",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "pane_index": pane_index,
        "running_ratio": running_ratio,
    }


def test_store_upserts_newer_pane_instead_of_double_counting():
    store = AggregateStore()

    assert store.upsert(aggregate(pane_index=0, running_ratio=0.1))
    assert store.upsert(aggregate(pane_index=1, running_ratio=0.9))

    frame = store.frame("apu_health_indicator")
    assert len(frame) == 1
    assert frame.iloc[0].running_ratio == 0.9
    assert store.messages_seen == 2


def test_store_ignores_an_older_replayed_pane():
    store = AggregateStore()
    store.upsert(aggregate(pane_index=2, running_ratio=0.9))

    assert not store.upsert(aggregate(pane_index=1, running_ratio=0.1))
    assert store.frame().iloc[0].running_ratio == 0.9


def test_summary_reports_signal_and_health_counts():
    store = AggregateStore()
    store.upsert(
        {
            "aggregate_id": "apu_signal_stats|apu-01|motor_current|2026-01-01T00:00:00Z",
            "metric_type": "apu_signal_stats",
            "asset_id": "apu-01",
            "pane_index": 0,
        }
    )
    store.upsert(aggregate(pane_index=0, running_ratio=0.1))

    summary = store.summary()
    assert summary["signal_stats"] == 1
    assert summary["health_indicators"] == 1
    assert summary["aggregates"] == 2
