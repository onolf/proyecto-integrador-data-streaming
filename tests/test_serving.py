"""Tests for the idempotent SQLite sink: monotone upsert by pane_index."""

from __future__ import annotations

from apu_streaming.serving import (
    count_aggregates,
    fetch_aggregate,
    open_connection,
    upsert_aggregate,
)


def aggregate(
    *, aggregate_id="apu_health_indicator|apu-01|2026-01-01T00:00:00Z", pane_index, **overrides
):
    base = {
        "aggregate_id": aggregate_id,
        "metric_type": "apu_health_indicator",
        "asset_id": "apu-01",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "pane_index": pane_index,
        "pane_timing": "ON_TIME",
        "is_last": False,
        "running_ratio": 0.5,
    }
    base.update(overrides)
    return base


def test_applying_same_aggregate_twice_leaves_one_row(tmp_path):
    conn = open_connection(tmp_path / "serving.db")
    agg = aggregate(pane_index=0)

    assert upsert_aggregate(conn, agg) is True
    assert upsert_aggregate(conn, agg) is True
    assert count_aggregates(conn) == 1
    conn.close()


def test_higher_pane_index_replaces_the_value(tmp_path):
    conn = open_connection(tmp_path / "serving.db")
    upsert_aggregate(conn, aggregate(pane_index=0, running_ratio=0.1))
    upsert_aggregate(conn, aggregate(pane_index=1, running_ratio=0.9))

    stored = fetch_aggregate(conn, "apu_health_indicator|apu-01|2026-01-01T00:00:00Z")
    conn.close()
    assert stored["running_ratio"] == 0.9
    assert stored["pane_index"] == 1


def test_lower_pane_index_does_not_modify_stored_value(tmp_path):
    conn = open_connection(tmp_path / "serving.db")
    upsert_aggregate(conn, aggregate(pane_index=3, running_ratio=0.9))
    applied = upsert_aggregate(conn, aggregate(pane_index=1, running_ratio=0.1))

    stored = fetch_aggregate(conn, "apu_health_indicator|apu-01|2026-01-01T00:00:00Z")
    conn.close()

    assert applied is False
    assert stored["running_ratio"] == 0.9
    assert stored["pane_index"] == 3


def test_two_distinct_aggregate_ids_coexist(tmp_path):
    conn = open_connection(tmp_path / "serving.db")
    upsert_aggregate(
        conn,
        aggregate(aggregate_id="apu_health_indicator|apu-01|2026-01-01T00:00:00Z", pane_index=0),
    )
    upsert_aggregate(
        conn,
        aggregate(aggregate_id="apu_health_indicator|apu-02|2026-01-01T00:00:00Z", pane_index=0),
    )
    total = count_aggregates(conn)
    conn.close()
    assert total == 2
