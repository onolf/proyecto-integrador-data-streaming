"""DeduplicateReadings state semantics: same event_id is dropped once per window,
and does NOT leak across distinct windows for the same asset."""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from apu_streaming.transforms import DeduplicateReadings, assign_event_timestamp


def _ev(eid: str, asset: str, event_time: str) -> dict:
    return {
        "event_id": eid,
        "asset_id": asset,
        "stream": "motor_current",
        "channel": "Motor_current",
        "unit": "A",
        "value": 1.0,
        "event_time": event_time,
        "ingestion_time": event_time,
    }


def test_duplicate_event_id_is_dropped_within_the_same_window():
    """Two events sharing (asset_id, event_id) in one window → one output."""
    # Both events sit in window [00:00, 00:05).
    e1 = _ev("e1", "apu-01", "2026-01-01T00:01:00Z")
    e1_dup = _ev("e1", "apu-01", "2026-01-01T00:02:00Z")  # same id, later in window
    e2 = _ev("e2", "apu-01", "2026-01-01T00:03:00Z")

    with TestPipeline() as p:
        out = (
            p
            | beam.Create([e1, e1_dup, e2])
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(beam.window.FixedWindows(300))
            | beam.Map(lambda e: (e["asset_id"], e))
            | beam.ParDo(DeduplicateReadings(allowed_lateness_seconds=60))
            | beam.Map(lambda e: e["event_id"])
        )
        assert_that(out, equal_to(["e1", "e2"]))


def test_same_event_id_in_distinct_windows_is_not_deduped_globally():
    """State is keyed by (asset_id, *window*): the SAME event_id occurring in
    two distinct windows must pass through twice — once per window."""
    # Window A: [00:00, 00:05); Window B: [00:05, 00:10)
    e_a = _ev("shared-id", "apu-01", "2026-01-01T00:01:00Z")
    e_b = _ev("shared-id", "apu-01", "2026-01-01T00:06:00Z")

    with TestPipeline() as p:
        out = (
            p
            | beam.Create([e_a, e_b])
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(beam.window.FixedWindows(300))
            | beam.Map(lambda e: (e["asset_id"], e))
            | beam.ParDo(DeduplicateReadings(allowed_lateness_seconds=60))
            | beam.Map(lambda e: (e["event_id"], e["event_time"]))
        )
        assert_that(
            out,
            equal_to(
                [
                    ("shared-id", "2026-01-01T00:01:00Z"),
                    ("shared-id", "2026-01-01T00:06:00Z"),
                ]
            ),
        )


def test_dedup_is_scoped_per_asset_not_global():
    """Same event_id on different assets must NOT collide: state is per key."""
    e_a = _ev("cross-id", "apu-01", "2026-01-01T00:01:00Z")
    e_b = _ev("cross-id", "apu-02", "2026-01-01T00:01:00Z")

    with TestPipeline() as p:
        out = (
            p
            | beam.Create([e_a, e_b])
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(beam.window.FixedWindows(300))
            | beam.Map(lambda e: (e["asset_id"], e))
            | beam.ParDo(DeduplicateReadings(allowed_lateness_seconds=60))
            | beam.Map(lambda e: (e["asset_id"], e["event_id"]))
        )
        assert_that(
            out,
            equal_to(
                [
                    ("apu-01", "cross-id"),
                    ("apu-02", "cross-id"),
                ]
            ),
        )
