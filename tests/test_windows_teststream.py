"""Event-time windowing behaviors via Beam TestStream.

DirectRunner does not drop late elements itself (that's a runner-capability
feature, honored by Flink/Dataflow); instead, it tags emitted panes with
late/ON_TIME timing. We assert observable window assignment and how
ParseAndAdmit's horizon-based rejection complements in-window lateness.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream, TimestampedValue
from apache_beam.testing.util import assert_that, equal_to

from apu_streaming.config import Settings
from apu_streaming.transforms import ParseAndAdmit, build_analytics

ASSET = "apu-01"
# Fixed windows are aligned to timestamp 0 → events at t in [0, 300) land in
# the first window. `window_param.start.to_utc_datetime()` returns naive.
WINDOW_START_ISO = "1970-01-01T00:00:00"


def _norm_event(eid: str, value: float, event_ts: float, ingest_ts: float | None = None) -> dict:
    if ingest_ts is None:
        ingest_ts = event_ts
    return {
        "event_id": eid,
        "asset_id": ASSET,
        "stream": "motor_current",
        "channel": "Motor_current",
        "unit": "A",
        "value": value,
        "event_time": _to_iso(event_ts),
        "ingestion_time": _to_iso(ingest_ts),
    }


def _raw_event(eid: str, value: float, event_ts: float, ingest_ts: float | None = None) -> dict:
    """Pre-normalization, wire-shaped event expected by ParseAndAdmit."""
    if ingest_ts is None:
        ingest_ts = event_ts
    return {
        "schema_version": 1,
        "event_id": eid,
        "asset_id": ASSET,
        "sensor": {"stream": "motor_current", "channel": "Motor_current", "unit": "A"},
        "value": value,
        "event_time": _to_iso(event_ts),
        "ingestion_time": _to_iso(ingest_ts),
    }


def _to_iso(ts: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")


def test_on_time_events_land_in_first_window_and_close_at_watermark():
    """All in-window events produce the expected signal_stats row."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=60)

    ts = (
        TestStream()
        .add_elements(
            [
                TimestampedValue(_norm_event("e1", 2.0, 130.0), 130.0),
                TimestampedValue(_norm_event("e2", 4.0, 140.0), 140.0),
            ]
        )
        .advance_watermark_to(500.0)
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        out = build_analytics(p | ts, settings, streaming_triggers=False)
        stats = (
            out
            | "OnlyStats" >> beam.Filter(lambda r: r["metric_type"] == "apu_signal_stats")
            | "KV" >> beam.Map(lambda r: (r["window_start"], r["readings"], r["value_mean"]))
        )
        # mean(2.0, 4.0) = 3.0; both events collapse into one window.
        assert_that(stats, equal_to([(WINDOW_START_ISO, 2, 3.0)]))


def test_late_event_within_lateness_lands_in_correct_window_and_is_included():
    """A late event (arrived after watermark crossed end) but with an event_ts
    still inside the window IS admitted and the aggregate includes it."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=120)

    ts = (
        TestStream()
        .add_elements(
            [
                TimestampedValue(_norm_event("e1", 2.0, 130.0), 130.0),
                TimestampedValue(_norm_event("e2", 4.0, 140.0), 140.0),
            ]
        )
        .advance_watermark_to(310.0)
        .add_elements([TimestampedValue(_norm_event("e_late", 6.0, 200.0), 200.0)])
        .advance_watermark_to(1000.0)
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        out = build_analytics(p | ts, settings, streaming_triggers=False)
        stats = (
            out
            | "OnlyStats2" >> beam.Filter(lambda r: r["metric_type"] == "apu_signal_stats")
            | "KV2" >> beam.Map(lambda r: (r["window_start"], r["readings"], r["value_max"]))
        )
        # Final accumulated aggregate includes the late event.
        assert_that(stats, equal_to([(WINDOW_START_ISO, 3, 6.0)]))


def test_events_in_two_adjacent_windows_are_separated():
    """Events on either side of the window boundary produce two distinct keys."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=60)

    ts = (
        TestStream()
        .add_elements(
            [
                TimestampedValue(_norm_event("e1", 2.0, 100.0), 100.0),  # [0,300)
                TimestampedValue(_norm_event("e2", 8.0, 350.0), 350.0),  # [300,600)
            ]
        )
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        out = build_analytics(p | ts, settings, streaming_triggers=False)
        stats = (
            out
            | "OnlyStats3" >> beam.Filter(lambda r: r["metric_type"] == "apu_signal_stats")
            | "KV3" >> beam.Map(lambda r: (r["window_start"], r["readings"], r["value_mean"]))
        )
        # Each adjacent window sees its own single event; they're not mixed.
        assert_that(
            stats,
            equal_to(
                [
                    ("1970-01-01T00:00:00", 1, 2.0),
                    ("1970-01-01T00:05:00", 1, 8.0),
                ]
            ),
        )


def test_parse_and_admit_rejects_beyond_horizon_regardless_of_watermark():
    """The "drop" path is enforced upstream via ingestion-vs-event-time lag,
    independent of watermark state. An event whose lag exceeds the configured
    horizon is routed to the too_late output even if the watermark is fresh."""
    import json as _json

    settings = Settings(allowed_lateness_seconds=60)

    on_time = _raw_event("e_ontime", 1.0, 130.0, ingest_ts=140.0)  # lag=10
    too_late = _raw_event("e_toolate", 99.0, 130.0, ingest_ts=400.0)  # lag=270

    dofn = ParseAndAdmit(settings)

    on_time_out = list(dofn.process((b"apu-01", _json.dumps(on_time).encode())))
    too_late_out = list(dofn.process((b"apu-01", _json.dumps(too_late).encode())))

    assert len(on_time_out) == 1
    assert not hasattr(on_time_out[0], "tag")  # main output
    assert on_time_out[0]["event_id"] == "e_ontime"

    assert len(too_late_out) == 1
    assert too_late_out[0].tag == ParseAndAdmit.TOO_LATE
    assert too_late_out[0].value["reason"] == "dropped_by_horizon"
    assert too_late_out[0].value["event_id"] == "e_toolate"
