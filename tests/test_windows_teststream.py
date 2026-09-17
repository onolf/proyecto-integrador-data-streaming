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


# --- E1-E9: secuencia completa de tarea2 con streaming real -----------------
#
# Trazas de tarea2 (wm = maxET - 120 s) mapeadas a epoch [0, 300) para W1:
#   E1 ET 20 v4.0 · E2 ET 70 v4.4 · E3 ET 290 v5.0 · E4 ET 150 v4.2 (desord.
#   a tiempo) · E5 ET 370 v5.6 · E7 ET 450 v5.2 · E6 ET 220 v6.0 (tardío
#   aceptado: corrige 4.4 → 4.72) · E9 ET 1180 (cierra W1 y W2) · E8 ET 110
#   v9.9 con lag 1210 s > 720 → rechazado por ParseAndAdmit (dropped_by_horizon).
#
# pane_timing sale serializado como int ("0"=EARLY, "1"=ON_TIME, "2"=LATE)
# porque str(IntEnum) en Python 3.11+ produce el valor numérico.

ASSET_E9 = "apu-04"
W1_START_ISO = "1970-01-01T00:00:00"

EARLY, ON_TIME, LATE = "0", "1", "2"


def _to_iso(ts: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")


def _e9_pair(eid: str, value: float, et: float, at: float) -> tuple[bytes, bytes]:
    """Evento de la secuencia E1-E9 como par Kafka (key, payload)."""
    from apu_streaming.contracts import encode_event

    return (
        ASSET_E9.encode(),
        encode_event({**_raw_event(eid, value, et, at), "asset_id": ASSET_E9}),
    )


def test_e1_e9_sequence_panes_accumulate_and_correct():
    """Secuencia E1-E9 de tarea2 end-to-end en un solo TestStream:
    ParseAndAdmit → windowing con triggers streaming → panes EARLY/ON_TIME/LATE
    con pane_index monótono y corrección del valor por E6."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=720, early_firing_seconds=10)

    pair = _e9_pair
    ts = (
        TestStream()
        # --- E1, early pane n=1, wm 20-120=-100
        .add_elements([TimestampedValue(pair("E1", 4.0, 20, 45), 20.0)])
        .advance_processing_time(10.0)  # EARLY pane: n=1, mean 4.0
        .advance_watermark_to(-100.0)
        # --- E2, early pane n=2
        .add_elements([TimestampedValue(pair("E2", 4.4, 70, 90), 70.0)])
        .advance_processing_time(10.0)  # EARLY pane: n=2, mean 4.2
        .advance_watermark_to(-50.0)
        # --- duplicado de E2: no debe contar dos veces (dedup con estado)
        .add_elements([TimestampedValue(pair("E2", 4.4, 70, 95), 70.0)])
        # --- E3, early pane n=3
        .add_elements([TimestampedValue(pair("E3", 5.0, 290, 305), 290.0)])
        .advance_processing_time(10.0)  # EARLY pane: n=3, mean 4.4667
        .advance_watermark_to(170.0)
        # --- E4 desordenado pero a tiempo (wm 170 < fin 300)
        .add_elements([TimestampedValue(pair("E4", 4.2, 150, 320), 150.0)])
        .advance_processing_time(10.0)  # EARLY pane: n=4, mean 4.4
        # --- E5, E7: empujan maxET a 450 → wm 330 > fin W1 → ON_TIME n=4
        .add_elements([TimestampedValue(pair("E5", 5.6, 370, 385), 370.0)])
        .advance_watermark_to(250.0)
        .add_elements([TimestampedValue(pair("E7", 5.2, 450, 470), 450.0)])
        .advance_watermark_to(330.0)  # W1 ON_TIME: n=4, mean 4.4
        # --- E6 tardío aceptado: corrige a n=5 mean 4.72, pane LATE
        .add_elements([TimestampedValue(pair("E6", 6.0, 220, 540), 220.0)])
        # --- E9 empuja wm a 1060 > horizonte W1 (300+720=1020)
        .add_elements([TimestampedValue(pair("E9", 5.1, 1180, 1200), 1180.0)])
        .advance_watermark_to(1060.0)
        # --- E8: ET 110, AT 1320 → lag 1210 > 720 → dropped_by_horizon
        .add_elements([TimestampedValue(pair("E8", 9.9, 110, 1320), 110.0)])
        .advance_watermark_to_infinity()
    )

    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
        parsed = (
            p
            | ts
            | beam.ParDo(ParseAndAdmit(settings)).with_outputs(
                ParseAndAdmit.QUARANTINE, ParseAndAdmit.TOO_LATE, main="valid"
            )
        )
        aggregates = build_analytics(parsed.valid, settings, streaming_triggers=True)

        # W1 pane sequence, keyed by pane_index
        w1_panes = (
            aggregates
            | "W1Stats"
            >> beam.Filter(
                lambda r: (
                    r["metric_type"] == "apu_signal_stats" and r["window_start"] == W1_START_ISO
                )
            )
            | "W1KV"
            >> beam.Map(
                lambda r: (r["pane_index"], r["pane_timing"], r["readings"], r["value_mean"])
            )
        )
        assert_that(
            w1_panes,
            equal_to(
                [
                    (0, EARLY, 1, 4.0),
                    (1, EARLY, 2, 4.2),
                    (2, EARLY, 3, 4.4667),  # mean(4.0, 4.4, 5.0)
                    (3, EARLY, 4, 4.4),  # E4 desordenado entra al pane temprano
                    (4, ON_TIME, 4, 4.4),  # wm cruza el fin (330 > 300)
                    (5, LATE, 5, 4.72),  # E6 corrige: pane_index mayor
                ]
            ),
        )

        # E8 fue desviado ANTES de las ventanas: nunca genera pane en W1.
        too_late_rows = parsed.too_late | "TLIds" >> beam.Map(lambda r: r["event_id"])
        assert_that(too_late_rows, equal_to(["E8"]), label="E8RejectedByHorizon")

        # W2 ([300,600)) cerró on-time con E5+E7: n=2, mean 5.4.
        w2_panes = (
            aggregates
            | "W2Stats"
            >> beam.Filter(
                lambda r: (
                    r["metric_type"] == "apu_signal_stats"
                    and r["window_start"] == "1970-01-01T00:05:00"
                )
            )
            | "W2KV" >> beam.Map(lambda r: (r["pane_timing"], r["readings"], r["value_mean"]))
        )
        assert_that(
            w2_panes,
            equal_to([(ON_TIME, 2, 5.4)]),
            label="W2ClosedOnTime",
        )


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
