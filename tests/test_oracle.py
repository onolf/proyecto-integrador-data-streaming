"""Reproduce the E1-E9 sequence from `../tarea2.pdf` through the pure oracle."""

from __future__ import annotations

from apu_streaming.contracts import make_event_id
from apu_streaming.oracle import align_window, summarize_readings

ASSET = "apu-04"
WINDOW_SECONDS = 300
ALLOWED_LATENESS_SECONDS = 720


def reading(event_id_suffix, *, event_time, ingestion_time, value=5.66, stream="motor_current"):
    return {
        "event_id": make_event_id(ASSET, stream, event_id_suffix),
        "asset_id": ASSET,
        "stream": stream,
        "value": value,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
    }


def test_align_window_is_half_open_and_clock_aligned():
    start, end = align_window(605.0, 300)
    assert (start, end) == (600, 900)


def test_out_of_order_but_on_time_event_is_accepted_like_e4():
    # E4 llega desordenado (antes que E3 en tiempo de evento) pero dentro del
    # horizonte de lateness: se acepta y entra en la misma ventana que E1-E3.
    events = [
        reading("e1", event_time="2026-01-01T10:00:20Z", ingestion_time="2026-01-01T10:00:45Z"),
        reading("e2", event_time="2026-01-01T10:01:10Z", ingestion_time="2026-01-01T10:01:30Z"),
        reading("e3", event_time="2026-01-01T10:04:50Z", ingestion_time="2026-01-01T10:05:05Z"),
        reading("e4", event_time="2026-01-01T10:02:30Z", ingestion_time="2026-01-01T10:05:20Z"),
    ]
    result = summarize_readings(
        events, window_seconds=WINDOW_SECONDS, allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS
    )
    assert len(result["accepted"]) == 4
    assert result["too_late"] == []


def test_late_event_within_horizon_is_accepted_and_corrects_the_mean_like_e6():
    within = [
        reading(
            "e1",
            event_time="2026-01-01T10:00:20Z",
            ingestion_time="2026-01-01T10:00:45Z",
            value=4.0,
        ),
        reading(
            "e2",
            event_time="2026-01-01T10:01:10Z",
            ingestion_time="2026-01-01T10:01:30Z",
            value=4.4,
        ),
    ]
    before = summarize_readings(
        within, window_seconds=WINDOW_SECONDS, allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS
    )
    mean_before = next(iter(before["signal_stats"].values()))["value_mean"]

    late = reading(
        "e6", event_time="2026-01-01T10:03:40Z", ingestion_time="2026-01-01T10:09:00Z", value=6.0
    )
    after = summarize_readings(
        within + [late],
        window_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS,
    )
    mean_after = next(iter(after["signal_stats"].values()))["value_mean"]

    assert late["event_id"] not in {e["event_id"] for e in after["too_late"]}
    assert mean_after != mean_before
    assert mean_after == (4.0 + 4.4 + 6.0) / 3


def test_event_beyond_horizon_is_diverted_like_e8():
    too_late_event = reading(
        "e8", event_time="2026-01-01T10:01:50Z", ingestion_time="2026-01-01T10:22:00Z", value=9.9
    )
    result = summarize_readings(
        [too_late_event],
        window_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS,
    )
    assert result["accepted"] == []
    assert result["too_late"] == [too_late_event]


def test_duplicate_event_id_is_deduplicated_within_the_same_window():
    events = [
        reading(
            "e1",
            event_time="2026-01-01T10:00:20Z",
            ingestion_time="2026-01-01T10:00:45Z",
            value=4.0,
        ),
        reading(
            "e1",
            event_time="2026-01-01T10:00:20Z",
            ingestion_time="2026-01-01T10:00:46Z",
            value=4.0,
        ),
    ]
    result = summarize_readings(
        events, window_seconds=WINDOW_SECONDS, allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS
    )
    assert len(result["accepted"]) == 1
    assert result["duplicates"] == 1


def test_health_indicator_flags_air_leak_when_motor_never_idles_and_oil_is_hot():
    events = [
        reading(
            f"m{i}",
            event_time=f"2026-01-01T10:0{i}:00Z",
            ingestion_time=f"2026-01-01T10:0{i}:05Z",
            value=5.5,
        )
        for i in range(4)
    ] + [
        reading(
            f"o{i}",
            event_time=f"2026-01-01T10:0{i}:00Z",
            ingestion_time=f"2026-01-01T10:0{i}:05Z",
            value=76.0,
            stream="oil_temperature",
        )
        for i in range(4)
    ]
    result = summarize_readings(
        events, window_seconds=WINDOW_SECONDS, allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS
    )
    health = next(iter(result["health"].values()))
    assert health["running_ratio"] == 1.0
    assert health["air_leak_suspected"] is True


def test_health_indicator_does_not_flag_a_healthy_cycling_motor():
    events = [
        reading(
            "m0",
            event_time="2026-01-01T10:00:00Z",
            ingestion_time="2026-01-01T10:00:05Z",
            value=0.04,
        ),
        reading(
            "m1",
            event_time="2026-01-01T10:01:00Z",
            ingestion_time="2026-01-01T10:01:05Z",
            value=1.21,
        ),
        reading(
            "o0",
            event_time="2026-01-01T10:00:00Z",
            ingestion_time="2026-01-01T10:00:05Z",
            value=55.78,
            stream="oil_temperature",
        ),
    ]
    result = summarize_readings(
        events, window_seconds=WINDOW_SECONDS, allowed_lateness_seconds=ALLOWED_LATENESS_SECONDS
    )
    health = next(iter(result["health"].values()))
    assert health["air_leak_suspected"] is False
