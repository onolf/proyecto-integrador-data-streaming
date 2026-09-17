"""Tests for Apache Beam transformations, combiners, and analytics DAG."""

from __future__ import annotations

import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from apu_streaming.config import Settings
from apu_streaming.contracts import encode_event, make_event_id, normalize_event
from apu_streaming.oracle import summarize_readings
from apu_streaming.transforms import (
    HealthIndicatorCombineFn,
    ParseAndAdmit,
    SignalStatsCombineFn,
    aggregate_to_kafka_record,
    build_analytics,
    lateral_to_kafka_record,
)


def test_signal_stats_combine_fn_empty_accumulator():
    fn = SignalStatsCombineFn()
    acc = fn.create_accumulator()
    output = fn.extract_output(acc)
    assert output["readings"] == 0
    assert output["value_mean"] == 0.0
    assert output["value_min"] is None
    assert output["value_max"] is None
    assert output["value_stddev"] == 0.0


def test_signal_stats_combine_fn_is_associative_and_calculates_stddev():
    fn = SignalStatsCombineFn()
    inputs = [{"value": 2.0}, {"value": 4.0}, {"value": 4.0}, {"value": 6.0}]

    # Direct accumulation
    direct_acc = fn.create_accumulator()
    for item in inputs:
        direct_acc = fn.add_input(direct_acc, item)
    direct_out = fn.extract_output(direct_acc)

    # Split into two partitions and merge
    acc1 = fn.create_accumulator()
    acc1 = fn.add_input(acc1, inputs[0])
    acc1 = fn.add_input(acc1, inputs[1])

    acc2 = fn.create_accumulator()
    acc2 = fn.add_input(acc2, inputs[2])
    acc2 = fn.add_input(acc2, inputs[3])

    merged_acc = fn.merge_accumulators([acc1, acc2])
    merged_out = fn.extract_output(merged_acc)

    assert direct_out == merged_out
    assert direct_out["readings"] == 4
    assert direct_out["value_mean"] == 4.0
    assert direct_out["value_min"] == 2.0
    assert direct_out["value_max"] == 6.0
    # Values: [2, 4, 4, 6], Mean: 4.0, Variance: (4+0+0+4)/4 = 2.0, Stddev: sqrt(2) ≈ 1.4142
    assert direct_out["value_stddev"] == 1.4142


def test_health_indicator_combine_fn_discriminates_healthy_vs_failure():
    fn = HealthIndicatorCombineFn()

    # Healthy: cycling motor, low oil temperature
    healthy_inputs = [
        {"stream": "motor_current", "value": 0.04},
        {"stream": "motor_current", "value": 0.04},
        {"stream": "motor_current", "value": 1.20},
        {"stream": "oil_temperature", "value": 55.0},
    ]
    h_acc = fn.create_accumulator()
    for item in healthy_inputs:
        h_acc = fn.add_input(h_acc, item)
    h_out = fn.extract_output(h_acc)
    assert h_out["running_ratio"] == round(1 / 3, 4)
    assert h_out["oil_temperature_mean"] == 55.0
    assert h_out["air_leak_suspected"] is False

    # Failure: continuous run, hot oil
    failure_inputs = [
        {"stream": "motor_current", "value": 5.5},
        {"stream": "motor_current", "value": 5.8},
        {"stream": "oil_temperature", "value": 78.0},
    ]
    f_acc = fn.create_accumulator()
    for item in failure_inputs:
        f_acc = fn.add_input(f_acc, item)
    f_out = fn.extract_output(f_acc)
    assert f_out["running_ratio"] == 1.0
    assert f_out["oil_temperature_mean"] == 78.0
    assert f_out["air_leak_suspected"] is True


def test_parse_and_admit_routes_to_three_tags():
    settings = Settings(allowed_lateness_seconds=720)
    dofn = ParseAndAdmit(settings)

    # 1. Valid event
    valid_event = {
        "schema_version": 1,
        "event_id": "e1",
        "asset_id": "apu-01",
        "sensor": {"stream": "motor_current", "channel": "Motor_current", "unit": "A"},
        "value": 1.5,
        "event_time": "2026-09-16T10:00:00Z",
        "ingestion_time": "2026-09-16T10:01:00Z",
    }
    encoded_valid = (b"apu-01", encode_event(valid_event))
    res_valid = list(dofn.process(encoded_valid))
    assert len(res_valid) == 1
    assert res_valid[0]["event_id"] == "e1"
    assert res_valid[0]["stream"] == "motor_current"

    # 2. Quarantine: malformed JSON
    encoded_corrupt = (b"apu-01", b"not-json")
    res_corrupt = list(dofn.process(encoded_corrupt))
    assert len(res_corrupt) == 1
    assert res_corrupt[0].tag == ParseAndAdmit.QUARANTINE
    assert res_corrupt[0].value["reason"] == "invalid_json"

    # 3. Too late: lag exceeds allowed lateness
    late_event = {
        **valid_event,
        "event_id": "e-late",
        "ingestion_time": "2026-09-16T11:00:00Z",  # 3600s lag > 720s
    }
    encoded_late = (b"apu-01", encode_event(late_event))
    res_late = list(dofn.process(encoded_late))
    assert len(res_late) == 1
    assert res_late[0].tag == ParseAndAdmit.TOO_LATE
    assert res_late[0].value["reason"] == "dropped_by_horizon"


def test_kafka_record_encoders():
    agg = {
        "aggregate_id": "apu_health_indicator|apu-01|2026-01-01T00:00:00Z",
        "metric_type": "apu_health_indicator",
        "asset_id": "apu-01",
        "air_leak_suspected": False,
    }
    k, v = aggregate_to_kafka_record(agg)
    assert k == b"apu_health_indicator|apu-01|2026-01-01T00:00:00Z"
    assert json.loads(v.decode())["asset_id"] == "apu-01"

    lateral = {"asset_id": "apu-02", "reason": "invalid_json"}
    k2, v2 = lateral_to_kafka_record(lateral)
    assert k2 == b"apu-02"
    assert json.loads(v2.decode())["reason"] == "invalid_json"


def test_build_analytics_in_beam_matches_oracle_reference():
    settings = Settings(window_seconds=300, allowed_lateness_seconds=720)

    # 4 readings in the same 5-min window [10:00, 10:05)
    raw_events = [
        {
            "schema_version": 1,
            "event_id": make_event_id("apu-04", "motor_current", "t1"),
            "asset_id": "apu-04",
            "sensor": {"stream": "motor_current", "channel": "Motor_current", "unit": "A"},
            "value": 5.0,
            "event_time": "2026-01-01T10:01:00Z",
            "ingestion_time": "2026-01-01T10:01:05Z",
        },
        {
            "schema_version": 1,
            "event_id": make_event_id("apu-04", "motor_current", "t2"),
            "asset_id": "apu-04",
            "sensor": {"stream": "motor_current", "channel": "Motor_current", "unit": "A"},
            "value": 7.0,
            "event_time": "2026-01-01T10:02:00Z",
            "ingestion_time": "2026-01-01T10:02:05Z",
        },
        # Duplicate of t2 to test dedup
        {
            "schema_version": 1,
            "event_id": make_event_id("apu-04", "motor_current", "t2"),
            "asset_id": "apu-04",
            "sensor": {"stream": "motor_current", "channel": "Motor_current", "unit": "A"},
            "value": 7.0,
            "event_time": "2026-01-01T10:02:00Z",
            "ingestion_time": "2026-01-01T10:02:05Z",
        },
        {
            "schema_version": 1,
            "event_id": make_event_id("apu-04", "oil_temperature", "t1"),
            "asset_id": "apu-04",
            "sensor": {"stream": "oil_temperature", "channel": "Oil_temperature", "unit": "degC"},
            "value": 75.0,
            "event_time": "2026-01-01T10:01:00Z",
            "ingestion_time": "2026-01-01T10:01:05Z",
        },
    ]

    normalized_events = [normalize_event(e) for e in raw_events]

    # Compute expected result via oracle
    oracle_res = summarize_readings(
        normalized_events,
        window_seconds=settings.window_seconds,
        allowed_lateness_seconds=settings.allowed_lateness_seconds,
    )
    expected_signal = next(iter(oracle_res["signal_stats"].values()))
    expected_health = next(iter(oracle_res["health"].values()))

    with TestPipeline() as p:
        analytics = build_analytics(
            p | beam.Create(normalized_events), settings, streaming_triggers=False
        )
        signal_rows = (
            analytics
            | "Filter signal stats" >> beam.Filter(lambda r: r["metric_type"] == "apu_signal_stats")
            | "Extract signal values"
            >> beam.Map(
                lambda r: (
                    r["asset_id"],
                    r["stream"],
                    r["readings"],
                    r["value_mean"],
                    r["value_min"],
                    r["value_max"],
                )
            )
        )
        assert_that(
            signal_rows,
            equal_to(
                [
                    (
                        expected_signal["asset_id"],
                        expected_signal["stream"],
                        expected_signal["readings"],
                        expected_signal["value_mean"],
                        expected_signal["value_min"],
                        expected_signal["value_max"],
                    ),
                    ("apu-04", "oil_temperature", 1, 75.0, 75.0, 75.0),
                ]
            ),
        )

    with TestPipeline() as p2:
        analytics2 = build_analytics(
            p2 | beam.Create(normalized_events), settings, streaming_triggers=False
        )
        health_rows = (
            analytics2
            | "Filter health" >> beam.Filter(lambda r: r["metric_type"] == "apu_health_indicator")
            | "Extract health values"
            >> beam.Map(
                lambda r: (
                    r["asset_id"],
                    r["motor_readings"],
                    r["running_ratio"],
                    r["oil_temperature_mean"],
                    r["air_leak_suspected"],
                )
            )
        )
        assert_that(
            health_rows,
            equal_to(
                [
                    (
                        expected_health["asset_id"],
                        expected_health["motor_readings"],
                        expected_health["running_ratio"],
                        expected_health["oil_temperature_mean"],
                        expected_health["air_leak_suspected"],
                    )
                ]
            ),
        )
