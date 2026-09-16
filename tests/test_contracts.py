from __future__ import annotations

import pytest

from apu_streaming.contracts import (
    ContractError,
    decode_event,
    encode_event,
    event_lag_seconds,
    make_event_id,
    normalize_event,
    reading_events,
)


def sample_row(**overrides):
    row = {
        "asset_id": "apu-04",
        "source_event_time": "2020-04-18T08:00:00Z",
        "Motor_current": 5.66,
        "Oil_temperature": 75.0,
        "TP2": 1.2,
        "DV_pressure": 0.3,
        "TP3": 9.1,
        "H1": 9.0,
        "Reservoirs": 9.1,
    }
    row.update(overrides)
    return row


def base_event(**overrides) -> dict:
    events = reading_events(sample_row(), schema_version=1)
    event = events[0].as_dict()
    event.update(overrides)
    return event


def test_event_id_is_stable_across_runs_and_survives_a_duplicate():
    events_a = reading_events(sample_row(), schema_version=1)
    events_b = reading_events(sample_row(), schema_version=1)
    assert [e.event_id for e in events_a] == [e.event_id for e in events_b]
    assert events_a[0].event_id == make_event_id("apu-04", "motor_current", "2020-04-18T08:00:00Z")


def test_event_id_ignores_replay_shifted_event_time():
    shifted = reading_events(
        sample_row(), schema_version=1, event_time="2026-09-16T07:05:00Z"
    )
    unshifted = reading_events(sample_row(), schema_version=1)
    assert shifted[0].event_id == unshifted[0].event_id


def test_round_trip_is_deterministic():
    event = base_event()
    encoded = encode_event(event)
    assert encode_event(decode_event(encoded)) == encoded


def test_schema_v1_yields_four_streams():
    events = reading_events(sample_row(), schema_version=1)
    assert {e.stream for e in events} == {
        "motor_current",
        "oil_temperature",
        "tp2_pressure",
        "dv_pressure",
    }


def test_schema_v2_yields_seven_streams():
    events = reading_events(sample_row(), schema_version=2)
    assert len(events) == 7


def test_unsupported_schema_version_is_rejected():
    event = base_event(schema_version=3)
    with pytest.raises(ContractError, match="unsupported_schema_version"):
        decode_event(encode_event(event))


def test_unknown_field_is_ignored():
    event = base_event()
    event["extra_future_field"] = "whatever"
    decoded = decode_event(encode_event(event))
    assert decoded["extra_future_field"] == "whatever"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda e: e.pop("asset_id"), "missing_field"),
        (lambda e: e["sensor"].update(stream="unknown_stream"), "unknown_stream"),
        (lambda e: e.update(value=float("nan")), "non_finite_value"),
    ],
)
def test_rejection_rules_report_stable_reasons(mutate, reason):
    event = base_event()
    mutate(event)
    with pytest.raises(ContractError, match=reason):
        decode_event(encode_event(event))


def test_future_event_time_is_rejected():
    event = base_event(event_time="2026-09-16T12:00:00Z", ingestion_time="2026-09-16T11:00:00Z")
    with pytest.raises(ContractError, match="future_event_time"):
        decode_event(encode_event(event))


def test_stale_event_time_is_rejected():
    event = base_event(event_time="2020-01-01T00:00:00Z", ingestion_time="2020-01-03T00:00:00Z")
    with pytest.raises(ContractError, match="stale_event_time"):
        decode_event(encode_event(event))


def test_invalid_json_is_rejected():
    with pytest.raises(ContractError, match="invalid_json"):
        decode_event(b"not json")


def test_malformed_timestamp_is_rejected_not_raised_raw():
    event = base_event(event_time="not-a-timestamp")
    with pytest.raises(ContractError, match="invalid_timestamp"):
        decode_event(encode_event(event))


def test_normalize_event_flattens_sensor_and_coerces_value_to_float():
    event = base_event(value="5.66")
    decoded = decode_event(encode_event({**event, "value": 5.66}))
    flat = normalize_event(decoded)
    assert flat["stream"] == "motor_current"
    assert flat["channel"] == "Motor_current"
    assert flat["unit"] == "A"
    assert flat["value"] == 5.66
    assert "sensor" not in flat


def test_event_lag_seconds_matches_ingestion_minus_event_time():
    event = base_event(event_time="2026-09-16T07:00:00Z", ingestion_time="2026-09-16T07:05:00Z")
    assert event_lag_seconds(event) == 300.0
