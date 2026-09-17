"""Reusable Apache Beam transforms for windowed APU streaming analytics."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger, window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    SetStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.window import TimestampedValue

from apu_streaming.config import (
    MOTOR_RUNNING_THRESHOLD_A,
    OIL_TEMPERATURE_ALERT_C,
    RUNNING_RATIO_ALERT,
    Settings,
)
from apu_streaming.contracts import (
    ContractError,
    decode_event,
    event_lag_seconds,
    normalize_event,
)


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class ParseAndAdmit(beam.DoFn):
    """Decode raw Kafka byte pairs, validate contract, and route to outputs."""

    QUARANTINE = "quarantine"
    TOO_LATE = "too_late"

    def __init__(self, settings: Settings) -> None:
        self.allowed_lateness_seconds = settings.allowed_lateness_seconds
        self.stale_event_seconds = settings.stale_event_seconds

    def process(self, element: tuple[bytes, bytes] | dict[str, Any] | str | bytes):
        kafka_key: str | None = None
        raw_payload: str = ""

        if isinstance(element, tuple):
            key_bytes, val_bytes = element
            kafka_key = key_bytes.decode(errors="replace") if key_bytes else None
            raw_payload = (
                val_bytes.decode(errors="replace")
                if isinstance(val_bytes, bytes)
                else str(val_bytes)
            )
            payload_input = val_bytes
        elif isinstance(element, dict):
            payload_input = json.dumps(element)
            raw_payload = payload_input
            kafka_key = str(element.get("asset_id", ""))
        else:
            raw_payload = (
                element.decode(errors="replace") if isinstance(element, bytes) else str(element)
            )
            payload_input = element

        try:
            decoded = decode_event(payload_input, stale_event_seconds=self.stale_event_seconds)
        except ContractError as error:
            Metrics.counter("apu_streaming", "quarantined").inc()
            yield pvalue.TaggedOutput(
                self.QUARANTINE,
                {
                    "reason": error.reason,
                    "detail": str(error),
                    "payload": raw_payload,
                    "kafka_key": kafka_key,
                },
            )
            return

        lag = event_lag_seconds(decoded)
        if lag > self.allowed_lateness_seconds:
            Metrics.counter("apu_streaming", "dropped_by_horizon").inc()
            yield pvalue.TaggedOutput(
                self.TOO_LATE,
                {
                    "reason": "dropped_by_horizon",
                    "event_id": decoded.get("event_id"),
                    "asset_id": decoded.get("asset_id"),
                    "event_lag_seconds": lag,
                    "payload": decoded,
                    "kafka_key": kafka_key,
                },
            )
            return

        normalized = normalize_event(decoded)
        if kafka_key:
            normalized["kafka_key"] = kafka_key

        Metrics.counter("apu_streaming", "admitted").inc()
        yield normalized


def assign_event_timestamp(event: dict[str, Any]) -> TimestampedValue:
    parsed = _parse_iso(event["event_time"])
    return TimestampedValue(event, parsed.timestamp())


def _windowed(events, settings: Settings, *, streaming_triggers: bool = True):
    kwargs: dict[str, Any] = {
        "windowfn": window.FixedWindows(settings.window_seconds),
        "allowed_lateness": settings.allowed_lateness_seconds,
    }
    if streaming_triggers:
        kwargs["trigger"] = trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(settings.early_firing_seconds),
            late=trigger.AfterCount(1),
        )
        kwargs["accumulation_mode"] = trigger.AccumulationMode.ACCUMULATING
    return events | "Fixed event-time windows" >> beam.WindowInto(**kwargs)


class DeduplicateReadings(beam.DoFn):
    """Stateful DoFn to deduplicate event_id per (asset_id, window) with watermark expiry."""

    SEEN_IDS = SetStateSpec("seen_event_ids", StrUtf8Coder())
    EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

    def __init__(self, allowed_lateness_seconds: int = 720) -> None:
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        window_param=beam.DoFn.WindowParam,
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):
        _asset_id, event = element
        event_id = event["event_id"]
        if event_id in seen_ids.read():
            Metrics.counter("apu_streaming", "duplicates_dropped").inc()
            return

        seen_ids.add(event_id)
        # Expiry timer set at window.end + allowed_lateness
        horizon = window_param.end + self.allowed_lateness_seconds
        expiry.set(horizon)
        yield event

    @on_timer(EXPIRY)
    def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
        seen_ids.clear()


class SignalStatsCombineFn(beam.CombineFn):
    """Incremental statistics (count, mean, min, max, stddev) for one stream."""

    def create_accumulator(self) -> tuple[int, float, float, float, float]:
        return (0, 0.0, 0.0, float("inf"), float("-inf"))

    def add_input(
        self,
        accumulator: tuple[int, float, float, float, float],
        event: dict[str, Any],
    ) -> tuple[int, float, float, float, float]:
        count, total, total_sq, mn, mx = accumulator
        val = float(event["value"])
        return (
            count + 1,
            total + val,
            total_sq + (val * val),
            min(mn, val),
            max(mx, val),
        )

    def merge_accumulators(
        self,
        accumulators: list[tuple[int, float, float, float, float]],
    ) -> tuple[int, float, float, float, float]:
        total_count = 0
        total_sum = 0.0
        total_sq = 0.0
        global_min = float("inf")
        global_max = float("-inf")
        for count, s, s_sq, mn, mx in accumulators:
            total_count += count
            total_sum += s
            total_sq += s_sq
            if mn < global_min:
                global_min = mn
            if mx > global_max:
                global_max = mx
        return (total_count, total_sum, total_sq, global_min, global_max)

    def extract_output(
        self,
        accumulator: tuple[int, float, float, float, float],
    ) -> dict[str, Any]:
        count, total, total_sq, mn, mx = accumulator
        if count == 0:
            return {
                "readings": 0,
                "value_mean": 0.0,
                "value_min": None,
                "value_max": None,
                "value_stddev": 0.0,
            }
        mean = total / count
        variance = max(0.0, (total_sq / count) - (mean * mean))
        stddev = math.sqrt(variance)
        return {
            "readings": count,
            "value_mean": round(mean, 4),
            "value_min": round(mn, 4),
            "value_max": round(mx, 4),
            "value_stddev": round(stddev, 4),
        }


class HealthIndicatorCombineFn(beam.CombineFn):
    """Incremental APU operational health metrics across streams."""

    def create_accumulator(self) -> tuple[int, int, int, float]:
        return (0, 0, 0, 0.0)

    def add_input(
        self,
        accumulator: tuple[int, int, int, float],
        event: dict[str, Any],
    ) -> tuple[int, int, int, float]:
        mc, rc, oc, ot = accumulator
        stream = event.get("stream")
        val = float(event.get("value", 0.0))
        if stream == "motor_current":
            mc += 1
            if val >= MOTOR_RUNNING_THRESHOLD_A:
                rc += 1
        elif stream == "oil_temperature":
            oc += 1
            ot += val
        return (mc, rc, oc, ot)

    def merge_accumulators(
        self,
        accumulators: list[tuple[int, int, int, float]],
    ) -> tuple[int, int, int, float]:
        total_mc = sum(a[0] for a in accumulators)
        total_rc = sum(a[1] for a in accumulators)
        total_oc = sum(a[2] for a in accumulators)
        total_ot = sum(a[3] for a in accumulators)
        return (total_mc, total_rc, total_oc, total_ot)

    def extract_output(
        self,
        accumulator: tuple[int, int, int, float],
    ) -> dict[str, Any]:
        mc, rc, oc, ot = accumulator
        running_ratio = (rc / mc) if mc > 0 else 0.0
        oil_mean = (ot / oc) if oc > 0 else 0.0
        air_leak_suspected = (
            running_ratio >= RUNNING_RATIO_ALERT and oil_mean >= OIL_TEMPERATURE_ALERT_C
        )
        return {
            "motor_readings": mc,
            "running_ratio": round(running_ratio, 4),
            "oil_temperature_mean": round(oil_mean, 4),
            "air_leak_suspected": air_leak_suspected,
        }


class FormatAggregate(beam.DoFn):
    """Format combined results into standard analytical records with pane metadata."""

    def __init__(self, metric_type: str, *, allowed_lateness_seconds: int = 720) -> None:
        self.metric_type = metric_type
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def process(
        self,
        element: tuple[Any, dict[str, Any]],
        window_param=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        dimension, metrics = element
        start_dt = window_param.start.to_utc_datetime()
        end_dt = window_param.end.to_utc_datetime()
        start = start_dt.isoformat().replace("+00:00", "Z")
        end = end_dt.isoformat().replace("+00:00", "Z")
        now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        close_at = (
            (end_dt + timedelta(seconds=self.allowed_lateness_seconds))
            .isoformat()
            .replace("+00:00", "Z")
        )

        if self.metric_type == "apu_signal_stats":
            asset_id, stream, unit = dimension
            key = f"apu_signal_stats|{asset_id}|{stream}|{start}"
            yield {
                "schema_version": 1,
                "aggregate_id": key,
                "metric_type": self.metric_type,
                "asset_id": asset_id,
                "stream": stream,
                "unit": unit,
                "window_start": start,
                "window_end": end,
                "pane_index": pane_info.index,
                "pane_timing": str(pane_info.timing),
                "is_first": pane_info.is_first,
                "is_last": pane_info.is_last,
                "emitted_at": now_iso,
                "accumulation": "ACCUMULATING",
                "corrections_close_at": close_at,
                **metrics,
            }
        else:
            asset_id = dimension
            key = f"apu_health_indicator|{asset_id}|{start}"
            yield {
                "schema_version": 1,
                "aggregate_id": key,
                "metric_type": self.metric_type,
                "asset_id": asset_id,
                "window_start": start,
                "window_end": end,
                "pane_index": pane_info.index,
                "pane_timing": str(pane_info.timing),
                "is_first": pane_info.is_first,
                "is_last": pane_info.is_last,
                "emitted_at": now_iso,
                "accumulation": "ACCUMULATING",
                "corrections_close_at": close_at,
                **metrics,
            }


def build_analytics(events, settings: Settings, *, streaming_triggers: bool = True):
    """Build windowed, deduplicated signal stats and health indicators."""
    timestamped = events | "Assign domain timestamp" >> beam.Map(assign_event_timestamp)
    windowed_events = _windowed(timestamped, settings, streaming_triggers=streaming_triggers)

    deduplicated = (
        windowed_events
        | "Key by asset id for dedup" >> beam.Map(lambda e: (e["asset_id"], e))
        | "Deduplicate in window"
        >> beam.ParDo(DeduplicateReadings(settings.allowed_lateness_seconds))
    )

    signal_stats = (
        deduplicated
        | "Key by (asset, stream, unit)"
        >> beam.Map(lambda e: ((e["asset_id"], e["stream"], e.get("unit", "")), e))
        | "Combine signal statistics" >> beam.CombinePerKey(SignalStatsCombineFn())
        | "Format signal stats"
        >> beam.ParDo(
            FormatAggregate(
                "apu_signal_stats", allowed_lateness_seconds=settings.allowed_lateness_seconds
            )
        )
    )

    health_indicator = (
        deduplicated
        | "Key by asset id for health" >> beam.Map(lambda e: (e["asset_id"], e))
        | "Combine health indicator" >> beam.CombinePerKey(HealthIndicatorCombineFn())
        | "Format health indicator"
        >> beam.ParDo(
            FormatAggregate(
                "apu_health_indicator", allowed_lateness_seconds=settings.allowed_lateness_seconds
            )
        )
    )

    return (signal_stats, health_indicator) | "Merge analytical streams" >> beam.Flatten()


def aggregate_to_kafka_record(aggregate: dict[str, Any]) -> tuple[bytes, bytes]:
    """Encode an analytical record to (key, value) bytes for KafkaIO sink."""
    return (
        aggregate["aggregate_id"].encode(),
        json.dumps(aggregate, sort_keys=True, separators=(",", ":")).encode(),
    )


def lateral_to_kafka_record(record: dict[str, Any]) -> tuple[bytes, bytes]:
    """Encode a lateral (quarantine/too_late) record to Kafka."""
    key = str(record.get("asset_id") or record.get("kafka_key") or "").encode()
    value = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
    return key, value
