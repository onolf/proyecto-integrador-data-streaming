"""Event contract for APU sensor readings: streams, validation, and codecs."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION_MAX = 2
ASSET_TYPE = "apu_compresor_motor_trifasico"

REQUIRED_FIELDS = (
    "schema_version",
    "event_id",
    "asset_id",
    "sensor",
    "value",
    "event_time",
    "ingestion_time",
)


@dataclass(frozen=True)
class StreamSpec:
    """A named sensor stream sourced from an exact MetroPT-3 column."""

    stream: str  # nombre lógico normalizado
    channel: str  # identificador EXACTO de la fuente MetroPT-3; nunca normalizar
    unit: str
    schema_version: int


STREAM_SPECS: tuple[StreamSpec, ...] = (
    StreamSpec("motor_current", "Motor_current", "A", 1),
    StreamSpec("oil_temperature", "Oil_temperature", "degC", 1),
    StreamSpec("tp2_pressure", "TP2", "bar", 1),
    StreamSpec("dv_pressure", "DV_pressure", "bar", 1),
    StreamSpec("tp3_pressure", "TP3", "bar", 2),
    StreamSpec("h1_pressure", "H1", "bar", 2),
    StreamSpec("reservoirs_pressure", "Reservoirs", "bar", 2),
)

_STREAMS_BY_NAME: dict[str, StreamSpec] = {spec.stream: spec for spec in STREAM_SPECS}


def _iso_utc(value: datetime | str) -> str:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


@dataclass(frozen=True)
class SensorReading:
    """One reading of one stream of one asset: the canonical input event."""

    schema_version: int
    event_id: str
    asset_id: str
    asset_type: str
    stream: str
    channel: str
    unit: str
    value: float
    event_time: str
    ingestion_time: str
    source_event_time: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "sensor": {"stream": self.stream, "channel": self.channel, "unit": self.unit},
            "value": self.value,
            "event_time": self.event_time,
            "ingestion_time": self.ingestion_time,
            "source_event_time": self.source_event_time,
        }


def make_event_id(asset_id: str, stream: str, source_event_time: str) -> str:
    return f"{asset_id}|{stream}|{source_event_time}"


def reading_events(
    row: dict[str, Any],
    *,
    schema_version: int = SCHEMA_VERSION_MAX,
    event_time: str | None = None,
    ingestion_time: str | None = None,
) -> tuple[SensorReading, ...]:
    """Expand one wide row (asset_id, source_event_time, <channels>) into readings.

    `event_time` defaults to `source_event_time` (no replay shift applied);
    the producer overrides it when shifting the timeline.
    """
    asset_id = str(row["asset_id"])
    source_event_time = _iso_utc(row["source_event_time"])
    resolved_event_time = event_time if event_time is not None else source_event_time
    resolved_ingestion_time = ingestion_time if ingestion_time is not None else resolved_event_time

    readings: list[SensorReading] = []
    for spec in STREAM_SPECS:
        if spec.schema_version > schema_version:
            continue
        raw_value = row.get(spec.channel)
        value = _finite(raw_value)
        if value is None:
            continue
        readings.append(
            SensorReading(
                schema_version=schema_version,
                event_id=make_event_id(asset_id, spec.stream, source_event_time),
                asset_id=asset_id,
                asset_type=ASSET_TYPE,
                stream=spec.stream,
                channel=spec.channel,
                unit=spec.unit,
                value=value,
                event_time=resolved_event_time,
                ingestion_time=resolved_ingestion_time,
                source_event_time=source_event_time,
            )
        )
    return tuple(readings)


def encode_event(event: SensorReading | dict[str, Any]) -> bytes:
    payload = event.as_dict() if isinstance(event, SensorReading) else event
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


class ContractError(ValueError):
    """Raised with a stable, machine-checkable rejection reason."""

    def __init__(self, reason: str, *, detail: str = "") -> None:
        self.reason = reason
        message = f"{reason}: {detail}" if detail else reason
        super().__init__(message)


def decode_event(payload: bytes | str, *, stale_event_seconds: int = 86400) -> dict[str, Any]:
    """Decode and validate a raw event payload.

    Raises `ContractError` with one of the stable reasons documented in the
    RFC section 3: invalid_json, missing_field, unsupported_schema_version,
    unknown_stream, non_finite_value, invalid_timestamp, future_event_time,
    stale_event_time.
    """
    try:
        decoded = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
    except (ValueError, TypeError, UnicodeDecodeError) as error:
        raise ContractError("invalid_json", detail=str(error)) from error

    if not isinstance(decoded, dict):
        raise ContractError("invalid_json", detail="payload is not a JSON object")

    for field in REQUIRED_FIELDS:
        if field not in decoded:
            raise ContractError("missing_field", detail=field)

    schema_version = decoded["schema_version"]
    if not isinstance(schema_version, int) or not (1 <= schema_version <= SCHEMA_VERSION_MAX):
        raise ContractError("unsupported_schema_version", detail=str(schema_version))

    sensor = decoded["sensor"]
    stream = sensor.get("stream") if isinstance(sensor, dict) else None
    if stream not in _STREAMS_BY_NAME:
        raise ContractError("unknown_stream", detail=str(stream))

    if _finite(decoded["value"]) is None:
        raise ContractError("non_finite_value", detail=str(decoded["value"]))

    try:
        event_time = _parse_iso(decoded["event_time"])
        ingestion_time = _parse_iso(decoded["ingestion_time"])
    except (ValueError, TypeError, AttributeError) as error:
        raise ContractError("invalid_timestamp", detail=str(error)) from error

    if event_time > ingestion_time:
        raise ContractError("future_event_time")

    if (ingestion_time - event_time).total_seconds() > stale_event_seconds:
        raise ContractError("stale_event_time")

    return decoded


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    """Flatten the wire shape (`sensor.stream/channel/unit`) into top-level
    fields and coerce `value` to `float`, producing the internal shape that
    `oracle.summarize_readings` and the Beam transforms both operate on.
    Call this once, right after `decode_event`, so the oracle and the
    pipeline never diverge on event shape.
    """
    flat = dict(event)
    sensor = flat.pop("sensor", None)
    if isinstance(sensor, dict):
        flat.setdefault("stream", sensor.get("stream"))
        flat.setdefault("channel", sensor.get("channel"))
        flat.setdefault("unit", sensor.get("unit"))
    value = _finite(flat.get("value"))
    if value is not None:
        flat["value"] = value
    return flat


def event_lag_seconds(event: dict[str, Any]) -> float:
    ingestion_time = _parse_iso(event["ingestion_time"])
    event_time = _parse_iso(event["event_time"])
    return (ingestion_time - event_time).total_seconds()
