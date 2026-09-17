"""Offline end-to-end smoke: dataset sample → ParseAndAdmit → Beam analytics →
oracle comparison → idempotent SQLite sink.

Runs entirely without Docker/Kafka on the DirectRunner. Exits non-zero if any
stage diverges. Designed as the cheapest full-path check before pushing or
presenting the project."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import apache_beam as beam

from apu_streaming.config import Settings, sample_dataset_path
from apu_streaming.contracts import encode_event
from apu_streaming.oracle import summarize_readings
from apu_streaming.producer import load_readings
from apu_streaming.serving import count_aggregates, open_connection, upsert_aggregate
from apu_streaming.transforms import ParseAndAdmit, build_analytics


def _window_key(record: dict) -> tuple[str, str, int]:
    """(asset_id, stream_or_HEALTH, window_start_epoch) for comparison."""
    from datetime import UTC, datetime

    parsed = datetime.fromisoformat(record["window_start"])
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    stream = record.get("stream") or "<health>"
    return (record["asset_id"], stream, int(parsed.timestamp()))


def run_smoke(
    *,
    max_readings: int = 1500,
    db_path: Path | None = None,
) -> dict:
    settings = Settings()  # window_seconds=300, allowed_lateness_seconds=720
    sample = sample_dataset_path()
    readings = load_readings(sample)[:max_readings]
    assert readings, "sample dataset produced zero readings"

    # 1) Wire shape: encode each reading as bytes, as Kafka would carry it.
    raw_events = [r.as_dict() for r in readings]
    wire_pairs = [(r.asset_id.encode(), encode_event(r)) for r in readings]

    # 2) ParseAndAdmit: admission + quarantine routing, in-process.
    admit = ParseAndAdmit(settings)
    admitted: list[dict] = []
    quarantined = 0
    too_late = 0
    for pair in wire_pairs:
        for out in admit.process(pair):
            tag = getattr(out, "tag", None)
            if tag == ParseAndAdmit.QUARANTINE:
                quarantined += 1
            elif tag == ParseAndAdmit.TOO_LATE:
                too_late += 1
            else:
                admitted.append(out)

    assert admitted, "ParseAndAdmit admitted zero events; dataset unusable"

    # 3) Beam analytics on DirectRunner (bounded; single on-time pane per window).
    #    Results are collected via a throwaway text sink.
    beam_aggs = _run_beam_collecting(admitted, settings)

    # 4) Oracle: pure-Python reference on the same wire input.
    oracle_out = summarize_readings(
        raw_events,
        window_seconds=settings.window_seconds,
        allowed_lateness_seconds=settings.allowed_lateness_seconds,
    )

    expected_keys = {_window_key(v) for v in oracle_out["signal_stats"].values()} | {
        _window_key(v) for v in oracle_out["health"].values()
    }
    beam_keys = {_window_key(r) for r in beam_aggs}

    missing_in_beam = expected_keys - beam_keys
    extra_in_beam = beam_keys - expected_keys

    # 5) Idempotent sink: upsert twice, counts must not change.
    tmp_db = db_path or Path(tempfile.mkdtemp(prefix="apu-smoke-")) / "serving.db"
    conn = open_connection(tmp_db)
    try:
        applied_first = sum(upsert_aggregate(conn, r) for r in beam_aggs)
        after_first = count_aggregates(conn)
        applied_second = sum(upsert_aggregate(conn, r) for r in beam_aggs)
        after_second = count_aggregates(conn)
        idempotent = after_first == after_second and applied_second >= applied_first >= 0
    finally:
        conn.close()

    summary = {
        "sample_path": str(sample),
        "readings_loaded": len(readings),
        "admitted": len(admitted),
        "quarantined": quarantined,
        "too_late": too_late,
        "oracle_keys": len(expected_keys),
        "beam_keys": len(beam_keys),
        "missing_in_beam": sorted(map(str, missing_in_beam))[:10],
        "extra_in_beam": sorted(map(str, extra_in_beam))[:10],
        "sink_rows_after_first_pass": after_first,
        "sink_rows_after_second_pass": after_second,
        "sink_idempotent": bool(idempotent),
        "ok": not missing_in_beam and not extra_in_beam and bool(idempotent),
    }
    return summary


def _run_beam_collecting(admitted: list[dict], settings: Settings) -> list[dict]:
    """Run analytics via DirectRunner; results are collected via a text sink."""
    out_dir = Path(tempfile.mkdtemp(prefix="apu-smoke-beam-"))

    pipeline = beam.Pipeline()
    events = pipeline | beam.Create(admitted)
    _ = (
        build_analytics(events, settings, streaming_triggers=False)
        | beam.Map(json.dumps, sort_keys=True)
        | beam.io.WriteToText(str(out_dir / "out"), file_name_suffix=".json")
    )
    result = pipeline.run()
    result.wait_until_finish()

    rows: list[dict] = []
    for path in sorted(out_dir.glob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-readings", type=int, default=1500)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    try:
        summary = run_smoke(max_readings=args.max_readings, db_path=args.db_path)
    except Exception as error:  # noqa: BLE001 - smoke must surface any failure
        print(json.dumps({"ok": False, "error": f"{type(error).__name__}: {error}"}))
        sys.exit(2)

    print(json.dumps(summary, indent=2, sort_keys=True))
    sys.exit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
