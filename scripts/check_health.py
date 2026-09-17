"""Operational health probe for the APU streaming stack.

Checks:
1. Kafka reachability (topic metadata for the three pipeline topics).
2. Serving DB: exists, has rows, and rows are fresh (max updated_at within
   a staleness budget) — evidence the materializer is alive.

Exits 0 when every enabled check passes, 1 otherwise. Designed to be cheap
enough for cron / CI post-deploy gates.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

from apu_streaming.config import Settings, serving_db_path
from apu_streaming.serving import count_aggregates


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def check_kafka(settings: Settings, timeout_ms: int) -> dict:
    """Probe broker metadata for the three pipeline topics."""
    try:
        from confluent_kafka import KafkaException, Producer
    except ImportError:
        return {"ok": False, "error": "confluent_kafka not installed"}

    try:
        producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        metadata = producer.list_topics(timeout=timeout_ms / 1000.0)
        available = set(metadata.topics.keys())
    except KafkaException as error:
        return {"ok": False, "error": str(error)}
    except Exception as error:  # noqa: BLE001 - probe must never crash
        return {"ok": False, "error": f"{type(error).__name__}: {error}"}

    expected = {
        settings.raw_topic,
        settings.features_topic,
        settings.quarantine_topic,
        settings.too_late_topic,
    }
    missing = sorted(expected - available)
    return {
        "ok": not missing,
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "topics_expected": sorted(expected),
        "topics_present": sorted(expected & available),
        "topics_missing": missing,
    }


def check_serving_db(db_path: Path, max_staleness_seconds: int) -> dict:
    if not db_path.exists():
        return {"ok": False, "error": f"{db_path} does not exist"}

    conn = sqlite3.connect(db_path)
    try:
        total = count_aggregates(conn)
        by_type = {
            metric: count_aggregates(conn, metric_type=metric)
            for metric in ("apu_signal_stats", "apu_health_indicator")
        }
        row = conn.execute("SELECT MAX(updated_at) FROM aggregates").fetchone()
        latest = row[0] if row and row[0] else None
    finally:
        conn.close()

    if total == 0:
        return {"ok": False, "rows": 0, "error": "no aggregates materialized yet"}

    staleness_s: float | None = None
    fresh = None
    if latest:
        staleness_s = (datetime.now(UTC) - _parse_iso(latest)).total_seconds()
        fresh = staleness_s <= max_staleness_seconds

    return {
        "ok": bool(fresh),
        "rows": total,
        "by_metric_type": by_type,
        "latest_updated_at": latest,
        "staleness_seconds": round(staleness_s, 1) if staleness_s is not None else None,
        "max_staleness_seconds": max_staleness_seconds,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=None, help="override KAFKA_BOOTSTRAP_SERVERS")
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--max-staleness-seconds", type=int, default=900)
    parser.add_argument("--timeout-ms", type=int, default=5000)
    parser.add_argument("--skip-kafka", action="store_true", help="only probe the serving DB")
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.bootstrap:
        settings = Settings(**{**settings.__dict__, "kafka_bootstrap_servers": args.bootstrap})

    report: dict = {"ok": True, "checks": {}}

    if not args.skip_kafka:
        report["checks"]["kafka"] = check_kafka(settings, args.timeout_ms)
        report["ok"] = report["ok"] and report["checks"]["kafka"]["ok"]

    report["checks"]["serving_db"] = check_serving_db(
        args.db_path or serving_db_path(), args.max_staleness_seconds
    )
    report["ok"] = report["ok"] and report["checks"]["serving_db"]["ok"]

    print(json.dumps(report, indent=2, sort_keys=True))
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
