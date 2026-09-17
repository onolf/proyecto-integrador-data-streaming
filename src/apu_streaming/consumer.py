"""Kafka consumer that materializes features.asset into the idempotent sink.

Runs as its own process (the `materializer` compose service) so that
data/serving.db is populated independently of whether anyone opens the
marimo dashboard.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from confluent_kafka import Consumer, KafkaError

from apu_streaming.config import Settings, serving_db_path
from apu_streaming.serving import count_aggregates, open_connection, upsert_aggregate


@dataclass
class AggregateStore:
    """In-memory upsert store mirroring the sink's pane_index monotonicity rule."""

    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages_seen: int = 0

    def upsert(self, aggregate: dict[str, Any]) -> bool:
        self.messages_seen += 1
        aggregate_id = aggregate["aggregate_id"]
        existing = self.records.get(aggregate_id)
        if existing is not None and aggregate["pane_index"] < existing["pane_index"]:
            return False
        self.records[aggregate_id] = aggregate
        return True

    def frame(self, metric_type: str | None = None) -> pd.DataFrame:
        rows = list(self.records.values())
        if metric_type is not None:
            rows = [r for r in rows if r["metric_type"] == metric_type]
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame(rows)
        return frame.sort_values(["window_start", "asset_id"])

    def health_frame(self) -> pd.DataFrame:
        return self.frame("apu_health_indicator")

    def summary(self) -> dict[str, int]:
        return {
            "messages_seen": self.messages_seen,
            "aggregates": len(self.records),
            "signal_stats": sum(
                1 for r in self.records.values() if r["metric_type"] == "apu_signal_stats"
            ),
            "health_indicators": sum(
                1 for r in self.records.values() if r["metric_type"] == "apu_health_indicator"
            ),
        }


def build_consumer(
    settings: Settings,
    *,
    group_id: str = "apu-materializer-v1",
    offset_reset: str = "earliest",
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([settings.features_topic])
    return consumer


def poll_into_store(
    consumer: Consumer,
    store: AggregateStore,
    *,
    max_messages: int = 1000,
    timeout_seconds: float = 1.0,
) -> dict[str, int]:
    """Poll up to max_messages and upsert valid ones into the store. Returns counters."""
    accepted = 0
    errors = 0
    for _ in range(max_messages):
        msg = consumer.poll(timeout_seconds)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            errors += 1
            continue
        try:
            aggregate = json.loads(msg.value())
        except (ValueError, TypeError):
            errors += 1
            continue
        store.upsert(aggregate)
        accepted += 1
    return {"accepted": accepted, "errors": errors, **store.summary()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default="apu-materializer-v1", help="Kafka consumer group id")
    parser.add_argument("--db-path", default=None, help="serving database path")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=0.0,
        help="seconds with no messages before exiting (0 = run indefinitely)",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    db_path = args.db_path or serving_db_path()
    conn = open_connection(db_path)
    consumer = build_consumer(settings, group_id=args.group_id)

    store = AggregateStore()
    last_message_at = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if (
                    args.idle_timeout > 0
                    and (time.monotonic() - last_message_at) > args.idle_timeout
                ):
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue

            try:
                aggregate = json.loads(msg.value())
            except (ValueError, TypeError):
                continue

            store.upsert(aggregate)
            upsert_aggregate(conn, aggregate)
            last_message_at = time.monotonic()
            print(json.dumps({"materialized": True, **store.summary()}))
    finally:
        consumer.close()
        summary = {
            **store.summary(),
            "sink_total": count_aggregates(conn),
        }
        conn.close()
        print(json.dumps(summary))


if __name__ == "__main__":
    main()
