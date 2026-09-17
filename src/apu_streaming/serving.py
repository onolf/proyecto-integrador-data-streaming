"""Idempotent, durable sink for aggregate records: SQLite upsert by aggregate_id.

RN-06: apply an incoming aggregate only if its pane_index >= the stored one.
This makes the sink monotone: reapplying the same pane is a no-op, and a
stale/reordered pane can never roll back a value already corrected.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS aggregates (
  aggregate_id TEXT PRIMARY KEY,
  metric_type TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  stream TEXT,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  pane_index INTEGER NOT NULL,
  pane_timing TEXT NOT NULL,
  is_last INTEGER NOT NULL,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL
)
"""

_UPSERT_SQL = """
INSERT INTO aggregates
  (aggregate_id, metric_type, asset_id, stream, window_start, window_end,
   pane_index, pane_timing, is_last, payload, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(aggregate_id) DO UPDATE SET
  metric_type = excluded.metric_type,
  asset_id = excluded.asset_id,
  stream = excluded.stream,
  window_start = excluded.window_start,
  window_end = excluded.window_end,
  pane_index = excluded.pane_index,
  pane_timing = excluded.pane_timing,
  is_last = excluded.is_last,
  payload = excluded.payload,
  updated_at = excluded.updated_at
WHERE excluded.pane_index >= aggregates.pane_index
"""


def open_connection(db_path: Path | str) -> sqlite3.Connection:
    """Open (creating if needed) the serving database and ensure the schema exists."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


def upsert_aggregate(conn: sqlite3.Connection, aggregate: dict[str, Any]) -> bool:
    """Upsert one aggregate record. Returns True if the row was inserted or updated.

    A pane_index lower than the stored one leaves the row untouched (the WHERE
    clause on the UPDATE fails), so this returns False for stale reentries.
    """
    aggregate_id = aggregate["aggregate_id"]

    before = conn.execute(
        "SELECT pane_index FROM aggregates WHERE aggregate_id = ?", (aggregate_id,)
    ).fetchone()

    conn.execute(
        _UPSERT_SQL,
        (
            aggregate_id,
            aggregate["metric_type"],
            aggregate["asset_id"],
            aggregate.get("stream"),
            aggregate["window_start"],
            aggregate["window_end"],
            aggregate["pane_index"],
            aggregate["pane_timing"],
            1 if aggregate.get("is_last") else 0,
            json.dumps(aggregate, sort_keys=True, separators=(",", ":")),
            aggregate.get("emitted_at", ""),
        ),
    )
    conn.commit()

    if before is None:
        return True
    return aggregate["pane_index"] >= before[0]


def fetch_aggregate(conn: sqlite3.Connection, aggregate_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT payload FROM aggregates WHERE aggregate_id = ?", (aggregate_id,)
    ).fetchone()
    return json.loads(row[0]) if row else None


def count_aggregates(conn: sqlite3.Connection, *, metric_type: str | None = None) -> int:
    if metric_type is None:
        return conn.execute("SELECT COUNT(*) FROM aggregates").fetchone()[0]
    return conn.execute(
        "SELECT COUNT(*) FROM aggregates WHERE metric_type = ?", (metric_type,)
    ).fetchone()[0]


def fetch_health_indicators(
    conn: sqlite3.Connection, *, only_last: bool = True
) -> list[dict[str, Any]]:
    """Fetch apu_health_indicator rows as parsed payload dicts, ordered by asset_id."""
    query = "SELECT payload FROM aggregates WHERE metric_type = 'apu_health_indicator'"
    if only_last:
        query += " AND is_last = 1"
    query += " ORDER BY asset_id"
    rows = conn.execute(query).fetchall()
    return [json.loads(r[0]) for r in rows]
