"""Download MetroPT-3, synthesize a 6-asset fleet, and write the sample fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from apu_streaming.config import (
    manifest_path,
    processed_dataset_path,
    project_root,
    sample_dataset_path,
)
from apu_streaming.contracts import encode_event, reading_events

METROPT3_URL = "https://archive.ics.uci.edu/static/public/791/metropt+3+dataset.zip"
METROPT3_MEMBER = "MetroPT3(AirCompressor).csv"

USECOLS = [
    "timestamp",
    "TP2",
    "DV_pressure",
    "Oil_temperature",
    "Motor_current",
    "TP3",
    "H1",
    "Reservoirs",
]


@dataclass(frozen=True)
class FleetSegment:
    asset_id: str
    start: str
    end: str
    condition: Literal["sano", "falla_fuga_aire"]
    failure_ref: str | None


FLEET_SEGMENTS: tuple[FleetSegment, ...] = (
    FleetSegment("apu-01", "2020-02-03 08:00:00", "2020-02-03 12:00:00", "sano", None),
    FleetSegment("apu-02", "2020-03-02 08:00:00", "2020-03-02 12:00:00", "sano", None),
    FleetSegment("apu-03", "2020-07-01 08:00:00", "2020-07-01 12:00:00", "sano", None),
    FleetSegment(
        "apu-04", "2020-04-18 08:00:00", "2020-04-18 12:00:00", "falla_fuga_aire", "falla #1"
    ),
    FleetSegment(
        "apu-05", "2020-06-05 10:00:00", "2020-06-05 14:00:00", "falla_fuga_aire", "falla #3"
    ),
    FleetSegment(
        "apu-06", "2020-07-15 15:00:00", "2020-07-15 19:00:00", "falla_fuga_aire", "falla #4"
    ),
)

# Conteos verificados leyendo el CSV completo en la sesión de planificación;
# protegen contra un cambio silencioso del dataset upstream.
EXPECTED_ROWS = {
    "apu-01": 1452,
    "apu-02": 1453,
    "apu-03": 1452,
    "apu-04": 1453,
    "apu-05": 1453,
    "apu-06": 1439,
}
EXPECTED_TOTAL_ROWS = sum(EXPECTED_ROWS.values())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, destination: Path, *, force: bool = False) -> Path:
    """Download `url` atomically: write to a `.part` sibling, then rename.

    An interrupted download never leaves a truncated file at `destination`,
    so a later run's `exists() and not force` check can trust it fully.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not force:
        return destination
    partial = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=300) as response, partial.open("wb") as fh:
        shutil.copyfileobj(response, fh)
    os.replace(partial, destination)
    return destination


def _read_metropt3_csv(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive, archive.open(METROPT3_MEMBER) as member:
        return pd.read_csv(member, usecols=USECOLS, parse_dates=["timestamp"])


def prepare_dataset(*, force: bool = False) -> Path:
    """Download MetroPT-3, cut the 6 fleet segments, and write the wide parquet."""
    cache_zip = project_root() / "data/cache/metropt3.zip"
    download(METROPT3_URL, cache_zip, force=force)
    checksum = sha256(cache_zip)

    frame = _read_metropt3_csv(cache_zip)
    frame = frame.set_index("timestamp")

    segments: list[pd.DataFrame] = []
    segment_manifest: list[dict] = []
    for segment in FLEET_SEGMENTS:
        start = pd.Timestamp(segment.start)
        end = pd.Timestamp(segment.end)
        cut = frame.loc[(frame.index >= start) & (frame.index < end)].copy()
        rows = len(cut)
        expected = EXPECTED_ROWS[segment.asset_id]
        if rows != expected:
            raise RuntimeError(
                f"segment {segment.asset_id} has {rows} rows, expected {expected}; "
                "the MetroPT-3 upstream CSV may have changed"
            )
        cut["asset_id"] = segment.asset_id
        cut = cut.reset_index().rename(columns={"timestamp": "source_event_time"})
        segments.append(cut)
        segment_manifest.append({**asdict(segment), "rows": rows})

    fleet = pd.concat(segments, ignore_index=True).sort_values(["asset_id", "source_event_time"])
    fleet["source_event_time"] = fleet["source_event_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    ordered_columns = [
        "asset_id",
        "source_event_time",
        "Motor_current",
        "Oil_temperature",
        "TP2",
        "DV_pressure",
        "TP3",
        "H1",
        "Reservoirs",
    ]
    fleet = fleet[ordered_columns]

    if len(fleet) != EXPECTED_TOTAL_ROWS:
        raise RuntimeError(f"fleet has {len(fleet)} rows, expected {EXPECTED_TOTAL_ROWS}")

    out_path = processed_dataset_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fleet.to_parquet(out_path, index=False)

    manifest = {
        "url": METROPT3_URL,
        "sha256": checksum,
        "member": METROPT3_MEMBER,
        "rows_total": len(fleet),
        "segments": segment_manifest,
    }
    manifest_path().write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return out_path


def write_sample(*, limit_minutes: int = 5) -> Path:
    """Write a small, git-tracked JSONL sample: first `limit_minutes` per asset, as v1 events."""
    fleet_path = processed_dataset_path()
    if not fleet_path.exists():
        raise RuntimeError("run prepare_dataset() before write_sample()")

    fleet = pd.read_parquet(fleet_path)
    fleet["source_event_time"] = pd.to_datetime(fleet["source_event_time"])

    rows: list[str] = []
    for _asset_id, group in fleet.groupby("asset_id"):
        group = group.sort_values("source_event_time")
        cutoff = group["source_event_time"].iloc[0] + pd.Timedelta(minutes=limit_minutes)
        window = group[group["source_event_time"] < cutoff]
        for _, row in window.iterrows():
            row_dict = row.to_dict()
            row_dict["source_event_time"] = row_dict["source_event_time"].strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for event in reading_events(row_dict, schema_version=1):
                rows.append(encode_event(event).decode())

    out_path = sample_dataset_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return out_path


def clean() -> None:
    """Remove cached/processed artifacts, keeping `.gitkeep` markers. Windows-portable."""
    root = project_root()
    for directory in (root / "data/cache", root / "data/processed"):
        if not directory.exists():
            continue
        for entry in directory.iterdir():
            if entry.name == ".gitkeep":
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download even if cached")
    parser.add_argument("--sample-only", action="store_true", help="only regenerate the sample")
    parser.add_argument(
        "--clean", action="store_true", help="delete cached/processed data and exit"
    )
    args = parser.parse_args()

    if args.clean:
        clean()
        print(json.dumps({"cleaned": True}))
        return

    if not args.sample_only:
        prepare_dataset(force=args.force)
    sample_path = write_sample()
    print(json.dumps({"sample": str(sample_path)}))


if __name__ == "__main__":
    main()
