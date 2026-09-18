"""Configuration shared by notebooks, command-line processes, and tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RAW_TOPIC = "sensor.readings.raw"
FEATURES_TOPIC = "features.asset"
QUARANTINE_TOPIC = "sensor.readings.quarantine"
TOO_LATE_TOPIC = "sensor.readings.too_late"

# Umbrales de dominio calibrados sobre los datos reales de MetroPT-3:
# apu-01/02/03 (sanos) miden running_ratio en 0.270-0.628 y
# oil_temperature_mean en 55.78-66.57 °C; apu-04/05/06 (falla de
# fuga de aire) miden 0.999-1.000 y 75.00-83.64 °C respectivamente.
MOTOR_RUNNING_THRESHOLD_A = 1.0
RUNNING_RATIO_ALERT = 0.95
OIL_TEMPERATURE_ALERT_C = 70.0


@dataclass(frozen=True)
class Settings:
    """Runtime settings with Docker-friendly defaults."""

    kafka_bootstrap_servers: str = "kafka:9092"
    raw_topic: str = RAW_TOPIC
    features_topic: str = FEATURES_TOPIC
    quarantine_topic: str = QUARANTINE_TOPIC
    too_late_topic: str = TOO_LATE_TOPIC
    window_seconds: int = 300
    allowed_lateness_seconds: int = 720
    early_firing_seconds: int = 10
    stale_event_seconds: int = 86400
    parallelism: int = 2
    job_endpoint: str = "beam-job-server:8099"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            raw_topic=os.getenv("KAFKA_RAW_TOPIC", RAW_TOPIC),
            features_topic=os.getenv("KAFKA_FEATURES_TOPIC", FEATURES_TOPIC),
            quarantine_topic=os.getenv("KAFKA_QUARANTINE_TOPIC", QUARANTINE_TOPIC),
            too_late_topic=os.getenv("KAFKA_TOO_LATE_TOPIC", TOO_LATE_TOPIC),
            window_seconds=int(os.getenv("WINDOW_SECONDS", "300")),
            allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "720")),
            early_firing_seconds=int(os.getenv("EARLY_FIRING_SECONDS", "10")),
            stale_event_seconds=int(os.getenv("STALE_EVENT_SECONDS", "86400")),
            parallelism=int(os.getenv("BEAM_PARALLELISM", "2")),
            job_endpoint=os.getenv("BEAM_JOB_ENDPOINT", "beam-job-server:8099"),
        )


def project_root() -> Path:
    """Return the laboratory root both from source and an installed wheel."""
    configured = os.getenv("APU_LAB_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[2]


def processed_dataset_path() -> Path:
    default = project_root() / "data/processed/apu_fleet.parquet"
    return Path(os.getenv("APU_DATASET_PATH", default))


def sample_dataset_path() -> Path:
    default = project_root() / "data/sample/apu_fleet_sample.jsonl"
    return Path(os.getenv("APU_SAMPLE_PATH", default))


def manifest_path() -> Path:
    return project_root() / "data/processed/manifest.json"


def serving_db_path() -> Path:
    default = project_root() / "data/serving.db"
    return Path(os.getenv("APU_SERVING_DB_PATH", default))
