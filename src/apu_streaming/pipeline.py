"""Kafka-to-Kafka Apache Beam streaming pipeline executed by Flink."""

from __future__ import annotations

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from apu_streaming.config import Settings
from apu_streaming.transforms import (
    ParseAndAdmit,
    aggregate_to_kafka_record,
    build_analytics,
    lateral_to_kafka_record,
)


def java_kafka_expansion_service():
    """Run Java KafkaIO stages as processes inside Flink TaskManagers."""
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
        ]
    )


def pipeline_options(settings: Settings, *, job_name: str) -> PipelineOptions:
    return PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={settings.job_endpoint}",
            "--environment_type=PROCESS",
            '--environment_config={"command":"/opt/fpuna-lab/python-sdk/boot"}',
            "--streaming",
            f"--parallelism={settings.parallelism}",
            f"--job_name={job_name}",
            "--experiments=use_sdf_read",
        ]
    )


def build_pipeline(
    pipeline: beam.Pipeline,
    settings: Settings,
    *,
    group_id: str,
    max_num_records: int | None = None,
    max_read_time: int | None = None,
    expansion_service=None,
):
    """Wire Kafka sources, ParseAndAdmit, analytics, and all Kafka sinks."""
    if expansion_service is None:
        expansion_service = java_kafka_expansion_service()

    read_kwargs = {}
    if max_num_records is not None:
        read_kwargs["max_num_records"] = max_num_records
    if max_read_time is not None:
        read_kwargs["max_read_time"] = max_read_time

    raw = pipeline | "Read APU events from Kafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[settings.raw_topic],
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=expansion_service,
        **read_kwargs,
    )

    parsed = raw | "Parse and admit events" >> beam.ParDo(ParseAndAdmit(settings)).with_outputs(
        ParseAndAdmit.QUARANTINE, ParseAndAdmit.TOO_LATE, main="valid"
    )

    aggregates = build_analytics(parsed.valid, settings)

    (
        aggregates
        | "Encode aggregate records"
        >> beam.Map(aggregate_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Write aggregates to Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.features_topic,
            expansion_service=expansion_service,
        )
    )

    (
        parsed.quarantine
        | "Encode quarantine records"
        >> beam.Map(lateral_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Write quarantine to Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.quarantine_topic,
            expansion_service=expansion_service,
        )
    )

    (
        parsed.too_late
        | "Encode too late records"
        >> beam.Map(lateral_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Write too late to Kafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.too_late_topic,
            expansion_service=expansion_service,
        )
    )

    return aggregates, parsed.quarantine, parsed.too_late


def run(
    *,
    group_id: str = "beam-apu-analytics-v1",
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    settings = Settings.from_env()
    options = pipeline_options(
        settings,
        job_name=os.getenv("BEAM_JOB_NAME", "apu-analytics"),
    )
    pipeline = beam.Pipeline(options=options)
    build_pipeline(
        pipeline,
        settings,
        group_id=group_id,
        max_num_records=max_num_records,
        max_read_time=max_read_time,
    )
    result = pipeline.run()
    result.wait_until_finish()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--group-id",
        default="beam-apu-analytics-v1",
        help="Kafka consumer group id",
    )
    parser.add_argument(
        "--max-num-records",
        type=int,
        default=None,
        help="bounded read limit for smoke testing",
    )
    parser.add_argument(
        "--max-read-time",
        type=int,
        default=None,
        help="max execution time in seconds",
    )
    args = parser.parse_args()

    result = run(
        group_id=args.group_id,
        max_num_records=args.max_num_records,
        max_read_time=args.max_read_time,
    )
    print(json.dumps({"state": str(result.state)}))


if __name__ == "__main__":
    main()
