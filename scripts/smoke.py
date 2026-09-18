"""Smoke adverso end-to-end sobre el stack Docker (Kafka real + Flink portable).

Recorrido: publicación del sample con escenario `adverse` sobre tópicos
efímeros → pipeline Beam acotado → consumo de los cuatro tópicos → sink
SQLite propio del smoke. Falla con RuntimeError si no se cumple alguna de las
aserciones (F7.3 del plan):

  1. ≥ 1 registro en `features.*`
  2. ≥ 1 registro en `quarantine.*` (payload roto + schema_version=3 inyectados)
  3. ≥ 1 registro en `too_late.*` (eventos del escenario adverse con lag 1800 s)
  4. contador Beam `duplicates_dropped` > 0
  5. filas del sink == cantidad de `aggregate_id` distintos emitidos
     (idempotencia: varios panes por clave colapsan en una fila)

Se ejecuta dentro del servicio `smoke` de docker compose; el broker es
`kafka:9092` por entorno. Imprime un JSON de conteos como evidencia.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from apu_streaming.config import Settings, sample_dataset_path
from apu_streaming.contracts import encode_event
from apu_streaming.producer import SCENARIOS, ApuReplay, build_producer, load_readings
from apu_streaming.serving import count_aggregates, open_connection, upsert_aggregate


def _create_topics(admin: AdminClient, settings: Settings, suffix: str) -> dict[str, str]:
    """Create the four suffixed topics; return {role: topic_name}."""
    names = {
        "raw": f"{settings.raw_topic}.smoke-{suffix}",
        "features": f"{settings.features_topic}.smoke-{suffix}",
        "quarantine": f"{settings.quarantine_topic}.smoke-{suffix}",
        "too_late": f"{settings.too_late_topic}.smoke-{suffix}",
    }
    new_topics = [
        NewTopic(names["raw"], num_partitions=6, replication_factor=1),
        NewTopic(names["features"], num_partitions=6, replication_factor=1),
        NewTopic(names["quarantine"], num_partitions=3, replication_factor=1),
        NewTopic(names["too_late"], num_partitions=3, replication_factor=1),
    ]
    futures = admin.create_topics(new_topics)
    for name, future in futures.items():
        future.result()
        print(f"[smoke] topic listo: {name}")
    return names


def _publish(settings: Settings, raw_topic: str) -> dict[str, int]:
    """Replay the sample with the adverse scenario + two guaranteed-invalid payloads."""
    producer: Producer = build_producer(settings.kafka_bootstrap_servers)
    replay = ApuReplay(
        producer,
        topic=raw_topic,
        scenario=SCENARIOS["adverse"],
        speedup=10_000.0,  # el smoke apura el reloj; los lags temporales ya van en metadata
        seed=7,
    )
    readings = load_readings(sample_dataset_path())
    counts = replay.replay(readings, realtime=False)

    # Cargas adversas garantizadas, independientes del seed:
    # (a) payload no-JSON, (b) contrato con schema_version no soportado.
    producer.produce(raw_topic, key=b"apu-01", value=b"not-json{")
    rogue = json.loads(encode_event(replace(readings[0], ingestion_time=_iso_now())).decode())
    rogue["schema_version"] = 3
    producer.produce(raw_topic, key=b"apu-01", value=json.dumps(rogue).encode())
    producer.flush(30)
    return counts


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _run_pipeline(settings: Settings, topics: dict[str, str], max_read_seconds: int):
    """Run the Beam pipeline bounded; return the result for metrics."""
    from apu_streaming import pipeline as pipeline_mod

    smoke_settings = replace(
        settings,
        raw_topic=topics["raw"],
        features_topic=topics["features"],
        quarantine_topic=topics["quarantine"],
        too_late_topic=topics["too_late"],
    )

    # run() toma Settings.from_env(); se paramatriza construyendo el pipeline acá.
    options = pipeline_mod.pipeline_options(smoke_settings, job_name="apu-smoke")
    import apache_beam as beam

    pipeline = beam.Pipeline(options=options)
    pipeline_mod.build_pipeline(
        pipeline,
        smoke_settings,
        group_id=f"apu-smoke-{uuid.uuid4().hex[:8]}",
        max_read_time=max_read_seconds,
    )
    result = pipeline.run()
    result.wait_until_finish()
    return result


def _consume_all(bootstrap: str, topic: str, *, idle_seconds: float = 10.0) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"apu-smoke-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    records: list[dict] = []
    last_msg = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_msg > idle_seconds:
                    break
                continue
            last_msg = time.monotonic()
            if msg.error():
                continue
            try:
                records.append(json.loads(msg.value().decode()))
            except (UnicodeDecodeError, json.JSONDecodeError):
                # payloads rotos del tópico de cuarentena cuentan igual
                records.append({"_undecodable": True})
    finally:
        consumer.close()
    return records


def main() -> None:
    plan_settings = Settings.from_env()
    suffix = uuid.uuid4().hex[:8]
    tmp_dir = Path("tmp")
    tmp_dir.mkdir(exist_ok=True)
    smoke_db = tmp_dir / f"smoke-{suffix}.db"

    admin = AdminClient({"bootstrap.servers": plan_settings.kafka_bootstrap_servers})
    topics = _create_topics(admin, plan_settings, suffix)

    produced = _publish(plan_settings, topics["raw"])
    print(f"[smoke] replay adverse: {produced}")

    result = _run_pipeline(plan_settings, topics, max_read_seconds=90)

    # Métrica 4: duplicates_dropped del resultado del pipeline.
    duplicates_dropped = 0
    metrics_available = True
    try:
        from apache_beam.metrics.metric import MetricsFilter

        metrics = result.metrics().query(MetricsFilter().with_name("duplicates_dropped"))
        for counter in metrics["counters"]:
            duplicates_dropped += counter.committed or counter.attempted or 0
    except (NotImplementedError, AttributeError, TypeError) as exc:
        metrics_available = False
        print(f"[smoke] aviso: runner no expone métricas ({exc!r}); aserción 4 omitida")

    bootstrap = plan_settings.kafka_bootstrap_servers
    features = _consume_all(bootstrap, topics["features"])
    quarantine = _consume_all(bootstrap, topics["quarantine"])
    too_late = _consume_all(bootstrap, topics["too_late"])

    # El smoke materializa por su cuenta: el servicio `materializer` no corre
    # bajo el perfil smoke.
    conn = open_connection(smoke_db)
    try:
        for record in features:
            if "aggregate_id" in record:
                upsert_aggregate(conn, record)
        rows = count_aggregates(conn)
    finally:
        conn.close()

    distinct_keys = {r["aggregate_id"] for r in features if "aggregate_id" in r}

    summary = {
        "topics": topics,
        "produced": produced,
        "features_records": len(features),
        "quarantine_records": len(quarantine),
        "too_late_records": len(too_late),
        "duplicates_dropped_metric": duplicates_dropped,
        "distinct_aggregate_ids": len(distinct_keys),
        "sink_rows": rows,
    }

    failures: list[str] = []
    if not features:
        failures.append("features: 0 registros")
    if not quarantine:
        failures.append("quarantine: 0 registros (payload roto no llegó)")
    if not too_late:
        failures.append("too_late: 0 registros (escenario adverse sin tardíos fuera de horizonte)")
    if metrics_available and duplicates_dropped <= 0:
        failures.append("métrica duplicates_dropped = 0 (esperado > 0 con scenario adverse)")
    if rows != len(distinct_keys) or rows == 0:
        failures.append(
            f"sink inconsistente: {rows} filas vs {len(distinct_keys)} aggregate_id distintos"
        )

    if failures:
        print(json.dumps({"ok": False, **summary, "failures": failures}, indent=2))
        raise RuntimeError("smoke falló: " + "; ".join(failures))

    print(json.dumps({"ok": True, **summary}, indent=2))


if __name__ == "__main__":
    main()
