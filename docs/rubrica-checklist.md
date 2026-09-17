# Checklist contra la rúbrica — Proyecto Integrador Data Streaming

Mapeo de cada criterio de la rúbrica (sección 5 y 6 del enunciado) a la
evidencia concreta dentro de este repositorio. Estado al commit `HEAD` de
`main`.

## Criterio 1 · Caso de uso y arquitectura (10 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Problema relevante y usuarios del resultado | RFC §1: monitoreo de flota APU, detección de fuga de aire; usuario = mantenimiento de planta | `docs/rfc-001-apu-streaming.md` |
| Flujo end-to-end fuente → Kafka → Beam → salida | Compose orquesta producer → raw topic → pipeline Beam (Flink) → features.asset → materializer → SQLite → dashboard | `docker-compose.yml` |
| Decisiones ligadas al dominio | Umbrales calibrados sobre MetroPT-3 (`MOTOR_RUNNING_THRESHOLD_A`, `RUNNING_RATIO_ALERT`, `OIL_TEMPERATURE_ALERT_C`) | `src/apu_streaming/config.py` |
| Diagrama de arquitectura | RFC §3 con diagrama de componentes | `docs/rfc-001-apu-streaming.md` |

## Criterio 2 · Modelado de eventos y Kafka (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| `event_id` único y estable para dedup | `make_event_id(asset_id, stream, source_event_time)` | `src/apu_streaming/contracts.py` |
| `key` de negocio justificada | Clave `asset_id` (orden por activo, heredado de tarea1) | RFC §4; `producer.py` |
| `event_time` separado de `ingestion_time` | Ambos campos en el contrato; lag = ingestion − event | `contracts.py: event_lag_seconds` |
| Payload validado | `decode_event` con razones estables (`invalid_json`, …) | `contracts.py`; `tests/test_contracts.py` |
| Versionado de esquema | `schema_version` con `SCHEMA_VERSION_MAX = 2` | `contracts.py` |
| Tópico de entrada y de salida + laterales | `sensor.readings.raw` → `features.asset` + `quarantine` + `too_late` | `docker-compose.yml` (`kafka-init`) |
| Particiones justificadas | 6 particiones (raw/features), 3 laterales | `docker-compose.yml`; RFC §4 |
| Replay reproducible | `ApuReplay` con seed fija, speedup controlable, escenarios `normal` / `adverse` (duplicados 2 %, desorden 5 %, tardíos 2 %, demasiado tardíos 0.5 %) | `src/apu_streaming/producer.py`; `tests/test_producer.py` |

## Criterio 3 · Pipeline Apache Beam (20 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Lectura desde Kafka con KafkaIO | `ReadFromKafka` vía expansion service Java en Flink | `src/apu_streaming/pipeline.py` |
| Validación + separación de inválidos | `ParseAndAdmit` con salidas `quarantine` y `too_late` | `src/apu_streaming/transforms.py`; `tests/test_transforms.py::test_parse_and_admit_routes_to_three_tags` |
| Transformaciones de dominio | `SignalStatsCombineFn`, `HealthIndicatorCombineFn`, `FormatAggregate` | `transforms.py` |
| Agregación incremental por clave (CombineFn) | Ambos combiners incrementales y asociativos | `tests/test_transforms.py::test_signal_stats_combine_fn_is_associative_and_calculates_stddev` |
| Salida a Kafka + sink consumible | `features.asset` → `materializer` → SQLite | `consumer.py`, `serving.py` |
| Runner documentado | Flink 1.19 + Beam job server 2.74; DirectRunner en tests/smoke | `docker-compose.yml`; RFC §3 |
| Oráculo independiente | `summarize_readings` replica el DAG sin Beam | `src/apu_streaming/oracle.py`; `tests/test_oracle.py` |

## Criterio 4 · Tiempo de evento y ventanas (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Timestamps desde `event_time` del dominio | `assign_event_timestamp` | `transforms.py` |
| Ventana fija justificada | 5 min (300 s), alineada a epoch | RFC §5 (política temporal, continuidad de tarea2) |
| Watermark + allowed lateness | `allowed_lateness_seconds = 720`; trigger AfterWatermark con early firings y late por count en producción | `transforms.py::_windowed` |
| Política de tardíos probada con TestStream | Eventos on-time cierran por watermark; tardío dentro de lateness entra al window correcto; ventanas adyacentes no se mezclan | `tests/test_windows_teststream.py` (4 tests) |
| Panes y modo de acumulación | `ACCUMULATING`, metadata `pane_index/pane_timing/is_first/is_last` en cada agregado | `transforms.py::FormatAggregate` |
| Horizonte upstream | Lag ingestion−event > allowed_lateness → topic `too_late` (RN-05) | `transforms.py::ParseAndAdmit`; test dedicado en `test_windows_teststream.py` |

## Criterio 5 · Confiabilidad y corrección (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Detección de duplicados con horizonte | `DeduplicateReadings`: estado por `(asset_id, ventana)`, timer de expiración por watermark en `window.end + allowed_lateness` | `transforms.py`; `tests/test_dedup_state.py` (3 tests) |
| Claves estables en la salida para upsert | `aggregate_id = metric\|asset\|window_start` | `transforms.py::FormatAggregate` |
| Sink idempotente | RN-06: upsert monotone por `pane_index`; pane viejo nunca retrocede valor | `src/apu_streaming/serving.py`; `tests/test_serving.py` |
| Materialización del pane más reciente | `AggregateStore` aplica la misma regla en memoria | `consumer.py`; `tests/test_consumer.py` |
| Semántica declarada sin sobreprometer | At-least-once en tramo Kafka→Beam; efectivamente-once en la salida por dedup + sink idempotente; límites explícitos | RFC §6 |

## Criterio 6 · Pruebas y evidencia E2E (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Pruebas unitarias de transformación/agregación | combiners, encoders, contrato, oráculo | 50 tests verdes en `uv run pytest` |
| Prueba de ventanas con TestStream | `tests/test_windows_teststream.py` | ✔ |
| Escenario con duplicado + evidencia | `test_duplicate_event_id_is_dropped_within_the_same_window` + escenario `adverse` del productor | ✔ |
| Escenario con tardío/desordenado + evidencia | `test_late_event_within_lateness_…`, `test_parse_and_admit_rejects_beyond_horizon_…` | ✔ |
| Smoke test fuente → … → salida | `scripts/smoke.py`: sample real → ParseAndAdmit → Beam (DirectRunner) vs oráculo (45 ventanas) → sink idempotente (doble aplicación, conteo estable) | ✔ (`"ok": true`) |
| Demostración con Docker | `make run` + `make smoke` (perfil compose) + dashboard en :2718 | Requiere Docker Desktop corriendo |
| Observabilidad | Métricas Beam (`admitted`, `quarantined`, `duplicates_dropped`, `dropped_by_horizon`) + `scripts/check_health.py` (Kafka + frescura de serving.db) + logs por servicio | `transforms.py`, `scripts/check_health.py` |

## Criterio 7 · Documentación y presentación (10 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| README reproducible | Quickstart, arranque detallado, topología de topics, escenarios, operación, troubleshooting | `README.md` |
| Documento técnico breve | RFC-001: problema, usuarios, arquitectura, contrato, política temporal, garantías, límites | `docs/rfc-001-apu-streaming.md` |
| Comandos de inicio/prueba/demo/parada | `make install / dataset / run / smoke / test / check / stop / logs` | `Makefile` |
| Datos de ejemplo en el repo | `data/sample/apu_fleet_sample.jsonl` + manifest | `data/` |
| Integrantes y contribuciones | RFC §final | `docs/rfc-001-apu-streaming.md` |

## Verificación reproducible (lo que un externo debe poder correr)

```bash
uv sync                                     # 1. entorno
uv run pytest -q                            # 2. 50 tests verdes
uv run ruff check .                         # 3. lint limpio
docker compose config --quiet               # 4. compose válido
uv run marimo check --strict dashboard_notebook.py   # 5. dashboard válido
uv run python scripts/smoke.py              # 6. smoke offline → "ok": true
docker compose up --build                   # 7. stack completo (requiere Docker)
uv run python scripts/check_health.py       # 8. probe Kafka + serving.db
```

## Brechas conocidas / límites declarados

- La prueba de descarte por watermark estricto (> allowed_lateness) se valida
  upstream en `ParseAndAdmit`; el DirectRunner no descarta elementos tardíos a
  nivel de ventana (limitación documentada del runner, no del pipeline). En
  Flink la ventana sí expira según `allowed_lateness`.
- El smoke de compose (`make smoke`) requiere Docker Desktop; el smoke local
  (`scripts/smoke.py`) cubre el recorrido sin el broker.
