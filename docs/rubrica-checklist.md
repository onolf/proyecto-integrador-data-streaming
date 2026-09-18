# Checklist contra la rúbrica — Proyecto Integrador Data Streaming

Mapeo de cada criterio de la rúbrica (sección 5 y 6 del enunciado) a la
evidencia concreta dentro de este repositorio. Estado al commit `HEAD` de
`main`.

## Criterio 1 · Caso de uso y arquitectura (10 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Problema relevante y usuarios del resultado | documento técnico §1: monitoreo de flota APU, detección de fuga de aire; usuario = mantenimiento de planta | `docs/apu-streaming.md` |
| Flujo end-to-end fuente → Kafka → Beam → salida | Compose orquesta producer → raw topic → pipeline Beam (Flink) → features.asset → materializer → SQLite → dashboard | `docker-compose.yml` |
| Decisiones ligadas al dominio | Umbrales calibrados sobre MetroPT-3 (`MOTOR_RUNNING_THRESHOLD_A`, `RUNNING_RATIO_ALERT`, `OIL_TEMPERATURE_ALERT_C`) | `src/apu_streaming/config.py` |
| Diagrama de arquitectura | documento técnico §3 con diagrama de componentes | `docs/apu-streaming.md` |

## Criterio 2 · Modelado de eventos y Kafka (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| `event_id` único y estable para dedup | `make_event_id(asset_id, stream, source_event_time)` | `src/apu_streaming/contracts.py` |
| `key` de negocio justificada | Clave `asset_id` (orden por activo, heredado de tarea1) | documento técnico §4; `producer.py` |
| `event_time` separado de `ingestion_time` | Ambos campos en el contrato; lag = ingestion − event | `contracts.py: event_lag_seconds` |
| Payload validado | `decode_event` con razones estables (`invalid_json`, …) | `contracts.py`; `tests/test_contracts.py` |
| Versionado de esquema | `schema_version` con `SCHEMA_VERSION_MAX = 2` | `contracts.py` |
| Tópico de entrada y de salida + laterales | `sensor.readings.raw` → `features.asset` + `quarantine` + `too_late` | `docker-compose.yml` (`kafka-init`) |
| Particiones justificadas | 6 particiones (raw/features), 3 laterales | `docker-compose.yml`; documento técnico §4 |
| Replay reproducible | `ApuReplay` con seed fija, speedup controlable, escenarios `normal` / `adverse` (duplicados 2 %, desorden 5 %, tardíos 2 %, demasiado tardíos 0.5 %) | `src/apu_streaming/producer.py`; `tests/test_producer.py` |

## Criterio 3 · Pipeline Apache Beam (20 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Lectura desde Kafka con KafkaIO | `ReadFromKafka` vía expansion service Java en Flink | `src/apu_streaming/pipeline.py` |
| Validación + separación de inválidos | `ParseAndAdmit` con salidas `quarantine` y `too_late` | `src/apu_streaming/transforms.py`; `tests/test_transforms.py::test_parse_and_admit_routes_to_three_tags` |
| Transformaciones de dominio | `SignalStatsCombineFn`, `HealthIndicatorCombineFn`, `FormatAggregate` | `transforms.py` |
| Agregación incremental por clave (CombineFn) | Ambos combiners incrementales y asociativos | `tests/test_transforms.py::test_signal_stats_combine_fn_is_associative_and_calculates_stddev` |
| Salida a Kafka + sink consumible | `features.asset` → `materializer` → SQLite | `consumer.py`, `serving.py` |
| Runner documentado | Flink 1.19 + Beam job server 2.74; DirectRunner en tests/smoke | `docker-compose.yml`; documento técnico §3 |
| Oráculo independiente | `summarize_readings` replica el DAG sin Beam | `src/apu_streaming/oracle.py`; `tests/test_oracle.py` |

## Criterio 4 · Tiempo de evento y ventanas (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Timestamps desde `event_time` del dominio | `assign_event_timestamp` | `transforms.py` |
| Ventana fija justificada | 5 min (300 s), alineada a epoch | documento técnico §5 (política temporal, continuidad de tarea2) |
| Watermark + allowed lateness | `allowed_lateness_seconds = 720`; trigger AfterWatermark con early firings y late por count en producción | `transforms.py::_windowed` |
| Política de tardíos probada con TestStream | Eventos on-time cierran por watermark; tardío dentro de lateness entra al window correcto; ventanas adyacentes no se mezclan | `tests/test_windows_teststream.py` (4 tests) |
| Panes y modo de acumulación | `ACCUMULATING`, metadata `pane_index/pane_timing/is_first/is_last` en cada agregado | `transforms.py::FormatAggregate` |
| Horizonte upstream | Lag ingestion−event > allowed_lateness → topic `too_late` | `transforms.py::ParseAndAdmit`; test dedicado en `test_windows_teststream.py` |

## Criterio 5 · Confiabilidad y corrección (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Detección de duplicados con horizonte | `DeduplicateReadings`: estado por `(asset_id, ventana)`, timer de expiración por watermark en `window.end + allowed_lateness` | `transforms.py`; `tests/test_dedup_state.py` (3 tests) |
| Claves estables en la salida para upsert | `aggregate_id = metric\|asset\|window_start` | `transforms.py::FormatAggregate` |
| Sink idempotente | Upsert monotone por `pane_index`; pane viejo nunca retrocede valor | `src/apu_streaming/serving.py`; `tests/test_serving.py` |
| Materialización del pane más reciente | `AggregateStore` aplica la misma regla en memoria | `consumer.py`; `tests/test_consumer.py` |
| Semántica declarada sin sobreprometer | At-least-once en tramo Kafka→Beam; efectivamente-once en la salida por dedup + sink idempotente; límites explícitos | documento técnico §6 |

## Criterio 6 · Pruebas y evidencia E2E (15 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| Pruebas unitarias de transformación/agregación | combiners, encoders, contrato, oráculo | 50 tests verdes en `uv run pytest` |
| Prueba de ventanas con TestStream | `tests/test_windows_teststream.py` | ✔ |
| Escenario con duplicado + evidencia | `test_duplicate_event_id_is_dropped_within_the_same_window` + escenario `adverse` del productor | ✔ |
| Escenario con tardío/desordenado + evidencia | `test_late_event_within_lateness_…`, `test_parse_and_admit_rejects_beyond_horizon_…` | ✔ |
| Smoke test fuente → … → salida | Doble: `scripts/smoke_offline.py` (DirectRunner vs oráculo, 45 ventanas, sink idempotente) y `scripts/smoke.py` (compose: tópicos efímeros, escenario adverse, 5 aserciones adversas) | offline ✔ (`"ok": true`); compose pendiente de corrida con Docker |
| Demostración con Docker | `make run` + `make smoke` (perfil compose) + dashboard en :2718 + `scripts/check_health.py` | Requiere Docker Desktop corriendo |
| Observabilidad | Métricas Beam (`admitted`, `quarantined`, `duplicates_dropped`, `dropped_by_horizon`) + `scripts/check_health.py` (separación de flota sobre serving.db) + logs por servicio | `transforms.py`, `scripts/check_health.py` |

## Criterio 7 · Documentación y presentación (10 %)

| Requisito | Evidencia | Estado |
| --- | --- | --- |
| README reproducible | Quickstart, arranque detallado, topología de topics, escenarios, operación, troubleshooting | `README.md` |
| Documento técnico breve | documento técnico: problema, usuarios, arquitectura, contrato, política temporal, garantías, límites | `docs/apu-streaming.md` |
| Comandos de inicio/prueba/demo/parada | `make install / dataset / run / smoke / test / check / stop / logs` | `Makefile` |
| Datos de ejemplo en el repo | `data/sample/apu_fleet_sample.jsonl` + manifest | `data/` |
| Integrantes y contribuciones | documento técnico §final | `docs/apu-streaming.md` |

## Verificación reproducible (lo que un externo debe poder correr)

```bash
uv sync                                     # 1. entorno
uv run pytest -q                            # 2. 50 tests verdes
uv run ruff check .                         # 3. lint limpio
docker compose config --quiet               # 4. compose válido
uv run marimo check --strict dashboard_notebook.py   # 5. dashboard válido
uv run python scripts/smoke_offline.py      # 6. smoke offline → "ok": true
docker compose up --build                   # 7. stack completo (requiere Docker)
uv run python scripts/check_health.py       # 8. separación de flota (post compose)
```

## Brechas conocidas / límites declarados

- La prueba de descarte por watermark estricto (> allowed_lateness) se valida
  upstream en `ParseAndAdmit`; el DirectRunner no descarta elementos tardíos a
  nivel de ventana (limitación documentada del runner, no del pipeline). En
  Flink la ventana sí expira según `allowed_lateness`.
- `is_last = 1` nunca llega a materializarse con el trigger configurado
  (`AfterWatermark` + refiring tardío): el verificador de salida lee la fila
  almacenada, que por la regla de upsert idempotente ya es el pane más reciente de cada `aggregate_id`.
- El smoke adverso de compose (`scripts/smoke.py`, tópicos efímeros, 5
  aserciones) está implementado pero no se ejecutó en esta sesión: requiere el
  stack Docker completo. El smoke offline (`scripts/smoke_offline.py`) sí se
  corre y pasa.
- El recorrido completo sobre Docker (`docker compose up --build` + pipeline
  en Flink + verificador) no se ejecutó en esta sesión; la separación de
  flota sí quedó probada sobre DirectRunner con el dataset completo
  (34.808 lecturas → 1.440 agregados; veredicto OK).

## Verificación de descriptores (logro destacado)

| Descriptor exigido (rúbrica §6) | Evidencia en este repo |
| --- | --- |
| 1: decisiones ligadas al dominio, diagrama claro | documento técnico §1 (maint. anticipado, usuario=planta), umbrales calibrados sobre tramos medidos; diagrama mermaid consistente con compose |
| 2: contrato versionado, claves/particiones por orden/paralelismo/skew | `contracts.py` (v1/v2, rechazo por motivo estable); documento técnico §4 analiza orden por `asset_id`, 6 particiones vs paralelismo 2 y el skew del tramo apu-06 |
| 3: pipeline modular, salidas laterales, combinadores incrementales | `transforms.py`: ParseAndAdmit (3 salidas), 2 CombineFn incrementales asociativos probados |
| 4: política temporal completa y probada | E1–E9 con `TestStream` + `streaming=True`: panes EARLY/ON_TIME/LATE con pane_index monótono y corrección por E6 |
| 5: dedup con horizonte explícito, sink idempotente, sin sobreprometer | `DeduplicateReadings` con timer `window.end + lateness` (test de expiración); upsert monótono; documento técnico §7 declara no-exactly-once |
| 6: cobertura de lógica/ventanas/fallos, escenarios reproducibles, evidencia E2E verificable | 52 tests; productor determinista por seed; dos smokes; verificador de flota con caso negativo probado |
| 7: documentación precisa, operación de un comando, contribuciones | README completo (quickstart, puertos, inspección, troubleshooting, atribución); integrantes aquí y en documento técnico §final |

## Prueba de reproducibilidad externa

Pendiente de registro tras la corrida con Docker (pasos 7–8 de la sección
anterior). El protocolo queda fijado acá: clonar en directorio limpio, seguir
solo el README, registrar el resultado en esta tabla:

| Paso | Comando/README | Resultado registrado | Fecha |
| --- | --- | --- | --- |
| Clonar + levantar | `git clone … && docker compose up --build` | pendiente | — |
| Verificar salida | `uv run python scripts/check_health.py` | pendiente | — |
| Smoke adverso | `make smoke` | pendiente | — |
