# Proyecto Integrador · Pipeline de streaming APU

Pipeline de streaming end-to-end que detecta fugas de aire en una flota de
compresores APU (Air Production Unit, motores de inducción trifásicos) a partir
del dataset MetroPT-3. Lee lecturas sintetizadas por Kafka, las procesa con
Apache Beam sobre Flink (ventanas fijas de 5 min en tiempo de evento, dedup por
`event_id`, `allowed_lateness` con paneles tardíos) y materializa agregados en
SQLite idempotente que alimenta un dashboard marimo.

La arquitectura, el contrato de eventos, las reglas RN-01…RN-0N y la
justificación de cada decisión viven en
[`docs/rfc-001-apu-streaming.md`](docs/rfc-001-apu-streaming.md).

## Requisitos

- Python 3.12+ con [`uv`](https://docs.astral.sh/uv/)
- Docker Desktop (para el stack completo: Kafka + Flink + Beam job server)

## Quickstart

```bash
make install          # uv sync: crea .venv y fija dependencias de uv.lock
make dataset          # descarga MetroPT-3, sintetiza flota de 6 activos
make smoke            # smoke E2E en docker (perfil smoke) — ver sección abajo
make run              # levanta kafka + flink + pipeline + producer + materializer + dashboard
```

El dashboard queda en <http://localhost:2718> una vez que `materializer` haya
empezado a poblar `data/serving.db`.

## Puertos expuestos

| Puerto | Servicio | Qué muestra |
| --- | --- | --- |
| `2718` | `dashboard` | Dashboard marimo (4 paneles de evidencia) |
| `8081` | `jobmanager` | Flink Web UI: job, checkpoints, métricas del pipeline |
| `8099` | `beam-job-server` | Endpoint de sumisión de jobs Beam (uso interno) |
| `29092` | `kafka` | Broker expuesto al host |

## Arranque detallado (sin make)

```bash
uv sync
uv run python -m apu_streaming.dataset     # una sola vez; usa data/cache
docker compose up --build                  # stack completo
```

Para el smoke **sin Docker** (offline, DirectRunner local):

```bash
uv run python scripts/smoke_offline.py --max-readings 1500
```

Reproduce `data/sample/apu_fleet_sample.jsonl` por `ParseAndAdmit`, corre el
DAG de `build_analytics`, compara contra el oráculo puro
(`apu_streaming.oracle.summarize_readings`) y verifica la idempotencia del sink
aplicando los agregados dos veces. Exit 0 si todo cuadra.

## Topología de topics

| Topic                       | Particiones | Contenido                                   |
| --------------------------- | ----------- | ------------------------------------------- |
| `sensor.readings.raw`       | 6           | Lecturas crudas (productor replay)          |
| `features.asset`            | 6           | Agregados por ventana (sink del pipeline)   |
| `sensor.readings.quarantine`| 3           | Eventos que violan el contrato              |
| `sensor.readings.too_late`  | 3           | Eventos cuyo lag supera `allowed_lateness`  |

Clave: `asset_id` para el raw; `aggregate_id` (`metric|asset|window_start`)
para los agregados. La creación la hace `kafka-init` al arranque.

## Escenarios del productor

`apu_streaming.producer.SCENARIOS`:

- `normal`: replay ordenado, sin duplicados.
- `adverse` (default en compose): 2 % duplicados, 5 % out-of-order (60 s),
  2 % tardíos (300 s) y 0.5 % demasiado tardíos (1800 s) → ejercita dedup,
  panes tardíos y descarte por horizonte.

Overrides por entorno (ver `Settings.from_env`):
`WINDOW_SECONDS`, `ALLOWED_LATENESS_SECONDS`, `EARLY_FIRING_SECONDS`,
`BEAM_PARALLELISM`, `KAFKA_*_TOPIC`, `KAFKA_BOOTSTRAP_SERVERS`.

## Operación

### Smoke checks

```bash
# offline, sin Docker: DirectRunner vs oráculo + sink idempotente
uv run python scripts/smoke_offline.py --max-readings 600

# adverso, sobre el stack real: `make smoke` levanta el perfil smoke
docker compose --profile smoke up --build --abort-on-container-exit --exit-code-from smoke smoke
```

`scripts/smoke.py` (compose) crea tópicos efímeros con sufijo uuid, publica el
sample con el escenario `adverse` más payloads inválidos, corre el pipeline
acotado y verifica: features ≥ 1, cuarentena ≥ 1, too_late ≥ 1,
`duplicates_dropped > 0`, y que las filas materializadas igualan los
`aggregate_id` distintos (idempotencia).

### Verificador de salida (separación de flota)

```bash
uv run python scripts/check_health.py                      # lee data/serving.db
uv run python scripts/check_health.py --db-path otra.db    # ruta alternativa
```

Lee las filas `apu_health_indicator` materializadas (cada fila ya es el pane
más reciente por upsert) e imprime la tabla
`asset_id / running_ratio / oil_temperature_mean / air_leak_suspected`. Sale
con código 1 si `apu-04/05/06` no quedan en `true` y `apu-01/02/03` en
`false`, o si los seis activos caen del mismo lado del umbral — es la prueba
de que umbral, agregación y ventanas funcionan end-to-end.

### Dashboard

```bash
docker compose up dashboard           # stack ya levantado
uv run marimo run dashboard_notebook.py   # edición local (sin stack)
uv run marimo check --strict dashboard_notebook.py   # gate de formato
```

Paneles: indicador de salud por activo, serie de `motor_current`, timeline de
panes (early/on-time/late), contadores de cuarentena y duplicados.

## Tests y validación

```bash
make test            # uv run pytest  (50 tests: contrato, oráculo, productor,
                     # transforms, ventanas TestStream, dedup, sink, consumer)
make check           # uv run ruff check .
docker compose config --quiet   # valida el compose sin levantarlo
```

Verificación de paridad Beam↔oráculo cubierta en `tests/test_transforms.py` y
en `scripts/smoke_offline.py`; comportamiento temporal (watermark, secuencia
E1–E9 con panes EARLY/ON_TIME/LATE, ventana adyacente) en
`tests/test_windows_teststream.py`; semántica de estado del dedup y expiración
por timer en `tests/test_dedup_state.py`; idempotencia RN-06 del sink en
`tests/test_serving.py`.

### Inspección de tópicos (con el stack levantado)

```bash
# cuarentena: eventos que violan el contrato
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic sensor.readings.quarantine \
  --from-beginning --timeout-ms 10000

# too_late: eventos rechazados por el horizonte upstream
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic sensor.readings.too_late \
  --from-beginning --timeout-ms 10000

# features.asset: aggregate_id repetido con pane_index creciente = corrección
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic features.asset \
  --from-beginning --timeout-ms 10000 --property print.key=true
```

En la Flink Web UI (`http://localhost:8081`) los contadores `admitted`,
`quarantined`, `dropped_by_horizon` y `duplicates_dropped` deben aparecer en
las métricas del job.

## Estructura

```
src/apu_streaming/
  config.py       Settings + rutas (env-overridable)
  contracts.py    Esquema del evento, codec, validación, ContractError
  oracle.py       Réplica pura del DAG de Beam (ground truth)
  dataset.py      Descarga MetroPT-3, sintetiza flota, manifest, sample
  producer.py     Replay a Kafka con escenarios normal/adverse
  transforms.py   ParseAndAdmit, DeduplicateReadings, combiners, FormatAggregate
  pipeline.py     Wiring Kafka→Beam→Kafka (Flink job server)
  consumer.py     materializer: features.asset → serving.db (+ AggregateStore)
  serving.py      Sink SQLite idempotente (upsert monotone por pane_index)
scripts/
  smoke_offline.py E2E offline: sample → Beam vs oráculo → sink
  smoke.py        E2E adverso en compose: tópicos efímeros, 5 aserciones
  check_health.py Verificador de la salida: separación de flota en serving.db
dashboard_notebook.py   Dashboard marimo de 4 paneles
docker-compose.yml      kafka, kafka-init, flink (jm/tm), beam-job-server,
                        pipeline, producer, materializer, dashboard, smoke
docs/rfc-001-apu-streaming.md   RFC con decisiones RN-xx
```

## Decisiones clave (resumen; detalle en la RFC)

- **Ventana fija de 5 min en event-time** con watermark heurístico y
  `allowed_lateness = 720 s`; paneles tardíos en modo ACCUMULATING.
- **Dedup por `event_id`** con estado scoped a `(asset_id, ventana)` y timer de
  expiración por watermark (`window.end + allowed_lateness`).
- **Horizonte upstream** en `ParseAndAdmit`: lag `ingestion−event` mayor que
  `allowed_lateness` se deriva a `sensor.readings.too_late`, nunca toca el
  aggregate (RN-05).
- **Sink idempotente RN-06**: upsert por `aggregate_id` que solo aplica panes
  con `pane_index >=` al almacenado — re-entregas y reordenamientos no hacen
  retroceder valores corregidos.
- **Clave Kafka `asset_id`**: garantiza orden por activo (justificado en
  `tarea1.pdf`).

## Troubleshooting

- `make run` se queda sin RAM: el stack pide ~6 GB; bajar `BEAM_PARALLELISM=1`
  y replicas de `taskmanager` en `docker-compose.yml`.
- Dashboard vacío: revisar `docker compose logs materializer` — el dashboard
  solo lee `data/serving.db`; el `materializer` es quien lo pobla.
- Kafka no arranca tras hibernación en Windows: `docker compose down -v` y
  levantar de nuevo (volumen `kafka-data` queda con offsets viejos).

## Datos y atribución

Dataset: **MetroPT-3**, UCI Machine Learning Repository id 791, DOI
`10.24432/C5VW3R`, licencia **CC BY 4.0** — Davari, Veloso, Ribeiro & Gama
(2021). El repo versiona solo una muestra derivada
(`data/sample/apu_fleet_sample.jsonl`, ver `data/sample/ATRIBUCION.md`); el
ZIP completo se descarga con `make dataset`.

Los 6 activos (`apu-01..06`) son una **síntesis explícita** de un solo APU
físico mediante tramos temporales disjuntos — declarado en el RFC §1, no se
presenta como flota real.

## Integrantes y contribuciones

Equipo individual — **Odilón Nolf Sánchez** (`onolf@outlook.com`):

- Continuidad de `tarea1.pdf`/`tarea2.pdf` (arquitectura Kafka y política
  temporal diseñadas previamente).
- Diseño del contrato de eventos y calibración de umbrales sobre MetroPT-3.
- Implementación completa: síntesis de flota, productor de replay, pipeline
  Beam, sink idempotente, dashboard, pruebas (incluida E1–E9 con TestStream),
  smokes offline/compose, documentación.

