import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import pandas as pd

    from apu_streaming.config import Settings, serving_db_path
    from apu_streaming.consumer import AggregateStore, build_consumer, poll_into_store
    from apu_streaming.serving import fetch_health_indicators, open_connection

    return (
        AggregateStore,
        Settings,
        alt,
        build_consumer,
        fetch_health_indicators,
        mo,
        open_connection,
        pd,
        poll_into_store,
        serving_db_path,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tablero APU · mantenimiento predictivo

    Este tablero **solo lee**. La materialización real de `features.asset`
    corre en el proceso `materializer` (`apu_streaming.consumer`), que escribe
    en `data/serving.db` de forma idempotente por `aggregate_id`. Este notebook
    consume el mismo tópico con su propio grupo de consumidores (para no robar
    offsets al materializador) y muestra una vista viva de panes, más el
    estado ya materializado.
    """)
    return


@app.cell
def _(AggregateStore, Settings, build_consumer):
    dashboard_settings = Settings.from_env()
    aggregate_store = AggregateStore()
    aggregate_consumer = build_consumer(dashboard_settings, group_id="apu-dashboard-v1")
    return aggregate_consumer, aggregate_store, dashboard_settings


@app.cell
def _(mo):
    refresh_stream = mo.ui.refresh(options=["1s", "2s", "5s", "10s"], default_interval="2s")
    max_messages = mo.ui.slider(10, 2000, value=500, step=10, label="Mensajes por poll")
    mo.hstack([refresh_stream, max_messages], justify="start")
    return max_messages, refresh_stream


@app.cell
def _(aggregate_consumer, aggregate_store, max_messages, poll_into_store, refresh_stream):
    refresh_stream.value
    poll_result = poll_into_store(
        aggregate_consumer,
        aggregate_store,
        max_messages=int(max_messages.value),
        timeout_seconds=0.2,
    )
    signal_frame = aggregate_store.frame("apu_signal_stats")
    health_frame = aggregate_store.health_frame()
    return health_frame, poll_result, signal_frame


@app.cell
def _(mo, poll_result):
    mo.hstack(
        [
            mo.stat(poll_result["messages_seen"], label="Mensajes leídos"),
            mo.stat(poll_result["aggregates"], label="Agregados vivos"),
            mo.stat(poll_result["signal_stats"], label="apu_signal_stats"),
            mo.stat(poll_result["health_indicators"], label="apu_health_indicator"),
            mo.stat(poll_result["errors"], label="Errores del poll"),
        ],
        widths="equal",
    )
    return


@app.cell
def _(health_frame, mo):
    if health_frame.empty:
        health_view = mo.callout(
            mo.md("Todavía no hay indicadores de salud. Iniciá el pipeline y el productor."),
            kind="warn",
        )
    else:
        columns = [
            "asset_id",
            "window_start",
            "running_ratio",
            "oil_temperature_mean",
            "air_leak_suspected",
            "pane_index",
            "pane_timing",
        ]
        health_view = mo.ui.table(
            health_frame[columns].sort_values(["asset_id", "window_start"]),
            selection=None,
            pagination=True,
        )
    mo.vstack([mo.md("## (a) Indicador de salud por activo"), health_view])
    return


@app.cell
def _(alt, mo, signal_frame):
    motor_signal = (
        signal_frame[signal_frame["stream"] == "motor_current"]
        if not signal_frame.empty
        else signal_frame
    )
    if motor_signal.empty:
        motor_view = mo.md("Esperando lecturas de `motor_current`…")
    else:
        motor_view = (
            alt.Chart(motor_signal)
            .mark_line(point=True)
            .encode(
                x=alt.X("window_start:T", title="Inicio de ventana"),
                y=alt.Y("value_mean:Q", title="Corriente media (A)"),
                color=alt.Color("asset_id:N", title="Activo"),
                tooltip=["asset_id", "window_start:T", "value_mean", "value_min", "value_max"],
            )
            .properties(height=320, title="motor_current promedio por ventana")
        )
    mo.vstack([mo.md("## (b) Serie de motor_current por activo y ventana"), motor_view])
    return


@app.cell
def _(alt, health_frame, mo, signal_frame):
    combined = None
    if not signal_frame.empty or not health_frame.empty:
        import pandas as _pd

        combined = _pd.concat(
            [
                signal_frame[["aggregate_id", "pane_index", "pane_timing", "window_start"]]
                if not signal_frame.empty
                else _pd.DataFrame(),
                health_frame[["aggregate_id", "pane_index", "pane_timing", "window_start"]]
                if not health_frame.empty
                else _pd.DataFrame(),
            ],
            ignore_index=True,
        )
    if combined is None or combined.empty:
        pane_view = mo.md("Esperando revisiones de panes…")
    else:
        multi_pane_ids = combined.groupby("aggregate_id")["pane_index"].nunique()
        multi_pane_ids = multi_pane_ids[multi_pane_ids > 1].index
        pane_subset = combined[combined["aggregate_id"].isin(multi_pane_ids)]
        if pane_subset.empty:
            pane_view = mo.md(
                "Todavía ningún `aggregate_id` acumuló más de un pane. "
                "Una corrección tardía (pane_timing=LATE) aparecerá aquí "
                "cuando el escenario adverso entregue un evento fuera de horizonte."
            )
        else:
            pane_view = mo.ui.table(
                pane_subset.sort_values(["aggregate_id", "pane_index"]),
                selection=None,
                pagination=True,
            )
    mo.vstack([mo.md("## (c) Línea de tiempo de panes por aggregate_id"), pane_view])
    return


@app.cell
def _(mo, poll_result):
    quarantine_note = mo.callout(
        mo.md(
            "Los contadores de `quarantined`, `dropped_by_horizon` y "
            "`duplicates_dropped` viven como métricas de Beam "
            '(`Metrics.counter("apu_streaming", ...)`), visibles en la Flink '
            "Web UI (`http://localhost:8081`) y en `PipelineResult.metrics()`. "
            "Este panel muestra la única señal indirecta disponible desde el "
            "tópico agregado: mensajes leídos vs. agregados materializados."
        ),
        kind="info",
    )
    mo.vstack(
        [
            mo.md("## (d) Cuarentena, tardíos y duplicados descartados"),
            mo.hstack(
                [
                    mo.stat(poll_result["messages_seen"], label="Mensajes leídos"),
                    mo.stat(poll_result["aggregates"], label="Agregados materializados"),
                ],
                widths="equal",
            ),
            quarantine_note,
        ]
    )
    return


@app.cell
def _(mo, signal_frame):
    mo.vstack(
        [
            mo.md("## Registros materializados (vivo)"),
            mo.ui.table(signal_frame.tail(200), selection=None, pagination=True)
            if not signal_frame.empty
            else mo.md("Sin filas todavía."),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
