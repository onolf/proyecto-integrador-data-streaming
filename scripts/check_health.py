"""Verificador de la salida materializada: separación real de la flota.

Lee `data/serving.db`, toma las filas `metric_type = 'apu_health_indicator'`,
imprime la tabla `asset_id / running_ratio / oil_temperature_mean /
air_leak_suspected` y sale con código 1 si el veredicto por activo no coincide
con la condición declarada en `dataset.FLEET_SEGMENTS`.

Cada fila almacenada ya es el pane más reciente de su `aggregate_id`: el
upsert monótono por `pane_index` descarta los anteriores. Por eso el
verificador no filtra por `is_last`, que bajo el trigger configurado
(`AfterWatermark` con refiring tardío) nunca llega a ser verdadero mientras la
ventana admita correcciones. `--only-final-panes` fuerza ese filtro.

Si los seis activos caen del mismo lado, el umbral, la agregación o la
asignación de ventanas están mal. Comando de una sola línea, ejecutable en
PowerShell.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from apu_streaming.config import (
    OIL_TEMPERATURE_ALERT_C,
    RUNNING_RATIO_ALERT,
    serving_db_path,
)
from apu_streaming.dataset import FLEET_SEGMENTS
from apu_streaming.serving import fetch_health_indicators

LEAK_CONDITION = "falla_fuga_aire"

# Condición esperada por activo, derivada de los tramos reales del dataset:
# apu-01/02/03 sanos, apu-04/05/06 con falla de fuga de aire.
EXPECTED_LEAK: dict[str, bool] = {
    segment.asset_id: segment.condition == LEAK_CONDITION for segment in FLEET_SEGMENTS
}


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def summarize_by_asset(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Colapsa los panes finales de cada activo en un veredicto por activo.

    Un activo se considera con fuga si `air_leak_suspected` es verdadero en la
    mayoría de sus ventanas finales; así una ventana de borde con pocas
    lecturas (el hueco de 139 s de apu-06) no invierte el diagnóstico.
    """
    by_asset: dict[str, dict[str, Any]] = {}
    for row in rows:
        asset = row["asset_id"]
        acc = by_asset.setdefault(
            asset,
            {"windows": 0, "flagged": 0, "running_ratios": [], "oil_means": []},
        )
        acc["windows"] += 1
        if row.get("air_leak_suspected"):
            acc["flagged"] += 1
        if row.get("running_ratio") is not None:
            acc["running_ratios"].append(float(row["running_ratio"]))
        if row.get("oil_temperature_mean") is not None:
            acc["oil_means"].append(float(row["oil_temperature_mean"]))

    summary: dict[str, dict[str, Any]] = {}
    for asset, acc in sorted(by_asset.items()):
        windows = acc["windows"]
        flagged = acc["flagged"]
        summary[asset] = {
            "windows": windows,
            "flagged_windows": flagged,
            "running_ratio": _mean(acc["running_ratios"]),
            "oil_temperature_mean": _mean(acc["oil_means"]),
            "air_leak_suspected": flagged * 2 > windows,
        }
    return summary


def evaluate(summary: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Compara el veredicto observado contra la condición declarada del tramo."""
    checked: list[dict[str, Any]] = []
    for asset, expected in sorted(EXPECTED_LEAK.items()):
        observed = summary.get(asset)
        if observed is None:
            checked.append(
                {
                    "asset_id": asset,
                    "expected_air_leak": expected,
                    "observed_air_leak": None,
                    "ok": False,
                    "error": "sin filas apu_health_indicator con is_last = 1",
                }
            )
            continue
        ok = observed["air_leak_suspected"] == expected
        checked.append(
            {
                "asset_id": asset,
                "expected_air_leak": expected,
                "observed_air_leak": observed["air_leak_suspected"],
                "running_ratio": observed["running_ratio"],
                "oil_temperature_mean": observed["oil_temperature_mean"],
                "windows": observed["windows"],
                "flagged_windows": observed["flagged_windows"],
                "ok": ok,
                "error": None if ok else "veredicto opuesto a la condición del tramo",
            }
        )

    unexpected = sorted(set(summary) - set(EXPECTED_LEAK))
    observed_flags = {c["observed_air_leak"] for c in checked if c["observed_air_leak"] is not None}
    return {
        "ok": all(c["ok"] for c in checked) and not unexpected,
        "assets": checked,
        "unexpected_assets": unexpected,
        # Si todos los activos caen del mismo lado, el umbral o la agregación
        # están mal aunque coincidieran por casualidad con lo esperado.
        "separation_observed": len(observed_flags) == 2,
        "thresholds": {
            "running_ratio_alert": RUNNING_RATIO_ALERT,
            "oil_temperature_alert_c": OIL_TEMPERATURE_ALERT_C,
        },
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    return f"{value}"


def print_table(report: dict[str, Any]) -> None:
    header = f"{'asset_id':<10} {'running_ratio':>14} {'oil_temp_mean':>14} {'air_leak':>9} {'':>4}"
    print(header)
    print("-" * len(header))
    for row in report["assets"]:
        mark = "OK" if row["ok"] else "FAIL"
        print(
            f"{row['asset_id']:<10} "
            f"{_fmt(row.get('running_ratio')):>14} "
            f"{_fmt(row.get('oil_temperature_mean')):>14} "
            f"{_fmt(row['observed_air_leak']):>9} "
            f"{mark:>4}"
        )
    print()
    if report["unexpected_assets"]:
        print(f"activos inesperados: {report['unexpected_assets']}")
    if not report["separation_observed"]:
        print("los activos no se separan: todos cayeron del mismo lado del umbral")
    print(f"Resultado: {'OK' if report['ok'] else 'FAIL'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=None, help="default: APU_SERVING_DB_PATH")
    parser.add_argument(
        "--only-final-panes",
        action="store_true",
        help="filtrar is_last = 1 (vacío mientras la ventana admita correcciones)",
    )
    parser.add_argument("--json", action="store_true", help="imprimir solo JSON")
    args = parser.parse_args()

    db_path = args.db_path or serving_db_path()
    if not db_path.exists():
        payload = {"ok": False, "error": f"{db_path} no existe; ¿corrió el materializer?"}
        print(json.dumps(payload) if args.json else payload["error"])
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = fetch_health_indicators(conn, only_last=args.only_final_panes)
    finally:
        conn.close()

    report = evaluate(summarize_by_asset(rows))
    report["db_path"] = str(db_path)
    report["rows_read"] = len(rows)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_table(report)

    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
