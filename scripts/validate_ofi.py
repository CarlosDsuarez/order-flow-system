"""OLS validation of L1 OFI vs next-window mid on a Parquet capture.

Writes ``docs/math/ofi_validation.md``. Requires the ``notebooks`` extra (statsmodels).
Callers: CLI, ``notebooks/ofi_cvd_validation.ipynb``. User: script is the source of
truth for the 1s/5s/10s lead-1 OLS table.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE
from order_flow.metrics.batch import cvd_from_capture, ofi_events_from_capture
from order_flow.metrics.ofi import compute_ofi_time_windows
from order_flow.storage.parquet import read_events
from order_flow.storage.report import capture_stats
from order_flow.utils.time import NS_PER_S

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "math" / "ofi_validation.md"
DEFAULT_SYMBOL = "BTCUSDT"
WINDOW_SECONDS = (1, 5, 10)
MIN_OLS_OBS = 8
PAPER_R2 = 0.65
PAPER_WINDOW_S = 10


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Regress next-window Δmid on OFI from a Parquet capture (Spanish report)."
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Capture directory (Parquet hive + optional capture_meta.json)",
    )
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--json", action="store_true", help="Also print the structured dict")
    return parser


def _hac_lags(n_obs: int) -> int:
    """Newey-West lag: floor(4 (n/100)^{2/9}), at least 1."""
    if n_obs <= 1:
        return 1
    return max(1, int(np.floor(4.0 * (n_obs / 100.0) ** (2.0 / 9.0))))


def fit_ols(y: np.ndarray[Any, Any], x: np.ndarray[Any, Any]) -> dict[str, float]:
    """``y = a + b x + e`` with HAC (Newey-West) standard errors."""
    try:
        import statsmodels.api as sm
    except ImportError as exc:
        msg = "statsmodels is required: uv sync --extra notebooks"
        raise SystemExit(msg) from exc
    mask = np.isfinite(y) & np.isfinite(x)
    y_ok = np.asarray(y[mask], dtype=np.float64)
    x_ok = np.asarray(x[mask], dtype=np.float64)
    n_obs = int(y_ok.size)
    nan = math.nan
    if n_obs < MIN_OLS_OBS:
        return {
            "n": float(n_obs),
            "alpha": nan,
            "beta": nan,
            "r2": nan,
            "beta_se": nan,
            "beta_t": nan,
            "hac_lags": float(_hac_lags(n_obs)),
        }
    design = sm.add_constant(x_ok, has_constant="add")
    lags = _hac_lags(n_obs)
    fit = sm.OLS(y_ok, design).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    params = np.asarray(fit.params, dtype=np.float64)
    bse = np.asarray(fit.bse, dtype=np.float64)
    tvalues = np.asarray(fit.tvalues, dtype=np.float64)
    return {
        "n": float(n_obs),
        "alpha": float(params[0]),
        "beta": float(params[1]),
        "r2": float(fit.rsquared),
        "beta_se": float(bse[1]),
        "beta_t": float(tvalues[1]),
        "hac_lags": float(lags),
    }


def _ns_to_s(value_ns: float) -> float:
    if not math.isfinite(value_ns):
        return math.nan
    return value_ns / 1e9


def evaluate_capture(root: Path, *, exchange: str, symbol: str) -> dict[str, Any]:
    """Build 1s/5s/10s OFI bars and run contemporaneous + lead-1 OLS."""
    series = ofi_events_from_capture(root, exchange=exchange, symbol=symbol)
    trades = read_events(root, "trade", exchange=exchange, symbol=symbol)
    cvd = cvd_from_capture(root, exchange=exchange, symbol=symbol)
    stats = capture_stats(root, exchange=exchange, symbol=symbol)
    meta_path = root / "capture_meta.json"
    meta: dict[str, Any] = {}
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    duration_s = float(stats.duration_ns) / float(NS_PER_S) if stats.duration_ns else 0.0
    if series.state_ts_ns.size >= 2:
        duration_s = float(series.state_ts_ns[-1] - series.state_ts_ns[0]) / float(NS_PER_S)

    n_states = int(series.state_ts_ns.size)
    n_events = int(series.e_n.size)
    n_epochs = int(np.unique(series.epoch).size) if series.epoch.size else 0
    updates_per_s = (n_events / duration_s) if duration_s > 0 else math.nan

    latency = meta.get("latency_ns") or {}
    windows: dict[str, Any] = {}
    for tau_s in WINDOW_SECONDS:
        frame = compute_ofi_time_windows(
            series.state_ts_ns,
            series.bid_px,
            series.bid_qty,
            series.ask_px,
            series.ask_qty,
            window_ns=tau_s * NS_PER_S,
            epoch=series.epoch,
        )
        n_bars = int(frame.start_ns.size)
        n_valid = int(np.count_nonzero(frame.valid))
        dropped_pct = (100.0 * (n_bars - n_valid) / n_bars) if n_bars else math.nan

        lead_y = frame.delta_mid_lead1[:-1] if n_bars else np.empty(0)
        lead_x = frame.ofi[:-1] if n_bars else np.empty(0)
        lead_ok = frame.valid[:-1] & frame.valid[1:] if n_bars >= 2 else np.zeros(0, dtype=np.bool_)
        if lead_ok.size:
            lead_y = np.where(lead_ok, lead_y, np.nan)
            lead_x = np.where(lead_ok, lead_x, np.nan)

        contemp_y = frame.delta_mid[1:] if n_bars else np.empty(0)
        contemp_x = frame.ofi[1:] if n_bars else np.empty(0)
        contemp_ok = (
            frame.valid[1:] & frame.valid[:-1] if n_bars >= 2 else np.zeros(0, dtype=np.bool_)
        )
        if contemp_ok.size:
            contemp_y = np.where(contemp_ok, contemp_y, np.nan)
            contemp_x = np.where(contemp_ok, contemp_x, np.nan)

        windows[f"{tau_s}s"] = {
            "tau_s": tau_s,
            "n_bars": n_bars,
            "n_valid": n_valid,
            "dropped_pct": dropped_pct,
            "lead1": fit_ols(lead_y, lead_x),
            "contemporaneous": fit_ols(contemp_y, contemp_x),
        }

    mean_lat = latency.get("mean")
    p99_lat = latency.get("p99")
    return {
        "generated_utc": datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "host": platform.node(),
        "capture_root": str(root.resolve()),
        "exchange": exchange,
        "symbol": symbol,
        "duration_s": duration_s,
        "duration_meta_s": meta.get("duration_s_elapsed"),
        "duration_requested_s": meta.get("duration_s_requested"),
        "n_l1_states": n_states,
        "n_ofi_events": n_events,
        "n_epochs": n_epochs,
        "updates_per_s": updates_per_s,
        "n_trades": int(trades.height),
        "cvd_last": float(cvd[-1]) if cvd.size else math.nan,
        "deltas_per_s": stats.deltas_per_s,
        "trades_per_s": stats.trades_per_s,
        "latency_mean_s": _ns_to_s(float(mean_lat)) if mean_lat is not None else math.nan,
        "latency_p99_s": _ns_to_s(float(p99_lat)) if p99_lat is not None else math.nan,
        "gaps": meta.get("gaps"),
        "resyncs": meta.get("resyncs"),
        "reconnects": meta.get("reconnects"),
        "dual_sockets": meta.get("dual_sockets"),
        "capture_error": meta.get("error"),
        "windows": windows,
        "insufficient": duration_s < 1800.0,
        "paper": {
            "r2_mean": PAPER_R2,
            "window_s": PAPER_WINDOW_S,
            "alignment": "contemporaneous",
            "venue": "US equities TAQ, 50 S&P 500 names, April 2010",
        },
    }


def _fmt(value: object, digits: int = 4) -> str:
    if value is None:
        return "n/d"
    if isinstance(value, float) and not math.isfinite(value):
        return "n/d"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _conclusion(result: dict[str, Any]) -> str:
    ten = result["windows"]["10s"]
    lead_r2 = float(ten["lead1"]["r2"])
    cont_r2 = float(ten["contemporaneous"]["r2"])
    insufficient = bool(result["insufficient"])
    prefix = (
        "La muestra dura menos de 30 min: los números son un humo, no una conclusión estable. "
        if insufficient
        else ""
    )
    if not math.isfinite(lead_r2) or not math.isfinite(cont_r2):
        return prefix + "No hay observaciones suficientes para estimar OLS."
    if lead_r2 < 0.05 and cont_r2 >= 0.15:
        return (
            prefix + "OFI es **principalmente contemporáneo** en este setup (explica el Δmid de la "
            "misma ventana, no la siguiente). Como input **predictivo** para market making "
            "pasivo es débil: no uses β de la siguiente barra como si fuera el 65 % del paper."
        )
    if lead_r2 < 0.05 and cont_r2 < 0.15:
        return (
            prefix + "Ni la especificación contemporánea ni la predictiva (lead-1) alcanzan poder "
            "explicativo usable en BTCUSDT L2 100 ms. El 65 % del paper no se reproduce aquí; "
            "OFI L1 no es, por sí solo, un input predictivo de MM en esta muestra."
        )
    if lead_r2 >= 0.15:
        return (
            prefix + "Hay señal **predictiva** medible (R² lead-1 no despreciable a 10 s). Sigue "
            "siendo un test corto, un símbolo, crypto vs acciones: no es el 65 % de Cont et al."
        )
    return (
        prefix
        + "R² predictivo bajo-moderado. Útil como feature contemporánea / de estado, no como "
        "pronóstico fuerte de la siguiente ventana."
    )


def render_markdown(result: dict[str, Any]) -> str:
    """Spanish validation report."""
    paper = result["paper"]
    rows_lead: list[str] = []
    rows_cont: list[str] = []
    for key in ("1s", "5s", "10s"):
        block = result["windows"][key]
        lead = block["lead1"]
        cont = block["contemporaneous"]
        rows_lead.append(
            f"| {key} | {_fmt(lead['n'], 0)} | {_fmt(lead['beta'])} | {_fmt(lead['beta_se'])} | "
            f"{_fmt(lead['beta_t'])} | {_fmt(lead['r2'])} | {_fmt(lead['hac_lags'], 0)} | "
            f"{_fmt(block['dropped_pct'], 2)} |"
        )
        rows_cont.append(
            f"| {key} | {_fmt(cont['n'], 0)} | {_fmt(cont['beta'])} | {_fmt(cont['beta_se'])} | "
            f"{_fmt(cont['beta_t'])} | {_fmt(cont['r2'])} | {_fmt(cont['hac_lags'], 0)} | "
            f"{_fmt(block['dropped_pct'], 2)} |"
        )
    header = (
        "| τ | n | β | EE HAC | t | R² | lags NW | % barras inválidas |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    duration_note = ""
    if result["insufficient"]:
        duration_note = (
            "\n\n**Aviso:** la captura dura **menos de 30 minutos**. Carlos pidió 30-60 min; "
            "esta corrida no cumple el umbral. Interpreta R²/β como diagnóstico de pipeline, "
            "no como evidencia estable.\n"
        )
    trades_note = (
        f"{result['n_trades']} trades persistidos; CVD terminal = `{_fmt(result['cvd_last'])}`."
        if result["n_trades"]
        else (
            "0 trades en Parquet. CVD no se valida en datos reales de esta captura "
            "(OFI no necesita trades). Causa histórica: `@aggTrade` en el WS de fstream "
            "estuvo silencioso el 2026-09-02; el feed usa `@trade` + dual sockets."
        )
    )
    return "\n".join(
        [
            "# Validación empírica de OFI (Cont-Kukanov-Stoikov 2014, L1)",
            "",
            f"Generado: **{result['generated_utc']}** en `{result['host']}`.",
            "Script: `scripts/validate_ofi.py` (fuente de verdad; el notebook solo lo documenta).",
            duration_note,
            "## Setup",
            "",
            f"- Exchange: `{result['exchange']}` (Binance USD-M, mainnet `fapi` / `fstream`)",
            f"- Símbolo: `{result['symbol']}`",
            f"- Directorio: `{result['capture_root']}`",
            f"- Duración de la serie L1: **{_fmt(result['duration_s'], 1)} s** "
            f"(meta elapsed `{_fmt(result['duration_meta_s'], 1)} s`, "
            f"pedida `{_fmt(result['duration_requested_s'], 1)} s`)",
            f"- Estados L1 sincronizados: {result['n_l1_states']}; eventos $e_n$: "
            f"{result['n_ofi_events']}; épocas (resyncs de libro): {result['n_epochs']}",
            f"- Updates L1 / s: {_fmt(result['updates_per_s'], 2)}; "
            f"deltas/s (storage): {_fmt(result['deltas_per_s'], 2)}",
            f"- Latencia observada (recv - event): media {_fmt(result['latency_mean_s'], 4)} s, "
            f"p99 {_fmt(result['latency_p99_s'], 4)} s",
            f"- Gaps / resyncs / reconnects (meta): {result['gaps']} / {result['resyncs']} / "
            f"{result['reconnects']}",
            f"- Dual sockets: {result['dual_sockets']}",
            f"- Trades: {trades_note}",
            "",
            "Tick de BTCUSDT USD-M: **0.1 USD**. Δmid se reporta en **precio** (USD), no en ticks.",
            "",
            "## Especificación",
            "",
            "Muestreo: cada snapshot/delta aplicado con BBO válido (diffs `@depth@100ms`, no L3).",
            r"`e_n` es la formula L1 de Cont et al. section 2.1 (ver `docs/math/ofi.md`).",
            r"Ventana \(k\): \([t_0 + k\tau,\ t_0 + (k+1)\tau)\), \(\tau \in \{1,5,10\}\) s.",
            r"\(\mathrm{OFI}_k = \sum e_n\) con \(\tau_n\) en la ventana.",
            "Mid al cierre de barra: último mid L1 con timestamp en la barra, carry-forward.",
            "Se descartan barras con época mezclada (resync) o mid no finito.",
            "",
            "**Lead-1 (lo que pidió Carlos, predictivo):**",
            "",
            r"\[\Delta \mathrm{mid}_{k+1} = \alpha + \beta\,\mathrm{OFI}_k + \varepsilon_{k+1}\]",
            "",
            r"donde \(\Delta\mathrm{mid}_{k+1} = \mathrm{mid}_{k+1} - \mathrm{mid}_k\).",
            "",
            "**Contemporánea (lo que estima el paper, eq. (4)):**",
            "",
            r"\[\Delta \mathrm{mid}_k = \alpha + \beta\,\mathrm{OFI}_k + \varepsilon_k\]",
            "",
            r"OLS con errores HAC Newey-West, lags \(= \lfloor 4(n/100)^{2/9}\rfloor\).",
            "",
            "## Resultados - lead-1 (siguiente ventana)",
            "",
            header,
            *rows_lead,
            "",
            "## Resultados - contemporánea (misma ventana, como el paper)",
            "",
            header,
            *rows_cont,
            "",
            "## Comparación con Cont, Kukanov & Stoikov (2014)",
            "",
            f"El paper reporta **R² medio ≈ {int(paper['r2_mean'] * 100)} %** en la "
            f"especificación **contemporánea** a **{paper['window_s']} s**, "
            f"en {paper['venue']}. No es un test de ΔP de la siguiente ventana.",
            "Un footnote avisa tautología: eventos que *mueven* el quote entran en OFI; "
            "excluyéndolos el R² bajó a 35-60 % y siguió siendo alto.",
            "",
            "Este experimento es más duro: perpetuo crypto, un símbolo, diffs L2 a 100 ms "
            "(no TAQ L1 tick-a-tick), y la columna lead-1 pide predicción, no explicación.",
            "**No esperes ~65 %.** Si el R² contemporáneo a 10 s ya es mucho menor, el gap "
            "es venue + muestreo + horizonte, no un bug de e_n (los unit tests cubren "
            "la casuística del paper).",
            "",
            "## Conclusión",
            "",
            _conclusion(result),
            "",
            "## Caveats",
            "",
            "- 30-60 min es corto frente a un mes de TAQ en 50 nombres.",
            "- Un símbolo (BTCUSDT) vs 50 acciones US.",
            "- Crypto perpetuo 24/7 vs equity RTH; tick 0.1 USD vs 0.01 USD.",
            "- Reloj: `ts_event` del exchange vs recepción local; p99 de latencia arriba.",
            "- Libro L2 agregado 100 ms, no el L3 del paper.",
            "- Barras con resync se tiran; un burst de gaps reduce n.",
            "",
            "## Reproducción",
            "",
            "```bash",
            "uv run --extra notebooks python scripts/validate_ofi.py \\",
            f"  --root {result['capture_root']} --report docs/math/ofi_validation.md",
            "```",
            "",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.expanduser().resolve()
    if not root.is_dir():
        print(f"capture root not found: {root}", file=sys.stderr)
        return 2
    result = evaluate_capture(root, exchange=args.exchange, symbol=args.symbol.upper())
    markdown = render_markdown(result)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(markdown, encoding="utf-8")
    print(markdown)
    print(f"wrote {args.report}")
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
