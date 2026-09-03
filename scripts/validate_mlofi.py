"""OLS L1 OFI vs MLOFI-5 vs MLOFI-10 on a Parquet capture (same spec as Phase 4).

Writes ``docs/math/mlofi_validation.md``. Requires the ``notebooks`` extra (statsmodels).
Callers: CLI, ``notebooks/mlofi_vpin_validation.ipynb``. User: source of truth for
the 1s/5s/10s L1 vs M5 vs M10 table; do not mix VPIN into this regression.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import numpy as np

from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE
from order_flow.metrics.batch import mlofi_events_from_capture
from order_flow.metrics.mlofi import MlofiWindowFrame, compute_mlofi_time_windows
from order_flow.storage.parquet import read_events
from order_flow.storage.report import capture_stats
from order_flow.utils.time import NS_PER_S

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "math" / "mlofi_validation.md"
DEFAULT_SYMBOL = "BTCUSDT"
WINDOW_SECONDS = (1, 5, 10)
MIN_OLS_OBS = 8
UNI_FEATURES = ("l1", "m5_sum", "m10_sum")
MULTI_FEATURES = ("m5_multi", "m10_multi")
SpecName = Literal["lead1", "contemporaneous"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Regress Δmid on L1 OFI / MLOFI-5 / MLOFI-10 (Spanish report)."
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


def _empty_uni(n_obs: int) -> dict[str, float]:
    nan = math.nan
    return {
        "n": float(n_obs),
        "alpha": nan,
        "beta": nan,
        "r2": nan,
        "beta_se": nan,
        "beta_t": nan,
        "hac_lags": float(_hac_lags(n_obs)),
        "k": 1.0,
    }


def _empty_multi(n_obs: int, k: int) -> dict[str, float]:
    nan = math.nan
    return {
        "n": float(n_obs),
        "r2": nan,
        "hac_lags": float(_hac_lags(n_obs)),
        "k": float(k),
    }


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
    if n_obs < MIN_OLS_OBS:
        return _empty_uni(n_obs)
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
        "k": 1.0,
    }


def fit_ols_multi(y: np.ndarray[Any, Any], x: np.ndarray[Any, Any]) -> dict[str, float]:
    """``y = a + X b + e`` (in-sample R²). Extra columns lift R² mechanically."""
    try:
        import statsmodels.api as sm
    except ImportError as exc:
        msg = "statsmodels is required: uv sync --extra notebooks"
        raise SystemExit(msg) from exc
    design_x = np.asarray(x, dtype=np.float64)
    if design_x.ndim == 1:
        design_x = design_x[:, np.newaxis]
    mask = np.isfinite(y) & np.all(np.isfinite(design_x), axis=1)
    y_ok = np.asarray(y[mask], dtype=np.float64)
    x_ok = np.asarray(design_x[mask], dtype=np.float64)
    n_obs = int(y_ok.size)
    k = int(x_ok.shape[1]) if x_ok.size else int(design_x.shape[1])
    if n_obs < MIN_OLS_OBS or k < 1:
        return _empty_multi(n_obs, k)
    design = sm.add_constant(x_ok, has_constant="add")
    lags = _hac_lags(n_obs)
    fit = sm.OLS(y_ok, design).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    return {
        "n": float(n_obs),
        "r2": float(fit.rsquared),
        "hac_lags": float(lags),
        "k": float(k),
    }


def _ns_to_s(value_ns: float) -> float:
    if not math.isfinite(value_ns):
        return math.nan
    return value_ns / 1e9


def _feature_map(mlofi: np.ndarray[Any, Any]) -> dict[str, np.ndarray[Any, Any]]:
    """Scalar sums (equal weights) and multivariate design matrices."""
    filled = np.nan_to_num(mlofi, nan=0.0)
    n_levels = int(filled.shape[1]) if filled.ndim == 2 else 1
    m5 = min(5, n_levels)
    return {
        "l1": filled[:, 0] if n_levels else filled,
        "m5_sum": filled[:, :m5].sum(axis=1),
        "m10_sum": filled.sum(axis=1),
        "m5_multi": filled[:, :m5],
        "m10_multi": filled,
    }


def _aligned_xy(
    frame: MlofiWindowFrame,
    feature: np.ndarray[Any, Any],
    spec: SpecName,
) -> tuple[np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    n_bars = int(frame.start_ns.size)
    empty_y = np.empty(0, dtype=np.float64)
    if n_bars < 2:
        empty_x = np.empty((0, feature.shape[1])) if feature.ndim == 2 else empty_y
        return empty_y, empty_x
    if spec == "lead1":
        y = np.asarray(frame.delta_mid_lead1[:-1], dtype=np.float64)
        x = np.asarray(feature[:-1], dtype=np.float64)
        ok = np.asarray(frame.valid[:-1] & frame.valid[1:], dtype=np.bool_)
    else:
        y = np.asarray(frame.delta_mid[1:], dtype=np.float64)
        x = np.asarray(feature[1:], dtype=np.float64)
        ok = np.asarray(frame.valid[1:] & frame.valid[:-1], dtype=np.bool_)
    y_out = np.where(ok, y, np.nan)
    if x.ndim == 1:
        return y_out, np.where(ok, x, np.nan)
    return y_out, np.where(ok[:, np.newaxis], x, np.nan)


def evaluate_capture(root: Path, *, exchange: str, symbol: str) -> dict[str, Any]:
    """Build 1s/5s/10s MLOFI-10 bars and run contemporaneous + lead-1 OLS."""
    series = mlofi_events_from_capture(root, exchange=exchange, symbol=symbol, levels=10)
    trades = read_events(root, "trade", exchange=exchange, symbol=symbol)
    stats = capture_stats(root, exchange=exchange, symbol=symbol)
    meta_path = root / "capture_meta.json"
    meta: dict[str, Any] = {}
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    duration_s = float(stats.duration_ns) / float(NS_PER_S) if stats.duration_ns else 0.0
    if series.state_ts_ns.size >= 2:
        duration_s = float(series.state_ts_ns[-1] - series.state_ts_ns[0]) / float(NS_PER_S)

    n_states = int(series.state_ts_ns.size)
    n_events = int(series.e_n.shape[0])
    n_epochs = int(np.unique(series.epoch).size) if series.epoch.size else 0
    updates_per_s = (n_events / duration_s) if duration_s > 0 else math.nan
    n_levels = int(series.e_n.shape[1]) if series.e_n.ndim == 2 else 0

    latency = meta.get("latency_ns") or {}
    windows: dict[str, Any] = {}
    for tau_s in WINDOW_SECONDS:
        frame = compute_mlofi_time_windows(
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
        features = _feature_map(frame.mlofi)
        specs: dict[str, Any] = {}
        for spec in ("lead1", "contemporaneous"):
            fits: dict[str, Any] = {}
            for name in UNI_FEATURES:
                y, x = _aligned_xy(frame, features[name], spec)
                fits[name] = fit_ols(y, x)
            for name in MULTI_FEATURES:
                y, x = _aligned_xy(frame, features[name], spec)
                fits[name] = fit_ols_multi(y, x)
            specs[spec] = fits
        windows[f"{tau_s}s"] = {
            "tau_s": tau_s,
            "n_bars": n_bars,
            "n_valid": n_valid,
            "dropped_pct": dropped_pct,
            **specs,
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
        "n_mlofi_events": n_events,
        "n_levels": n_levels,
        "n_epochs": n_epochs,
        "updates_per_s": updates_per_s,
        "n_trades": int(trades.height),
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
        "recapture": False,
    }


def _fmt(value: object, digits: int = 4) -> str:
    if value is None:
        return "n/d"
    if isinstance(value, float) and not math.isfinite(value):
        return "n/d"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _r2(block: dict[str, Any], spec: str, feature: str) -> float:
    return float(block[spec][feature]["r2"])


def _conclusion(result: dict[str, Any]) -> str:
    ten = result["windows"]["10s"]
    lead_l1 = _r2(ten, "lead1", "l1")
    lead_m5 = _r2(ten, "lead1", "m5_sum")
    lead_m10 = _r2(ten, "lead1", "m10_sum")
    lead_m5m = _r2(ten, "lead1", "m5_multi")
    lead_m10m = _r2(ten, "lead1", "m10_multi")
    cont_l1 = _r2(ten, "contemporaneous", "l1")
    cont_m5 = _r2(ten, "contemporaneous", "m5_sum")
    cont_m10 = _r2(ten, "contemporaneous", "m10_sum")
    cont_m5m = _r2(ten, "contemporaneous", "m5_multi")
    cont_m10m = _r2(ten, "contemporaneous", "m10_multi")
    prefix = (
        "La muestra dura menos de 30 min: los números son un humo, no una conclusión estable. "
        if result["insufficient"]
        else ""
    )
    needed = (
        lead_l1,
        lead_m5,
        lead_m10,
        lead_m5m,
        lead_m10m,
        cont_l1,
        cont_m5,
        cont_m10,
        cont_m5m,
        cont_m10m,
    )
    if any(not math.isfinite(value) for value in needed):
        return prefix + "No hay observaciones suficientes para estimar OLS."

    uni_gain = max(cont_m5, cont_m10) - cont_l1
    lead_gain = max(lead_m5, lead_m10) - lead_l1
    multi_lift = max(cont_m5m, cont_m10m) - max(cont_m5, cont_m10, cont_l1)
    parts: list[str] = [prefix] if prefix else []
    if lead_l1 < 0.05 and lead_m5 < 0.05 and lead_m10 < 0.05:
        parts.append(
            "Lead-1 sigue ~0 al añadir profundidad: **MLOFI-5/10 no convierten OFI en un "
            "predictor** de la siguiente ventana en esta muestra."
        )
    if uni_gain < 0.01 and abs(lead_gain) < 0.01:
        parts.append(
            "En el OLS **univariante** (suma de niveles), extra profundidad **no mejora** el R² "
            f"de forma material (ΔR² contemporáneo a 10 s ≈ {uni_gain:.4f}). En *estos* datos "
            "es ruido / colinealidad, no una victoria de MLOFI sobre L1."
        )
    elif uni_gain >= 0.01:
        parts.append(
            f"La suma M=5/10 **sí mueve** el R² contemporáneo univariante (Δ ≈ {uni_gain:.4f} "
            "vs L1 a 10 s). Sigue siendo un símbolo, ~40 min, cripto: no transfiere el paper "
            "de Xu (Nasdaq, Ridge, OOS)."
        )
    if multi_lift >= 0.02:
        parts.append(
            f"El OLS **multivariante** in-sample sube más (ΔR² ≈ {multi_lift:.4f} sobre la suma): "
            "eso es **mecánico** (más parámetros). No interpretarlo como evidencia OOS."
        )
    parts.append(
        "No se encontró un paper que afirme que MLOFI gana a OFI L1 «irrefutablemente» en R²; "
        "Xu et al. (2019) reportan RMSE Ridge OOS en acciones US contemporáneas, otro venue y "
        "otro estimador. VPIN no entra en esta regresión."
    )
    return " ".join(part.strip() for part in parts if part.strip())


def _uni_rows(result: dict[str, Any], spec: SpecName) -> list[str]:
    labels = {"l1": "L1 OFI", "m5_sum": "M5-sum", "m10_sum": "M10-sum"}
    spec_label = "lead-1" if spec == "lead1" else "contemp."
    rows: list[str] = []
    for key in ("1s", "5s", "10s"):
        block = result["windows"][key]
        for feat in UNI_FEATURES:
            fit = block[spec][feat]
            rows.append(
                f"| {key} | {spec_label} | {labels[feat]} | {_fmt(fit['n'], 0)} | "
                f"{_fmt(fit['beta'])} | {_fmt(fit['beta_se'])} | {_fmt(fit['beta_t'])} | "
                f"{_fmt(fit['r2'])} | {_fmt(fit['hac_lags'], 0)} |"
            )
    return rows


def _multi_rows(result: dict[str, Any], spec: SpecName) -> list[str]:
    labels = {"m5_multi": "M5-multi", "m10_multi": "M10-multi"}
    spec_label = "lead-1" if spec == "lead1" else "contemp."
    rows: list[str] = []
    for key in ("1s", "5s", "10s"):
        block = result["windows"][key]
        for feat in MULTI_FEATURES:
            fit = block[spec][feat]
            rows.append(
                f"| {key} | {spec_label} | {labels[feat]} | {_fmt(fit['n'], 0)} | "
                f"{_fmt(fit['k'], 0)} | {_fmt(fit['r2'])} | {_fmt(fit['hac_lags'], 0)} |"
            )
    return rows


def render_markdown(result: dict[str, Any]) -> str:
    """Spanish validation report."""
    uni_header = (
        "| τ | spec | feature | n | β | EE HAC | t | R² | lags NW |\n"
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    multi_header = (
        "| τ | spec | feature | n | k | R² | lags NW |\n"
        "| --- | --- | --- | ---: | ---: | ---: | ---: |"
    )
    duration_note = ""
    if result["insufficient"]:
        duration_note = (
            "\n\n**Aviso:** la captura dura **menos de 30 minutos**. Interpreta R²/β como "
            "diagnóstico de pipeline, no como evidencia estable.\n"
        )
    return "\n".join(
        [
            "# Validación empírica de MLOFI (Xu-Gould-Howison 2019) vs OFI L1",
            "",
            f"Generado: **{result['generated_utc']}** en `{result['host']}`.",
            "Script: `scripts/validate_mlofi.py` "
            "(fuente de verdad; el notebook solo lo documenta).",
            duration_note,
            "## Setup",
            "",
            f"- Exchange: `{result['exchange']}` (Binance USD-M, mainnet `fapi` / `fstream`)",
            f"- Símbolo: `{result['symbol']}`",
            f"- Directorio: `{result['capture_root']}`",
            f"- Duración de la serie L1: **{_fmt(result['duration_s'], 1)} s** "
            f"(meta elapsed `{_fmt(result['duration_meta_s'], 1)} s`, "
            f"pedida `{_fmt(result['duration_requested_s'], 1)} s`)",
            f"- Estados sincronizados: {result['n_l1_states']}; eventos $e^m_n$: "
            f"{result['n_mlofi_events']}; niveles pedidos: {result['n_levels']}; "
            f"épocas: {result['n_epochs']}",
            f"- Updates / s: {_fmt(result['updates_per_s'], 2)}; "
            f"deltas/s (storage): {_fmt(result['deltas_per_s'], 2)}",
            f"- Latencia observada (recv - event): media {_fmt(result['latency_mean_s'], 4)} s, "
            f"p99 {_fmt(result['latency_p99_s'], 4)} s",
            f"- Gaps / resyncs / reconnects (meta): {result['gaps']} / {result['resyncs']} / "
            f"{result['reconnects']}",
            f"- Dual sockets: {result['dual_sockets']}",
            f"- Trades persistidos: {result['n_trades']} (no entran en esta regresión)",
            "- Recapture: **no**. Los snapshots guardan el libro REST completo "
            "(>> 10 niveles). Misma captura que la fase 4.",
            "",
            "Tick de BTCUSDT USD-M: **0.1 USD**. Δmid en **precio** (USD), no en ticks.",
            "",
            "## Especificación",
            "",
            "Misma rejilla y alineamiento que `scripts/validate_ofi.py` / "
            "[ofi_validation.md](ofi_validation.md).",
            r"Ventana \(k\): \([t_0 + k\tau,\ t_0 + (k+1)\tau)\), \(\tau \in \{1,5,10\}\) s.",
            r"Nivel \(m\): la misma \(e_n\) de Cont et al. en el \(m\)-ésimo best "
            r"(Xu et al. §3.1). Columna 0 ≡ OFI L1.",
            r"\(\mathrm{MLOFI}^m_k = \sum e^m_n\) en la ventana.",
            "Mid: último mid **L1** de la barra (carry-forward). Barras con resync se tiran.",
            "",
            "**Lead-1 (lo que pidió Carlos, predictivo):**",
            "",
            r"\[\Delta \mathrm{mid}_{k+1} = \alpha + \beta\, X_k + \varepsilon_{k+1}\]",
            "",
            "**Contemporánea (lo que estima Cont et al. y Xu et al.):**",
            "",
            r"\[\Delta \mathrm{mid}_k = \alpha + \beta\, X_k + \varepsilon_k\]",
            "",
            "Features univariantes (comparación justa, un solo β):",
            "",
            "- **L1 OFI**: $X_k = \\mathrm{MLOFI}^1_k$ (≡ OFI)",
            "- **M5-sum**: $X_k = \\sum_{m=1}^{5} e^m_k$ (pesos iguales; **no** viene de Xu)",
            "- **M10-sum**: $X_k = \\sum_{m=1}^{10} e^m_k$",
            "",
            "Features multivariantes (extra, **R² in-sample sube al añadir columnas**):",
            "",
            "- **M5-multi** / **M10-multi**: $X_k$ es el vector de 5 (resp. 10) niveles.",
            "",
            r"OLS con errores HAC Newey-West, lags \(= \lfloor 4(n/100)^{2/9}\rfloor\).",
            "**VPIN no se mezcla** en esta regresión.",
            "",
            "## Resultados univariantes (L1 vs suma M=5 vs suma M=10)",
            "",
            uni_header,
            *_uni_rows(result, "lead1"),
            *_uni_rows(result, "contemporaneous"),
            "",
            "## Resultados multivariantes (aviso: más parámetros ⇒ más R² in-sample)",
            "",
            multi_header,
            *_multi_rows(result, "lead1"),
            *_multi_rows(result, "contemporaneous"),
            "",
            "## Literatura vs estos datos",
            "",
            "Xu, Gould & Howison (2019), *Market Microstructure and Liquidity* / "
            "arXiv:1907.06230: en 6 acciones Nasdaq 2016, Ridge **contemporáneo** mejora el "
            "RMSE OOS al pasar de $M=1$ a $M=10$. **No** es lead-1, **no** es cripto, **no** "
            "es OLS univariante con suma de niveles.",
            "",
            "Búsqueda (2026-09-02) de un paper que afirme que MLOFI gana a OFI L1 "
            "**«irrefutablemente»** en R²: **no encontrado**. Cont et al. apéndice B3 "
            "vieron solo una mejora leve con 5 niveles y OLS.",
            "",
            "## Conclusión",
            "",
            _conclusion(result),
            "",
            "## Caveats",
            "",
            "- Misma captura ~40 min / un símbolo (BTCUSDT) que la fase 4.",
            "- Suma de niveles es conveniencia nuestra; Xu regresa el vector (Ridge).",
            "- R² multivariante in-sample no es evidencia out-of-sample.",
            "- Libro L2 agregado 100 ms, no el L3 de Xu.",
            "- No se asume que los resultados de Xu se transfieran a perps cripto.",
            "",
            "## Reproducción",
            "",
            "```bash",
            "uv run --extra notebooks python scripts/validate_mlofi.py \\",
            f"  --root {result['capture_root']} --report docs/math/mlofi_validation.md",
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
