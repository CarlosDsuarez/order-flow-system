"""Run the OFI-skewed MM pipeline test on a Parquet capture (Spanish report).

Requires ``uv sync --extra backtest``. Not a CI test. Default: maker post-only
and a crossing variant on the same capture / fee schedule.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from order_flow.backtest.runner import BacktestResult, run_ofi_mm_backtest
from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "backtest" / "ofi_mm_results.md"
DEFAULT_ROOT = REPO_ROOT / "data" / "live-btcusdt-45min"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest OFI-skewed one-lot MM via nautilus_trader (Spanish report)."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Capture directory")
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--maker-fee", type=float, default=0.0002)
    parser.add_argument("--taker-fee", type=float, default=0.0004)
    parser.add_argument("--spread-ticks", type=int, default=2)
    parser.add_argument("--ofi-threshold", type=float, default=5.0)
    parser.add_argument("--trade-size", type=float, default=0.001)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--skip-cross",
        action="store_true",
        help="Only run the post-only maker scenario",
    )
    return parser


def _pct(fee: float) -> str:
    return f"{fee * 100:.3f}%"


def _row(label: str, result: BacktestResult) -> str:
    return (
        f"| {label} | {result.total_pnl:.4f} | {result.realized_pnl:.4f} | "
        f"{result.unrealized_pnl:.4f} | {result.fees:.4f} | {result.n_submitted} | "
        f"{result.n_unique_filled} | {result.n_fill_events} | {result.n_maker_fills} | "
        f"{result.n_taker_fills} | {100.0 * result.fill_rate:.2f}% | "
        f"{result.n_canceled} | {result.n_rejected} |"
    )


def _crossing_verdict(maker: BacktestResult, crossing: BacktestResult) -> str:
    if crossing.total_pnl < maker.total_pnl:
        return (
            "**Crossing empeoró el PnL** respecto al maker post-only en esta muestra "
            "(hipótesis: cruzar el spread mata la estrategia). Sigue sin ser un test "
            "de edge de OFI: es un test de fees + fill model."
        )
    if crossing.total_pnl > maker.total_pnl:
        return (
            "En **esta** muestra el crossing no destruyó el PnL vs maker. No generalices: "
            "40 min, un símbolo, fill model de L2 sin cola real. La hipótesis de que "
            "cruzar mata la estrategia **no** se confirma aquí."
        )
    return "PnL maker y crossing coinciden (posible muestra demasiado corta o sin fills)."


def render_markdown(maker: BacktestResult, crossing: BacktestResult | None) -> str:
    duration_s = maker.duration_ns / 1e9
    duration_min = duration_s / 60.0
    header = (
        "| Escenario | PnL total USDT | Realizado | No realizado | Fees | "
        "Órdenes enviadas | Órdenes con fill | Eventos fill | Maker fills | "
        "Taker fills | Fill rate | Canceladas | Rechazadas |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
        " ---: | ---: | ---: |"
    )
    table = [header, _row("Maker post-only", maker)]
    if crossing is not None:
        table.append(_row("Crossing (límite agresivo)", crossing))
        verdict = _crossing_verdict(maker, crossing)
    else:
        verdict = "No se corrió el modo agresivo (`--skip-cross`)."
    return "\n".join(
        [
            "# Backtest OFI-MM (pipeline, no una ventaja)",
            "",
            f"Generado: **{datetime.now(tz=UTC).strftime('%Y-%m-%d %H:%M:%SZ')}**.",
            "Script: `scripts/run_ofi_mm_backtest.py`. Motor: **nautilus_trader "
            f"{maker.nautilus_version}** (estable PyPI; no 2.0.0rc).",
            "",
            "> **Esto no es evidencia de un edge de OFI.** En esta captura el R² "
            "lead-1 de OFI L1 es ~0-1.4 % y el contemporáneo ~22-29 % "
            "([docs/math/ofi_validation.md](../math/ofi_validation.md)). Un PnL "
            "positivo o negativo aquí solo prueba que datos → métrica → orden "
            "simulada está cableado. Limitaciones: "
            "[docs/backtest_limitations.md](../backtest_limitations.md).",
            "",
            "## Captura y parámetros",
            "",
            f"- Directorio: `{maker.capture}`",
            f"- Exchange / símbolo: `{maker.exchange}` / `{maker.symbol}`",
            f"- Instrumento nautilus: `{maker.instrument_id}` (tick 0.1, size 0.001)",
            f"- Duración de la serie (min-max `ts_event`): **{duration_s:.1f} s** "
            f"(~{duration_min:.1f} min)",
            f"- Batches de libro: {maker.n_book_batches}; trades públicos: {maker.n_public_trades}",
            f"- Fees: maker {_pct(maker.maker_fee)} / taker {_pct(maker.taker_fee)} "
            "(Binance USD-M típico ~0.02% / ~0.04%)",
            f"- Spread: {maker.spread_ticks} ticks alrededor del mid, luego sesgo OFI "
            f"±{maker.max_skew} tick si `|OFI_1s| > {maker.ofi_threshold}`",
            f"- OFI: suma móvil de `e_n` (Cont et al., `OfiAccumulator`) en "
            f"{maker.ofi_window_ns / 1e9:.0f} s",
            f"- Tamaño: {maker.trade_size} BTC por lado; post-only GTC salvo crossing",
            f"- Capital inicial: {maker.starting_balance:.0f} USDT, apalancamiento 1x, "
            "sin modelo de latencia ni liquidación",
            "",
            "OFI positivo ⇒ cotizaciones **arriba** (bid y ask más altos). OFI negativo ⇒ abajo.",
            "",
            "## Resultados",
            "",
            *table,
            "",
            verdict,
            "",
            "## Cómo se alimentó el motor",
            "",
            "Parquet hive `snapshots/` + `deltas/` + `trades/` → `capture_to_ops` "
            "(CLEAR+ADD por snapshot; qty 0 → DELETE; qty > 0 → UPDATE) → "
            "`OrderBookDeltas` / `TradeTick`. `BacktestEngine.add_data` por tipo, "
            "luego `sort_data()`. Venue `BINANCE`, `BookType.L2_MBP`, "
            "`trade_execution=True`, `queue_position=True`, `liquidity_consumption=True`.",
            "",
            "## Reproducción",
            "",
            "```bash",
            "uv sync --extra backtest",
            "uv run python scripts/run_ofi_mm_backtest.py \\",
            f'  --root "{maker.capture}" --report docs/backtest/ofi_mm_results.md',
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
    print(f"running maker post-only on {root} ...", flush=True)
    maker = run_ofi_mm_backtest(
        root,
        exchange=args.exchange,
        symbol=args.symbol.upper(),
        maker_fee=args.maker_fee,
        taker_fee=args.taker_fee,
        spread_ticks=args.spread_ticks,
        ofi_threshold=args.ofi_threshold,
        trade_size=args.trade_size,
        cross_spread=False,
    )
    crossing: BacktestResult | None = None
    if not args.skip_cross:
        print("running crossing variant ...", flush=True)
        crossing = run_ofi_mm_backtest(
            root,
            exchange=args.exchange,
            symbol=args.symbol.upper(),
            maker_fee=args.maker_fee,
            taker_fee=args.taker_fee,
            spread_ticks=args.spread_ticks,
            ofi_threshold=args.ofi_threshold,
            trade_size=args.trade_size,
            cross_spread=True,
        )
    markdown = render_markdown(maker, crossing)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(markdown, encoding="utf-8")
    print(markdown)
    print(f"wrote {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
