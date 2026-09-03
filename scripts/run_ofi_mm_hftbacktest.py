"""Run the OFI-skewed MM pipeline on a Parquet capture via hftbacktest.

Requires ``uv sync --extra hftbacktest``. Not a CI test. Same economics as
``scripts/run_ofi_mm_backtest.py`` (OFI from ``OfiAccumulator``, post-only GTX
or crossing GTC). No live execution.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from order_flow.backtest.hft_runner import HftBacktestResult, run_ofi_mm_hftbacktest
from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "backtest" / "ofi_mm_hftbacktest_results.md"
DEFAULT_ROOT = REPO_ROOT / "data" / "qa-audit-15min"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest OFI-skewed one-lot MM via hftbacktest (Spanish report)."
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


def _row(label: str, result: HftBacktestResult) -> str:
    return (
        f"| {label} | {result.total_pnl:.4f} | {result.realized_pnl:.4f} | "
        f"{result.unrealized_pnl:.4f} | {result.fees:.4f} | {result.n_submitted} | "
        f"{result.n_unique_filled} | {result.n_fill_events} | {result.n_maker_fills} | "
        f"{result.n_taker_fills} | {100.0 * result.fill_rate:.2f}% | "
        f"{result.n_canceled} | {result.n_rejected} |"
    )


def _crossing_verdict(maker: HftBacktestResult, crossing: HftBacktestResult) -> str:
    if crossing.total_pnl < maker.total_pnl:
        return (
            "**Crossing empeoró el PnL** respecto al maker post-only en esta muestra "
            "(hipótesis: cruzar el spread mata la estrategia). Sigue sin ser un test "
            "de edge de OFI: es un test de fees + fill model de cola."
        )
    if crossing.total_pnl > maker.total_pnl:
        return (
            "En **esta** muestra el crossing no destruyó el PnL vs maker. No generalices: "
            "un símbolo, cola L2 modelada, no L3."
        )
    return "PnL maker y crossing coinciden (posible muestra demasiado corta o sin fills)."


def render_markdown(maker: HftBacktestResult, crossing: HftBacktestResult | None) -> str:
    duration_s = maker.duration_ns / 1e9
    duration_min = duration_s / 60.0
    header = (
        "| Escenario | PnL total USDT | Realizado | No realizado | Fees | "
        "Órdenes enviadas | Órdenes con fill | Eventos fill | Maker fills | "
        "Taker fills | Fill rate | Canceladas | Rechazadas |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
        " ---: | ---: | ---: |"
    )
    table = [header, _row("Maker post-only (GTX)", maker)]
    if crossing is not None:
        table.append(_row("Crossing (límite agresivo GTC)", crossing))
        verdict = _crossing_verdict(maker, crossing)
    else:
        verdict = "No se corrió el modo agresivo (`--skip-cross`)."
    return "\n".join(
        [
            "# Backtest OFI-MM con cola hftbacktest (pipeline, no una ventaja)",
            "",
            f"Generado: **{datetime.now(tz=UTC).strftime('%Y-%m-%d %H:%M:%SZ')}**.",
            "Script: `scripts/run_ofi_mm_hftbacktest.py`. Motor: **hftbacktest "
            f"{maker.hftbacktest_version}**. Modelo de cola: **{maker.queue_model}**.",
            "",
            "> **Esto no es evidencia de un edge de OFI.** Comparación con nautilus: "
            "[queue_position_comparison.md](queue_position_comparison.md). Limitaciones: "
            "[docs/backtest_limitations.md](../backtest_limitations.md), "
            "[docs/backtest/hftbacktest_queue.md](hftbacktest_queue.md).",
            "",
            "## Captura y parámetros",
            "",
            f"- Directorio: `{maker.capture}`",
            f"- Exchange / símbolo: `{maker.exchange}` / `{maker.symbol}`",
            "- Tick 0.1 / lote 0.001 (igual que nautilus BTCUSDT-PERP)",
            f"- Duración de la serie (min-max `exch_ts` del feed incremental): "
            f"**{duration_s:.1f} s** (~{duration_min:.1f} min)",
            f"- Snapshots in: {maker.n_snapshots_in}; periódicos omitidos: "
            f"{maker.n_snapshots_skipped}; resyncs: {maker.n_resync_snapshots}",
            f"- Eventos incrementales: {maker.n_feed_events}; trades públicos: "
            f"{maker.n_public_trades}; qty=0 dropped: {maker.n_qty0_trades_dropped}",
            f"- Conservación adapter Δ: **{maker.conservation_gap}**",
            f"- Fees: maker {_pct(maker.maker_fee)} / taker {_pct(maker.taker_fee)}",
            f"- Spread: {maker.spread_ticks} ticks alrededor del mid, sesgo OFI "
            f"±{maker.max_skew} tick si `|OFI_1s| > {maker.ofi_threshold}`",
            f"- OFI: suma móvil de `e_n` (Cont et al., `OfiAccumulator`) en "
            f"{maker.ofi_window_ns / 1e9:.0f} s",
            f"- Tamaño: {maker.trade_size} BTC por lado; GTX post-only salvo crossing (GTC)",
            "- Latencia de orden: 0 ns (comparable a nautilus sin `latency_model`)",
            "",
            "OFI positivo ⇒ cotizaciones **arriba**. OFI negativo ⇒ abajo.",
            "",
            "## Resultados",
            "",
            *table,
            "",
            verdict,
            "",
            "## Cómo se alimentó el motor",
            "",
            "Parquet hive → `capture_to_hft_feed` (segundo adapter, **no** "
            "`OrderBookDeltas`): primer snapshot = `initial_snapshot` "
            "(`DEPTH_SNAPSHOT_EVENT`); diffs = `DEPTH_EVENT` (qty 0 borra); "
            "trades = `TRADE_EVENT` (BUY/SELL = agresor). Snapshots periódicos "
            "con el mismo `last_update_id` se omiten para no resetear la cola. "
            "`HashMapMarketDepthBacktest`, `no_partial_fill_exchange`, "
            f"`power_prob_queue_model(2.0)` = {maker.queue_model}.",
            "",
            "## Reproducción",
            "",
            "```bash",
            "uv sync --extra hftbacktest",
            "uv run python scripts/run_ofi_mm_hftbacktest.py \\",
            f'  --root "{maker.capture}" --report docs/backtest/ofi_mm_hftbacktest_results.md',
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
    print(f"running maker post-only (GTX) on {root} ...", flush=True)
    maker = run_ofi_mm_hftbacktest(
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
    crossing: HftBacktestResult | None = None
    if not args.skip_cross:
        print("running crossing variant ...", flush=True)
        crossing = run_ofi_mm_hftbacktest(
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
