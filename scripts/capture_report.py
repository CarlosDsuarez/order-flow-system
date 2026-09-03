"""Summarise a Parquet capture: rates, sizes, temporal gaps, updates/s histogram.

Works on whatever duration is in the directory (5 or 10 minutes). Optional DuckDB
query if the ``analytics`` extra is installed; otherwise polars.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any

from order_flow.ingestion.binance_futures import EXCHANGE
from order_flow.storage.report import (
    DEFAULT_GAP_NS,
    capture_stats,
    format_capture_report,
    updates_per_second_histogram,
)
from order_flow.utils.logging import configure_logging
from order_flow.utils.time import NS_PER_MS, NS_PER_S


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Print rates, file sizes and temporal gaps for a capture directory "
            "(any duration; designed for 5-10 minute Binance L2 recordings)."
        )
    )
    parser.add_argument("root", type=Path, help="Capture directory (Parquet root)")
    parser.add_argument("--exchange", default=EXCHANGE)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--gap-ms",
        type=float,
        default=DEFAULT_GAP_NS / NS_PER_MS,
        help="Gap threshold in milliseconds (default 200, for @depth@100ms)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional markdown output path",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def _duckdb_histogram(root: Path, exchange: str, symbol: str) -> str | None:
    """Optional analytical query. Returns None if DuckDB is not installed."""
    try:
        import duckdb
    except ImportError:
        return None
    pattern = str(
        root / "deltas" / f"exchange={exchange}" / f"symbol={symbol}" / "date=*" / "*.parquet"
    )
    files = list(Path(root).glob(f"deltas/exchange={exchange}/symbol={symbol}/date=*/*.parquet"))
    if not files:
        return "(sin archivos de deltas)"
    con = duckdb.connect()
    frame = con.execute(
        """
        SELECT CAST(ts_event_ns / 1000000000 AS BIGINT) AS second,
               COUNT(*) AS n
        FROM read_parquet(?)
        GROUP BY 1
        ORDER BY 1
        """,
        [pattern],
    ).fetchdf()
    if frame.empty:
        return "(vacío)"
    n = frame["n"]
    return (
        f"DuckDB histogram: seconds={len(frame)} min={int(n.min())} "
        f"median={float(n.median()):.1f} max={int(n.max())} "
        f"mean={float(n.mean()):.2f}"
    )


def _polars_histogram_summary(root: Path, exchange: str, symbol: str) -> str:
    hist = updates_per_second_histogram(root, exchange=exchange, symbol=symbol)
    if hist.height == 0:
        return "polars histogram: (sin deltas)"
    n_vals = [int(v) for v in hist["n"].to_list()]
    return (
        f"polars histogram: seconds={hist.height} min={min(n_vals)} "
        f"median={statistics.median(n_vals)} max={max(n_vals)} "
        f"mean={sum(n_vals) / len(n_vals):.2f}"
    )


def render_report(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    gap_ns: int,
    meta: dict[str, Any] | None = None,
) -> str:
    """Build a Spanish markdown report for ``root``."""
    stats = capture_stats(root, exchange=exchange, symbol=symbol, threshold_ns=gap_ns)
    lines = [
        f"# Informe de captura `{root}`",
        "",
        format_capture_report(stats),
        "",
        "## Histograma de deltas / s",
        "",
        _polars_histogram_summary(root, exchange, symbol),
    ]
    duck = _duckdb_histogram(root, exchange, symbol)
    if duck is None:
        lines.append(
            "DuckDB no instalado (opcional: `uv sync --extra analytics`); "
            "el histograma de arriba usa polars."
        )
    else:
        lines.append(duck)
    duration_s = stats.duration_ns / NS_PER_S if stats.duration_ns else 0.0
    lines.extend(
        [
            "",
            f"Duración en directorio: {duration_s:.1f} s "
            "(el script no exige 10 minutos; usa lo que haya).",
            "",
        ]
    )
    if meta:
        lines.extend(
            [
                "## Meta de la grabación",
                "",
                "```json",
                json.dumps(meta, indent=2, default=str),
                "```",
                "",
            ]
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    if not args.root.exists():
        print(f"capture root not found: {args.root}", file=sys.stderr)
        return 1
    meta_path = args.root / "capture_meta.json"
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    text = render_report(
        args.root,
        exchange=args.exchange,
        symbol=args.symbol.upper(),
        gap_ns=int(args.gap_ms * NS_PER_MS),
        meta=meta,
    )
    print(text)
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
        print(f"wrote {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
