"""CLI: 60s honesty run against Binance USD-M L2. Does not write market-data Parquet."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from order_flow.ingestion.live import (
    format_live_report_md,
    live_duration_s,
    run_live_validation,
    write_live_report,
)
from order_flow.utils.logging import configure_logging

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "ingestion" / "live-validation.md"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Binance USD-M L2 feed for N seconds and print an honesty report."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="Duration (default: BINANCE_LIVE_SECONDS or 60)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
        help="Markdown report path (Spanish)",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--json", action="store_true", help="Also print the structured dict")
    return parser


async def _run(symbol: str, seconds: float | None, report_path: Path, as_json: bool) -> int:
    duration = live_duration_s() if seconds is None else seconds
    result = await run_live_validation(symbol=symbol, duration_s=duration)
    write_live_report(result, report_path)
    print(format_live_report_md(result))
    if as_json:
        print(json.dumps(result, indent=2, default=str))
    print(f"wrote {report_path}")
    return 1 if result.get("error") else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    return asyncio.run(_run(args.symbol, args.seconds, args.report, args.json))


if __name__ == "__main__":
    sys.exit(main())
