"""Check a capture's integrity and write ``quality.json`` next to it.

Exit code 1 when there are issues (broken ``pu`` chain, crossed snapshots, duplicate
trade ids, off-grid prices), so a pipeline can refuse to replay a damaged tape.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from order_flow.ingestion.binance_futures import EXCHANGE
from order_flow.storage.quality import check_capture


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check a capture's integrity and write quality.json next to it."
    )
    parser.add_argument("root", type=Path, help="Capture directory (Parquet root)")
    parser.add_argument("--exchange", default=EXCHANGE)
    parser.add_argument("--symbol", default="BTCUSDT")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.expanduser().resolve()
    if not root.is_dir():
        print(f"capture root not found: {root}", file=sys.stderr)
        return 2
    quality = check_capture(root, exchange=args.exchange, symbol=args.symbol.upper())
    (root / "quality.json").write_text(
        json.dumps(quality.to_dict(), indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"{quality.symbol}: snapshots={quality.n_snapshots} deltas={quality.n_deltas} "
        f"trades={quality.n_trades} epochs={quality.n_epochs}"
    )
    for issue in quality.issues:
        print(f"ISSUE   {issue}")
    for warning in quality.warnings:
        print(f"WARNING {warning}")
    print("OK" if quality.ok else "NOT OK", f"-> {root / 'quality.json'}")
    return 0 if quality.ok else 1


if __name__ == "__main__":
    sys.exit(main())
