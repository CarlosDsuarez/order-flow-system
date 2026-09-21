"""Offline alignment audit of REST probes vs a Parquet tape (no network).

See :mod:`order_flow.storage.probe_audit`. Exit code is 1 when any alignable
(non-stale) probe exceeds ``--warn`` mismatch rate: a breach is a failure,
not "residual race".
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from order_flow.storage.probe_audit import run_probe_audit


def build_parser() -> argparse.ArgumentParser:
    """CLI for the offline probe-alignment audit."""
    parser = argparse.ArgumentParser(
        description="Align REST probes to a Parquet tape by lastUpdateId and compare top-N."
    )
    parser.add_argument("--tape", type=Path, required=True, help="Parquet capture root")
    parser.add_argument(
        "--probes",
        type=Path,
        default=None,
        help="rest_probes.jsonl path (default: <tape>/rest_probes.jsonl)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSONL path (default: <tape>/probe_alignment.jsonl)",
    )
    parser.add_argument("--exchange", default="binance_futures")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--levels", type=int, default=20)
    parser.add_argument(
        "--warn",
        type=float,
        default=0.10,
        help="Fail (exit 1) if any alignable probe exceeds this mismatch rate",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point: audit probes, exit 1 on any alignable breach of ``--warn``."""
    args = build_parser().parse_args(argv)
    tape: Path = args.tape
    probes_path = args.probes if args.probes is not None else tape / "rest_probes.jsonl"
    out_path = args.out if args.out is not None else tape / "probe_alignment.jsonl"
    return run_probe_audit(
        tape=tape,
        probes_path=probes_path,
        out_path=out_path,
        exchange=args.exchange,
        symbol=args.symbol,
        levels=args.levels,
        warn=args.warn,
    )


if __name__ == "__main__":
    sys.exit(main())
