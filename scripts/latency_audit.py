"""CLI: public Binance USD-M WebSocket latency audit. Does not place orders."""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
from pathlib import Path
from typing import Any

import orjson

from order_flow.ingestion.latency_audit import (
    DEFAULT_N_EVENTS,
    DEFAULT_REST_URL,
    DEFAULT_SYMBOL,
    DEFAULT_WS_URL,
    GAP_THRESHOLD_NS,
    MACHINE_CONTEXT,
    OFFSET_SAMPLES_PER_PHASE,
    format_institutional_table,
    format_latency_report_md,
    results_to_jsonable,
    run_latency_audit,
)
from order_flow.utils.logging import configure_logging
from order_flow.utils.time import NS_PER_MS

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "docs" / "latency" / "latency_audit_results.md"
DEFAULT_JSON = REPO_ROOT / "docs" / "latency" / "latency_audit_results.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Measure end-to-end latency of Binance USD-M public market-data WebSocket "
            "(depth@100ms + optional @trade). Does not place orders. Clock offset is "
            "sampled via GET /fapi/v1/time (offset = local - server)."
        )
    )
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="USD-M perpetual symbol (default: %(default)s)",
    )
    parser.add_argument(
        "--n-events",
        type=int,
        default=DEFAULT_N_EVENTS,
        help="Number of consecutive depth WebSocket messages to collect (default: 10000)",
    )
    parser.add_argument(
        "--no-trades",
        action="store_true",
        help="Subscribe only to @depth@100ms (default also samples @trade)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
        help="Markdown sidecar path",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=DEFAULT_JSON,
        help="JSON sidecar path",
    )
    parser.add_argument(
        "--offset-samples",
        type=int,
        default=OFFSET_SAMPLES_PER_PHASE,
        help="GET /fapi/v1/time samples per phase (start/mid/end)",
    )
    parser.add_argument(
        "--gap-threshold-ms",
        type=float,
        default=GAP_THRESHOLD_NS / NS_PER_MS,
        help="Inter-arrival gap threshold in milliseconds",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=None,
        help="Wall-clock cap (default: ~0.3 s per depth event + 120 s slack)",
    )
    parser.add_argument("--rest-url", default=DEFAULT_REST_URL)
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL)
    parser.add_argument("--log-level", default="INFO")
    return parser


def _write_sidecars(payload: dict[str, Any], report_path: Path, json_path: Path) -> str:
    markdown = format_latency_report_md(payload)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(markdown, encoding="utf-8")
    json_path.write_bytes(orjson.dumps(results_to_jsonable(payload), option=orjson.OPT_INDENT_2))
    return markdown


async def _run(args: argparse.Namespace) -> int:
    if args.n_events < 1:
        print("n-events must be >= 1", file=sys.stderr)
        return 2
    payload = await run_latency_audit(
        symbol=args.symbol,
        n_events=args.n_events,
        rest_url=args.rest_url,
        ws_url=args.ws_url,
        include_trades=not args.no_trades,
        offset_samples=args.offset_samples,
        gap_threshold_ns=int(args.gap_threshold_ms * NS_PER_MS),
        max_seconds=args.max_seconds,
        hostname=socket.gethostname(),
        machine_context=MACHINE_CONTEXT,
    )
    markdown = _write_sidecars(payload, args.report, args.json_out)
    print(markdown)
    print()
    print("Comparación institucional (también en el markdown):")
    print(format_institutional_table())
    print()
    print(f"wrote {args.report}")
    print(f"wrote {args.json_out}")
    n_depth = int(payload.get("n_depth") or 0)
    if n_depth < args.n_events:
        print(
            f"incomplete: n_depth={n_depth} < n_events={args.n_events}",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
