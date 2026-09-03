"""Summaries of a Parquet capture: rates, sizes, and temporal gaps."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from order_flow.storage.parquet import PARTITION_DIR, read_events
from order_flow.utils.time import NS_PER_S

if TYPE_CHECKING:
    from order_flow.ingestion.events import EventType

DEFAULT_GAP_NS: int = 200_000_000  # 200 ms; @depth@100ms should tick ~every 100 ms
Clock = Literal["event", "recv"]


@dataclass(frozen=True, slots=True)
class TimeGap:
    """A hole between consecutive depth events on one clock."""

    clock: Clock
    prev_ts_ns: int
    ts_ns: int
    gap_ns: int


@dataclass(frozen=True, slots=True)
class CaptureStats:
    """Aggregate counters for one capture directory (one exchange/symbol)."""

    exchange: str
    symbol: str
    n_snapshots: int
    n_deltas: int
    n_trades: int
    duration_ns: int
    deltas_per_s: float
    trades_per_s: float
    bytes_total: int
    bytes_snapshots: int
    bytes_deltas: int
    bytes_trades: int
    n_event_gaps: int
    n_recv_gaps: int
    gaps: tuple[TimeGap, ...]
    min_ts_event_ns: int | None
    max_ts_event_ns: int | None


def _kind_bytes(root: Path, event_type: EventType, exchange: str, symbol: str) -> int:
    kind = PARTITION_DIR[event_type]
    files = list(Path(root).glob(f"{kind}/exchange={exchange}/symbol={symbol}/date=*/*.parquet"))
    return sum(path.stat().st_size for path in files)


def detect_gaps(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    threshold_ns: int = DEFAULT_GAP_NS,
) -> list[TimeGap]:
    """Gaps between consecutive *deltas* whose clock delta exceeds ``threshold_ns``.

    Event-time gaps use ``ts_event_ns`` (exchange ``E``). Recv-time gaps use
    ``ts_recv_ns``. A quiet market can produce event-time holes; recv-time holes
    usually mean the local capture stalled.
    """
    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    if deltas.height <= 1:
        return []
    gaps: list[TimeGap] = []
    event_ts = deltas["ts_event_ns"].to_list()
    recv_ts = deltas["ts_recv_ns"].to_list()
    for clock, series in (("event", event_ts), ("recv", recv_ts)):
        typed_clock: Clock = "event" if clock == "event" else "recv"
        for prev, cur in pairwise(series):
            gap = int(cur) - int(prev)
            if gap > threshold_ns:
                gaps.append(
                    TimeGap(
                        clock=typed_clock,
                        prev_ts_ns=int(prev),
                        ts_ns=int(cur),
                        gap_ns=gap,
                    )
                )
    return gaps


def _rate(count: int, duration_ns: int) -> float:
    if duration_ns <= 0:
        return 0.0
    return count / (duration_ns / NS_PER_S)


def capture_stats(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    threshold_ns: int = DEFAULT_GAP_NS,
) -> CaptureStats:
    """Count events, bytes and gaps for ``exchange``/``symbol`` under ``root``."""
    snapshots = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    trades = read_events(root, "trade", exchange=exchange, symbol=symbol)
    ts_cols: list[pl.Series] = []
    for frame in (snapshots, deltas, trades):
        if frame.height:
            ts_cols.append(frame["ts_event_ns"])
    min_ts: int | None
    max_ts: int | None
    if ts_cols:
        all_ts = pl.concat(ts_cols)
        ts_list = [int(value) for value in all_ts.to_list()]
        min_ts = min(ts_list)
        max_ts = max(ts_list)
        duration_ns = max_ts - min_ts
    else:
        min_ts = None
        max_ts = None
        duration_ns = 0
    gaps = tuple(detect_gaps(root, exchange=exchange, symbol=symbol, threshold_ns=threshold_ns))
    bytes_snapshots = _kind_bytes(root, "book_snapshot", exchange, symbol)
    bytes_deltas = _kind_bytes(root, "book_delta", exchange, symbol)
    bytes_trades = _kind_bytes(root, "trade", exchange, symbol)
    return CaptureStats(
        exchange=exchange,
        symbol=symbol,
        n_snapshots=snapshots.height,
        n_deltas=deltas.height,
        n_trades=trades.height,
        duration_ns=duration_ns,
        deltas_per_s=_rate(deltas.height, duration_ns),
        trades_per_s=_rate(trades.height, duration_ns),
        bytes_total=bytes_snapshots + bytes_deltas + bytes_trades,
        bytes_snapshots=bytes_snapshots,
        bytes_deltas=bytes_deltas,
        bytes_trades=bytes_trades,
        n_event_gaps=sum(1 for gap in gaps if gap.clock == "event"),
        n_recv_gaps=sum(1 for gap in gaps if gap.clock == "recv"),
        gaps=gaps,
        min_ts_event_ns=min_ts,
        max_ts_event_ns=max_ts,
    )


def updates_per_second_histogram(
    root: Path,
    *,
    exchange: str,
    symbol: str,
) -> pl.DataFrame:
    """Count of depth deltas per UTC second (polars; DuckDB is optional in the CLI)."""
    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    if deltas.height == 0:
        return pl.DataFrame(
            {"second": pl.Series(dtype=pl.Int64()), "n": pl.Series(dtype=pl.UInt32())}
        )
    return (
        deltas.with_columns((pl.col("ts_event_ns") // NS_PER_S).alias("second"))
        .group_by("second")
        .len()
        .rename({"len": "n"})
        .sort("second")
    )


def format_capture_report(stats: CaptureStats) -> str:
    """Spanish markdown fragment for a capture directory."""
    duration_s = stats.duration_ns / NS_PER_S if stats.duration_ns else 0.0
    return "\n".join(
        [
            f"- Símbolo: `{stats.symbol}` (`{stats.exchange}`)",
            f"- Duración (event time): {duration_s:.1f} s",
            f"- Snapshots: {stats.n_snapshots}",
            f"- Deltas: {stats.n_deltas} ({stats.deltas_per_s:.2f} / s)",
            f"- Trades: {stats.n_trades} ({stats.trades_per_s:.2f} / s)",
            f"- Tamaño total: {stats.bytes_total} bytes "
            f"(snapshots {stats.bytes_snapshots}, deltas {stats.bytes_deltas}, "
            f"trades {stats.bytes_trades})",
            f"- Huecos event-time (>200 ms): {stats.n_event_gaps}",
            f"- Huecos recv-time (>200 ms): {stats.n_recv_gaps}",
        ]
    )
