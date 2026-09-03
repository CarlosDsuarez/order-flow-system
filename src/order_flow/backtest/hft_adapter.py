"""Convert a hive Parquet capture into hftbacktest structured arrays.

This is a **second** adapter: nautilus wants ``OrderBookDelta`` batches;
hftbacktest 2.4.4 wants a NumPy structured array
``(ev, exch_ts, local_ts, px, qty, order_id, ival, fval)``. Do not import
``nautilus_trader`` or ``hftbacktest`` here — event flags are copied from
``hftbacktest.types`` (2.4.4) so ``uv sync`` without extras stays green.

Feed layout the library expects (Data Preparation, retrieved 2026-09-02):

- First REST/local snapshot → ``initial_snapshot`` as ``DEPTH_SNAPSHOT_EVENT``
  per positive-qty level (book starts empty).
- Incremental diffs → ``DEPTH_EVENT`` (qty 0 deletes the level).
- Public trades → ``TRADE_EVENT`` with ``BUY_EVENT`` / ``SELL_EVENT`` = taker.
- Periodic snapshots whose ``last_update_id`` already matches the live book
  are skipped (same idea as ``reconstruct_book``). A snapshot with a **new**
  id is a resync: ``DEPTH_CLEAR_EVENT`` then ``DEPTH_SNAPSHOT_EVENT``.

Intentional drops (conservation must account for them): qty-0 trades, qty-0
snapshot levels, empty deltas, duplicate/periodic snapshots.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import numpy as np

from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel, Side, Trade
from order_flow.storage.parquet import (
    deltas_from_frame,
    read_events,
    snapshots_from_frame,
    trades_from_frame,
)

if TYPE_CHECKING:
    from pathlib import Path

    from numpy.typing import NDArray

# Flags copied from hftbacktest 2.4.4 ``hftbacktest.types`` (no extra import).
DEPTH_EVENT: Final = 1
TRADE_EVENT: Final = 2
DEPTH_CLEAR_EVENT: Final = 3
DEPTH_SNAPSHOT_EVENT: Final = 4
EXCH_EVENT: Final = 1 << 31
LOCAL_EVENT: Final = 1 << 30
BUY_EVENT: Final = 1 << 29
SELL_EVENT: Final = 1 << 28
KIND_MASK: Final = 0xFF
_BOTH: Final = EXCH_EVENT | LOCAL_EVENT

EVENT_DTYPE: Final = np.dtype(
    [
        ("ev", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("px", "f8"),
        ("qty", "f8"),
        ("order_id", "u8"),
        ("ival", "i8"),
        ("fval", "f8"),
    ],
    align=True,
)

EventRow = tuple[int, int, int, float, float, int, int, float]


def hftbacktest_available() -> bool:
    """``True`` when the optional ``hftbacktest`` extra is installed."""
    return importlib.util.find_spec("hftbacktest") is not None


@dataclass(frozen=True, slots=True)
class HftFeed:
    """hftbacktest arrays plus the counters a conservation test asserts on."""

    initial_snapshot: NDArray[Any]
    data: NDArray[Any]
    n_snapshots_in: int
    n_deltas_in: int
    n_trades_in: int
    n_initial_snapshots: int
    n_snapshots_skipped: int
    n_resync_snapshots: int
    n_qty0_trades_dropped: int
    n_qty0_snapshot_levels_dropped: int
    n_empty_deltas_dropped: int
    n_feed_depth_events: int
    n_feed_trade_events: int
    n_feed_clear_events: int
    n_feed_snapshot_events: int
    n_delta_levels_in: int
    n_delta_batches_emitted: int
    n_initial_positive_levels: int
    n_resync_positive_levels: int

    def conservation_gap(self) -> int:
        """Absolute unexplained remainder. Zero means in = out + documented drops."""
        snap = self.n_snapshots_in - (
            self.n_initial_snapshots + self.n_snapshots_skipped + self.n_resync_snapshots
        )
        trades = self.n_trades_in - (self.n_feed_trade_events + self.n_qty0_trades_dropped)
        deltas = self.n_deltas_in - (self.n_delta_batches_emitted + self.n_empty_deltas_dropped)
        depth = self.n_delta_levels_in - self.n_feed_depth_events
        initial = self.n_initial_positive_levels - int(self.initial_snapshot.shape[0])
        resync = self.n_resync_positive_levels - self.n_feed_snapshot_events
        return abs(snap) + abs(trades) + abs(deltas) + abs(depth) + abs(initial) + abs(resync)


def _local_ts(exch_ts: int, recv_ts: int) -> int:
    """hftbacktest requires local_ts > exch_ts (Data Validation, 2026-09-02)."""
    return recv_ts if recv_ts > exch_ts else exch_ts + 1


def _row(
    ev: int,
    exch_ts: int,
    recv_ts: int,
    px: float,
    qty: float,
    *,
    order_id: int = 0,
) -> EventRow:
    return (ev, exch_ts, _local_ts(exch_ts, recv_ts), px, qty, order_id, 0, 0.0)


def _side_flag(is_bid: bool) -> int:
    return BUY_EVENT if is_bid else SELL_EVENT


def _positive_levels(levels: tuple[PriceLevel, ...]) -> list[PriceLevel]:
    return [level for level in levels if level.qty > 0]


def _snapshot_level_rows(
    snapshot: BookSnapshot, *, kind: int
) -> tuple[list[EventRow], int, int]:
    """Return (rows, n_positive, n_qty0) for one snapshot's resting levels."""
    rows: list[EventRow] = []
    n_qty0 = 0
    n_positive = 0
    for is_bid, levels in ((True, snapshot.bids), (False, snapshot.asks)):
        for level in levels:
            if level.qty <= 0:
                n_qty0 += 1
                continue
            n_positive += 1
            rows.append(
                _row(
                    kind | _BOTH | _side_flag(is_bid),
                    snapshot.ts_event_ns,
                    snapshot.ts_recv_ns,
                    level.price,
                    level.qty,
                )
            )
    return rows, n_positive, n_qty0


def _resync_rows(snapshot: BookSnapshot) -> tuple[list[EventRow], int, int, int]:
    """CLEAR up to the farthest price, then SNAPSHOT levels. Matches Binance converter."""
    rows: list[EventRow] = []
    n_clear = 0
    n_positive = 0
    n_qty0 = 0
    for is_bid, levels in ((True, snapshot.bids), (False, snapshot.asks)):
        qty0 = sum(1 for level in levels if level.qty <= 0)
        positive = _positive_levels(levels)
        n_qty0 += qty0
        if not positive:
            continue
        farthest = (
            min(level.price for level in positive)
            if is_bid
            else max(level.price for level in positive)
        )
        rows.append(
            _row(
                DEPTH_CLEAR_EVENT | _BOTH | _side_flag(is_bid),
                snapshot.ts_event_ns,
                snapshot.ts_recv_ns,
                farthest,
                0.0,
            )
        )
        n_clear += 1
        for level in positive:
            n_positive += 1
            rows.append(
                _row(
                    DEPTH_SNAPSHOT_EVENT | _BOTH | _side_flag(is_bid),
                    snapshot.ts_event_ns,
                    snapshot.ts_recv_ns,
                    level.price,
                    level.qty,
                )
            )
    return rows, n_clear, n_positive, n_qty0


def _delta_rows(delta: BookDelta) -> list[EventRow]:
    rows: list[EventRow] = []
    for is_bid, levels in ((True, delta.bids), (False, delta.asks)):
        for level in levels:
            rows.append(
                _row(
                    DEPTH_EVENT | _BOTH | _side_flag(is_bid),
                    delta.ts_event_ns,
                    delta.ts_recv_ns,
                    level.price,
                    level.qty,
                )
            )
    return rows


def _trade_row(trade: Trade) -> EventRow:
    side = BUY_EVENT if trade.aggressor is Side.BUY else SELL_EVENT
    return _row(
        TRADE_EVENT | _BOTH | side,
        trade.ts_event_ns,
        trade.ts_recv_ns,
        trade.price,
        trade.qty,
        order_id=max(0, trade.trade_id),
    )


def _kind_rank(ev: int) -> int:
    kind = ev & KIND_MASK
    if kind == TRADE_EVENT:
        return 0
    if kind == DEPTH_EVENT:
        return 1
    if kind == DEPTH_CLEAR_EVENT:
        return 2
    return 3


def _to_array(rows: list[EventRow]) -> NDArray[Any]:
    if not rows:
        return np.empty(0, dtype=EVENT_DTYPE)
    return np.array(rows, dtype=EVENT_DTYPE)


@dataclass
class _BookAcc:
    """Mutable counters while walking snapshot/delta order."""

    initial_rows: list[EventRow]
    feed_rows: list[EventRow]
    last_id: int | None
    n_initial: int = 0
    n_skipped: int = 0
    n_resync: int = 0
    n_qty0_snap: int = 0
    n_empty_deltas: int = 0
    n_delta_levels: int = 0
    n_delta_batches: int = 0
    n_initial_positive: int = 0
    n_resync_positive: int = 0
    n_clear: int = 0
    n_feed_snap: int = 0


def _apply_snapshot(acc: _BookAcc, snapshot: BookSnapshot) -> None:
    if acc.last_id is None:
        rows, n_pos, n_qty0 = _snapshot_level_rows(snapshot, kind=DEPTH_SNAPSHOT_EVENT)
        acc.initial_rows.extend(rows)
        acc.n_initial_positive += n_pos
        acc.n_qty0_snap += n_qty0
        acc.n_initial += 1
        acc.last_id = snapshot.last_update_id
        return
    if snapshot.last_update_id == acc.last_id:
        acc.n_skipped += 1
        return
    rows, n_c, n_pos, n_qty0 = _resync_rows(snapshot)
    acc.feed_rows.extend(rows)
    acc.n_clear += n_c
    acc.n_feed_snap += n_pos
    acc.n_resync_positive += n_pos
    acc.n_qty0_snap += n_qty0
    acc.n_resync += 1
    acc.last_id = snapshot.last_update_id


def _apply_delta(acc: _BookAcc, delta: BookDelta) -> None:
    n_levels = len(delta.bids) + len(delta.asks)
    acc.n_delta_levels += n_levels
    acc.last_id = delta.final_update_id
    if n_levels == 0:
        acc.n_empty_deltas += 1
        return
    acc.feed_rows.extend(_delta_rows(delta))
    acc.n_delta_batches += 1


def _append_trades(feed_rows: list[EventRow], trades: list[Trade]) -> tuple[int, int]:
    n_qty0 = 0
    n_kept = 0
    for trade in trades:
        if trade.qty <= 0:
            n_qty0 += 1
            continue
        feed_rows.append(_trade_row(trade))
        n_kept += 1
    return n_qty0, n_kept


def capture_to_hft_feed(
    root: Path,
    *,
    exchange: str,
    symbol: str,
) -> HftFeed:
    """Load a hive capture and emit hftbacktest ``initial_snapshot`` + incremental ``data``.

    Periodic snapshots that repeat the live ``last_update_id`` are skipped so the
    queue model is not reset every second. Qty-0 trades are dropped (no print).
    """
    snapshots = snapshots_from_frame(
        read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    )
    deltas = deltas_from_frame(read_events(root, "book_delta", exchange=exchange, symbol=symbol))
    trades = trades_from_frame(read_events(root, "trade", exchange=exchange, symbol=symbol))
    if not snapshots:
        msg = "capture has no book snapshots; hftbacktest needs an initial snapshot"
        raise ValueError(msg)

    merged: list[tuple[int, int, int, BookSnapshot | BookDelta]] = []
    merged.extend((snap.ts_event_ns, snap.last_update_id, 1, snap) for snap in snapshots)
    merged.extend((delta.ts_event_ns, delta.final_update_id, 0, delta) for delta in deltas)
    merged.sort()

    acc = _BookAcc(initial_rows=[], feed_rows=[], last_id=None)
    for _ts, _uid, _kind, event in merged:
        if isinstance(event, BookSnapshot):
            _apply_snapshot(acc, event)
        else:
            _apply_delta(acc, event)

    n_qty0_trades, n_trade_events = _append_trades(acc.feed_rows, trades)
    acc.feed_rows.sort(key=lambda row: (row[1], _kind_rank(row[0]), row[2]))
    data = _to_array(acc.feed_rows)
    n_feed_depth = int(np.sum((data["ev"] & KIND_MASK) == DEPTH_EVENT)) if data.size else 0

    return HftFeed(
        initial_snapshot=_to_array(acc.initial_rows),
        data=data,
        n_snapshots_in=len(snapshots),
        n_deltas_in=len(deltas),
        n_trades_in=len(trades),
        n_initial_snapshots=acc.n_initial,
        n_snapshots_skipped=acc.n_skipped,
        n_resync_snapshots=acc.n_resync,
        n_qty0_trades_dropped=n_qty0_trades,
        n_qty0_snapshot_levels_dropped=acc.n_qty0_snap,
        n_empty_deltas_dropped=acc.n_empty_deltas,
        n_feed_depth_events=n_feed_depth,
        n_feed_trade_events=n_trade_events,
        n_feed_clear_events=acc.n_clear,
        n_feed_snapshot_events=acc.n_feed_snap,
        n_delta_levels_in=acc.n_delta_levels,
        n_delta_batches_emitted=acc.n_delta_batches,
        n_initial_positive_levels=acc.n_initial_positive,
        n_resync_positive_levels=acc.n_resync_positive,
    )
