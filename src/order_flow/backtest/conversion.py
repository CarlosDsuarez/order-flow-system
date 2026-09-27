"""Engine-agnostic conversion of captured LOB events into nautilus-shaped ops.

Nautilus 1.231 represents L2 as ``OrderBookDelta`` with ``BookAction``
``ADD`` / ``UPDATE`` / ``DELETE`` / ``CLEAR``. This module produces the same
actions as frozen dataclasses so unit tests never import nautilus.

Snapshot → ``CLEAR`` + ``ADD`` per resting level. Incremental ``qty == 0`` →
``DELETE``; ``qty > 0`` → ``UPDATE`` (L2 upsert). Periodic snapshots whose
``last_update_id`` matches the live book are skipped.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel, Side, Trade
from order_flow.storage.parquet import (
    deltas_from_frame,
    read_events,
    snapshots_from_frame,
    trades_from_frame,
)
from order_flow.storage.reconstruct import book_stream

if TYPE_CHECKING:
    from pathlib import Path

# nautilus_trader.model.enums.RecordFlag (v1.231.0)
FLAG_LAST: Final = 128
FLAG_SNAPSHOT: Final = 32


class BookOp(StrEnum):
    """L2 book action matching nautilus ``BookAction`` names."""

    ADD = "add"
    UPDATE = "update"
    DELETE = "delete"
    CLEAR = "clear"


@dataclass(frozen=True, slots=True)
class ConvertedOrder:
    """One L2 price level payload (nautilus ``BookOrder``)."""

    side: str
    price: float
    size: float
    order_id: int


@dataclass(frozen=True, slots=True)
class ConvertedDelta:
    """One nautilus-shaped ``OrderBookDelta`` before the factory materializes it."""

    action: BookOp
    sequence: int
    ts_event_ns: int
    ts_init_ns: int
    flags: int = 0
    order: ConvertedOrder | None = None


@dataclass(frozen=True, slots=True)
class ConvertedTrade:
    """One nautilus-shaped ``TradeTick`` before the factory materializes it."""

    trade_id: int
    price: float
    qty: float
    aggressor: Side
    ts_event_ns: int
    ts_init_ns: int


def l2_order_id(price: float, tick: float) -> int:
    """Deterministic MBP order id: price expressed in ticks (never zero)."""
    if tick <= 0:
        msg = "tick must be > 0"
        raise ValueError(msg)
    return max(1, round(price / tick))


def _level_order(level: PriceLevel, side: str, tick: float) -> ConvertedOrder:
    return ConvertedOrder(
        side=side,
        price=level.price,
        size=level.qty,
        order_id=l2_order_id(level.price, tick),
    )


def _mark_last(ops: list[ConvertedDelta]) -> list[ConvertedDelta]:
    if not ops:
        return ops
    last = ops[-1]
    ops[-1] = replace(last, flags=last.flags | FLAG_LAST)
    return ops


def snapshot_to_ops(snapshot: BookSnapshot, *, tick: float) -> list[ConvertedDelta]:
    """``CLEAR`` plus ``ADD`` for every positive-qty level."""
    ops: list[ConvertedDelta] = [
        ConvertedDelta(
            action=BookOp.CLEAR,
            sequence=snapshot.last_update_id,
            ts_event_ns=snapshot.ts_event_ns,
            ts_init_ns=snapshot.ts_recv_ns,
            flags=FLAG_SNAPSHOT,
        )
    ]
    for level in snapshot.bids:
        if level.qty <= 0:
            continue
        ops.append(
            ConvertedDelta(
                action=BookOp.ADD,
                sequence=snapshot.last_update_id,
                ts_event_ns=snapshot.ts_event_ns,
                ts_init_ns=snapshot.ts_recv_ns,
                order=_level_order(level, "bid", tick),
            )
        )
    for level in snapshot.asks:
        if level.qty <= 0:
            continue
        ops.append(
            ConvertedDelta(
                action=BookOp.ADD,
                sequence=snapshot.last_update_id,
                ts_event_ns=snapshot.ts_event_ns,
                ts_init_ns=snapshot.ts_recv_ns,
                order=_level_order(level, "ask", tick),
            )
        )
    return _mark_last(ops)


def _delta_op(level: PriceLevel, side: str, delta: BookDelta, tick: float) -> ConvertedDelta:
    action = BookOp.DELETE if level.qty == 0 else BookOp.UPDATE
    return ConvertedDelta(
        action=action,
        sequence=delta.final_update_id,
        ts_event_ns=delta.ts_event_ns,
        ts_init_ns=delta.ts_recv_ns,
        order=_level_order(level, side, tick),
    )


def delta_to_ops(delta: BookDelta, *, tick: float) -> list[ConvertedDelta]:
    """Absolute qty 0 → ``DELETE``; qty > 0 → ``UPDATE``."""
    ops: list[ConvertedDelta] = []
    for level in delta.bids:
        ops.append(_delta_op(level, "bid", delta, tick))
    for level in delta.asks:
        ops.append(_delta_op(level, "ask", delta, tick))
    return _mark_last(ops)


def trade_to_print(trade: Trade) -> ConvertedTrade:
    """Copy a public trade; aggressor is already the taker side."""
    return ConvertedTrade(
        trade_id=trade.trade_id,
        price=trade.price,
        qty=trade.qty,
        aggressor=trade.aggressor,
        ts_event_ns=trade.ts_event_ns,
        ts_init_ns=trade.ts_recv_ns,
    )


def capture_to_ops(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    tick: float,
) -> tuple[list[list[ConvertedDelta]], list[ConvertedTrade]]:
    """Load a hive capture and emit book-delta batches plus trades.

    Book events come from :func:`~order_flow.storage.reconstruct.book_stream`: update-id
    order, periodic snapshots of an intact chain skipped (each CLEAR would reset
    nautilus's queue estimate), and a snapshot after any chain break always applied as a
    resync (``CLEAR`` + ``ADD``), so no stale level survives it.
    """
    stream = book_stream(
        snapshots_from_frame(read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)),
        deltas_from_frame(read_events(root, "book_delta", exchange=exchange, symbol=symbol)),
    )
    trades = trades_from_frame(read_events(root, "trade", exchange=exchange, symbol=symbol))
    batches: list[list[ConvertedDelta]] = []
    for event in stream.events:
        ops = (
            snapshot_to_ops(event, tick=tick)
            if isinstance(event, BookSnapshot)
            else delta_to_ops(event, tick=tick)
        )
        if ops:
            batches.append(ops)
    prints = [trade_to_print(trade) for trade in trades if trade.qty > 0]
    return batches, prints
