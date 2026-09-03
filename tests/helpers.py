"""Typed factories for synthetic events used across the test-suite."""

from __future__ import annotations

from collections.abc import Iterable

from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel, Side, Trade

EXCHANGE = "binance_futures"
SYMBOL = "BTCUSDT"
# 2024-09-02T00:00:00Z in nanoseconds; keeps Parquet date partitions predictable.
T0_NS = 1_725_235_200_000_000_000
NS_PER_MS = 1_000_000

Levels = Iterable[tuple[float, float]]


def levels(pairs: Levels) -> tuple[PriceLevel, ...]:
    """Build ``PriceLevel`` tuples from ``(price, qty)`` pairs."""
    return tuple(PriceLevel(price, qty) for price, qty in pairs)


def make_snapshot(
    last_update_id: int = 100,
    bids: Levels = ((100.0, 10.0), (99.0, 5.0)),
    asks: Levels = ((101.0, 8.0), (102.0, 3.0)),
    *,
    ts_event_ns: int = T0_NS,
    symbol: str = SYMBOL,
) -> BookSnapshot:
    """Two-level snapshot: bids 100x10, 99x5; asks 101x8, 102x3 by default."""
    return BookSnapshot(
        exchange=EXCHANGE,
        symbol=symbol,
        ts_event_ns=ts_event_ns,
        ts_recv_ns=ts_event_ns + NS_PER_MS,
        last_update_id=last_update_id,
        bids=levels(bids),
        asks=levels(asks),
    )


def make_delta(
    first_update_id: int,
    final_update_id: int,
    prev_final_update_id: int,
    bids: Levels = (),
    asks: Levels = (),
    *,
    ts_event_ns: int = T0_NS,
    symbol: str = SYMBOL,
) -> BookDelta:
    """Delta with explicit sequence ids and optional level changes."""
    return BookDelta(
        exchange=EXCHANGE,
        symbol=symbol,
        ts_event_ns=ts_event_ns,
        ts_recv_ns=ts_event_ns + NS_PER_MS,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        prev_final_update_id=prev_final_update_id,
        bids=levels(bids),
        asks=levels(asks),
    )


def make_trade(
    trade_id: int,
    price: float,
    qty: float,
    aggressor: Side,
    *,
    ts_event_ns: int = T0_NS,
    symbol: str = SYMBOL,
) -> Trade:
    """Single trade print."""
    return Trade(
        exchange=EXCHANGE,
        symbol=symbol,
        ts_event_ns=ts_event_ns,
        ts_recv_ns=ts_event_ns + NS_PER_MS,
        trade_id=trade_id,
        price=price,
        qty=qty,
        aggressor=aggressor,
    )
