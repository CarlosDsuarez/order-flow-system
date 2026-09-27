"""Typed factories for synthetic events used across the test-suite."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import TYPE_CHECKING

from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel, Side, Trade
from order_flow.ingestion.instruments import InstrumentSpec, write_instrument_spec
from order_flow.storage.parquet import ParquetWriter

if TYPE_CHECKING:
    from pathlib import Path

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


DOGE = "DOGEUSDT"
#: Mid of :func:`write_doge_capture` once both deltas are applied.
DOGE_LAST_MID = (0.12345 + 0.12346) / 2


def doge_spec() -> InstrumentSpec:
    """DOGEUSDT grid from ``GET /fapi/v1/exchangeInfo`` (2026-09-26): tick 1e-5, lot 1."""
    return InstrumentSpec(
        exchange=EXCHANGE,
        symbol=DOGE,
        base_asset="DOGE",
        quote_asset="USDT",
        tick_size=Decimal("0.000010"),
        lot_size=Decimal("1"),
        min_price=Decimal("0.002440"),
        max_price=Decimal("30"),
        min_qty=Decimal("1"),
        max_qty=Decimal("300000000"),
        min_notional=Decimal("5"),
    )


def write_doge_capture(root: Path) -> None:
    """Tiny DOGEUSDT tape (sub-cent prices, qty in thousands) plus its ``instrument.json``."""
    with ParquetWriter(root, EXCHANGE, DOGE) as writer:
        writer.write(
            [
                make_snapshot(
                    last_update_id=100,
                    bids=((0.12344, 50_000.0), (0.12343, 80_000.0)),
                    asks=((0.12347, 40_000.0), (0.12348, 90_000.0)),
                    ts_event_ns=T0_NS,
                    symbol=DOGE,
                ),
                make_delta(
                    101,
                    105,
                    100,
                    bids=((0.12345, 12_000.0),),
                    ts_event_ns=T0_NS + NS_PER_MS,
                    symbol=DOGE,
                ),
                make_trade(
                    1, 0.12345, 1_000.0, Side.SELL, ts_event_ns=T0_NS + 2 * NS_PER_MS, symbol=DOGE
                ),
                make_trade(
                    2, 0.12347, 2_000.0, Side.BUY, ts_event_ns=T0_NS + 3 * NS_PER_MS, symbol=DOGE
                ),
                make_delta(
                    106,
                    110,
                    105,
                    asks=((0.12346, 4_000.0),),
                    ts_event_ns=T0_NS + 4 * NS_PER_MS,
                    symbol=DOGE,
                ),
            ]
        )
    write_instrument_spec(root, doge_spec())
