"""Immutable market-data event types shared by every layer.

Prices and quantities are ``float`` in phase 1. Exchanges send decimal strings and
parsing the same string always yields the same float, so price keys match between a
snapshot and the deltas that follow it. The roadmap migrates to integer ticks/lots.

Timestamps are integer nanoseconds since the Unix epoch (UTC): ``ts_event_ns`` is the
exchange-side time, ``ts_recv_ns`` the local receive time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, Literal

EventType = Literal["book_snapshot", "book_delta", "trade"]


class Side(StrEnum):
    """Order book side.

    ``BUY`` and ``SELL`` are aliases of ``BID`` and ``ASK`` used for trade aggressors:
    a trade whose *taker* was a buyer has ``aggressor is Side.BUY`` (``== Side.BID``).
    """

    BID = "bid"
    ASK = "ask"
    BUY = "bid"
    SELL = "ask"

    @property
    def sign(self) -> int:
        """``+1`` for bid/buy, ``-1`` for ask/sell (the CVD/VPIN sign convention)."""
        return 1 if self is Side.BID else -1

    @classmethod
    def from_sign(cls, sign: float) -> Side:
        """Inverse of :attr:`sign`: positive values map to ``BID``, negative to ``ASK``."""
        if sign == 0:
            msg = "sign must be non-zero"
            raise ValueError(msg)
        return cls.BID if sign > 0 else cls.ASK


@dataclass(slots=True, frozen=True)
class PriceLevel:
    """A single L2 price level (aggregate resting quantity at ``price``)."""

    price: float
    qty: float


@dataclass(slots=True, frozen=True)
class BookSnapshot:
    """Full L2 state of one instrument at ``last_update_id``.

    ``bids`` are sorted best (highest) first and ``asks`` best (lowest) first when they
    come from an exchange snapshot; :class:`~order_flow.orderbook.book.OrderBook` does
    not rely on that ordering.
    """

    EVENT_TYPE: ClassVar[EventType] = "book_snapshot"

    exchange: str
    symbol: str
    ts_event_ns: int
    ts_recv_ns: int
    last_update_id: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


@dataclass(slots=True, frozen=True)
class BookDelta:
    """Incremental L2 update covering update ids ``first_update_id..final_update_id``.

    Each level carries the *absolute* new quantity; ``qty == 0`` removes the level.
    ``prev_final_update_id`` is the ``final_update_id`` of the previous delta and is
    used to detect gaps (Binance ``pu``, OKX ``prevSeqId``; Bybit implies ``u - 1``).
    """

    EVENT_TYPE: ClassVar[EventType] = "book_delta"

    exchange: str
    symbol: str
    ts_event_ns: int
    ts_recv_ns: int
    first_update_id: int
    final_update_id: int
    prev_final_update_id: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


@dataclass(slots=True, frozen=True)
class Trade:
    """A (possibly aggregated) trade print.

    ``aggressor`` is the taker side: ``Side.BUY`` when the buyer lifted the ask,
    ``Side.SELL`` when the seller hit the bid. Crypto venues publish this flag directly
    (Binance ``m`` = "buyer is maker", i.e. the aggressor is the seller).
    """

    EVENT_TYPE: ClassVar[EventType] = "trade"

    exchange: str
    symbol: str
    ts_event_ns: int
    ts_recv_ns: int
    trade_id: int
    price: float
    qty: float
    aggressor: Side


MarketEvent = BookSnapshot | BookDelta | Trade
"""Union of every event a :class:`~order_flow.ingestion.base.MarketDataFeed` can yield."""
