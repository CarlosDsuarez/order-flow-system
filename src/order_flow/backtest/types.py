"""Domain types shared by the future backtest/execution adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.events import Trade
    from order_flow.orderbook.book import OrderBook


class OrderSide(StrEnum):
    """Direction of an order."""

    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    """Execution type. ``POST_ONLY`` is a limit order rejected if it would take liquidity."""

    LIMIT = "limit"
    MARKET = "market"
    POST_ONLY = "post_only"


@dataclass(slots=True, frozen=True)
class Order:
    """An order request emitted by a :class:`Strategy`.

    ``price`` is required for ``LIMIT``/``POST_ONLY`` and ignored for ``MARKET``.
    """

    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    qty: float
    price: float | None = None
    ts_ns: int = 0

    def __post_init__(self) -> None:
        if self.qty <= 0:
            msg = "qty must be > 0"
            raise ValueError(msg)
        if self.order_type is not OrderType.MARKET and self.price is None:
            msg = f"{self.order_type.value} orders require a price"
            raise ValueError(msg)


@dataclass(slots=True, frozen=True)
class Fill:
    """A (partial) execution of an order. ``fee`` is positive when paid, negative if rebated."""

    order_id: str
    symbol: str
    side: OrderSide
    price: float
    qty: float
    ts_ns: int
    fee: float = 0.0
    is_maker: bool = True


@dataclass(slots=True)
class Position:
    """Net position with average-price accounting.

    ``qty`` is signed (``> 0`` long, ``< 0`` short). ``realized_pnl`` is net of fees.
    """

    symbol: str
    qty: float = 0.0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    fees_paid: float = 0.0

    @property
    def is_flat(self) -> bool:
        """``True`` when there is no open quantity."""
        return self.qty == 0.0

    def unrealized_pnl(self, mark_price: float) -> float:
        """Mark-to-market PnL of the open quantity."""
        return (mark_price - self.avg_price) * self.qty

    def apply_fill(self, fill: Fill) -> None:
        """Update quantity, average price and realised PnL with ``fill``.

        Increasing a position updates the volume-weighted average price; reducing it
        realises ``closed_qty * (fill_price - avg_price)`` (sign-adjusted); a fill larger
        than the open quantity flips the position at the fill price.
        """
        if fill.qty <= 0:
            msg = "fill qty must be > 0"
            raise ValueError(msg)
        signed = fill.qty if fill.side is OrderSide.BUY else -fill.qty
        if self.qty == 0.0 or (self.qty > 0) == (signed > 0):
            new_qty = self.qty + signed
            self.avg_price = (self.avg_price * abs(self.qty) + fill.price * abs(signed)) / abs(
                new_qty
            )
            self.qty = new_qty
        else:
            closed = min(abs(signed), abs(self.qty))
            direction = 1.0 if self.qty > 0 else -1.0
            self.realized_pnl += closed * (fill.price - self.avg_price) * direction
            self.qty += signed
            if abs(signed) > closed:
                self.avg_price = fill.price
            elif self.qty == 0.0:
                self.avg_price = 0.0
        self.fees_paid += fill.fee
        self.realized_pnl -= fill.fee


@runtime_checkable
class Strategy(Protocol):
    """Event-driven strategy interface the phase-2 adapters will drive.

    Callbacks return the orders to submit (possibly none); order management (cancels,
    amendments) will be added together with the first engine adapter.
    """

    def on_book(self, book: OrderBook) -> Sequence[Order]:
        """Called after every book update."""
        ...

    def on_trade(self, trade: Trade) -> Sequence[Order]:
        """Called for every public trade."""
        ...

    def on_fill(self, fill: Fill) -> None:
        """Called when one of the strategy's orders is (partially) filled."""
        ...
