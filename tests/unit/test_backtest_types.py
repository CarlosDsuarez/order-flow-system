"""Backtest domain types: order validation, position accounting, Strategy protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.backtest.types import Fill, Order, OrderSide, OrderType, Position, Strategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.events import Trade
    from order_flow.orderbook.book import OrderBook


def fill(side: OrderSide, qty: float, price: float, fee: float = 0.0) -> Fill:
    return Fill("o1", "BTCUSDT", side, price, qty, ts_ns=0, fee=fee)


def test_order_validation() -> None:
    Order("1", "BTCUSDT", OrderSide.BUY, OrderType.MARKET, 1.0)
    Order("2", "BTCUSDT", OrderSide.SELL, OrderType.POST_ONLY, 1.0, price=100.0)
    with pytest.raises(ValueError, match="require a price"):
        Order("3", "BTCUSDT", OrderSide.BUY, OrderType.LIMIT, 1.0)
    with pytest.raises(ValueError, match="qty"):
        Order("4", "BTCUSDT", OrderSide.BUY, OrderType.MARKET, 0.0)


def test_enum_values() -> None:
    assert OrderSide.BUY.value == "buy"
    assert OrderType.POST_ONLY.value == "post_only"


def test_position_lifecycle() -> None:
    position = Position("BTCUSDT")
    assert position.is_flat

    position.apply_fill(fill(OrderSide.BUY, 2.0, 100.0))
    assert (position.qty, position.avg_price) == (2.0, 100.0)

    position.apply_fill(fill(OrderSide.BUY, 2.0, 110.0))
    assert (position.qty, position.avg_price) == (4.0, 105.0)
    assert position.unrealized_pnl(115.0) == pytest.approx(40.0)

    position.apply_fill(fill(OrderSide.SELL, 1.0, 120.0))
    assert position.qty == 3.0
    assert position.avg_price == 105.0
    assert position.realized_pnl == pytest.approx(15.0)

    position.apply_fill(fill(OrderSide.SELL, 5.0, 90.0))  # closes 3 @ -15 each, flips short 2
    assert position.qty == -2.0
    assert position.avg_price == 90.0
    assert position.realized_pnl == pytest.approx(15.0 - 45.0)

    position.apply_fill(fill(OrderSide.BUY, 2.0, 80.0))  # covers short: +10 each
    assert position.is_flat
    assert position.avg_price == 0.0
    assert position.realized_pnl == pytest.approx(-30.0 + 20.0)


def test_fees_reduce_realized_pnl() -> None:
    position = Position("BTCUSDT")
    position.apply_fill(fill(OrderSide.SELL, 1.0, 100.0, fee=0.5))
    position.apply_fill(fill(OrderSide.BUY, 1.0, 100.0, fee=0.25))
    assert position.fees_paid == pytest.approx(0.75)
    assert position.realized_pnl == pytest.approx(-0.75)


def test_zero_fill_rejected() -> None:
    with pytest.raises(ValueError, match="fill qty"):
        Position("BTCUSDT").apply_fill(fill(OrderSide.BUY, 0.0, 1.0))


class NoOpStrategy:
    def on_book(self, book: OrderBook) -> Sequence[Order]:
        del book
        return ()

    def on_trade(self, trade: Trade) -> Sequence[Order]:
        del trade
        return ()

    def on_fill(self, fill: Fill) -> None:
        del fill


def test_strategy_protocol_is_structural() -> None:
    strategy: Strategy = NoOpStrategy()
    assert isinstance(strategy, Strategy)
    assert not isinstance(object(), Strategy)
