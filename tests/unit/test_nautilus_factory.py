"""Factory maps engine-agnostic ops onto nautilus-shaped constructors (injected models).

Callers: pytest. Affected API: ``order_flow.backtest.nautilus_factory``. User:
"Prefer: conversion types in our code; nautilus object construction behind a thin
layer that's mocked in unit tests".
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from order_flow.backtest.conversion import BookOp, ConvertedDelta, ConvertedOrder, ConvertedTrade
from order_flow.backtest.nautilus_factory import (
    btcusdt_perp,
    nautilus_available,
    to_order_book_deltas,
    to_trade_tick,
)
from order_flow.ingestion.events import Side


class _FakeDelta:
    def __init__(self, *args: Any) -> None:
        self.args = args

    @classmethod
    def clear(cls, instrument_id: Any, sequence: int, ts_event: int, ts_init: int) -> _FakeDelta:
        return cls("clear", instrument_id, sequence, ts_event, ts_init)


def _fake_models() -> SimpleNamespace:
    return SimpleNamespace(
        BookAction=SimpleNamespace(ADD="ADD", UPDATE="UPDATE", DELETE="DELETE", CLEAR="CLEAR"),
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        AggressorSide=SimpleNamespace(BUYER="BUYER", SELLER="SELLER"),
        Price=lambda value, precision: ("P", value, precision),
        Quantity=lambda value, precision: ("Q", value, precision),
        BookOrder=lambda side, price, qty, order_id: ("ORD", side, price, qty, order_id),
        OrderBookDelta=_FakeDelta,
        OrderBookDeltas=lambda instrument_id, items: ("BATCH", instrument_id, items),
        TradeTick=lambda *args: ("TRADE", args),
        TradeId=lambda value: ("TID", value),
    )


def test_empty_ops_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        to_order_book_deltas([], "ID", models=_fake_models())


def test_payloadless_non_clear_is_dropped() -> None:
    ops = [
        ConvertedDelta(action=BookOp.UPDATE, sequence=1, ts_event_ns=1, ts_init_ns=1, order=None),
    ]
    with pytest.raises(ValueError, match="produced no deltas"):
        to_order_book_deltas(ops, "ID", models=_fake_models())


def test_snapshot_ops_become_clear_then_adds() -> None:
    ops = [
        ConvertedDelta(action=BookOp.CLEAR, sequence=9, ts_event_ns=10, ts_init_ns=11, flags=32),
        ConvertedDelta(
            action=BookOp.ADD,
            sequence=9,
            ts_event_ns=10,
            ts_init_ns=11,
            flags=128,
            order=ConvertedOrder(side="bid", price=100.0, size=1.5, order_id=1000),
        ),
        ConvertedDelta(
            action=BookOp.DELETE,
            sequence=9,
            ts_event_ns=10,
            ts_init_ns=11,
            flags=0,
            order=ConvertedOrder(side="ask", price=101.0, size=0.0, order_id=1010),
        ),
    ]
    batch = to_order_book_deltas(ops, "BTCUSDT-PERP.BINANCE", models=_fake_models())
    assert batch[0] == "BATCH"
    items = batch[2]
    assert items[0].args[0] == "clear"
    assert items[1].args[1] == "ADD"
    assert items[2].args[1] == "DELETE"
    assert items[1].args[2][1] == "BUY"
    assert items[2].args[2][1] == "SELL"
    assert items[2].args[2][3] == ("Q", 0.0, 3)


def test_trade_tick_maps_aggressor_buyer_and_seller() -> None:
    models = _fake_models()
    buy = ConvertedTrade(
        trade_id=7, price=101.0, qty=0.2, aggressor=Side.BUY, ts_event_ns=1, ts_init_ns=2
    )
    sell = ConvertedTrade(
        trade_id=8, price=99.0, qty=0.3, aggressor=Side.SELL, ts_event_ns=3, ts_init_ns=4
    )
    buy_tick = to_trade_tick(buy, "ID", models=models)
    sell_tick = to_trade_tick(sell, "ID", models=models)
    assert buy_tick[1][3] == "BUYER"
    assert sell_tick[1][3] == "SELLER"
    assert buy_tick[1][4] == ("TID", "7")


def test_missing_nautilus_raises_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("order_flow.backtest.nautilus_factory.nautilus_available", lambda: False)
    with pytest.raises(ImportError, match="uv sync --extra backtest"):
        to_order_book_deltas(
            [ConvertedDelta(action=BookOp.CLEAR, sequence=1, ts_event_ns=1, ts_init_ns=1)],
            "ID",
        )
    with pytest.raises(ImportError, match="uv sync --extra backtest"):
        to_trade_tick(
            ConvertedTrade(
                trade_id=1, price=1.0, qty=0.001, aggressor=Side.BUY, ts_event_ns=1, ts_init_ns=1
            ),
            "ID",
        )
    with pytest.raises(ImportError, match="uv sync --extra backtest"):
        btcusdt_perp(maker_fee=Decimal("0.0002"), taker_fee=Decimal("0.0004"))


def test_nautilus_available_is_bool() -> None:
    assert isinstance(nautilus_available(), bool)
