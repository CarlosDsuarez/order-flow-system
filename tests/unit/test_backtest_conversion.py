"""Convert our Parquet LOB events into engine-agnostic nautilus-shaped ops.

Callers: pytest. Affected API: ``order_flow.backtest.conversion``. A missing
``CLEAR`` on snapshot or a qty-0 level that is not ``DELETE`` must fail these tests.
User: "Adapter: synthetic snapshot+deltas → nautilus objects; qty 0 → Delete;
snapshot → Clear+Adds" and "unit-test the conversion on a tiny synthetic capture".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.backtest.conversion import (
    FLAG_LAST,
    FLAG_SNAPSHOT,
    BookOp,
    capture_to_ops,
    delta_to_ops,
    l2_order_id,
    snapshot_to_ops,
    trade_to_print,
)
from order_flow.ingestion.events import Side
from order_flow.storage.parquet import ParquetWriter
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

TICK = 0.1
NS = 1_000_000


def test_l2_order_id_rejects_nonpositive_tick() -> None:
    with pytest.raises(ValueError, match="tick"):
        l2_order_id(100.0, 0.0)


def test_snapshot_starts_with_clear_then_adds() -> None:
    snap = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS,
    )
    ops = snapshot_to_ops(snap, tick=TICK)
    assert ops[0].action is BookOp.CLEAR
    assert ops[0].flags & FLAG_SNAPSHOT
    assert ops[0].order is None
    adds = ops[1:]
    assert {op.action for op in adds} == {BookOp.ADD}
    assert all(op.order is not None and op.order.size > 0 for op in adds)
    prices = sorted(op.order.price for op in adds if op.order is not None)
    assert prices == [99.0, 100.0, 101.0, 102.0]
    assert ops[-1].flags & FLAG_LAST
    assert ops[0].sequence == 100


def test_qty_zero_delta_is_delete() -> None:
    delta = make_delta(
        101,
        105,
        100,
        bids=((100.0, 0.0),),
        asks=((101.0, 4.0),),
        ts_event_ns=T0_NS + NS,
    )
    ops = delta_to_ops(delta, tick=TICK)
    by_price = {op.order.price: op for op in ops if op.order is not None}
    assert by_price[100.0].action is BookOp.DELETE
    assert by_price[101.0].action is BookOp.UPDATE
    assert by_price[101.0].order is not None
    assert by_price[101.0].order.size == 4.0
    assert ops[-1].flags & FLAG_LAST
    assert ops[-1].sequence == 105


def test_snapshot_skips_zero_qty_levels() -> None:
    snap = make_snapshot(
        last_update_id=1,
        bids=((100.0, 1.0), (99.0, 0.0)),
        asks=((101.0, 1.0),),
        ts_event_ns=T0_NS,
    )
    ops = snapshot_to_ops(snap, tick=TICK)
    prices = [op.order.price for op in ops if op.order is not None]
    assert 99.0 not in prices


def test_empty_delta_yields_no_ops() -> None:
    delta = make_delta(101, 102, 100, ts_event_ns=T0_NS)
    assert delta_to_ops(delta, tick=TICK) == []


def test_trade_print_carries_aggressor_and_ids() -> None:
    trade = make_trade(7, 101.0, 0.5, Side.BUY, ts_event_ns=T0_NS)
    printed = trade_to_print(trade)
    assert printed.price == 101.0
    assert printed.qty == 0.5
    assert printed.aggressor is Side.BUY
    assert printed.trade_id == 7
    assert printed.ts_event_ns == T0_NS


def test_capture_skips_periodic_snapshot_with_same_update_id(tmp_path: Path) -> None:
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    delta = make_delta(
        95,
        105,
        90,
        bids=((100.0, 12.0),),
        ts_event_ns=T0_NS + NS,
    )
    periodic = make_snapshot(
        last_update_id=105,
        bids=((100.0, 12.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS + 2 * NS,
    )
    resync = make_snapshot(
        last_update_id=300,
        bids=((100.5, 1.0),),
        asks=((100.6, 1.0),),
        ts_event_ns=T0_NS + 3 * NS,
    )
    trade = make_trade(1, 101.0, 0.01, Side.SELL, ts_event_ns=T0_NS + NS + 1)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, delta, periodic, resync, trade])

    book_batches, trades = capture_to_ops(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, tick=TICK)
    sequences = [batch[0].sequence for batch in book_batches]
    assert sequences == [100, 105, 300]
    assert book_batches[0][0].action is BookOp.CLEAR
    assert book_batches[1][0].action is BookOp.UPDATE
    assert book_batches[2][0].action is BookOp.CLEAR
    assert len(trades) == 1
    assert trades[0].trade_id == 1


def test_capture_drops_zero_qty_trades(tmp_path: Path) -> None:
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    zero = make_trade(2, 101.0, 0.0, Side.BUY, ts_event_ns=T0_NS + NS)
    ok = make_trade(3, 101.0, 0.001, Side.SELL, ts_event_ns=T0_NS + 2 * NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, zero, ok])
    _batches, trades = capture_to_ops(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, tick=TICK)
    assert [trade.trade_id for trade in trades] == [3]
