"""Optional nautilus 1.231 checks: apply converted deltas; tiny engine smoke.

Skipped unless ``nautilus_trader`` is installed (``uv sync --extra backtest``) or
``RUN_NAUTILUS=1``. User: "plus one test that nautilus can load/apply a few deltas"
and "Optional: smoke backtest on a few seconds of synthetic ticks".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.backtest.conversion import snapshot_to_ops
from order_flow.backtest.nautilus_factory import to_order_book_deltas
from order_flow.ingestion.events import Side
from order_flow.storage.parquet import ParquetWriter
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [
    pytest.mark.nautilus,
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
    pytest.mark.filterwarnings("ignore::UserWarning"),
]

NS = 1_000_000


def test_converted_snapshot_applies_to_nautilus_l2_book() -> None:
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.model.identifiers import InstrumentId

    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    snap = make_snapshot(
        last_update_id=10,
        bids=((100.0, 1.5), (99.9, 2.0)),
        asks=((100.1, 0.8),),
        ts_event_ns=T0_NS,
    )
    batch = to_order_book_deltas(snapshot_to_ops(snap, tick=0.1), iid)
    book = OrderBook(iid, BookType.L2_MBP)
    book.apply_deltas(batch)
    assert float(book.best_bid_price()) == pytest.approx(100.0)
    assert float(book.best_bid_size()) == pytest.approx(1.5)
    assert float(book.best_ask_price()) == pytest.approx(100.1)


def test_qty_zero_delete_removes_nautilus_level() -> None:
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.model.identifiers import InstrumentId

    from order_flow.backtest.conversion import delta_to_ops

    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    snap = make_snapshot(last_update_id=1, ts_event_ns=T0_NS)
    book = OrderBook(iid, BookType.L2_MBP)
    book.apply_deltas(to_order_book_deltas(snapshot_to_ops(snap, tick=0.1), iid))
    delta = make_delta(2, 2, 1, bids=((100.0, 0.0),), ts_event_ns=T0_NS + NS)
    book.apply_deltas(to_order_book_deltas(delta_to_ops(delta, tick=0.1), iid))
    assert book.best_bid_price() is None or float(book.best_bid_price()) == pytest.approx(99.0)


def test_engine_smoke_on_synthetic_capture(tmp_path: Path) -> None:
    from order_flow.backtest.runner import run_ofi_mm_backtest

    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(
            [
                make_snapshot(last_update_id=100, ts_event_ns=T0_NS),
                make_delta(
                    101,
                    105,
                    100,
                    bids=((100.0, 12.0),),
                    ts_event_ns=T0_NS + NS,
                ),
                make_trade(1, 100.0, 0.5, Side.SELL, ts_event_ns=T0_NS + 2 * NS),
                make_trade(2, 101.0, 0.5, Side.BUY, ts_event_ns=T0_NS + 3 * NS),
                make_delta(
                    106,
                    110,
                    105,
                    asks=((101.0, 4.0),),
                    ts_event_ns=T0_NS + 4 * NS,
                ),
            ]
        )
    maker = run_ofi_mm_backtest(tmp_path, cross_spread=False)
    assert maker.n_book_batches >= 1
    assert maker.n_public_trades == 2
    assert maker.nautilus_version
    cross = run_ofi_mm_backtest(tmp_path, cross_spread=True)
    assert cross.cross_spread is True
