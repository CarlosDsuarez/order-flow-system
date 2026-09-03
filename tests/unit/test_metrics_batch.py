"""Batch adapters: Parquet capture → same OFI/CVD core as streaming.

Callers: pytest, ``scripts/validate_ofi.py``. Affected API:
``ofi_events_from_capture``, ``cvd_from_capture``. User: batch from Parquet
via reconstruct; streaming accumulator == batch on the same event list.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from order_flow.ingestion.events import Side
from order_flow.metrics.batch import (
    cvd_from_capture,
    mlofi_events_from_capture,
    ofi_events_from_capture,
    vpin_from_capture,
)
from order_flow.metrics.cvd import compute_cvd
from order_flow.metrics.mlofi import compute_mlofi_events
from order_flow.metrics.ofi import compute_ofi_events
from order_flow.metrics.stream import CvdAccumulator, MlofiAccumulator, OfiAccumulator
from order_flow.metrics.vpin import compute_vpin
from order_flow.orderbook.book import OrderBook
from order_flow.storage.parquet import ParquetWriter
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

NS = 1_000_000


def test_ofi_from_capture_skips_periodic_snapshot_and_matches_accumulator(tmp_path: Path) -> None:
    rest = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS,
    )
    d1 = make_delta(95, 105, 90, bids=((100.0, 12.0),), ts_event_ns=T0_NS + NS)
    d2 = make_delta(106, 110, 105, asks=((101.0, 6.0),), ts_event_ns=T0_NS + 2 * NS)
    live = OrderBook()
    live.apply_snapshot(rest)
    live.apply_delta(d1)
    periodic = live.snapshot()
    live.apply_delta(d2)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, d1, periodic, d2])

    series = ofi_events_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    acc = OfiAccumulator()
    book = OrderBook()
    book.apply_snapshot(rest)
    assert acc.observe_book(book) is None
    book.apply_delta(d1)
    first = acc.observe_book(book)
    book.apply_delta(d2)
    second = acc.observe_book(book)
    assert first is not None
    assert second is not None
    np.testing.assert_allclose(series.e_n, np.array([first, second], dtype=np.float64))
    batch = compute_ofi_events(series.bid_px, series.bid_qty, series.ask_px, series.ask_qty)
    np.testing.assert_allclose(series.e_n, batch)


def test_cvd_from_capture_uses_stored_aggressor_sign(tmp_path: Path) -> None:
    trades = [
        make_trade(1, 100.0, 1.0, Side.BUY, ts_event_ns=T0_NS),
        make_trade(2, 100.0, 2.0, Side.SELL, ts_event_ns=T0_NS + NS),
    ]
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(trades)
    cvd = cvd_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    np.testing.assert_allclose(cvd, compute_cvd([1.0, 2.0], [1, -1]))
    acc = CvdAccumulator()
    streamed = [acc.observe_trade(trade) for trade in trades]
    np.testing.assert_allclose(cvd, streamed)


def test_cvd_from_empty_capture_is_empty(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([make_snapshot(ts_event_ns=T0_NS)])
    assert cvd_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL).shape == (0,)


def test_ofi_from_empty_bbo_capture_is_empty(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([make_snapshot(bids=(), asks=(), ts_event_ns=T0_NS)])
    series = ofi_events_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert series.e_n.shape == (0,)
    assert series.state_ts_ns.shape == (0,)


def test_vpin_from_capture_matches_core(tmp_path: Path) -> None:
    trades = [
        make_trade(1, 100.0, 1.0, Side.BUY, ts_event_ns=T0_NS),
        make_trade(2, 100.0, 1.0, Side.BUY, ts_event_ns=T0_NS + NS),
        make_trade(3, 100.0, 1.0, Side.SELL, ts_event_ns=T0_NS + 2 * NS),
        make_trade(4, 100.0, 1.0, Side.SELL, ts_event_ns=T0_NS + 3 * NS),
    ]
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(trades)
    vpin = vpin_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, bucket_size=2.0, window=1)
    expected = compute_vpin(
        [100.0, 100.0, 100.0, 100.0],
        [1.0, 1.0, 1.0, 1.0],
        2.0,
        1,
        aggressor=[1, 1, -1, -1],
    )
    np.testing.assert_allclose(vpin, expected)


def test_mlofi_from_capture_level_one_equals_ofi_and_matches_stream(tmp_path: Path) -> None:
    rest = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0), (98.0, 4.0)),
        asks=((101.0, 8.0), (102.0, 3.0), (103.0, 2.0)),
        ts_event_ns=T0_NS,
    )
    d1 = make_delta(95, 105, 90, bids=((100.0, 12.0),), ts_event_ns=T0_NS + NS)
    d2 = make_delta(106, 110, 105, asks=((101.0, 6.0),), ts_event_ns=T0_NS + 2 * NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, d1, d2])

    series = mlofi_events_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, levels=3)
    ofi = ofi_events_from_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    np.testing.assert_allclose(series.e_n[:, 0], ofi.e_n)
    batch = compute_mlofi_events(series.bid_px, series.bid_qty, series.ask_px, series.ask_qty)
    np.testing.assert_allclose(series.e_n, batch)

    acc = MlofiAccumulator(levels=3)
    book = OrderBook()
    book.apply_snapshot(rest)
    assert acc.observe_book(book) is None
    book.apply_delta(d1)
    first = acc.observe_book(book)
    book.apply_delta(d2)
    second = acc.observe_book(book)
    assert first is not None
    assert second is not None
    np.testing.assert_allclose(series.e_n, np.stack([first, second]))
