"""Streaming OFI/CVD accumulators wrapping the same numpy core as batch.

Callers: pytest. Affected API: ``order_flow.metrics.stream.OfiAccumulator`` /
``CvdAccumulator``. User: unit tests for unsynced skip, first observation,
streaming==batch, CVD ``m=true`` → negative.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from order_flow.ingestion.events import Side
from order_flow.metrics.cvd import compute_cvd
from order_flow.metrics.mlofi import compute_mlofi_events
from order_flow.metrics.ofi import compute_ofi_events
from order_flow.metrics.stream import (
    CvdAccumulator,
    MlofiAccumulator,
    OfiAccumulator,
    VpinAccumulator,
)
from order_flow.metrics.vpin import compute_vpin
from order_flow.orderbook.book import OrderBook
from tests.helpers import make_delta, make_snapshot, make_trade


def test_first_observation_yields_no_e_n() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot())
    acc = OfiAccumulator()
    assert acc.observe_book(book) is None


def test_second_l1_matches_paper_indicator_on_bid_size_up() -> None:
    book = OrderBook()
    book.apply_snapshot(
        make_snapshot(last_update_id=100, bids=((100.0, 10.0),), asks=((101.0, 8.0),))
    )
    acc = OfiAccumulator()
    assert acc.observe_book(book) is None
    book.apply_delta(make_delta(101, 105, 100, bids=((100.0, 12.0),)))
    assert acc.observe_book(book) == 2.0


def test_unsynced_book_updates_are_excluded_and_reset_state() -> None:
    book = OrderBook()
    book.apply_snapshot(
        make_snapshot(last_update_id=100, bids=((100.0, 10.0),), asks=((101.0, 8.0),))
    )
    acc = OfiAccumulator()
    acc.observe_book(book)
    book.apply_delta(make_delta(101, 105, 100, bids=((100.0, 12.0),)))
    assert acc.observe_book(book) == 2.0
    book.mark_unsynced()
    assert acc.observe_book(book) is None
    book.apply_snapshot(
        make_snapshot(last_update_id=200, bids=((100.0, 12.0),), asks=((101.0, 8.0),))
    )
    assert acc.observe_book(book) is None
    book.apply_delta(make_delta(201, 205, 200, bids=((100.0, 15.0),)))
    assert acc.observe_book(book) == 3.0


def test_empty_or_zero_size_bbo_is_skipped() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100, bids=(), asks=((101.0, 8.0),)))
    acc = OfiAccumulator()
    assert acc.observe_book(book) is None
    book.apply_snapshot(
        make_snapshot(last_update_id=101, bids=((100.0, 0.0),), asks=((101.0, 8.0),))
    )
    assert acc.observe_book(book) is None


def test_observe_l1_resets_when_not_synced() -> None:
    acc = OfiAccumulator()
    assert acc.observe_l1(100.0, 10.0, 101.0, 8.0, synced=True) is None
    assert acc.observe_l1(100.0, 12.0, 101.0, 8.0, synced=False) is None
    assert acc.observe_l1(100.0, 12.0, 101.0, 8.0, synced=True) is None


def test_streaming_accumulator_matches_batch_on_fixed_path() -> None:
    states = [
        (100.0, 10.0, 101.0, 8.0),
        (100.0, 12.0, 101.0, 8.0),
        (100.5, 5.0, 101.0, 6.0),
        (100.5, 5.0, 100.8, 4.0),
    ]
    acc = OfiAccumulator()
    streamed: list[float] = []
    for bid_px, bid_qty, ask_px, ask_qty in states:
        event = acc.observe_l1(bid_px, bid_qty, ask_px, ask_qty, synced=True)
        if event is not None:
            streamed.append(event)
    batch = compute_ofi_events(*zip(*states, strict=True))
    np.testing.assert_allclose(streamed, batch)


def test_cvd_binance_m_true_is_negative() -> None:
    acc = CvdAccumulator()
    assert acc.observe_binance_m(1.5, m=True) == -1.5
    assert acc.observe_binance_m(0.5, m=False) == -1.0
    assert acc.total == -1.0


def test_cvd_streaming_matches_batch_on_trade_list() -> None:
    trades = [
        make_trade(1, 100.0, 1.0, Side.BUY),
        make_trade(2, 100.0, 2.0, Side.SELL),
        make_trade(3, 100.0, 3.0, Side.BUY),
    ]
    acc = CvdAccumulator()
    streamed = [acc.observe_trade(trade) for trade in trades]
    qty = [trade.qty for trade in trades]
    signs = [trade.aggressor.sign for trade in trades]
    np.testing.assert_allclose(streamed, compute_cvd(qty, signs))


@given(
    qty=st.lists(
        st.floats(min_value=0.001, max_value=50, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=40,
    ),
    maker=st.lists(st.booleans(), min_size=1, max_size=40),
)
@settings(max_examples=40)
def test_cvd_stream_equals_batch_property(qty: list[float], maker: list[bool]) -> None:
    n = min(len(qty), len(maker))
    qty, maker = qty[:n], maker[:n]
    acc = CvdAccumulator()
    streamed = [acc.observe_binance_m(q, m=flag) for q, flag in zip(qty, maker, strict=True)]
    signs = [-1.0 if flag else 1.0 for flag in maker]
    np.testing.assert_allclose(streamed, compute_cvd(qty, signs))


def test_vpin_accumulator_matches_batch_and_splits_straddle() -> None:
    price = [10.0, 11.0, 12.0]
    qty = [1.5, 1.5, 1.0]
    signs = [1, -1, 1]
    acc = VpinAccumulator(bucket_size=2.0, window=1)
    streamed: list[float] = []
    for p, q, s in zip(price, qty, signs, strict=True):
        streamed.extend(acc.observe(p, q, s))
    batch = compute_vpin(price, qty, 2.0, 1, aggressor=signs)
    np.testing.assert_allclose(streamed, batch)
    assert acc.remainder == pytest.approx(0.0)
    assert np.all((np.asarray(streamed) >= 0.0) & (np.asarray(streamed) <= 1.0))


def test_vpin_accumulator_all_buy_is_one() -> None:
    acc = VpinAccumulator(bucket_size=2.0, window=2)
    emitted: list[float] = []
    for _ in range(8):
        emitted.extend(acc.observe(1.0, 1.0, 1))
    np.testing.assert_allclose(emitted, 1.0)


def test_vpin_accumulator_bvc_with_fixed_sigma_matches_batch() -> None:
    price = [100.0, 101.0, 102.0, 103.0]
    qty = [1.0, 1.0, 1.0, 1.0]
    acc = VpinAccumulator(bucket_size=1.0, window=2, classification="bvc", sigma=1.0)
    streamed: list[float] = []
    for p, q in zip(price, qty, strict=True):
        streamed.extend(acc.observe(p, q))
    batch = compute_vpin(price, qty, 1.0, 2, classification="bvc", sigma=1.0)
    np.testing.assert_allclose(streamed, batch)


def test_mlofi_accumulator_matches_batch_and_level_one_equals_ofi() -> None:
    states = [
        ((100.0, 99.0), (10.0, 5.0), (101.0, 102.0), (8.0, 3.0)),
        ((100.0, 99.0), (12.0, 5.0), (101.0, 102.0), (8.0, 3.0)),
        ((100.5, 100.0), (5.0, 10.0), (101.0, 102.0), (6.0, 3.0)),
    ]
    acc = MlofiAccumulator(levels=2)
    streamed: list[np.ndarray[Any, Any]] = []
    bid_px = np.array([s[0] for s in states], dtype=np.float64)
    bid_qty = np.array([s[1] for s in states], dtype=np.float64)
    ask_px = np.array([s[2] for s in states], dtype=np.float64)
    ask_qty = np.array([s[3] for s in states], dtype=np.float64)
    for i in range(len(states)):
        event = acc.observe_arrays(bid_px[i], bid_qty[i], ask_px[i], ask_qty[i], synced=True)
        if event is not None:
            streamed.append(event)
    batch = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    np.testing.assert_allclose(np.stack(streamed), batch)
    ofi = compute_ofi_events(bid_px[:, 0], bid_qty[:, 0], ask_px[:, 0], ask_qty[:, 0])
    np.testing.assert_allclose(np.stack(streamed)[:, 0], ofi)


def test_mlofi_accumulator_skips_unsynced_and_shallow_levels_are_nan() -> None:
    book = OrderBook()
    book.apply_snapshot(
        make_snapshot(last_update_id=100, bids=((100.0, 10.0),), asks=((101.0, 8.0),))
    )
    acc = MlofiAccumulator(levels=3)
    assert acc.observe_book(book) is None
    book.apply_delta(make_delta(101, 105, 100, bids=((100.0, 12.0),)))
    event = acc.observe_book(book)
    assert event is not None
    assert event.shape == (3,)
    assert event[0] == 2.0
    assert np.isnan(event[1])
    assert np.isnan(event[2])
    book.mark_unsynced()
    assert acc.observe_book(book) is None
