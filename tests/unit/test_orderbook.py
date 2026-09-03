"""OrderBook: snapshot/delta application, top-of-book metrics and sequence enforcement."""

from __future__ import annotations

import math

import numpy as np
import pytest

from order_flow.ingestion.events import PriceLevel
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import EmptyBookError, SequenceGapError
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot


@pytest.fixture
def book() -> OrderBook:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100))
    return book


def test_snapshot_loads_levels_and_metadata(book: OrderBook) -> None:
    assert (book.exchange, book.symbol) == (EXCHANGE, SYMBOL)
    assert book.last_update_id == 100
    assert book.n_levels == (2, 2)
    assert not book.is_empty
    assert book.best_bid() == PriceLevel(100.0, 10.0)
    assert book.best_ask() == PriceLevel(101.0, 8.0)


def test_snapshot_skips_zero_qty_levels() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(bids=((100.0, 0.0), (99.0, 1.0)), asks=((101.0, 1.0),)))
    assert book.best_bid() == PriceLevel(99.0, 1.0)


def test_top_of_book_metrics(book: OrderBook) -> None:
    assert book.mid_price() == pytest.approx(100.5)
    assert book.spread() == pytest.approx(1.0)
    # (Pa*Qb + Pb*Qa) / (Qa+Qb) = (101*10 + 100*8) / 18
    assert book.microprice() == pytest.approx(1810.0 / 18.0)
    assert book.imbalance() == pytest.approx((10.0 - 8.0) / 18.0)
    assert book.imbalance(levels=2) == pytest.approx((15.0 - 11.0) / 26.0)
    assert not book.is_crossed()


def test_depth_ordering(book: OrderBook) -> None:
    bids, asks = book.depth(5)
    assert [level.price for level in bids] == [100.0, 99.0]
    assert [level.price for level in asks] == [101.0, 102.0]
    with pytest.raises(ValueError, match="levels"):
        book.depth(0)


def test_first_delta_brackets_snapshot_then_updates_levels(book: OrderBook) -> None:
    delta = make_delta(
        95,
        105,
        90,
        bids=((100.0, 12.0), (98.0, 1.0)),
        asks=((101.0, 0.0),),
    )
    assert book.apply_delta(delta) is True
    assert book.last_update_id == 105
    assert book.best_bid() == PriceLevel(100.0, 12.0)
    assert book.best_ask() == PriceLevel(102.0, 3.0)  # 101 removed by qty 0
    assert book.n_levels == (3, 1)
    assert book.ts_event_ns == delta.ts_event_ns


def test_stale_delta_after_snapshot_is_ignored(book: OrderBook) -> None:
    assert book.apply_delta(make_delta(90, 99, 85, bids=((50.0, 1.0),))) is False
    assert book.last_update_id == 100
    assert book.n_levels == (2, 2)


def test_first_delta_that_skips_ahead_raises(book: OrderBook) -> None:
    with pytest.raises(SequenceGapError, match="bracket"):
        book.apply_delta(make_delta(102, 103, 101))


def test_contiguous_delta_right_after_snapshot_is_accepted(book: OrderBook) -> None:
    # Bybit/OKX style: prev id equals the snapshot id even though it does not bracket it.
    assert book.apply_delta(make_delta(101, 103, 100)) is True
    assert book.last_update_id == 103


def test_gap_after_sync_raises(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90))
    book.apply_delta(make_delta(106, 110, 105))
    with pytest.raises(SequenceGapError, match="expected prev_final_update_id=110"):
        book.apply_delta(make_delta(112, 115, 111))


def test_stale_delta_after_sync_also_raises(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90))
    with pytest.raises(SequenceGapError):
        book.apply_delta(make_delta(95, 105, 90))


def test_delta_before_snapshot_raises() -> None:
    with pytest.raises(EmptyBookError, match="apply_snapshot"):
        OrderBook().apply_delta(make_delta(1, 2, 0))


def test_empty_book_behaviour() -> None:
    book = OrderBook()
    assert book.is_empty
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert not book.is_crossed()
    with pytest.raises(EmptyBookError):
        book.mid_price()
    with pytest.raises(EmptyBookError):
        book.imbalance()


def test_one_sided_book(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90, asks=((101.0, 0.0), (102.0, 0.0))))
    assert book.best_ask() is None
    assert book.imbalance() == pytest.approx(1.0)
    with pytest.raises(EmptyBookError):
        book.spread()


def test_crossed_book_detected() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(bids=((101.0, 1.0),), asks=((100.0, 1.0),)))
    assert book.is_crossed()
    assert book.spread() < 0


def test_to_arrays_pads_with_nan(book: OrderBook) -> None:
    arrays = book.to_arrays(3)
    assert arrays.bid_px.shape == (3,)
    np.testing.assert_array_equal(arrays.bid_px[:2], [100.0, 99.0])
    np.testing.assert_array_equal(arrays.ask_qty[:2], [8.0, 3.0])
    assert math.isnan(arrays.bid_px[2])
    assert math.isnan(arrays.ask_qty[2])


def test_instrument_mismatch_rejected(book: OrderBook) -> None:
    with pytest.raises(ValueError, match="ETHUSDT"):
        book.apply_delta(make_delta(95, 105, 90, symbol="ETHUSDT"))


def test_snapshot_resets_sync_state(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90))
    book.apply_snapshot(make_snapshot(last_update_id=200))
    assert book.apply_delta(make_delta(150, 199, 140)) is False  # stale again
    assert book.apply_delta(make_delta(195, 205, 190)) is True


def test_is_synced_empty_is_false() -> None:
    assert OrderBook().is_synced is False


def test_is_synced_after_snapshot_is_true() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100))
    assert book.is_synced is True
    assert book.last_update_ts_ns == T0_NS


def test_is_synced_false_after_gap(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90, ts_event_ns=T0_NS + 1))
    with pytest.raises(SequenceGapError):
        book.apply_delta(make_delta(112, 115, 111))
    assert book.is_synced is False


def test_is_synced_true_after_resnapshot(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90, ts_event_ns=T0_NS + 1))
    with pytest.raises(SequenceGapError):
        book.apply_delta(make_delta(112, 115, 111))
    book.apply_snapshot(make_snapshot(last_update_id=200))
    assert book.is_synced is True


def test_mark_unsynced() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100))
    book.mark_unsynced()
    assert book.is_synced is False


def test_snapshot_after_mark_unsynced_is_true() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100))
    book.mark_unsynced()
    book.apply_snapshot(make_snapshot(last_update_id=200))
    assert book.is_synced is True


def test_depth_at_level_is_one_indexed(book: OrderBook) -> None:
    bid1, ask1 = book.depth_at_level(1)
    assert bid1 == PriceLevel(100.0, 10.0)
    assert ask1 == PriceLevel(101.0, 8.0)
    bid2, ask2 = book.depth_at_level(2)
    assert bid2 == PriceLevel(99.0, 5.0)
    assert ask2 == PriceLevel(102.0, 3.0)
    bid3, ask3 = book.depth_at_level(3)
    assert bid3 is None
    assert ask3 is None
    with pytest.raises(ValueError, match="1-indexed"):
        book.depth_at_level(0)


def test_snapshot_round_trip_restores_levels(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90, bids=((100.0, 12.0),), asks=((101.0, 0.0),)))
    payload = book.snapshot()
    clone = OrderBook()
    clone.apply_snapshot(payload)
    assert clone.best_bid() == book.best_bid()
    assert clone.best_ask() == book.best_ask()
    assert clone.last_update_id == book.last_update_id
    assert clone.n_levels == book.n_levels
    assert clone.depth(2) == book.depth(2)


def test_snapshot_on_empty_book_raises() -> None:
    with pytest.raises(EmptyBookError, match="snapshot"):
        OrderBook().snapshot()


def test_snapshot_of_book_with_no_levels() -> None:
    book = OrderBook()
    book.apply_snapshot(make_snapshot(last_update_id=100, bids=(), asks=()))
    payload = book.snapshot()
    assert payload.bids == ()
    assert payload.asks == ()
    assert payload.last_update_id == 100


def test_qty_zero_removes_level(book: OrderBook) -> None:
    book.apply_delta(make_delta(95, 105, 90, bids=((99.0, 0.0),)))
    bids, _asks = book.depth(5)
    assert [level.price for level in bids] == [100.0]
