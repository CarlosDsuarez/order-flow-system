"""Replays across a resync must end on the new snapshot's book, with no stale levels.

Pattern seen in data/live-btcusdt-45min and data/mi-captura: after a gap the feed
takes a REST snapshot and the first diff that brackets its ``lastUpdateId``. The REST
``E`` is the response time, so it is often *later* than that diff's ``E``. Ordering by
event time put the diff on the stale pre-gap book; once periodic snapshots stopped
emitting CLEAR every second (QA H2), nothing healed it: 650 of 1957 periodic snapshots
disagreed with the nautilus replay of the 45-minute capture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from order_flow.backtest.conversion import BookOp, capture_to_ops
from order_flow.backtest.hft_adapter import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    KIND_MASK,
    TRADE_EVENT,
    capture_to_hft_feed,
)
from order_flow.orderbook.book import OrderBook
from order_flow.storage import reconstruct
from order_flow.storage.parquet import ParquetWriter, snapshots_from_frame
from order_flow.storage.reconstruct import iter_l1_ticks
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot

if TYPE_CHECKING:
    from pathlib import Path

    import numpy.typing as npt
    import polars as pl

    from order_flow.backtest.conversion import ConvertedDelta
    from order_flow.ingestion.events import BookSnapshot, MarketEvent

NS = 1_000_000
Book = tuple[dict[float, float], dict[float, float]]
#: Book after the resync snapshot and its bracketing diff (bid 100.5 moved 1.0 -> 2.0).
AFTER_RESYNC: Book = ({100.5: 2.0}, {100.6: 1.0})


def write_resync_capture(root: Path, *, rest_u: int) -> None:
    """Pre-gap book, a gap, then REST ``rest_u`` stamped 1 ms after its bracketing diff."""
    book = OrderBook(exchange=EXCHANGE, symbol=SYMBOL)
    first = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS,
    )
    before_gap = make_delta(95, 105, 90, bids=((100.0, 12.0),), ts_event_ns=T0_NS + NS)
    book.apply_snapshot(first)
    book.apply_delta(before_gap)
    events: list[MarketEvent] = [first, before_gap, book.snapshot()]

    rest_bid = 1.0 if rest_u < 305 else 2.0  # a REST id at 305 already includes the diff
    rest = make_snapshot(
        last_update_id=rest_u,
        bids=((100.5, rest_bid),),
        asks=((100.6, 1.0),),
        ts_event_ns=T0_NS + 6 * NS,
    )
    bracketing = make_delta(299, 305, 290, bids=((100.5, 2.0),), ts_event_ns=T0_NS + 5 * NS)
    book.apply_snapshot(rest)
    book.apply_delta(bracketing)
    events += [rest, bracketing, book.snapshot()]  # record_l2 also snapshots its book
    with ParquetWriter(root, EXCHANGE, SYMBOL) as writer:
        writer.write(events)


def replay_ops(batches: list[list[ConvertedDelta]]) -> Book:
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    for ops in batches:
        for op in ops:
            if op.action is BookOp.CLEAR:
                bids, asks = {}, {}
            elif op.order is not None:
                side = bids if op.order.side == "bid" else asks
                if op.action is BookOp.DELETE:
                    side.pop(op.order.price, None)
                else:
                    side[op.order.price] = op.order.size
    return bids, asks


def replay_hft(initial: npt.NDArray[Any], data: npt.NDArray[Any]) -> Book:
    """hftbacktest semantics: CLEAR wipes from the touch out to ``px``; qty 0 deletes."""
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    for row in [*initial, *data]:
        kind = int(row["ev"]) & KIND_MASK
        if kind == TRADE_EVENT:
            continue
        is_bid = bool(int(row["ev"]) & BUY_EVENT)
        side = bids if is_bid else asks
        px, qty = float(row["px"]), float(row["qty"])
        if kind == DEPTH_CLEAR_EVENT:
            for price in [p for p in side if (p >= px if is_bid else p <= px)]:
                del side[price]
        elif qty <= 0:
            side.pop(px, None)
        else:
            side[px] = qty
    return bids, asks


@pytest.mark.parametrize("rest_u", [300, 305], ids=["rest-inside-diff", "rest-equals-diff-u"])
def test_nautilus_ops_end_on_the_resynced_book(tmp_path: Path, rest_u: int) -> None:
    write_resync_capture(tmp_path, rest_u=rest_u)
    batches, _ = capture_to_ops(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, tick=0.1)
    assert replay_ops(batches) == AFTER_RESYNC
    ts_init = [batch[0].ts_init_ns for batch in batches]
    assert ts_init == sorted(ts_init)  # nautilus re-sorts by ts_init


@pytest.mark.parametrize("rest_u", [300, 305], ids=["rest-inside-diff", "rest-equals-diff-u"])
def test_hft_feed_ends_on_the_resynced_book(tmp_path: Path, rest_u: int) -> None:
    write_resync_capture(tmp_path, rest_u=rest_u)
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.conservation_gap() == 0
    assert replay_hft(feed.initial_snapshot, feed.data) == AFTER_RESYNC


@pytest.mark.parametrize("rest_u", [300, 305], ids=["rest-inside-diff", "rest-equals-diff-u"])
def test_reconstructed_l1_ends_on_the_resynced_book(tmp_path: Path, rest_u: int) -> None:
    write_resync_capture(tmp_path, rest_u=rest_u)
    ticks = iter_l1_ticks(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    last = ticks[-1]
    assert (last.bid_px, last.bid_qty, last.ask_px, last.ask_qty) == (100.5, 2.0, 100.6, 1.0)
    assert [tick.epoch for tick in ticks][-1] == 1
    stamps = [tick.ts_event_ns for tick in ticks]
    assert stamps == sorted(stamps)


def test_rest_snapshot_is_applied_before_its_same_u_diff_to_keep_deep_levels(
    tmp_path: Path,
) -> None:
    # Seen at the start of data/mi-captura: REST (limit=1000) and the diff bracketing it
    # share u. The diff carries levels deeper than the REST depth; the feed applied REST
    # then diff, so they live in the book. Dropping the "already included" diff lost them.
    book = OrderBook(exchange=EXCHANGE, symbol=SYMBOL)
    rest = make_snapshot(
        last_update_id=305, bids=((100.5, 2.0),), asks=((100.6, 1.0),), ts_event_ns=T0_NS + 2
    )
    bracketing = make_delta(299, 305, 290, bids=((100.5, 2.0), (90.0, 7.0)), ts_event_ns=T0_NS + 1)
    after = make_delta(306, 310, 305, asks=((100.6, 3.0),), ts_event_ns=T0_NS + NS)
    book.apply_snapshot(rest)
    book.apply_delta(bracketing)
    book.apply_delta(after)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([bracketing, rest, after, book.snapshot()])

    expected: Book = ({100.5: 2.0, 90.0: 7.0}, {100.6: 3.0})
    batches, _ = capture_to_ops(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, tick=0.1)
    assert replay_ops(batches) == expected
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert replay_hft(feed.initial_snapshot, feed.data) == expected
    assert feed.conservation_gap() == 0


def test_replays_only_materialize_the_snapshots_they_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Periodic snapshots are ~1 per 10 s with thousands of levels each; building every
    # one as Python objects to skip nearly all of them made a week of data unreadable.
    write_resync_capture(tmp_path, rest_u=300)
    built: list[int] = []
    real = snapshots_from_frame

    def counting(frame: pl.DataFrame) -> list[BookSnapshot]:
        built.append(frame.height)
        return real(frame)

    monkeypatch.setattr(reconstruct, "snapshots_from_frame", counting)
    ticks = iter_l1_ticks(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert (ticks[-1].bid_qty, ticks[-1].ask_px) == (2.0, 100.6)
    assert sum(built) == 2  # first REST + resync REST; both periodic copies never built
