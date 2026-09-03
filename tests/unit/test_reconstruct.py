"""Reconstruct an OrderBook from Parquet snapshots + deltas (no network)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from order_flow.ingestion.events import PriceLevel
from order_flow.orderbook.book import OrderBook
from order_flow.storage.parquet import ParquetWriter
from order_flow.storage.reconstruct import (
    ReconstructionError,
    iter_l1_ticks,
    iter_lm_ticks,
    reconstruct_book,
)
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot

if TYPE_CHECKING:
    from pathlib import Path

NS = 1_000_000  # 1 ms, keeps timestamps distinct and ordered


def _persist_sequence(root: Path) -> None:
    """Known sequence: snapshot, two deltas, a periodic LOB snapshot, one more delta."""
    live = OrderBook()
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    live.apply_snapshot(rest)
    d1 = make_delta(
        95,
        105,
        90,
        bids=((100.0, 12.0),),
        asks=((101.0, 0.0),),
        ts_event_ns=T0_NS + NS,
    )
    live.apply_delta(d1)
    d2 = make_delta(106, 110, 105, asks=((103.0, 2.0),), ts_event_ns=T0_NS + 2 * NS)
    live.apply_delta(d2)
    periodic = live.snapshot()
    d3 = make_delta(111, 115, 110, bids=((99.0, 0.0),), ts_event_ns=T0_NS + 3 * NS)
    live.apply_delta(d3)
    with ParquetWriter(root, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, d1, d2, periodic, d3])


def test_reconstruct_at_several_timestamps_matches_live_book(tmp_path: Path) -> None:
    _persist_sequence(tmp_path)

    at_rest = reconstruct_book(tmp_path, T0_NS, exchange=EXCHANGE, symbol=SYMBOL)
    assert at_rest.best_bid() == PriceLevel(100.0, 10.0)
    assert at_rest.best_ask() == PriceLevel(101.0, 8.0)
    assert at_rest.depth_at_level(2) == (PriceLevel(99.0, 5.0), PriceLevel(102.0, 3.0))
    assert at_rest.last_update_id == 100

    after_d1 = reconstruct_book(tmp_path, T0_NS + NS, exchange=EXCHANGE, symbol=SYMBOL)
    assert after_d1.best_bid() == PriceLevel(100.0, 12.0)
    assert after_d1.best_ask() == PriceLevel(102.0, 3.0)
    assert after_d1.last_update_id == 105

    after_d2 = reconstruct_book(tmp_path, T0_NS + 2 * NS, exchange=EXCHANGE, symbol=SYMBOL)
    assert after_d2.best_bid() == PriceLevel(100.0, 12.0)
    assert after_d2.best_ask() == PriceLevel(102.0, 3.0)
    assert after_d2.depth_at_level(2)[1] == PriceLevel(103.0, 2.0)
    assert after_d2.last_update_id == 110

    after_d3 = reconstruct_book(tmp_path, T0_NS + 3 * NS, exchange=EXCHANGE, symbol=SYMBOL)
    bids, _asks = after_d3.depth(5)
    assert [level.price for level in bids] == [100.0]
    assert after_d3.last_update_id == 115
    assert after_d3.is_synced is True


def test_reconstruct_before_first_snapshot_raises(tmp_path: Path) -> None:
    _persist_sequence(tmp_path)
    with pytest.raises(ReconstructionError, match="snapshot"):
        reconstruct_book(tmp_path, T0_NS - 1, exchange=EXCHANGE, symbol=SYMBOL)


def test_reconstruct_snapshot_without_deltas(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([make_snapshot(last_update_id=100, ts_event_ns=T0_NS)])
    book = reconstruct_book(tmp_path, T0_NS + NS, exchange=EXCHANGE, symbol=SYMBOL)
    assert book.last_update_id == 100
    assert book.best_bid() == PriceLevel(100.0, 10.0)


def test_reconstruct_empty_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(ReconstructionError):
        reconstruct_book(tmp_path, T0_NS, exchange=EXCHANGE, symbol=SYMBOL)


def test_reconstruct_sequence_gap_raises(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(
            [
                make_snapshot(last_update_id=100, ts_event_ns=T0_NS),
                make_delta(200, 201, 199, ts_event_ns=T0_NS + NS),
            ]
        )
    with pytest.raises(ReconstructionError, match="sequence gap"):
        reconstruct_book(tmp_path, T0_NS + NS, exchange=EXCHANGE, symbol=SYMBOL)


def test_iter_l1_ticks_skips_gap_resyncs_and_empty_bbo(tmp_path: Path) -> None:
    rest = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0),),
        asks=((101.0, 8.0),),
        ts_event_ns=T0_NS,
    )
    stale = make_delta(90, 95, 85, ts_event_ns=T0_NS + NS)
    gap = make_delta(200, 201, 199, ts_event_ns=T0_NS + 2 * NS)
    resync = make_snapshot(
        last_update_id=300,
        bids=((100.0, 11.0),),
        asks=((101.0, 7.0),),
        ts_event_ns=T0_NS + 3 * NS,
    )
    d_ok = make_delta(301, 305, 300, bids=((100.0, 14.0),), ts_event_ns=T0_NS + 4 * NS)
    empty = make_snapshot(last_update_id=400, bids=(), asks=(), ts_event_ns=T0_NS + 5 * NS)
    orphan = make_delta(1, 2, 0, ts_event_ns=T0_NS - NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([orphan, rest, stale, gap, resync, d_ok, empty])
    ticks = iter_l1_ticks(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    epochs = [tick.epoch for tick in ticks]
    sizes = [(tick.bid_qty, tick.ask_qty) for tick in ticks]
    assert sizes[0] == (10.0, 8.0)
    assert 1 in epochs
    assert all(tick.bid_qty > 0 and tick.ask_qty > 0 for tick in ticks)


def test_iter_lm_ticks_matches_l1_and_pads_missing_levels(tmp_path: Path) -> None:
    rest = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS,
    )
    d1 = make_delta(95, 105, 90, bids=((100.0, 12.0),), ts_event_ns=T0_NS + NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, d1])
    l1 = iter_l1_ticks(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    lm = iter_lm_ticks(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, levels=3)
    assert len(lm) == len(l1)
    assert lm[0].bid_px[0] == l1[0].bid_px
    assert lm[0].bid_px.shape == (3,)
    assert np.isnan(lm[0].bid_px[2])
    assert lm[1].bid_qty[0] == 12.0
