"""Regression on a real tape: tests/fixtures/golden_btcusdt_resync (see its README).

Synthetic tapes only contain the cases someone thought of. This one is 60 s of real
BTCUSDT around a resync whose REST snapshot shares ``u`` with its bracketing diff, is
stamped after it, and is shallower than the book the diff builds. The pinned numbers
change only when replay or metric semantics change; update them with a reason.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pytest

from order_flow.backtest.conversion import BookOp, capture_to_ops
from order_flow.backtest.hft_adapter import capture_to_hft_feed
from order_flow.backtest.hft_runner import run_ofi_mm_hftbacktest
from order_flow.backtest.runner import run_ofi_mm_backtest
from order_flow.metrics.batch import ofi_events_from_capture
from order_flow.metrics.ofi import compute_ofi_time_windows
from order_flow.storage.parquet import deltas_from_frame, read_events, snapshots_from_frame
from order_flow.storage.quality import check_capture
from order_flow.storage.reconstruct import iter_l1_ticks
from tests.unit.test_replay_resync import replay_hft, replay_ops

if TYPE_CHECKING:
    from order_flow.ingestion.events import BookSnapshot

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden_btcusdt_resync"
KW = {"exchange": "binance_futures", "symbol": "BTCUSDT"}
NS_PER_S = 1_000_000_000


def periodic_snapshots() -> dict[int, BookSnapshot]:
    """Recorder book copies: snapshots sharing ``(u, ts)`` with the delta they followed."""
    deltas = {
        (d.final_update_id, d.ts_event_ns)
        for d in deltas_from_frame(read_events(GOLDEN, "book_delta", **KW))
    }
    snapshots = snapshots_from_frame(read_events(GOLDEN, "book_snapshot", **KW))
    return {s.last_update_id: s for s in snapshots if (s.last_update_id, s.ts_event_ns) in deltas}


def test_golden_tape_is_intact() -> None:
    quality = check_capture(GOLDEN, **KW)
    assert quality.ok, quality.issues
    assert (quality.n_snapshots, quality.n_deltas, quality.n_trades) == (12, 589, 3376)
    assert (quality.n_epochs, quality.n_chain_breaks, quality.n_off_grid_prices) == (1, 0, 0)
    assert quality.n_zero_qty_trades == 15


def test_nautilus_replay_equals_every_recorded_book() -> None:
    periodic = periodic_snapshots()
    assert len(periodic) >= 10
    batches, trades = capture_to_ops(GOLDEN, **KW, tick=0.1)
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    seen = 0
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
        snap = periodic.get(ops[-1].sequence)
        if snap is not None:
            seen += 1
            assert bids == {lvl.price: lvl.qty for lvl in snap.bids if lvl.qty > 0}
            assert asks == {lvl.price: lvl.qty for lvl in snap.asks if lvl.qty > 0}
    assert seen == len(periodic)
    assert (len(batches), len(trades)) == (590, 3376 - 15)


def test_hftbacktest_feed_ends_on_the_nautilus_book() -> None:
    feed = capture_to_hft_feed(GOLDEN, **KW)
    batches, _ = capture_to_ops(GOLDEN, **KW, tick=0.1)
    assert feed.conservation_gap() == 0
    assert replay_hft(feed.initial_snapshot, feed.data) == replay_ops(batches)


def test_reconstructed_l1_and_ofi_are_pinned() -> None:
    ticks = iter_l1_ticks(GOLDEN, **KW)
    assert (ticks[-1].bid_px, ticks[-1].ask_px) == (85802.9, 85803.0)
    series = ofi_events_from_capture(GOLDEN, **KW)
    assert (series.state_ts_ns.size, series.e_n.size) == (590, 589)
    assert float(np.nansum(series.e_n)) == pytest.approx(-59.558, abs=1e-6)
    assert float(np.nansum(np.abs(series.e_n))) == pytest.approx(1094.948, abs=1e-6)
    valid = []
    for seconds in (1, 5, 10):
        bars = compute_ofi_time_windows(
            series.state_ts_ns,
            series.bid_px,
            series.bid_qty,
            series.ask_px,
            series.ask_qty,
            window_ns=seconds * NS_PER_S,
            epoch=series.epoch,
        )
        valid.append(int(np.count_nonzero(bars.valid)))
    assert valid == [61, 13, 7]


@pytest.mark.nautilus
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::UserWarning")
def test_nautilus_engine_on_the_golden_tape() -> None:
    result = run_ofi_mm_backtest(GOLDEN, symbol="BTCUSDT")
    assert (result.n_book_batches, result.n_public_trades) == (590, 3361)
    assert (result.n_submitted, result.n_maker_fills, result.n_taker_fills) == (167, 42, 0)


@pytest.mark.hftbacktest
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::UserWarning")
def test_hftbacktest_engine_on_the_golden_tape() -> None:
    result = run_ofi_mm_hftbacktest(GOLDEN, symbol="BTCUSDT")
    assert (result.n_public_trades, result.conservation_gap) == (3361, 0)
    assert (result.n_submitted, result.n_maker_fills, result.n_taker_fills) == (236, 43, 0)
