"""Pure-function tests for the Binance USD-M Futures local-book reconstruction protocol.

Official first-event rule (NOT the spot ``lastUpdateId+1`` formula), retrieved 2026-09-02:

    The first processed event should have U <= lastUpdateId AND u >= lastUpdateId

    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
    https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
"""

from __future__ import annotations

from itertools import pairwise

import pytest

from order_flow.ingestion.events import PriceLevel
from order_flow.ingestion.sync import (
    DepthDecision,
    DepthSynchronizer,
    compare_top_levels,
    observed_latency_ns,
    reconnect_backoff,
    reconnect_delay,
)
from order_flow.orderbook.book import OrderBook
from tests.helpers import make_delta, make_snapshot


class TestBuffering:
    def test_diffs_arriving_before_snapshot_are_held(self) -> None:
        sync = DepthSynchronizer()
        delta = make_delta(98, 102, 95)
        assert sync.decide(delta) is DepthDecision.BUFFER
        assert sync.awaiting_snapshot
        assert sync.buffered == 1
        assert sync.decide(make_delta(103, 104, 102)) is DepthDecision.BUFFER
        assert sync.buffered == 2

    def test_install_snapshot_replays_buffer_and_drops_stale(self) -> None:
        sync = DepthSynchronizer()
        stale = make_delta(90, 95, 85)
        first = make_delta(98, 102, 95)
        next_ok = make_delta(103, 104, 102)
        for delta in (stale, first, next_ok):
            sync.decide(delta)
        results = sync.install_snapshot(100)
        assert [decision for _, decision in results] == [
            DepthDecision.DROP_STALE,
            DepthDecision.APPLY,
            DepthDecision.APPLY,
        ]
        assert sync.buffered == 0
        assert not sync.awaiting_snapshot
        assert sync.last_update_id == 104


class TestStaleAndFirstEvent:
    def test_stale_u_less_than_snapshot_id_is_dropped(self) -> None:
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        assert sync.decide(make_delta(900, 950, 880)) is DepthDecision.DROP_STALE
        assert sync.decide(make_delta(951, 999, 950)) is DepthDecision.DROP_STALE
        assert not sync.synced

    def test_first_event_accepted_when_u_brackets_last_update_id(self) -> None:
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        assert sync.decide(make_delta(995, 1003, 994)) is DepthDecision.APPLY
        assert sync.synced
        assert sync.last_update_id == 1003

    def test_first_event_with_u_equal_to_snapshot_id_is_applied(self) -> None:
        """Futures: ``U <= lastUpdateId <= u`` includes ``u == lastUpdateId``.

        Spot ``U <= lastUpdateId+1 <= u`` would *reject* this (1001 is not <= 1000).
        """
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        assert sync.decide(make_delta(990, 1000, 989)) is DepthDecision.APPLY

    def test_spot_plus_one_rule_is_not_used(self) -> None:
        """Spot would accept ``U=1001, u=1005`` against ``lastUpdateId=1000`` because
        ``1001 <= 1000+1 <= 1005``. Futures requires ``U <= lastUpdateId`` and must resync.
        """
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        assert sync.decide(make_delta(1001, 1005, 1000)) is DepthDecision.RESYNC
        assert sync.awaiting_snapshot


class TestSubsequentAndResync:
    def test_contiguous_pu_applies(self) -> None:
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        assert sync.decide(make_delta(995, 1003, 994)) is DepthDecision.APPLY
        assert sync.decide(make_delta(1004, 1010, 1003)) is DepthDecision.APPLY
        assert sync.last_update_id == 1010

    def test_pu_mismatch_triggers_resync_and_does_not_apply(self) -> None:
        sync = DepthSynchronizer()
        sync.install_snapshot(1000)
        sync.decide(make_delta(995, 1003, 994))
        gap = make_delta(1012, 1015, 1011)
        assert sync.decide(gap) is DepthDecision.RESYNC
        assert sync.awaiting_snapshot
        assert sync.last_update_id is None
        # Gap event itself must not be kept for the next snapshot replay.
        assert sync.buffered == 0
        # Further events buffer until a fresh snapshot — they are not applied.
        assert sync.decide(make_delta(1016, 1018, 1015)) is DepthDecision.BUFFER

    def test_install_snapshot_stops_replay_on_gap_and_keeps_leftover(self) -> None:
        sync = DepthSynchronizer()
        for delta in (
            make_delta(98, 102, 95),
            make_delta(110, 112, 108),  # gap vs 102
            make_delta(113, 115, 112),
        ):
            sync.decide(delta)
        results = sync.install_snapshot(100)
        assert [decision for _, decision in results] == [
            DepthDecision.APPLY,
            DepthDecision.RESYNC,
            DepthDecision.BUFFER,
        ]
        assert sync.awaiting_snapshot
        assert sync.buffered == 1
        replay = sync.install_snapshot(113)
        assert [decision for _, decision in replay] == [DepthDecision.APPLY]
        assert sync.last_update_id == 115


class TestDisconnectReset:
    def test_disconnect_requires_a_new_snapshot(self) -> None:
        sync = DepthSynchronizer()
        sync.install_snapshot(100)
        assert sync.decide(make_delta(98, 102, 95)) is DepthDecision.APPLY
        sync.on_disconnect()
        assert sync.awaiting_snapshot
        assert sync.buffered == 0
        assert sync.last_update_id is None
        assert sync.decide(make_delta(103, 104, 102)) is DepthDecision.BUFFER


class TestQtyZeroAndHonesty:
    def test_qty_zero_removes_level_via_order_book(self) -> None:
        book = OrderBook()
        book.apply_snapshot(make_snapshot(last_update_id=100))
        sync = DepthSynchronizer()
        sync.install_snapshot(100)
        delta = make_delta(95, 105, 90, asks=((101.0, 0.0),))
        assert sync.decide(delta) is DepthDecision.APPLY
        assert book.apply_delta(delta) is True
        assert book.best_ask() == PriceLevel(102.0, 3.0)

    def test_compare_top_levels_reports_mismatches(self) -> None:
        book = OrderBook()
        book.apply_snapshot(make_snapshot(last_update_id=100))
        rest = make_snapshot(last_update_id=100, bids=((100.0, 10.0), (99.0, 4.0)))
        report = compare_top_levels(book, rest, levels=2)
        assert report.compared == 4  # 2 bids + 2 asks
        assert report.mismatches == 1
        assert report.max_qty_discrepancy == pytest.approx(1.0)
        assert report.last_update_id_local == 100
        assert report.last_update_id_rest == 100


class TestLatencyAndBackoff:
    def test_latency_helper_is_recv_minus_event(self) -> None:
        assert observed_latency_ns(1_000_010_000, 1_000_000_000) == 10_000
        assert observed_latency_ns(5, 9) == -4

    def test_backoff_is_monotonic_and_capped(self) -> None:
        delays = [reconnect_backoff(i) for i in range(12)]
        assert delays[0] == pytest.approx(0.5)
        assert delays[1] == pytest.approx(1.0)
        assert delays[2] == pytest.approx(2.0)
        assert all(earlier <= later for earlier, later in pairwise(delays))
        assert delays[-1] == pytest.approx(30.0)
        assert all(delay <= 30.0 for delay in delays)

    def test_reconnect_delay_adds_bounded_jitter(self) -> None:
        class ZeroRng:
            def uniform(self, a: float, b: float) -> float:
                return 0.0

        class FullRng:
            def uniform(self, a: float, b: float) -> float:
                return b

        assert reconnect_delay(0, rng=ZeroRng()) == pytest.approx(0.5)
        assert reconnect_delay(0, rng=FullRng()) == pytest.approx(0.5 * 1.25)
        assert reconnect_delay(20, rng=FullRng()) == pytest.approx(30.0)
