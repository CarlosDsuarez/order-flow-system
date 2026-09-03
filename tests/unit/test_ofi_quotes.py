"""OFI skew quote decision is a pure function: positive OFI shifts bid and ask up.

Callers: pytest. Affected API: ``order_flow.backtest.quotes``. User: "OFI skew
decision function pure (positive OFI → bid/ask shifted up) unit-tested without
nautilus".
"""

from __future__ import annotations

import pytest

from order_flow.backtest.quotes import RollingOfi, aggressive_quotes, maker_quotes, skew_ticks
from order_flow.metrics.stream import OfiAccumulator

TICK = 0.1
MID = 100.0
SPREAD_TICKS = 2


def test_positive_ofi_above_threshold_skews_up() -> None:
    assert skew_ticks(ofi=10.0, threshold=1.0, max_skew=1) == 1
    assert skew_ticks(ofi=-10.0, threshold=1.0, max_skew=1) == -1
    assert skew_ticks(ofi=0.5, threshold=1.0, max_skew=1) == 0


def test_positive_ofi_shifts_maker_quotes_up() -> None:
    symmetric = maker_quotes(
        mid=MID, tick=TICK, spread_ticks=SPREAD_TICKS, ofi=0.0, threshold=1.0, max_skew=1
    )
    skewed = maker_quotes(
        mid=MID, tick=TICK, spread_ticks=SPREAD_TICKS, ofi=10.0, threshold=1.0, max_skew=1
    )
    assert symmetric is not None
    assert skewed is not None
    assert skewed.bid == pytest.approx(symmetric.bid + TICK)
    assert skewed.ask == pytest.approx(symmetric.ask + TICK)
    assert skewed.bid < skewed.ask


def test_negative_ofi_shifts_maker_quotes_down() -> None:
    symmetric = maker_quotes(
        mid=MID, tick=TICK, spread_ticks=SPREAD_TICKS, ofi=0.0, threshold=1.0, max_skew=1
    )
    skewed = maker_quotes(
        mid=MID, tick=TICK, spread_ticks=SPREAD_TICKS, ofi=-10.0, threshold=1.0, max_skew=1
    )
    assert symmetric is not None
    assert skewed is not None
    assert skewed.bid == pytest.approx(symmetric.bid - TICK)
    assert skewed.ask == pytest.approx(symmetric.ask - TICK)


def test_maker_quotes_stay_inside_the_spread_relative_to_bbo() -> None:
    quotes = maker_quotes(
        mid=100.05,
        tick=TICK,
        spread_ticks=1,
        ofi=0.0,
        threshold=1.0,
        max_skew=1,
        best_bid=100.0,
        best_ask=100.1,
    )
    assert quotes is not None
    assert quotes.bid < 100.1
    assert quotes.ask > 100.0
    assert quotes.bid < quotes.ask


def test_rolling_ofi_sums_last_second_using_core_e_n() -> None:
    rolling = RollingOfi(window_ns=1_000_000_000)
    t0 = 1_000_000_000
    # First L1 seeds the accumulator; no e_n yet.
    assert rolling.observe(t0, 100.0, 10.0, 101.0, 8.0) == 0.0
    # Bid qty 10 -> 12 at same price: e_n = +2 (same as ofi_contributions).
    assert rolling.observe(t0 + 100, 100.0, 12.0, 101.0, 8.0) == 2.0
    # Event older than the window is dropped.
    later = rolling.observe(t0 + 1_000_000_000 + 200, 100.0, 12.0, 101.0, 8.0)
    acc = OfiAccumulator()
    assert acc.observe_l1(100.0, 12.0, 101.0, 8.0, synced=True) is None
    assert later == 0.0


def test_aggressive_quotes_cross_the_spread() -> None:
    quotes = aggressive_quotes(best_bid=100.0, best_ask=100.1)
    assert quotes.bid == pytest.approx(100.1)
    assert quotes.ask == pytest.approx(100.0)


def test_quote_helpers_reject_bad_inputs() -> None:
    with pytest.raises(ValueError, match="threshold"):
        skew_ticks(ofi=1.0, threshold=-1.0, max_skew=1)
    with pytest.raises(ValueError, match="max_skew"):
        skew_ticks(ofi=1.0, threshold=1.0, max_skew=-1)
    with pytest.raises(ValueError, match="tick"):
        maker_quotes(mid=1.0, tick=0.0, spread_ticks=1, ofi=0.0, threshold=1.0, max_skew=1)
    with pytest.raises(ValueError, match="spread_ticks"):
        maker_quotes(mid=1.0, tick=0.1, spread_ticks=0, ofi=0.0, threshold=1.0, max_skew=1)
