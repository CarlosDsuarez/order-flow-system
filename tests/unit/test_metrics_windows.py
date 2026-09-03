"""Time-window aggregation of OFI vs mid (1s/5s/10s alignment).

Callers: pytest and ``scripts/validate_ofi.py``. Affected API:
``sum_in_time_windows``, ``compute_ofi_time_windows``. User: window aggregation
1s/5s/10s alignment; lead-1 Δmid vs OFI.
"""

from __future__ import annotations

import numpy as np
import pytest

from order_flow.metrics.ofi import compute_ofi_events, compute_ofi_time_windows
from order_flow.metrics.windows import NS_PER_S, last_in_time_windows, sum_in_time_windows
from order_flow.utils.time import NS_PER_S as UTILS_NS_PER_S


def test_window_constant_matches_utils() -> None:
    assert NS_PER_S == UTILS_NS_PER_S == 1_000_000_000


def test_sum_in_time_windows_1s_alignment() -> None:
    ts = np.array([0, 400_000_000, 1_200_000_000, 1_800_000_000, 5_100_000_000], dtype=np.int64)
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    bars = sum_in_time_windows(ts, values, NS_PER_S, origin_ns=0)
    np.testing.assert_array_equal(bars.start_ns, np.arange(0, 6, dtype=np.int64) * NS_PER_S)
    np.testing.assert_allclose(bars.values, [3.0, 7.0, 0.0, 0.0, 0.0, 5.0])
    np.testing.assert_array_equal(bars.counts, [2, 2, 0, 0, 0, 1])


def test_sum_in_time_windows_5s_and_10s() -> None:
    ts = np.array([0, 1, 6, 11], dtype=np.int64) * NS_PER_S
    values = np.array([1.0, 2.0, 3.0, 4.0])
    five = sum_in_time_windows(ts, values, 5 * NS_PER_S, origin_ns=0)
    np.testing.assert_allclose(five.values, [3.0, 3.0, 4.0])
    ten = sum_in_time_windows(ts, values, 10 * NS_PER_S, origin_ns=0)
    np.testing.assert_allclose(ten.values, [6.0, 4.0])


def test_sum_rejects_bad_window() -> None:
    with pytest.raises(ValueError, match="window"):
        sum_in_time_windows([0], [1.0], 0)


def test_ofi_time_windows_lead1_is_next_window_mid_change() -> None:
    bid_px = np.array([100.0, 100.0, 101.0, 101.0])
    bid_qty = np.array([10.0, 12.0, 12.0, 12.0])
    ask_px = np.array([102.0, 102.0, 102.0, 103.0])
    ask_qty = np.array([8.0, 8.0, 8.0, 8.0])
    state_ts = np.array([0, 500, 1500, 2500], dtype=np.int64) * 1_000_000
    events = compute_ofi_events(bid_px, bid_qty, ask_px, ask_qty)
    mids = (bid_px + ask_px) / 2.0
    frame = compute_ofi_time_windows(
        state_ts,
        bid_px,
        bid_qty,
        ask_px,
        ask_qty,
        window_ns=NS_PER_S,
        origin_ns=0,
    )
    np.testing.assert_allclose(frame.ofi, [events[0], events[1], events[2]])
    np.testing.assert_allclose(frame.mid, [mids[1], mids[2], mids[3]])
    np.testing.assert_allclose(frame.delta_mid[1:], np.diff(frame.mid))
    np.testing.assert_allclose(frame.delta_mid_lead1[:-1], np.diff(frame.mid))
    assert np.isnan(frame.delta_mid[0])
    assert np.isnan(frame.delta_mid_lead1[-1])


def test_windows_overlapping_epoch_change_are_dropped() -> None:
    ts2 = np.array([0, 400_000_000], dtype=np.int64)
    epoch2 = np.array([0, 1], dtype=np.int64)
    mixed = sum_in_time_windows(ts2, np.array([1.0, 2.0]), NS_PER_S, origin_ns=0, epoch=epoch2)
    assert not mixed.valid[0]
    same = sum_in_time_windows(
        ts2, np.array([1.0, 2.0]), NS_PER_S, origin_ns=0, epoch=np.array([0, 0])
    )
    assert same.valid[0]


def test_sum_rejects_bad_shapes() -> None:
    with pytest.raises(ValueError, match="1-D"):
        sum_in_time_windows(np.array([[0]]), np.array([[1.0]]), NS_PER_S)
    with pytest.raises(ValueError, match="same shape"):
        sum_in_time_windows([0, 1], [1.0], NS_PER_S)
    with pytest.raises(ValueError, match="epoch"):
        sum_in_time_windows([0], [1.0], NS_PER_S, epoch=[0, 1])


def test_sum_empty_is_empty() -> None:
    bars = sum_in_time_windows([], [], NS_PER_S)
    assert bars.start_ns.shape == (0,)


def test_last_in_time_windows_carries_forward() -> None:
    ts = np.array([1_500_000_000], dtype=np.int64)
    out = last_in_time_windows(ts, np.array([42.0]), NS_PER_S, origin_ns=0, n_bars=3, first_index=0)
    assert np.isnan(out[0])
    np.testing.assert_allclose(out[1:], [42.0, 42.0])
    empty = last_in_time_windows(
        np.array([], dtype=np.int64),
        np.array([], dtype=np.float64),
        NS_PER_S,
        origin_ns=0,
        n_bars=2,
        first_index=0,
    )
    assert empty.shape == (2,)
    assert np.isnan(empty).all()


def test_ofi_time_windows_rejects_mismatched_ts_and_epoch() -> None:
    with pytest.raises(ValueError, match="state_ts_ns"):
        compute_ofi_time_windows(
            [0],
            [100.0, 100.0],
            [1.0, 1.0],
            [101.0, 101.0],
            [1.0, 1.0],
            window_ns=NS_PER_S,
        )
    with pytest.raises(ValueError, match="epoch"):
        compute_ofi_time_windows(
            [0, 1],
            [100.0, 100.0],
            [1.0, 1.0],
            [101.0, 101.0],
            [1.0, 1.0],
            window_ns=NS_PER_S,
            epoch=[0],
        )


def test_ofi_time_windows_empty_and_single_bar() -> None:
    empty = compute_ofi_time_windows([0], [100.0], [1.0], [101.0], [1.0], window_ns=NS_PER_S)
    assert empty.ofi.shape == (0,)
    frame = compute_ofi_time_windows(
        np.array([0, 100], dtype=np.int64),
        [100.0, 100.0],
        [10.0, 12.0],
        [101.0, 101.0],
        [8.0, 8.0],
        window_ns=NS_PER_S,
        origin_ns=0,
    )
    assert frame.mid.shape == (1,)
    assert np.isnan(frame.delta_mid[0])
    assert np.isnan(frame.delta_mid_lead1[0])


def test_ofi_time_windows_marks_mixed_epoch_bar_invalid() -> None:
    frame = compute_ofi_time_windows(
        np.array([0, 300_000_000, 600_000_000], dtype=np.int64),
        [100.0, 100.0, 100.0],
        [10.0, 12.0, 15.0],
        [101.0, 101.0, 101.0],
        [8.0, 8.0, 8.0],
        window_ns=NS_PER_S,
        origin_ns=0,
        epoch=[0, 0, 1],
    )
    assert frame.start_ns.shape == (1,)
    assert not frame.valid[0]
