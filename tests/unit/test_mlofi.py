"""MLOFI (Xu, Gould & Howison, 2019): shapes, level-1 equivalence, weighted aggregation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from order_flow.metrics.mlofi import (
    DEFAULT_MLOFI_LEVELS,
    aggregate_mlofi_levels,
    compute_mlofi,
    compute_mlofi_events,
    compute_mlofi_time_windows,
    level_weights,
)
from order_flow.metrics.ofi import compute_ofi_events, compute_ofi_time_windows
from order_flow.metrics.windows import NS_PER_S

if TYPE_CHECKING:
    import numpy.typing as npt

    from tests.conftest import L1Series

Arrays = tuple[
    "npt.NDArray[np.float64]",
    "npt.NDArray[np.float64]",
    "npt.NDArray[np.float64]",
    "npt.NDArray[np.float64]",
]


def build_l2(series: L1Series, levels: int = 3) -> Arrays:
    """Stack the L1 series as level 0 and add deterministic deeper levels."""
    n = series.bid_px.shape[0]
    offsets = np.arange(levels)[np.newaxis, :]
    rng = np.random.default_rng(0)
    bid_px = series.bid_px[:, np.newaxis] - 0.1 * offsets
    ask_px = series.ask_px[:, np.newaxis] + 0.1 * offsets
    bid_qty = np.column_stack([series.bid_qty, *rng.uniform(1, 10, size=(levels - 1, n))])
    ask_qty = np.column_stack([series.ask_qty, *rng.uniform(1, 10, size=(levels - 1, n))])
    return bid_px, bid_qty, ask_px, ask_qty


def test_shape_and_level_one_equals_ofi(l1_series: L1Series) -> None:
    bid_px, bid_qty, ask_px, ask_qty = build_l2(l1_series)
    mlofi = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    assert mlofi.shape == (3, 3)
    np.testing.assert_allclose(mlofi[:, 0], l1_series.expected_ofi_events)
    np.testing.assert_allclose(
        mlofi[:, 0],
        compute_ofi_events(
            l1_series.bid_px, l1_series.bid_qty, l1_series.ask_px, l1_series.ask_qty
        ),
    )


def test_each_level_matches_ofi_on_that_level(l1_series: L1Series) -> None:
    bid_px, bid_qty, ask_px, ask_qty = build_l2(l1_series)
    mlofi = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    for m in range(3):
        expected = compute_ofi_events(bid_px[:, m], bid_qty[:, m], ask_px[:, m], ask_qty[:, m])
        np.testing.assert_allclose(mlofi[:, m], expected)


def test_one_dimensional_input_is_a_single_level(l1_series: L1Series) -> None:
    mlofi = compute_mlofi_events(*l1_series[:4])
    assert mlofi.shape == (3, 1)
    np.testing.assert_allclose(mlofi[:, 0], l1_series.expected_ofi_events)


def test_weighted_aggregation(l1_series: L1Series) -> None:
    bid_px, bid_qty, ask_px, ask_qty = build_l2(l1_series)
    events = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    np.testing.assert_allclose(aggregate_mlofi_levels(events), events.sum(axis=1))
    weights = np.array([1.0, 0.5, 0.25])
    np.testing.assert_allclose(aggregate_mlofi_levels(events, weights), events @ weights)
    with pytest.raises(ValueError, match="weights"):
        aggregate_mlofi_levels(events, [1.0, 2.0])


def test_windowed_mlofi(l1_series: L1Series) -> None:
    bid_px, bid_qty, ask_px, ask_qty = build_l2(l1_series)
    events = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    windowed = compute_mlofi(bid_px, bid_qty, ask_px, ask_qty, window=3)
    assert windowed.shape == (1, 3)
    np.testing.assert_allclose(windowed[0], events.sum(axis=0))
    weighted = compute_mlofi(bid_px, bid_qty, ask_px, ask_qty, window=3, weights=[1.0, 1.0, 1.0])
    assert weighted.shape == (1,)
    np.testing.assert_allclose(weighted, [events.sum()])


def test_nan_padded_levels_yield_nan() -> None:
    bid_px = np.array([[100.0, np.nan], [100.0, np.nan]])
    ones = np.ones_like(bid_px)
    ask_px = np.array([[101.0, np.nan], [101.0, np.nan]])
    mlofi = compute_mlofi_events(bid_px, ones, ask_px, ones)
    assert mlofi[0, 0] == 0.0
    assert np.isnan(mlofi[0, 1])


def test_degenerate_inputs() -> None:
    one_row = np.ones((1, 4))
    assert compute_mlofi_events(one_row, one_row, one_row, one_row).shape == (0, 4)
    with pytest.raises(ValueError, match="same shape"):
        compute_mlofi_events(np.ones((3, 2)), np.ones((3, 3)), np.ones((3, 2)), np.ones((3, 2)))
    cube = np.ones((2, 2, 2))
    with pytest.raises(ValueError, match="1-D or 2-D"):
        compute_mlofi_events(cube, cube, cube, cube)


def test_default_levels_is_five() -> None:
    assert DEFAULT_MLOFI_LEVELS == 5


def test_equal_weights_are_ones_not_inverse() -> None:
    np.testing.assert_allclose(level_weights(4), np.ones(4))
    np.testing.assert_allclose(level_weights(3, scheme="inverse"), [1.0, 0.5, 1.0 / 3.0])
    with pytest.raises(ValueError, match="scheme"):
        level_weights(2, scheme="exp")


def test_fewer_than_m_levels_are_nan_padded(l1_series: L1Series) -> None:
    n = l1_series.bid_px.shape[0]
    bid_px = np.full((n, 5), np.nan)
    bid_qty = np.full((n, 5), np.nan)
    ask_px = np.full((n, 5), np.nan)
    ask_qty = np.full((n, 5), np.nan)
    bid_px[:, 0] = l1_series.bid_px
    bid_qty[:, 0] = l1_series.bid_qty
    ask_px[:, 0] = l1_series.ask_px
    ask_qty[:, 0] = l1_series.ask_qty
    mlofi = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    assert mlofi.shape == (3, 5)
    np.testing.assert_allclose(mlofi[:, 0], l1_series.expected_ofi_events)
    assert np.all(np.isnan(mlofi[:, 1:]))


def test_time_windows_level_one_equals_ofi(l1_series: L1Series) -> None:
    bid_px, bid_qty, ask_px, ask_qty = build_l2(l1_series, levels=3)
    state_ts = np.array([0, 500, 1500, 2500], dtype=np.int64) * 1_000_000
    mlofi = compute_mlofi_time_windows(
        state_ts, bid_px, bid_qty, ask_px, ask_qty, window_ns=NS_PER_S, origin_ns=0
    )
    ofi = compute_ofi_time_windows(
        state_ts,
        l1_series.bid_px,
        l1_series.bid_qty,
        l1_series.ask_px,
        l1_series.ask_qty,
        window_ns=NS_PER_S,
        origin_ns=0,
    )
    assert mlofi.mlofi.shape[1] == 3
    np.testing.assert_allclose(mlofi.mlofi[:, 0], ofi.ofi)
    np.testing.assert_allclose(mlofi.mid, ofi.mid)
    np.testing.assert_allclose(mlofi.delta_mid_lead1, ofi.delta_mid_lead1)
