"""Cumulative Volume Delta: sign convention, cumulative sum and time-bar resampling."""

from __future__ import annotations

import numpy as np
import pytest

from order_flow.ingestion.events import Side
from order_flow.metrics.cvd import (
    aggressor_sign_from_binance_m,
    compute_cvd,
    compute_trade_delta,
    resample_cvd,
    sides_to_signs,
)

SECOND_NS = 1_000_000_000


def test_binance_m_true_is_seller_aggressor() -> None:
    """Official ``m``: 'Is the buyer the market maker?' → m=true ⇒ CVD negative."""
    assert aggressor_sign_from_binance_m(m=True) == -1
    assert aggressor_sign_from_binance_m(m=False) == 1
    np.testing.assert_allclose(
        compute_trade_delta(
            [2.0, 3.0],
            [aggressor_sign_from_binance_m(True), aggressor_sign_from_binance_m(False)],
        ),
        [-2.0, 3.0],
    )


def test_sides_to_signs() -> None:
    signs = sides_to_signs([Side.BUY, Side.SELL, Side.BID, Side.ASK])
    np.testing.assert_array_equal(signs, [1, -1, 1, -1])
    assert signs.dtype == np.int8


def test_trade_delta_sign_convention() -> None:
    delta = compute_trade_delta([1.0, 2.0, 3.0], [1, -1, 1])
    np.testing.assert_allclose(delta, [1.0, -2.0, 3.0])


def test_cvd_is_cumulative_sum() -> None:
    np.testing.assert_allclose(compute_cvd([1.0, 2.0, 3.0], [1, -1, 1]), [1.0, -1.0, 2.0])
    assert compute_cvd([], []).shape == (0,)


def test_invalid_signs_rejected() -> None:
    with pytest.raises(ValueError, match="aggressor"):
        compute_trade_delta([1.0], [0])
    with pytest.raises(ValueError, match="same shape"):
        compute_trade_delta([1.0, 2.0], [1])


def test_resample_fills_empty_bars() -> None:
    ts = [0, SECOND_NS + 5, 3 * SECOND_NS]
    bars = resample_cvd(ts, [1.0, -2.0, 3.0], SECOND_NS)
    np.testing.assert_array_equal(bars.bar_start_ns, [0, SECOND_NS, 2 * SECOND_NS, 3 * SECOND_NS])
    np.testing.assert_allclose(bars.delta, [1.0, -2.0, 0.0, 3.0])
    np.testing.assert_allclose(bars.cvd, [1.0, -1.0, -1.0, 2.0])
    assert bars.bar_start_ns.dtype == np.int64


def test_resample_is_order_independent_and_aligned_to_origin() -> None:
    ts = [7 * SECOND_NS, 2 * SECOND_NS, 2 * SECOND_NS + 1]
    bars = resample_cvd(ts, [1.0, 1.0, 1.0], 5 * SECOND_NS, origin_ns=2 * SECOND_NS)
    np.testing.assert_array_equal(bars.bar_start_ns, [2 * SECOND_NS, 7 * SECOND_NS])
    np.testing.assert_allclose(bars.delta, [2.0, 1.0])


def test_resample_empty_and_invalid() -> None:
    bars = resample_cvd([], [], SECOND_NS)
    assert bars.delta.shape == (0,)
    assert bars.bar_start_ns.shape == (0,)
    with pytest.raises(ValueError, match="bar_ns"):
        resample_cvd([0], [1.0], 0)
    with pytest.raises(ValueError, match="same shape"):
        resample_cvd([0, 1], [1.0], SECOND_NS)
    with pytest.raises(ValueError, match="1-D"):
        resample_cvd([[0]], [1.0], SECOND_NS)
