"""OFI (Cont, Kukanov & Stoikov, 2014)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest

from order_flow.metrics.ofi import compute_ofi, compute_ofi_events

if TYPE_CHECKING:
    from tests.conftest import L1Series


def test_events_match_hand_computation(l1_series: L1Series) -> None:
    events = compute_ofi_events(*l1_series[:4])
    np.testing.assert_allclose(events, l1_series.expected_ofi_events)
    assert events.dtype == np.float64
    assert events.shape == (3,)


def test_bid_side_cases() -> None:
    # price up: +qb_n ; price down: -qb_{n-1} ; same price: qb_n - qb_{n-1}
    ask_px = np.array([10.0, 10.0, 10.0, 10.0])
    ask_qty = np.array([1.0, 1.0, 1.0, 1.0])
    bid_px = np.array([9.0, 9.5, 9.0, 9.0])
    bid_qty = np.array([4.0, 6.0, 3.0, 7.0])
    np.testing.assert_allclose(
        compute_ofi_events(bid_px, bid_qty, ask_px, ask_qty), [6.0, -6.0, 4.0]
    )


def test_ask_side_cases() -> None:
    # price down: -qa_n ; price up: +qa_{n-1} ; same price: qa_{n-1} - qa_n
    bid_px = np.array([9.0, 9.0, 9.0, 9.0])
    bid_qty = np.array([1.0, 1.0, 1.0, 1.0])
    ask_px = np.array([10.0, 9.8, 10.2, 10.2])
    ask_qty = np.array([4.0, 6.0, 3.0, 7.0])
    np.testing.assert_allclose(
        compute_ofi_events(bid_px, bid_qty, ask_px, ask_qty), [-6.0, 6.0, -4.0]
    )


def test_windowed_aggregation(l1_series: L1Series) -> None:
    ofi = compute_ofi(*l1_series[:4], window=2)
    np.testing.assert_allclose(ofi, [9.0])
    ofi_partial = compute_ofi(*l1_series[:4], window=2, partial=True)
    np.testing.assert_allclose(ofi_partial, [9.0, -4.0])
    np.testing.assert_allclose(compute_ofi(*l1_series[:4], window=1), [2.0, 7.0, -4.0])


def test_first_observation_length_is_n_minus_one() -> None:
    events = compute_ofi_events([9.0, 9.0], [4.0, 5.0], [10.0, 10.0], [1.0, 1.0])
    assert events.shape == (1,)
    np.testing.assert_allclose(events, [1.0])


def test_paper_casuistry_price_up_down_unchanged_times_size() -> None:
    """Cont et al. section 2.1: bid up -> +q^B_n; bid down -> -q^B_{n-1}; unchanged -> dq^B."""
    ask_px = np.array([10.0, 10.0, 10.0, 10.0])
    ask_qty = np.array([1.0, 1.0, 1.0, 1.0])
    bid_px = np.array([9.0, 9.5, 9.0, 9.0])
    bid_qty = np.array([4.0, 6.0, 3.0, 7.0])
    # n=1 price up: +6; n=2 price down: -6; n=3 unchanged size 3->7: +4
    events = compute_ofi_events(bid_px, bid_qty, ask_px, ask_qty)
    np.testing.assert_allclose(events, [6.0, -6.0, 4.0])


def test_short_series_yield_empty() -> None:
    assert compute_ofi_events([1.0], [1.0], [2.0], [1.0]).shape == (0,)
    assert compute_ofi_events([], [], [], []).shape == (0,)
    assert compute_ofi([1.0], [1.0], [2.0], [1.0], window=3).shape == (0,)


def test_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="same shape"):
        compute_ofi_events([1.0, 2.0], [1.0], [2.0, 3.0], [1.0, 1.0])
    with pytest.raises(ValueError, match="1-D"):
        compute_ofi_events([[1.0]], [[1.0]], [[2.0]], [[1.0]])
    with pytest.raises(ValueError, match="window"):
        compute_ofi([1.0, 2.0], [1.0, 1.0], [2.0, 3.0], [1.0, 1.0], window=0)
