"""VPIN (Easley, Lopez de Prado & O'Hara, 2012) and BVC (2016)."""

from __future__ import annotations

import inspect
import math

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from order_flow.metrics import compute_retrospective_vpin as package_retrospective_vpin
from order_flow.metrics import compute_vpin as package_compute_vpin
from order_flow.metrics.vpin import (
    bucket_trades,
    bucket_trades_bvc,
    compute_retrospective_vpin,
    compute_vpin,
    compute_vpin_from_buckets,
)


def norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


class TestBucketing:
    def test_exact_boundaries(self) -> None:
        buckets = bucket_trades([1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0], 2.0, [1, -1, 1, 1])
        assert buckets.n_buckets == 2
        assert buckets.remainder == pytest.approx(0.0)
        np.testing.assert_allclose(buckets.buy_volume, [1.0, 2.0])
        np.testing.assert_allclose(buckets.sell_volume, [1.0, 0.0])

    def test_incomplete_tail_is_reported_as_remainder(self) -> None:
        buckets = bucket_trades([1.0, 1.0, 1.0], [1.0, 1.0, 1.0], 2.0, [1, 1, 1])
        assert buckets.n_buckets == 1
        assert buckets.remainder == pytest.approx(1.0)

    def test_straddling_trade_is_split_pro_rata(self) -> None:
        buckets = bucket_trades([1.0, 2.0], [1.5, 1.5], 2.0, [1, -1])
        assert buckets.n_buckets == 1
        np.testing.assert_allclose(buckets.buy_volume, [1.5])
        np.testing.assert_allclose(buckets.sell_volume, [0.5])
        assert buckets.remainder == pytest.approx(1.0)

    def test_trade_larger_than_bucket_spans_several_buckets(self) -> None:
        buckets = bucket_trades([5.0], [5.0], 2.0, [-1])
        assert buckets.n_buckets == 2
        np.testing.assert_allclose(buckets.sell_volume, [2.0, 2.0])
        np.testing.assert_allclose(buckets.buy_volume, [0.0, 0.0])
        np.testing.assert_allclose(buckets.close_price, [5.0, 5.0])
        assert buckets.remainder == pytest.approx(1.0)

    def test_close_price_is_price_of_completing_trade(self) -> None:
        buckets = bucket_trades([10.0, 11.0, 12.0, 13.0], [1.0, 1.0, 1.0, 1.0], 2.0, [1, 1, 1, 1])
        np.testing.assert_allclose(buckets.close_price, [11.0, 13.0])

    def test_zero_qty_trades_are_ignored(self) -> None:
        with_zero = bucket_trades([1.0, 9.0, 2.0], [1.0, 0.0, 1.0], 2.0, [1, -1, -1])
        without = bucket_trades([1.0, 2.0], [1.0, 1.0], 2.0, [1, -1])
        np.testing.assert_allclose(with_zero.buy_volume, without.buy_volume)
        np.testing.assert_allclose(with_zero.close_price, without.close_price)

    def test_float_round_off_at_boundary_still_completes_bucket(self) -> None:
        # 0.7 + 0.1 + 0.2 == 0.9999999999999999 in binary floating point.
        buckets = bucket_trades([1.0, 1.0, 1.0], [0.7, 0.1, 0.2], 1.0, [1, 1, 1])
        assert buckets.n_buckets == 1
        np.testing.assert_allclose(buckets.buy_volume, [1.0], atol=1e-9)

    def test_empty_input(self) -> None:
        buckets = bucket_trades([], [], 1.0, [])
        assert buckets.n_buckets == 0
        assert buckets.remainder == 0.0
        assert compute_vpin_from_buckets(buckets, 1).shape == (0,)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"bucket_size": 0.0}, "bucket_size"),
            ({"qty": [-1.0, 1.0]}, "non-negative"),
            ({"aggressor": [1, 2]}, "aggressor"),
            ({"aggressor": [1]}, "same shape"),
        ],
    )
    def test_validation(self, kwargs: dict[str, object], match: str) -> None:
        params: dict[str, object] = {
            "price": [1.0, 1.0],
            "qty": [1.0, 1.0],
            "bucket_size": 1.0,
            "aggressor": [1, -1],
        }
        params.update(kwargs)
        with pytest.raises(ValueError, match=match):
            bucket_trades(**params)  # type: ignore[arg-type]


class TestVpin:
    def test_all_buy_is_one(self) -> None:
        qty = np.ones(10)
        vpin = compute_vpin(np.ones(10), qty, bucket_size=2.0, window=3, aggressor=np.ones(10))
        assert vpin.shape == (3,)  # 5 buckets, window 3
        np.testing.assert_allclose(vpin, 1.0)

    def test_balanced_flow_is_zero(self) -> None:
        signs = np.tile([1, -1], 6)
        vpin = compute_vpin(np.ones(12), np.ones(12), bucket_size=2.0, window=2, aggressor=signs)
        np.testing.assert_allclose(vpin, 0.0)

    def test_rolling_window_arithmetic(self) -> None:
        # Buckets: [buy 2], [sell 2], [buy 1 sell 1] -> |imbalance| = 2, 2, 0
        buckets = bucket_trades([1.0] * 6, [1.0] * 6, 2.0, [1, 1, -1, -1, 1, -1])
        np.testing.assert_allclose(compute_vpin_from_buckets(buckets, 1), [1.0, 1.0, 0.0])
        np.testing.assert_allclose(compute_vpin_from_buckets(buckets, 2), [1.0, 0.5])
        np.testing.assert_allclose(compute_vpin_from_buckets(buckets, 3), [2.0 / 3.0])

    def test_window_longer_than_series_is_empty(self) -> None:
        buckets = bucket_trades([1.0, 1.0], [1.0, 1.0], 1.0, [1, 1])
        assert compute_vpin_from_buckets(buckets, 5).shape == (0,)
        with pytest.raises(ValueError, match="window"):
            compute_vpin_from_buckets(buckets, 0)

    def test_aggressor_classification_requires_signs(self) -> None:
        with pytest.raises(ValueError, match="aggressor signs are required"):
            compute_vpin([1.0], [1.0], 1.0, 1)

    def test_unknown_classification_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown classification"):
            compute_vpin([1.0], [1.0], 1.0, 1, classification="tick-rule")  # type: ignore[arg-type]

    @settings(max_examples=150, deadline=None)
    @given(
        trades=st.lists(
            st.tuples(
                st.floats(min_value=0.0, max_value=50.0, allow_nan=False),
                st.floats(min_value=0.001, max_value=20.0, allow_nan=False),
                st.sampled_from([-1, 1]),
            ),
            min_size=1,
            max_size=150,
        ),
        bucket_size=st.floats(min_value=0.1, max_value=25.0, allow_nan=False),
        window=st.integers(min_value=1, max_value=8),
    )
    def test_vpin_is_within_unit_interval(
        self, trades: list[tuple[float, float, int]], bucket_size: float, window: int
    ) -> None:
        price, qty, signs = (np.array(column, dtype=float) for column in zip(*trades, strict=True))
        vpin = compute_vpin(price, qty, bucket_size, window, aggressor=signs)
        assert np.all(vpin >= 0.0)
        assert np.all(vpin <= 1.0)
        expected_len = max(math.floor(qty.sum() / bucket_size + 1e-9) - window + 1, 0)
        assert vpin.shape == (expected_len,)

    @settings(max_examples=75, deadline=None)
    @given(
        trades=st.lists(
            st.tuples(
                st.floats(min_value=1.0, max_value=2.0, allow_nan=False),
                st.floats(min_value=0.01, max_value=5.0, allow_nan=False),
            ),
            min_size=1,
            max_size=100,
        ),
        window=st.integers(min_value=1, max_value=5),
    )
    def test_bvc_vpin_is_within_unit_interval(
        self, trades: list[tuple[float, float]], window: int
    ) -> None:
        price, qty = (np.array(column, dtype=float) for column in zip(*trades, strict=True))
        vpin = compute_vpin(price, qty, 1.0, window, classification="bvc")
        assert np.all(vpin >= 0.0)
        assert np.all(vpin <= 1.0)


class TestBulkVolumeClassification:
    def test_symmetric_price_moves(self) -> None:
        # Two buckets closing at 101 then 100 (first reference: first trade price 100).
        buckets = bucket_trades_bvc([100.0, 101.0, 100.0], [1.0, 1.0, 2.0], 2.0)
        d_price = np.array([1.0, -1.0])  # close - reference
        sigma = float(np.std(d_price, ddof=1))
        expected_buy = 2.0 * np.array([norm_cdf(1.0 / sigma), norm_cdf(-1.0 / sigma)])
        np.testing.assert_allclose(buckets.buy_volume, expected_buy)
        np.testing.assert_allclose(buckets.sell_volume, 2.0 - expected_buy)
        vpin = compute_vpin_from_buckets(buckets, 2)
        np.testing.assert_allclose(vpin, np.abs(2.0 - 2 * expected_buy).sum() / 4.0)

    def test_flat_prices_are_neutral(self) -> None:
        buckets = bucket_trades_bvc([100.0] * 4, [1.0] * 4, 2.0)
        np.testing.assert_allclose(buckets.buy_volume, [1.0, 1.0])
        np.testing.assert_allclose(compute_vpin_from_buckets(buckets, 2), [0.0])

    def test_explicit_sigma(self) -> None:
        buckets = bucket_trades_bvc([100.0, 101.0], [1.0, 1.0], 1.0, sigma=1.0)
        np.testing.assert_allclose(buckets.buy_volume, [0.5, norm_cdf(1.0)])

    def test_single_bucket_has_undefined_sigma_and_is_neutral(self) -> None:
        buckets = bucket_trades_bvc([100.0, 105.0], [0.5, 0.5], 1.0)
        np.testing.assert_allclose(buckets.buy_volume, [0.5])

    def test_end_to_end(self) -> None:
        vpin = compute_vpin(
            [100.0, 101.0, 102.0, 103.0], [1.0, 1.0, 1.0, 1.0], 1.0, 2, classification="bvc"
        )
        assert vpin.shape == (3,)
        assert np.all((vpin >= 0.0) & (vpin <= 1.0))

    def test_empty_input(self) -> None:
        assert bucket_trades_bvc([], [], 1.0).n_buckets == 0


class TestRetrospectiveApi:
    def test_compute_retrospective_vpin_matches_compute_vpin(self) -> None:
        price = np.ones(10)
        qty = np.ones(10)
        signs = np.ones(10)
        np.testing.assert_allclose(
            compute_retrospective_vpin(price, qty, 2.0, 3, aggressor=signs),
            compute_vpin(price, qty, 2.0, 3, aggressor=signs),
        )

    def test_docstring_signals_not_predictive(self) -> None:
        doc = compute_vpin.__doc__
        assert doc is not None
        assert "Not a predictive early-warning signal" in doc
        assert "Andersen" in doc
        retro = compute_retrospective_vpin.__doc__
        assert retro is not None
        assert "Not a predictive early-warning signal" in retro

    def test_function_body_mentions_flash_crash_timing(self) -> None:
        source = inspect.getsource(compute_vpin)
        assert "Flash Crash" in source
        assert "Andersen" in source

    def test_still_importable_as_compute_vpin(self) -> None:
        assert callable(package_compute_vpin)
        assert callable(package_retrospective_vpin)
