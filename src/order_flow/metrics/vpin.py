"""Volume-Synchronised Probability of Informed Trading (VPIN).

References:
    Easley, D., Lopez de Prado, M. & O'Hara, M. (2012). Flow Toxicity and Liquidity in a
    High-frequency World. Review of Financial Studies, 25(5), 1457-1493.
    Easley, D., Lopez de Prado, M. & O'Hara, M. (2016). Discerning Information from Trade
    Data. Journal of Financial Economics, 120(2), 269-285. (Bulk Volume Classification.)

Pipeline:

1. Split the trade sequence into consecutive *volume buckets* of size ``V``; a trade that
   straddles a boundary is split pro-rata between the two buckets.
2. Classify each bucket's volume into buy ``V_B`` and sell ``V_S = V - V_B``:

   * ``classification="aggressor"``: exact taker side (crypto venues publish it);
   * ``classification="bvc"``: Bulk Volume Classification,
     ``V_B = V * Phi(dP / sigma_dP)`` with ``dP`` the bucket-to-bucket price change and
     ``Phi`` the standard normal CDF.

3. ``VPIN_tau = sum_{j=tau-n+1..tau} |V_S,j - V_B,j| / (n * V)`` over a rolling window of
   ``n`` buckets; it lies in ``[0, 1]``. See ``docs/math/vpin.md``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import numpy as np

from order_flow.metrics._common import as_1d_float, check_same_shape

if TYPE_CHECKING:
    import numpy.typing as npt

Classification = Literal["aggressor", "bvc"]

_REL_TOL = 1e-9
"""Relative tolerance (in bucket units) absorbing float round-off at bucket boundaries."""


@dataclass(frozen=True, slots=True)
class VolumeBuckets:
    """Buy/sell volume per *complete* volume bucket.

    Attributes:
        bucket_size: Volume ``V`` of each bucket.
        buy_volume: Buyer-initiated volume per bucket, shape ``(n,)``.
        sell_volume: Seller-initiated volume per bucket (``V - buy_volume``).
        close_price: Price of the trade that completed each bucket.
        remainder: Volume accumulated in the incomplete trailing bucket.
    """

    bucket_size: float
    buy_volume: npt.NDArray[np.float64]
    sell_volume: npt.NDArray[np.float64]
    close_price: npt.NDArray[np.float64]
    remainder: float

    @property
    def n_buckets(self) -> int:
        """Number of complete buckets."""
        return int(self.buy_volume.shape[0])

    @property
    def imbalance(self) -> npt.NDArray[np.float64]:
        """``|V_S - V_B|`` per bucket."""
        return np.asarray(np.abs(self.sell_volume - self.buy_volume), dtype=np.float64)


# --------------------------------------------------------------------------- helpers
def _norm_cdf(z: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    erf = np.vectorize(math.erf, otypes=[np.float64])
    return np.asarray(0.5 * (1.0 + erf(z / math.sqrt(2.0))), dtype=np.float64)


def _validate_trades(
    price: npt.ArrayLike, qty: npt.ArrayLike, bucket_size: float
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.bool_]]:
    """Return float arrays plus a mask selecting trades with positive volume."""
    if not bucket_size > 0:
        msg = "bucket_size must be > 0"
        raise ValueError(msg)
    p = as_1d_float(price, "price")
    q = as_1d_float(qty, "qty")
    check_same_shape(price=p, qty=q)
    if np.any(q < 0):
        msg = "qty must be non-negative"
        raise ValueError(msg)
    return p, q, q > 0


def _bucket_grid(
    cum_volume: npt.NDArray[np.float64], bucket_size: float
) -> tuple[npt.NDArray[np.float64], float]:
    """Upper boundaries ``V, 2V, ..., nV`` of the complete buckets and the leftover volume."""
    total = float(cum_volume[-1]) if cum_volume.shape[0] else 0.0
    n_complete = math.floor(total / bucket_size + _REL_TOL)
    boundaries = bucket_size * np.arange(1, n_complete + 1, dtype=np.float64)
    remainder = max(total - n_complete * bucket_size, 0.0)
    return boundaries, remainder


def _close_prices(
    price: npt.NDArray[np.float64],
    cum_volume: npt.NDArray[np.float64],
    boundaries: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Price of the trade whose cumulative volume first reaches each boundary."""
    idx = np.searchsorted(cum_volume, boundaries * (1.0 - _REL_TOL), side="left")
    idx = np.minimum(idx, price.shape[0] - 1)
    return np.asarray(price[idx], dtype=np.float64)


def _empty_buckets(bucket_size: float, remainder: float) -> VolumeBuckets:
    empty = np.empty(0, dtype=np.float64)
    return VolumeBuckets(bucket_size, empty, empty.copy(), empty.copy(), remainder)


# --------------------------------------------------------------------------- bucketing
def bucket_trades(
    price: npt.ArrayLike,
    qty: npt.ArrayLike,
    bucket_size: float,
    aggressor: npt.ArrayLike,
) -> VolumeBuckets:
    """Equal-volume bucketing with exact aggressor-side classification.

    Args:
        price: Trade prices, shape ``(T,)``.
        qty: Trade volumes (``>= 0``), shape ``(T,)``.
        bucket_size: Bucket volume ``V`` (``> 0``).
        aggressor: ``+1`` for buyer-initiated, ``-1`` for seller-initiated trades.

    Buy volume within a bucket is obtained by linearly interpolating cumulative buy volume
    against cumulative total volume at the bucket boundaries, which splits straddling
    trades pro-rata and handles trades larger than ``V``.
    """
    p, q, keep = _validate_trades(price, qty, bucket_size)
    s = as_1d_float(aggressor, "aggressor")
    check_same_shape(qty=q, aggressor=s)
    if not np.all(np.isin(s, (-1.0, 1.0))):
        msg = "aggressor must contain only +1 (buy) and -1 (sell)"
        raise ValueError(msg)
    p, q, s = p[keep], q[keep], s[keep]
    cum = np.cumsum(q)
    boundaries, remainder = _bucket_grid(cum, bucket_size)
    if boundaries.shape[0] == 0:
        return _empty_buckets(bucket_size, remainder)
    grid = np.concatenate(([0.0], boundaries))
    xp = np.concatenate(([0.0], cum))
    fp_buy = np.concatenate(([0.0], np.cumsum(q * (s > 0))))
    buy = np.clip(np.diff(np.interp(grid, xp, fp_buy)), 0.0, bucket_size)
    buy = np.asarray(buy, dtype=np.float64)
    sell = np.asarray(bucket_size - buy, dtype=np.float64)
    return VolumeBuckets(bucket_size, buy, sell, _close_prices(p, cum, boundaries), remainder)


def bucket_trades_bvc(
    price: npt.ArrayLike,
    qty: npt.ArrayLike,
    bucket_size: float,
    *,
    sigma: float | None = None,
) -> VolumeBuckets:
    """Equal-volume bucketing with Bulk Volume Classification (Easley et al., 2016).

    ``V_B,tau = V * Phi(dP_tau / sigma)`` where ``dP_tau`` is the change in closing price
    from bucket ``tau-1`` to ``tau`` (the first bucket uses the first trade price as its
    reference) and ``sigma`` is the sample standard deviation of the ``dP`` series unless
    provided. When ``sigma`` is zero or undefined every bucket is classified 50/50.
    """
    p, q, keep = _validate_trades(price, qty, bucket_size)
    p, q = p[keep], q[keep]
    cum = np.cumsum(q)
    boundaries, remainder = _bucket_grid(cum, bucket_size)
    if boundaries.shape[0] == 0:
        return _empty_buckets(bucket_size, remainder)
    close = _close_prices(p, cum, boundaries)
    reference = np.concatenate(([p[0]], close[:-1]))
    d_price = close - reference
    if sigma is None:
        sigma = float(np.std(d_price, ddof=1)) if d_price.shape[0] > 1 else 0.0
    z = d_price / sigma if sigma > 0 else np.zeros_like(d_price)
    buy = np.asarray(bucket_size * _norm_cdf(z), dtype=np.float64)
    sell = np.asarray(bucket_size - buy, dtype=np.float64)
    return VolumeBuckets(bucket_size, buy, sell, close, remainder)


# --------------------------------------------------------------------------- vpin
def compute_vpin_from_buckets(buckets: VolumeBuckets, window: int) -> npt.NDArray[np.float64]:
    """Rolling VPIN over ``window`` buckets; length ``n_buckets - window + 1`` (or 0).

    Values are clipped to ``[0, 1]`` to remove floating-point noise; the estimator is
    bounded there by construction because ``|V_S - V_B| <= V`` in every bucket.
    """
    if window < 1:
        msg = "window must be >= 1"
        raise ValueError(msg)
    imbalance = buckets.imbalance
    if imbalance.shape[0] < window:
        return np.empty(0, dtype=np.float64)
    rolling = np.convolve(imbalance, np.ones(window, dtype=np.float64), mode="valid")
    vpin = rolling / (window * buckets.bucket_size)
    return np.asarray(np.clip(vpin, 0.0, 1.0), dtype=np.float64)


def compute_vpin(
    price: npt.ArrayLike,
    qty: npt.ArrayLike,
    bucket_size: float,
    window: int,
    *,
    classification: Classification = "aggressor",
    aggressor: npt.ArrayLike | None = None,
    sigma: float | None = None,
) -> npt.NDArray[np.float64]:
    """Retrospective volume-synchronized flow-toxicity statistic (Easley et al. 2012).

    Not a predictive early-warning signal; see Andersen & Bondarenko (2014).

    Args:
        price, qty: Trade prices and volumes.
        bucket_size: Volume ``V`` per bucket.
        window: Number of buckets ``n`` in the rolling window.
        classification: ``"aggressor"`` (requires ``aggressor`` signs) or ``"bvc"``.
        aggressor: ``+1``/``-1`` taker signs, used by the ``"aggressor"`` method.
        sigma: Optional fixed ``sigma_dP`` for BVC (e.g. estimated on a longer history).
    """
    # Andersen & Bondarenko (2014, J. Financial Markets 17:1-46): VPIN maxima
    # around the 6 May 2010 Flash Crash arrived AFTER the collapse, not before.
    # Treat this series as a retrospective toxicity descriptor, not an alarm.
    if classification == "aggressor":
        if aggressor is None:
            msg = "aggressor signs are required for classification='aggressor'"
            raise ValueError(msg)
        buckets = bucket_trades(price, qty, bucket_size, aggressor)
    elif classification == "bvc":
        buckets = bucket_trades_bvc(price, qty, bucket_size, sigma=sigma)
    else:
        raise ValueError(f"unknown classification: {classification!r}")
    return compute_vpin_from_buckets(buckets, window)


def compute_retrospective_vpin(
    price: npt.ArrayLike,
    qty: npt.ArrayLike,
    bucket_size: float,
    window: int,
    *,
    classification: Classification = "aggressor",
    aggressor: npt.ArrayLike | None = None,
    sigma: float | None = None,
) -> npt.NDArray[np.float64]:
    """Retrospective volume-synchronized flow-toxicity statistic (Easley et al. 2012).

    Not a predictive early-warning signal; see Andersen & Bondarenko (2014).
    """
    return compute_vpin(
        price,
        qty,
        bucket_size,
        window,
        classification=classification,
        aggressor=aggressor,
        sigma=sigma,
    )
