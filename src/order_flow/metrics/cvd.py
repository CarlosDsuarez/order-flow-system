"""Cumulative Volume Delta (CVD).

Practitioner metric: ``CVD_t = sum_{i <= t} s_i * v_i`` where ``s_i = +1`` for a
buyer-initiated trade, ``-1`` for a seller-initiated one, and ``v_i`` is its volume.

The classification foundation is Lee, C. M. C. & Ready, M. J. (1991). Inferring Trade
Direction from Intraday Data. Journal of Finance, 46(2), 733-746. Crypto exchanges
publish the aggressor side explicitly (Binance ``m`` = "buyer is maker"), so no inference
rule is needed here; :func:`sides_to_signs` maps that flag to ``s_i``.
See ``docs/math/cvd.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from order_flow.metrics._common import as_1d_float, check_same_shape

if TYPE_CHECKING:
    from collections.abc import Iterable

    import numpy.typing as npt

    from order_flow.ingestion.events import Side


@dataclass(frozen=True, slots=True)
class CvdBars:
    """Time-bar resampled volume delta.

    Attributes:
        bar_start_ns: Start of each bar (ns since epoch); bars are contiguous.
        delta: Signed volume per bar (zero for bars without trades).
        cvd: Running cumulative sum of ``delta``.
    """

    bar_start_ns: npt.NDArray[np.int64]
    delta: npt.NDArray[np.float64]
    cvd: npt.NDArray[np.float64]


def sides_to_signs(sides: Iterable[Side]) -> npt.NDArray[np.int8]:
    """Map aggressor sides to ``+1`` (buy) / ``-1`` (sell)."""
    return np.fromiter((side.sign for side in sides), dtype=np.int8)


def aggressor_sign_from_binance_m(m: bool) -> int:
    """Binance ``m``: buyer is the maker → seller is the aggressor → ``-1``."""
    return -1 if m else 1


def compute_trade_delta(qty: npt.ArrayLike, aggressor: npt.ArrayLike) -> npt.NDArray[np.float64]:
    """Signed volume ``s_i * v_i`` per trade.

    Args:
        qty: Trade volumes (``>= 0``).
        aggressor: ``+1`` buyer-initiated, ``-1`` seller-initiated.
    """
    q = as_1d_float(qty, "qty")
    s = as_1d_float(aggressor, "aggressor")
    check_same_shape(qty=q, aggressor=s)
    if not np.all(np.isin(s, (-1.0, 1.0))):
        msg = "aggressor must contain only +1 (buy) and -1 (sell)"
        raise ValueError(msg)
    return np.asarray(q * s, dtype=np.float64)


def compute_cvd(qty: npt.ArrayLike, aggressor: npt.ArrayLike) -> npt.NDArray[np.float64]:
    """Cumulative volume delta ``CVD_t = sum_{i <= t} s_i * v_i`` per trade."""
    return np.asarray(np.cumsum(compute_trade_delta(qty, aggressor)), dtype=np.float64)


def resample_cvd(
    ts_ns: npt.ArrayLike, delta: npt.ArrayLike, bar_ns: int, *, origin_ns: int = 0
) -> CvdBars:
    """Aggregate per-trade signed volume into fixed time bars.

    Bar ``k`` covers ``[origin_ns + k * bar_ns, origin_ns + (k + 1) * bar_ns)``; with the
    default ``origin_ns=0`` bars are aligned to the Unix epoch (e.g. whole minutes). Bars
    between the first and last trade with no activity are emitted with ``delta == 0`` so
    the cumulative series is a proper step function. Input order does not matter.
    """
    if bar_ns <= 0:
        msg = "bar_ns must be > 0"
        raise ValueError(msg)
    ts = np.asarray(ts_ns, dtype=np.int64)
    d = as_1d_float(delta, "delta")
    if ts.ndim != 1:
        msg = f"ts_ns must be 1-D, got shape {ts.shape}"
        raise ValueError(msg)
    if ts.shape != d.shape:
        msg = f"ts_ns and delta must have the same shape, got {ts.shape} and {d.shape}"
        raise ValueError(msg)
    if ts.shape[0] == 0:
        empty_f = np.empty(0, dtype=np.float64)
        return CvdBars(np.empty(0, dtype=np.int64), empty_f, empty_f.copy())
    bar_index = (ts - origin_ns) // bar_ns
    first = int(bar_index.min())
    n_bars = int(bar_index.max()) - first + 1
    per_bar = np.bincount(bar_index - first, weights=d, minlength=n_bars)
    starts = origin_ns + (first + np.arange(n_bars, dtype=np.int64)) * bar_ns
    per_bar_f = np.asarray(per_bar, dtype=np.float64)
    return CvdBars(
        np.asarray(starts, dtype=np.int64),
        per_bar_f,
        np.asarray(np.cumsum(per_bar_f), dtype=np.float64),
    )
