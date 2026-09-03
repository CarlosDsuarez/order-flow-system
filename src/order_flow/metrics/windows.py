"""Fixed-length time windows for summing event contributions (Cont et al. OFI_k).

Callers: ``compute_ofi_time_windows``, ``scripts/validate_ofi.py``. Affected API:
``sum_in_time_windows``, ``last_in_time_windows``, ``TimeWindowSums``.
User: interval aggregation 1s/5s/10s; alignment for next-window Δmid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from order_flow.utils.time import NS_PER_S

if TYPE_CHECKING:
    import numpy.typing as npt

__all__ = ["NS_PER_S", "TimeWindowSums", "last_in_time_windows", "sum_in_time_windows"]


@dataclass(frozen=True, slots=True)
class TimeWindowSums:
    """Sums of ``values`` on a uniform time grid.

    ``valid`` is False when more than one epoch contributed to the window (a resync
    crossed the bar). Empty windows are valid with ``values == 0`` and ``counts == 0``.
    """

    start_ns: npt.NDArray[np.int64]
    values: npt.NDArray[np.float64]
    counts: npt.NDArray[np.int64]
    valid: npt.NDArray[np.bool_]


def _window_index(
    ts_ns: npt.NDArray[np.int64], window_ns: int, origin_ns: int
) -> npt.NDArray[np.int64]:
    return (ts_ns - origin_ns) // window_ns


def sum_in_time_windows(
    ts_ns: npt.ArrayLike,
    values: npt.ArrayLike,
    window_ns: int,
    *,
    origin_ns: int | None = None,
    epoch: npt.ArrayLike | None = None,
) -> TimeWindowSums:
    """Sum ``values`` into ``[origin + kτ, origin + (k+1)τ)`` bars.

    Emits every bar from the first event through the last event (internal empty bars
    included). ``epoch`` marks contiguous synced periods; a bar that mixes epochs is
    ``valid=False``.
    """
    if window_ns <= 0:
        msg = "window_ns must be > 0"
        raise ValueError(msg)
    ts = np.asarray(ts_ns, dtype=np.int64)
    vals = np.asarray(values, dtype=np.float64)
    if ts.ndim != 1 or vals.ndim != 1:
        msg = f"ts_ns and values must be 1-D, got {ts.shape} and {vals.shape}"
        raise ValueError(msg)
    if ts.shape != vals.shape:
        msg = f"ts_ns and values must have the same shape, got {ts.shape} and {vals.shape}"
        raise ValueError(msg)
    if ts.shape[0] == 0:
        return TimeWindowSums(
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.float64),
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.bool_),
        )
    origin = int(ts.min() // window_ns * window_ns) if origin_ns is None else origin_ns
    index = _window_index(ts, window_ns, origin)
    first = int(index.min())
    n_bars = int(index.max()) - first + 1
    relative = index - first
    summed = np.bincount(relative, weights=vals, minlength=n_bars)
    counts = np.bincount(relative, minlength=n_bars).astype(np.int64)
    valid = np.ones(n_bars, dtype=np.bool_)
    if epoch is not None:
        ep = np.asarray(epoch, dtype=np.int64)
        if ep.shape != ts.shape:
            msg = f"epoch must have the same shape as ts_ns, got {ep.shape}"
            raise ValueError(msg)
        min_ep = np.full(n_bars, np.iinfo(np.int64).max, dtype=np.int64)
        max_ep = np.full(n_bars, np.iinfo(np.int64).min, dtype=np.int64)
        np.minimum.at(min_ep, relative, ep)
        np.maximum.at(max_ep, relative, ep)
        mixed = (counts > 0) & (min_ep != max_ep)
        valid &= ~mixed
    starts = origin + (first + np.arange(n_bars, dtype=np.int64)) * window_ns
    return TimeWindowSums(
        np.asarray(starts, dtype=np.int64),
        np.asarray(summed, dtype=np.float64),
        counts,
        valid,
    )


def last_in_time_windows(
    ts_ns: npt.ArrayLike,
    values: npt.ArrayLike,
    window_ns: int,
    *,
    origin_ns: int,
    n_bars: int,
    first_index: int,
) -> npt.NDArray[np.float64]:
    """Last observation in each bar, carrying forward across empty bars.

    Bars before the first observation stay NaN.
    """
    ts = np.asarray(ts_ns, dtype=np.int64)
    vals = np.asarray(values, dtype=np.float64)
    out = np.full(n_bars, np.nan, dtype=np.float64)
    if ts.shape[0] == 0:
        return out
    index = _window_index(ts, window_ns, origin_ns) - first_index
    in_range = (index >= 0) & (index < n_bars)
    last_by_bar = np.full(n_bars, np.nan, dtype=np.float64)
    for i in np.nonzero(in_range)[0]:
        last_by_bar[int(index[i])] = vals[i]
    running = np.nan
    for k in range(n_bars):
        if not np.isnan(last_by_bar[k]):
            running = last_by_bar[k]
        out[k] = running
    return out
