"""Multi-Level Order Flow Imbalance (MLOFI).

Reference:
    Xu, K., Gould, M. D. & Howison, S. D. (2019). Multi-Level Order-Flow Imbalance in a
    Limit Order Book. Market Microstructure and Liquidity, 4(3-4).
    https://arxiv.org/abs/1907.06230

The OFI event formula is applied independently at each depth level ``m = 1..M`` using the
``m``-th best bid/ask price and size::

    e^m_n =   1{Pb^m_n >= Pb^m_{n-1}} * qb^m_n  -  1{Pb^m_n <= Pb^m_{n-1}} * qb^m_{n-1}
            - 1{Pa^m_n <= Pa^m_{n-1}} * qa^m_n  +  1{Pa^m_n >= Pa^m_{n-1}} * qa^m_{n-1}

Level 1 reproduces OFI exactly. Levels can be combined with a weight vector
(``sum_m w_m * e^m_n``). Default weights are equal (ones) — a convenience of this
codebase, not a formula from Xu et al., who regress the vector. See ``docs/math/mlofi.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from order_flow.metrics._common import (
    MIN_STATES,
    aggregate_windows,
    as_2d_float,
    check_same_shape,
    ofi_contributions,
)
from order_flow.metrics.windows import last_in_time_windows, sum_in_time_windows

if TYPE_CHECKING:
    import numpy.typing as npt

DEFAULT_MLOFI_LEVELS = 5
"""Default depth ``M`` for streaming/batch adapters (Xu et al. also study ``M=10``)."""


@dataclass(frozen=True, slots=True)
class MlofiWindowFrame:
    """MLOFI and mid on a uniform time grid (same alignment as :class:`OfiWindowFrame`).

    ``mlofi`` has shape ``(K, M)``; column 0 equals L1 OFI. ``delta_mid`` /
    ``delta_mid_lead1`` are contemporaneous and next-window mid changes.
    """

    start_ns: npt.NDArray[np.int64]
    mlofi: npt.NDArray[np.float64]
    mid: npt.NDArray[np.float64]
    delta_mid: npt.NDArray[np.float64]
    delta_mid_lead1: npt.NDArray[np.float64]
    n_events: npt.NDArray[np.int64]
    valid: npt.NDArray[np.bool_]


def level_weights(n_levels: int, scheme: str = "equal") -> npt.NDArray[np.float64]:
    """Level weights for a scalar MLOFI sum. Default is equal (ones), not ``1/m``.

    ``scheme="inverse"`` is ``1/m`` for ``m=1..M``. Neither scheme is taken from Xu et al.
    """
    if n_levels < 1:
        msg = "n_levels must be >= 1"
        raise ValueError(msg)
    if scheme == "equal":
        return np.ones(n_levels, dtype=np.float64)
    if scheme == "inverse":
        return np.asarray(1.0 / np.arange(1, n_levels + 1, dtype=np.float64), dtype=np.float64)
    msg = f"unknown weight scheme: {scheme!r}"
    raise ValueError(msg)


def compute_mlofi_events(
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
) -> npt.NDArray[np.float64]:
    """Per-event, per-level contributions ``e^m_n`` (Xu, Gould & Howison, 2019).

    Args:
        bid_px, bid_qty, ask_px, ask_qty: arrays of shape ``(N, M)``; column ``m`` holds
            the ``m``-th best level (column 0 = best). 1-D inputs are treated as ``(N, 1)``.
            Levels missing from a shallow book may be NaN-padded; they yield NaN.

    Returns:
        Array of shape ``(N - 1, M)``; column 0 equals :func:`compute_ofi_events`.
    """
    bp = as_2d_float(bid_px, "bid_px")
    bq = as_2d_float(bid_qty, "bid_qty")
    ap = as_2d_float(ask_px, "ask_px")
    aq = as_2d_float(ask_qty, "ask_qty")
    check_same_shape(bid_px=bp, bid_qty=bq, ask_px=ap, ask_qty=aq)
    return ofi_contributions(bp, bq, ap, aq)


def aggregate_mlofi_levels(
    mlofi: npt.ArrayLike, weights: npt.ArrayLike | None = None
) -> npt.NDArray[np.float64]:
    """Collapse the level axis: ``sum_m w_m * e^m`` for each row.

    ``weights`` defaults to ones (plain sum across levels); it must have length ``M``.
    """
    values = as_2d_float(mlofi, "mlofi")
    n_levels = values.shape[1]
    w = np.ones(n_levels, dtype=np.float64) if weights is None else np.asarray(weights, np.float64)
    if w.shape != (n_levels,):
        msg = f"weights must have shape ({n_levels},), got {w.shape}"
        raise ValueError(msg)
    return np.asarray(values @ w, dtype=np.float64)


def compute_mlofi(
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
    window: int,
    *,
    weights: npt.ArrayLike | None = None,
    partial: bool = False,
) -> npt.NDArray[np.float64]:
    """MLOFI aggregated over consecutive groups of ``window`` events.

    Returns shape ``(K, M)`` when ``weights`` is ``None``, otherwise the weighted level sum
    with shape ``(K,)``. See :func:`~order_flow.metrics._common.aggregate_windows` for the
    handling of the incomplete trailing group.
    """
    events = compute_mlofi_events(bid_px, bid_qty, ask_px, ask_qty)
    windowed = aggregate_windows(events, window, partial=partial)
    if weights is None:
        return windowed
    return aggregate_mlofi_levels(windowed, weights)


def compute_mlofi_time_windows(
    state_ts_ns: npt.ArrayLike,
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
    *,
    window_ns: int,
    origin_ns: int | None = None,
    epoch: npt.ArrayLike | None = None,
) -> MlofiWindowFrame:
    """Sum per-level ``e^m_n`` on a time grid and pair with window-end L1 mids.

    Column 0 of ``mlofi`` equals :func:`compute_ofi_time_windows` ``ofi`` on the
    same L1 path. Mid is always the L1 mid (level 0).
    """
    ts = np.asarray(state_ts_ns, dtype=np.int64)
    bp = as_2d_float(bid_px, "bid_px")
    bq = as_2d_float(bid_qty, "bid_qty")
    ap = as_2d_float(ask_px, "ask_px")
    aq = as_2d_float(ask_qty, "ask_qty")
    check_same_shape(bid_px=bp, bid_qty=bq, ask_px=ap, ask_qty=aq)
    if ts.shape[0] != bp.shape[0]:
        msg = f"state_ts_ns must match book length, got {ts.shape} and {bp.shape}"
        raise ValueError(msg)
    n_levels = int(bp.shape[1])
    events = ofi_contributions(bp, bq, ap, aq)
    empty_i = np.empty(0, dtype=np.int64)
    empty_f = np.empty(0, dtype=np.float64)
    if events.shape[0] == 0:
        return MlofiWindowFrame(
            empty_i,
            np.empty((0, n_levels), dtype=np.float64),
            empty_f.copy(),
            empty_f.copy(),
            empty_f.copy(),
            empty_i.copy(),
            np.empty(0, dtype=np.bool_),
        )
    event_ts = ts[1:]
    event_epoch: npt.ArrayLike | None = None
    if epoch is not None:
        ep = np.asarray(epoch, dtype=np.int64)
        if ep.shape[0] != ts.shape[0]:
            msg = f"epoch must match book length, got {ep.shape}"
            raise ValueError(msg)
        crossed = ep[1:] != ep[:-1]
        events = np.asarray(np.where(crossed[:, np.newaxis], np.nan, events), dtype=np.float64)
        event_epoch = ep[1:]
    origin = int(event_ts.min() // window_ns * window_ns) if origin_ns is None else origin_ns
    cols: list[npt.NDArray[np.float64]] = []
    bars = sum_in_time_windows(
        event_ts,
        np.nan_to_num(events[:, 0], nan=0.0),
        window_ns,
        origin_ns=origin,
        epoch=event_epoch,
    )
    cols.append(np.asarray(bars.values, dtype=np.float64))
    for level in range(1, n_levels):
        level_bars = sum_in_time_windows(
            event_ts,
            np.nan_to_num(events[:, level], nan=0.0),
            window_ns,
            origin_ns=origin,
            epoch=event_epoch,
        )
        cols.append(np.asarray(level_bars.values, dtype=np.float64))
    stacked = np.column_stack(cols)
    first_index = int((bars.start_ns[0] - origin) // window_ns) if bars.start_ns.size else 0
    mids = (bp[:, 0] + ap[:, 0]) / 2.0
    mid = last_in_time_windows(
        ts,
        mids,
        window_ns,
        origin_ns=origin,
        n_bars=int(bars.start_ns.shape[0]),
        first_index=first_index,
    )
    delta = np.full(mid.shape[0], np.nan, dtype=np.float64)
    lead = np.full(mid.shape[0], np.nan, dtype=np.float64)
    if mid.shape[0] >= MIN_STATES:
        delta[1:] = np.diff(mid)
        lead[:-1] = np.diff(mid)
    valid = bars.valid & np.isfinite(mid)
    return MlofiWindowFrame(
        bars.start_ns,
        np.asarray(stacked, dtype=np.float64),
        mid,
        delta,
        lead,
        bars.counts,
        valid,
    )
