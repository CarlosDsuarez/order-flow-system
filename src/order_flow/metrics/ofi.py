"""Order Flow Imbalance (OFI) at the best bid/ask.

Reference:
    Cont, R., Kukanov, A. & Stoikov, S. (2014). The Price Impact of Order Book Events.
    Journal of Financial Econometrics, 12(1), 47-88. https://arxiv.org/abs/1011.6402

For consecutive top-of-book states ``n-1 -> n`` with bid price/size ``(Pb, qb)`` and ask
price/size ``(Pa, qa)`` the event contribution is::

    e_n =   1{Pb_n >= Pb_{n-1}} * qb_n  -  1{Pb_n <= Pb_{n-1}} * qb_{n-1}
          - 1{Pa_n <= Pa_{n-1}} * qa_n  +  1{Pa_n >= Pa_{n-1}} * qa_{n-1}

and OFI over a window is the sum of the ``e_n`` it contains. Positive values mean net
buying pressure (bid depth added / ask depth consumed). See ``docs/math/ofi.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from order_flow.metrics._common import (
    MIN_STATES,
    aggregate_windows,
    as_1d_float,
    check_same_shape,
    ofi_contributions,
)
from order_flow.metrics.windows import last_in_time_windows, sum_in_time_windows

if TYPE_CHECKING:
    import numpy.typing as npt


@dataclass(frozen=True, slots=True)
class OfiWindowFrame:
    """OFI and mid on a uniform time grid.

    ``delta_mid[k]`` is contemporaneous ``mid[k] - mid[k-1]`` (NaN at k=0).
    ``delta_mid_lead1[k]`` is next-window ``mid[k+1] - mid[k]`` (NaN at the last bar).
    """

    start_ns: npt.NDArray[np.int64]
    ofi: npt.NDArray[np.float64]
    mid: npt.NDArray[np.float64]
    delta_mid: npt.NDArray[np.float64]
    delta_mid_lead1: npt.NDArray[np.float64]
    n_events: npt.NDArray[np.int64]
    valid: npt.NDArray[np.bool_]


def compute_ofi_events(
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
) -> npt.NDArray[np.float64]:
    """Per-event contributions ``e_n`` (Cont, Kukanov & Stoikov, 2014, §2.1).

    Args:
        bid_px, bid_qty, ask_px, ask_qty: 1-D arrays of length ``N`` with the L1 state
            after each book event.

    Returns:
        Array of length ``N - 1`` (empty when ``N < 2``); element ``i`` is ``e_{i+1}``.
    """
    bp = as_1d_float(bid_px, "bid_px")
    bq = as_1d_float(bid_qty, "bid_qty")
    ap = as_1d_float(ask_px, "ask_px")
    aq = as_1d_float(ask_qty, "ask_qty")
    check_same_shape(bid_px=bp, bid_qty=bq, ask_px=ap, ask_qty=aq)
    return ofi_contributions(bp, bq, ap, aq)


def compute_ofi(
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
    window: int,
    *,
    partial: bool = False,
) -> npt.NDArray[np.float64]:
    """OFI aggregated over consecutive, non-overlapping groups of ``window`` events.

    ``OFI_k = sum(e_n for n in group k)``. An incomplete trailing group is dropped unless
    ``partial=True``.
    """
    events = compute_ofi_events(bid_px, bid_qty, ask_px, ask_qty)
    return aggregate_windows(events, window, partial=partial)


def compute_ofi_time_windows(
    state_ts_ns: npt.ArrayLike,
    bid_px: npt.ArrayLike,
    bid_qty: npt.ArrayLike,
    ask_px: npt.ArrayLike,
    ask_qty: npt.ArrayLike,
    *,
    window_ns: int,
    origin_ns: int | None = None,
    epoch: npt.ArrayLike | None = None,
) -> OfiWindowFrame:
    """Sum ``e_n`` on a time grid and pair with window-end mids.

    ``e_n`` is stamped with the later state's ``ts`` (Cont et al. event time). Mid at
    bar ``k`` is the last L1 mid with ``ts < origin + (k+1)τ``, carried forward across
    empty bars. ``delta_mid_lead1[k]`` is ``mid[k+1] - mid[k]`` (predictive next window).
    """
    ts = np.asarray(state_ts_ns, dtype=np.int64)
    bp = as_1d_float(bid_px, "bid_px")
    bq = as_1d_float(bid_qty, "bid_qty")
    ap = as_1d_float(ask_px, "ask_px")
    aq = as_1d_float(ask_qty, "ask_qty")
    check_same_shape(bid_px=bp, bid_qty=bq, ask_px=ap, ask_qty=aq)
    if ts.shape != bp.shape:
        msg = f"state_ts_ns must match L1 length, got {ts.shape} and {bp.shape}"
        raise ValueError(msg)
    events = ofi_contributions(bp, bq, ap, aq)
    if events.shape[0] == 0:
        empty_i = np.empty(0, dtype=np.int64)
        empty_f = np.empty(0, dtype=np.float64)
        return OfiWindowFrame(
            empty_i,
            empty_f,
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
        if ep.shape != ts.shape:
            msg = f"epoch must match L1 length, got {ep.shape}"
            raise ValueError(msg)
        crossed = ep[1:] != ep[:-1]
        events = np.asarray(np.where(crossed, np.nan, events), dtype=np.float64)
        event_epoch = ep[1:]
    origin = int(event_ts.min() // window_ns * window_ns) if origin_ns is None else origin_ns
    bars = sum_in_time_windows(
        event_ts, np.nan_to_num(events, nan=0.0), window_ns, origin_ns=origin, epoch=event_epoch
    )
    first_index = int((bars.start_ns[0] - origin) // window_ns) if bars.start_ns.size else 0
    mids = (bp + ap) / 2.0
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
    return OfiWindowFrame(
        bars.start_ns,
        np.asarray(bars.values, dtype=np.float64),
        mid,
        delta,
        lead,
        bars.counts,
        valid,
    )
