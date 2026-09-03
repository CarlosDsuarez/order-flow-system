"""Parquet capture adapters: replay to arrays, then the same OFI/CVD/MLOFI/VPIN numpy core."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from order_flow.metrics.cvd import compute_cvd
from order_flow.metrics.mlofi import DEFAULT_MLOFI_LEVELS, compute_mlofi_events
from order_flow.metrics.ofi import compute_ofi_events
from order_flow.metrics.vpin import Classification, compute_retrospective_vpin
from order_flow.storage.parquet import read_events, trades_from_frame
from order_flow.storage.reconstruct import iter_l1_ticks, iter_lm_ticks

if TYPE_CHECKING:
    from pathlib import Path

    import numpy.typing as npt


@dataclass(frozen=True, slots=True)
class OfiEventSeries:
    """Synced L1 states plus per-event ``e_n`` (length ``N_states - n_epochs``)."""

    ts_ns: npt.NDArray[np.int64]
    e_n: npt.NDArray[np.float64]
    bid_px: npt.NDArray[np.float64]
    bid_qty: npt.NDArray[np.float64]
    ask_px: npt.NDArray[np.float64]
    ask_qty: npt.NDArray[np.float64]
    mid: npt.NDArray[np.float64]
    epoch: npt.NDArray[np.int64]
    state_ts_ns: npt.NDArray[np.int64]


def ofi_events_from_capture(root: Path, *, exchange: str, symbol: str) -> OfiEventSeries:
    """Replay a capture and compute Cont et al. ``e_n`` within each synced epoch."""
    ticks = iter_l1_ticks(root, exchange=exchange, symbol=symbol)
    empty_i = np.empty(0, dtype=np.int64)
    empty_f = np.empty(0, dtype=np.float64)
    if not ticks:
        return OfiEventSeries(
            empty_i, empty_f, empty_f, empty_f, empty_f, empty_f, empty_f, empty_i, empty_i
        )
    bid_px = np.array([tick.bid_px for tick in ticks], dtype=np.float64)
    bid_qty = np.array([tick.bid_qty for tick in ticks], dtype=np.float64)
    ask_px = np.array([tick.ask_px for tick in ticks], dtype=np.float64)
    ask_qty = np.array([tick.ask_qty for tick in ticks], dtype=np.float64)
    state_ts = np.array([tick.ts_event_ns for tick in ticks], dtype=np.int64)
    epoch = np.array([tick.epoch for tick in ticks], dtype=np.int64)
    mid = (bid_px + ask_px) / 2.0
    e_parts: list[npt.NDArray[np.float64]] = []
    ts_parts: list[npt.NDArray[np.int64]] = []
    for ep in np.unique(epoch):
        mask = epoch == ep
        events = compute_ofi_events(bid_px[mask], bid_qty[mask], ask_px[mask], ask_qty[mask])
        e_parts.append(events)
        ts_parts.append(state_ts[mask][1:] if events.shape[0] else empty_i)
    e_n = np.concatenate(e_parts) if e_parts else empty_f
    ts_ns = np.concatenate(ts_parts) if ts_parts else empty_i
    return OfiEventSeries(
        ts_ns=np.asarray(ts_ns, dtype=np.int64),
        e_n=np.asarray(e_n, dtype=np.float64),
        bid_px=bid_px,
        bid_qty=bid_qty,
        ask_px=ask_px,
        ask_qty=ask_qty,
        mid=mid,
        epoch=epoch,
        state_ts_ns=state_ts,
    )


def cvd_from_capture(root: Path, *, exchange: str, symbol: str) -> npt.NDArray[np.float64]:
    """Cumulative signed volume from stored trades (``aggressor_sign``)."""
    frame = read_events(root, "trade", exchange=exchange, symbol=symbol)
    if frame.height == 0:
        return np.empty(0, dtype=np.float64)
    trades = trades_from_frame(frame)
    qty = [trade.qty for trade in trades]
    signs = [trade.aggressor.sign for trade in trades]
    return compute_cvd(qty, signs)


def vpin_from_capture(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    bucket_size: float,
    window: int,
    classification: Classification = "aggressor",
    sigma: float | None = None,
) -> npt.NDArray[np.float64]:
    """Retrospective VPIN from stored trades (same core as :func:`compute_vpin`)."""
    frame = read_events(root, "trade", exchange=exchange, symbol=symbol)
    empty = np.empty(0, dtype=np.float64)
    if frame.height == 0:
        return empty
    trades = trades_from_frame(frame)
    price = [trade.price for trade in trades]
    qty = [trade.qty for trade in trades]
    signs = [trade.aggressor.sign for trade in trades]
    aggressor: list[int] | None = signs if classification == "aggressor" else None
    return compute_retrospective_vpin(
        price,
        qty,
        bucket_size,
        window,
        classification=classification,
        aggressor=aggressor,
        sigma=sigma,
    )


@dataclass(frozen=True, slots=True)
class MlofiEventSeries:
    """Synced M-level states plus per-event ``e^m_n`` (shape ``(N_events, M)``)."""

    ts_ns: npt.NDArray[np.int64]
    e_n: npt.NDArray[np.float64]
    bid_px: npt.NDArray[np.float64]
    bid_qty: npt.NDArray[np.float64]
    ask_px: npt.NDArray[np.float64]
    ask_qty: npt.NDArray[np.float64]
    mid: npt.NDArray[np.float64]
    epoch: npt.NDArray[np.int64]
    state_ts_ns: npt.NDArray[np.int64]


def mlofi_events_from_capture(
    root: Path, *, exchange: str, symbol: str, levels: int = DEFAULT_MLOFI_LEVELS
) -> MlofiEventSeries:
    """Replay a capture and compute Xu et al. ``e^m_n`` within each synced epoch."""
    ticks = iter_lm_ticks(root, exchange=exchange, symbol=symbol, levels=levels)
    empty_i = np.empty(0, dtype=np.int64)
    empty_f = np.empty(0, dtype=np.float64)
    empty_2d = np.empty((0, levels), dtype=np.float64)
    if not ticks:
        return MlofiEventSeries(
            empty_i, empty_2d, empty_2d, empty_2d, empty_2d, empty_2d, empty_f, empty_i, empty_i
        )
    bid_px = np.stack([tick.bid_px for tick in ticks])
    bid_qty = np.stack([tick.bid_qty for tick in ticks])
    ask_px = np.stack([tick.ask_px for tick in ticks])
    ask_qty = np.stack([tick.ask_qty for tick in ticks])
    state_ts = np.array([tick.ts_event_ns for tick in ticks], dtype=np.int64)
    epoch = np.array([tick.epoch for tick in ticks], dtype=np.int64)
    mid = (bid_px[:, 0] + ask_px[:, 0]) / 2.0
    e_parts: list[npt.NDArray[np.float64]] = []
    ts_parts: list[npt.NDArray[np.int64]] = []
    for ep in np.unique(epoch):
        mask = epoch == ep
        events = compute_mlofi_events(bid_px[mask], bid_qty[mask], ask_px[mask], ask_qty[mask])
        e_parts.append(events)
        ts_parts.append(state_ts[mask][1:] if events.shape[0] else empty_i)
    e_n = np.concatenate(e_parts) if e_parts else empty_2d
    ts_ns = np.concatenate(ts_parts) if ts_parts else empty_i
    return MlofiEventSeries(
        ts_ns=np.asarray(ts_ns, dtype=np.int64),
        e_n=np.asarray(e_n, dtype=np.float64),
        bid_px=bid_px,
        bid_qty=bid_qty,
        ask_px=ask_px,
        ask_qty=ask_qty,
        mid=np.asarray(mid, dtype=np.float64),
        epoch=epoch,
        state_ts_ns=state_ts,
    )
