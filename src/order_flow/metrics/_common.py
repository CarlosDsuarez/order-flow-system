"""Shared array validation and windowing helpers for the metrics modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

MIN_STATES = 2
"""Consecutive book states needed to form one event contribution."""
MATRIX_NDIM = 2


def as_1d_float(values: npt.ArrayLike, name: str) -> npt.NDArray[np.float64]:
    """Coerce ``values`` to a 1-D float64 array or raise ``ValueError`` naming the argument."""
    arr = np.asarray(values, dtype=np.float64)
    if arr.ndim != 1:
        msg = f"{name} must be 1-D, got shape {arr.shape}"
        raise ValueError(msg)
    return arr


def as_2d_float(values: npt.ArrayLike, name: str) -> npt.NDArray[np.float64]:
    """Coerce ``values`` to a 2-D ``(N, M)`` float64 array; 1-D input becomes ``(N, 1)``."""
    arr = np.asarray(values, dtype=np.float64)
    if arr.ndim == 1:
        arr = arr[:, np.newaxis]
    if arr.ndim != MATRIX_NDIM:
        msg = f"{name} must be 1-D or 2-D, got shape {arr.shape}"
        raise ValueError(msg)
    return arr


def check_same_shape(**arrays: npt.NDArray[np.float64]) -> None:
    """Raise ``ValueError`` unless every keyword array has the same shape."""
    shapes = {name: arr.shape for name, arr in arrays.items()}
    if len(set(shapes.values())) > 1:
        msg = f"arrays must have the same shape, got {shapes}"
        raise ValueError(msg)


def ofi_contributions(
    bid_px: npt.NDArray[np.float64],
    bid_qty: npt.NDArray[np.float64],
    ask_px: npt.NDArray[np.float64],
    ask_qty: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Cont-Kukanov-Stoikov event contributions between consecutive rows (axis 0).

    ``e_n = 1{Pb_n >= Pb_{n-1}} qb_n - 1{Pb_n <= Pb_{n-1}} qb_{n-1}
           - 1{Pa_n <= Pa_{n-1}} qa_n + 1{Pa_n >= Pa_{n-1}} qa_{n-1}``

    Works element-wise on any trailing shape (``(N,)`` for OFI, ``(N, M)`` for MLOFI) and
    returns ``N - 1`` rows. A transition involving a NaN price (missing level) is NaN.
    """
    if bid_px.shape[0] < MIN_STATES:
        return np.empty((0, *bid_px.shape[1:]), dtype=np.float64)
    bid_up = bid_px[1:] >= bid_px[:-1]
    bid_down = bid_px[1:] <= bid_px[:-1]
    ask_down = ask_px[1:] <= ask_px[:-1]
    ask_up = ask_px[1:] >= ask_px[:-1]
    events = (
        bid_up * bid_qty[1:]
        - bid_down * bid_qty[:-1]
        - ask_down * ask_qty[1:]
        + ask_up * ask_qty[:-1]
    )
    undefined = np.isnan(bid_px[1:] + bid_px[:-1] + ask_px[1:] + ask_px[:-1])
    return np.asarray(np.where(undefined, np.nan, events), dtype=np.float64)


def aggregate_windows(
    values: npt.NDArray[np.float64], window: int, *, partial: bool = False
) -> npt.NDArray[np.float64]:
    """Sum consecutive, non-overlapping groups of ``window`` rows along axis 0.

    With ``partial=False`` (default) an incomplete trailing group is dropped; with
    ``partial=True`` its sum is appended as the last row.
    """
    if window < 1:
        msg = "window must be >= 1"
        raise ValueError(msg)
    n_rows = values.shape[0]
    n_full = n_rows // window
    head = values[: n_full * window].reshape(n_full, window, *values.shape[1:]).sum(axis=1)
    if partial and n_rows % window:
        tail = values[n_full * window :].sum(axis=0, keepdims=True)
        head = np.concatenate([head, tail], axis=0)
    return np.asarray(head, dtype=np.float64)
