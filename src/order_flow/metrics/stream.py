"""Streaming OFI/CVD/MLOFI/VPIN accumulators wrapping the same numpy core as batch."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from order_flow.metrics._common import ofi_contributions
from order_flow.metrics.cvd import aggressor_sign_from_binance_m
from order_flow.metrics.mlofi import DEFAULT_MLOFI_LEVELS
from order_flow.metrics.vpin import Classification, VolumeBuckets, compute_vpin_from_buckets

if TYPE_CHECKING:
    import numpy.typing as npt

    from order_flow.ingestion.events import Trade
    from order_flow.orderbook.book import OrderBook

_REL_TOL = 1e-9


@dataclass
class OfiAccumulator:
    """Per-event ``e_n`` from consecutive synced L1 states (Cont et al. §2.1)."""

    _prev: tuple[float, float, float, float] | None = field(default=None, repr=False)

    def observe_l1(
        self,
        bid_px: float,
        bid_qty: float,
        ask_px: float,
        ask_qty: float,
        *,
        synced: bool,
    ) -> float | None:
        """Return ``e_n`` or ``None`` when the transition must be skipped."""
        if not synced or bid_qty <= 0.0 or ask_qty <= 0.0:
            self._prev = None
            return None
        current = (float(bid_px), float(bid_qty), float(ask_px), float(ask_qty))
        prev = self._prev
        if prev is None:
            self._prev = current
            return None
        events = ofi_contributions(
            np.array([prev[0], current[0]], dtype=np.float64),
            np.array([prev[1], current[1]], dtype=np.float64),
            np.array([prev[2], current[2]], dtype=np.float64),
            np.array([prev[3], current[3]], dtype=np.float64),
        )
        self._prev = current
        return float(events[0])

    def observe_book(self, book: OrderBook) -> float | None:
        """Read L1 from ``book``; skip and reset when ``not book.is_synced``."""
        if not book.is_synced:
            self._prev = None
            return None
        bid, ask = book.best_bid(), book.best_ask()
        if bid is None or ask is None:
            self._prev = None
            return None
        return self.observe_l1(bid.price, bid.qty, ask.price, ask.qty, synced=True)


@dataclass
class CvdAccumulator:
    """Running ``sum s_i v_i`` using the same sign convention as :func:`compute_cvd`."""

    total: float = 0.0

    def observe(self, qty: float, aggressor_sign: int) -> float:
        """Add signed volume and return the new cumulative."""
        self.total += float(qty) * float(aggressor_sign)
        return self.total

    def observe_binance_m(self, qty: float, *, m: bool) -> float:
        """``m=True`` (buyer is maker) → negative contribution."""
        return self.observe(qty, aggressor_sign_from_binance_m(m))

    def observe_trade(self, trade: Trade) -> float:
        """Add a parsed :class:`~order_flow.ingestion.events.Trade`."""
        return self.observe(trade.qty, trade.aggressor.sign)


def _norm_cdf(z: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    erf = np.vectorize(math.erf, otypes=[np.float64])
    return np.asarray(0.5 * (1.0 + erf(z / math.sqrt(2.0))), dtype=np.float64)


@dataclass
class VpinAccumulator:
    """Incremental volume buckets feeding the same VPIN core as batch.

    A trade that straddles ``V`` is split; ``observe`` may emit several values.
    Retrospective descriptor only (Andersen & Bondarenko 2014); not an alarm.
    """

    bucket_size: float
    window: int
    classification: Classification = "aggressor"
    sigma: float | None = None
    _partial_vol: float = field(default=0.0, repr=False)
    _partial_buy: float = field(default=0.0, repr=False)
    _buy: list[float] = field(default_factory=list, repr=False)
    _sell: list[float] = field(default_factory=list, repr=False)
    _close: list[float] = field(default_factory=list, repr=False)
    _first_price: float | None = field(default=None, repr=False)
    _emitted: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        if not self.bucket_size > 0:
            msg = "bucket_size must be > 0"
            raise ValueError(msg)
        if self.window < 1:
            msg = "window must be >= 1"
            raise ValueError(msg)

    @property
    def remainder(self) -> float:
        """Volume in the incomplete trailing bucket."""
        return float(self._partial_vol)

    def observe(
        self, price: float, qty: float, aggressor_sign: float | int | None = None
    ) -> list[float]:
        """Ingest one trade; return newly completed rolling-VPIN values."""
        volume = float(qty)
        if volume <= 0.0:
            return []
        if self.classification == "aggressor":
            if aggressor_sign is None:
                msg = "aggressor signs are required for classification='aggressor'"
                raise ValueError(msg)
            sign = float(aggressor_sign)
            if sign not in (-1.0, 1.0):
                msg = "aggressor must contain only +1 (buy) and -1 (sell)"
                raise ValueError(msg)
        elif self.classification != "bvc":
            raise ValueError(f"unknown classification: {self.classification!r}")
        else:
            sign = 0.0
        if self._first_price is None:
            self._first_price = float(price)
        remaining = volume
        newly: list[float] = []
        while remaining > 0.0:
            space = self.bucket_size - self._partial_vol
            take = min(remaining, space)
            if self.classification == "aggressor" and sign > 0.0:
                self._partial_buy += take
            self._partial_vol += take
            remaining -= take
            if self._partial_vol + self.bucket_size * _REL_TOL >= self.bucket_size:
                self._close.append(float(price))
                if self.classification == "aggressor":
                    buy = min(max(self._partial_buy, 0.0), self.bucket_size)
                    self._buy.append(buy)
                    self._sell.append(self.bucket_size - buy)
                self._partial_vol = 0.0
                self._partial_buy = 0.0
                newly.extend(self._new_vpin_values())
        return newly

    def _bvc_volumes(
        self, close: npt.NDArray[np.float64]
    ) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
        first = self._first_price if self._first_price is not None else float(close[0])
        reference = np.concatenate(([first], close[:-1]))
        d_price = close - reference
        sigma = self.sigma
        if sigma is None:
            sigma = float(np.std(d_price, ddof=1)) if d_price.shape[0] > 1 else 0.0
        z = d_price / sigma if sigma > 0 else np.zeros_like(d_price)
        buy = np.asarray(self.bucket_size * _norm_cdf(z), dtype=np.float64)
        sell = np.asarray(self.bucket_size - buy, dtype=np.float64)
        return buy, sell

    def _new_vpin_values(self) -> list[float]:
        close = np.asarray(self._close, dtype=np.float64)
        if self.classification == "bvc":
            buy, sell = self._bvc_volumes(close)
        else:
            buy = np.asarray(self._buy, dtype=np.float64)
            sell = np.asarray(self._sell, dtype=np.float64)
        buckets = VolumeBuckets(self.bucket_size, buy, sell, close, self._partial_vol)
        series = compute_vpin_from_buckets(buckets, self.window)
        new = series[self._emitted :]
        self._emitted = int(series.shape[0])
        return [float(value) for value in new]


@dataclass
class MlofiAccumulator:
    """Per-event, per-level ``e^m_n`` from consecutive synced book states."""

    levels: int = DEFAULT_MLOFI_LEVELS
    _prev: (
        tuple[
            npt.NDArray[np.float64],
            npt.NDArray[np.float64],
            npt.NDArray[np.float64],
            npt.NDArray[np.float64],
        ]
        | None
    ) = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.levels < 1:
            msg = "levels must be >= 1"
            raise ValueError(msg)

    def _pad(self, values: npt.ArrayLike) -> npt.NDArray[np.float64]:
        arr = np.asarray(values, dtype=np.float64).reshape(-1)
        out = np.full(self.levels, np.nan, dtype=np.float64)
        n_keep = min(int(arr.shape[0]), self.levels)
        out[:n_keep] = arr[:n_keep]
        return out

    def observe_arrays(
        self,
        bid_px: npt.ArrayLike,
        bid_qty: npt.ArrayLike,
        ask_px: npt.ArrayLike,
        ask_qty: npt.ArrayLike,
        *,
        synced: bool,
    ) -> npt.NDArray[np.float64] | None:
        """Return shape ``(M,)`` or ``None`` when the transition must be skipped."""
        bp = self._pad(bid_px)
        bq = self._pad(bid_qty)
        ap = self._pad(ask_px)
        aq = self._pad(ask_qty)
        l1_ok = (
            np.isfinite(bp[0])
            and np.isfinite(bq[0])
            and np.isfinite(ap[0])
            and np.isfinite(aq[0])
            and bq[0] > 0.0
            and aq[0] > 0.0
        )
        if not synced or not l1_ok:
            self._prev = None
            return None
        current = (bp, bq, ap, aq)
        prev = self._prev
        if prev is None:
            self._prev = current
            return None
        events = ofi_contributions(
            np.stack([prev[0], bp]),
            np.stack([prev[1], bq]),
            np.stack([prev[2], ap]),
            np.stack([prev[3], aq]),
        )
        self._prev = current
        return np.asarray(events[0], dtype=np.float64)

    def observe_book(self, book: OrderBook) -> npt.NDArray[np.float64] | None:
        """Read top-``M`` arrays from ``book``; skip and reset when unsynced."""
        if not book.is_synced:
            self._prev = None
            return None
        arrays = book.to_arrays(self.levels)
        return self.observe_arrays(
            arrays.bid_px, arrays.bid_qty, arrays.ask_px, arrays.ask_qty, synced=True
        )
