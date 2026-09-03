"""Pure quote decision for the OFI-skewed market-making pipeline test.

Positive OFI (buying pressure) shifts both bid and ask **up** so the maker
buys less aggressively into informed flow. Negative OFI shifts both down.
``e_n`` itself is never reimplemented: :class:`RollingOfi` wraps
:class:`~order_flow.metrics.stream.OfiAccumulator`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from order_flow.metrics.stream import OfiAccumulator


@dataclass(frozen=True, slots=True)
class QuotePair:
    """One bid and one ask price, already snapped to the tick."""

    bid: float
    ask: float


def skew_ticks(*, ofi: float, threshold: float, max_skew: int) -> int:
    """Return ``+max_skew``, ``-max_skew`` or ``0`` from the sign of ``ofi``."""
    if threshold < 0:
        msg = "threshold must be >= 0"
        raise ValueError(msg)
    if max_skew < 0:
        msg = "max_skew must be >= 0"
        raise ValueError(msg)
    if ofi > threshold:
        return max_skew
    if ofi < -threshold:
        return -max_skew
    return 0


def _ticks(price: float, tick: float) -> int:
    return round(price / tick)


def _px(n_ticks: int, tick: float) -> float:
    return round(n_ticks * tick, 10)


def maker_quotes(
    *,
    mid: float,
    tick: float,
    spread_ticks: int,
    ofi: float,
    threshold: float,
    max_skew: int,
    best_bid: float | None = None,
    best_ask: float | None = None,
) -> QuotePair | None:
    """Symmetric quotes around ``mid``, then OFI skew, then a post-only clamp.

    The clamp keeps ``bid < best_ask`` and ``ask > best_bid`` (one tick inside
    the recorded BBO) so a ``post_only`` limit does not cross.
    """
    if tick <= 0:
        msg = "tick must be > 0"
        raise ValueError(msg)
    if spread_ticks < 1:
        msg = "spread_ticks must be >= 1"
        raise ValueError(msg)
    skew = skew_ticks(ofi=ofi, threshold=threshold, max_skew=max_skew)
    mid_ticks = _ticks(mid, tick)
    bid_ticks = mid_ticks - spread_ticks + skew
    ask_ticks = mid_ticks + spread_ticks + skew
    if best_ask is not None:
        bid_ticks = min(bid_ticks, _ticks(best_ask, tick) - 1)
    if best_bid is not None:
        ask_ticks = max(ask_ticks, _ticks(best_bid, tick) + 1)
    if bid_ticks >= ask_ticks:  # pragma: no cover - clamp widens; guard is defensive
        return None
    return QuotePair(bid=_px(bid_ticks, tick), ask=_px(ask_ticks, tick))


def aggressive_quotes(*, best_bid: float, best_ask: float) -> QuotePair:
    """Cross the spread: bid at the ask, ask at the bid (taker limits)."""
    return QuotePair(bid=best_ask, ask=best_bid)


@dataclass
class RollingOfi:
    """Sum of Cont et al. ``e_n`` over a trailing time window (default 1 s)."""

    window_ns: int = 1_000_000_000
    _acc: OfiAccumulator = field(default_factory=OfiAccumulator, repr=False)
    _events: deque[tuple[int, float]] = field(default_factory=deque, repr=False)

    def observe(
        self,
        ts_ns: int,
        bid_px: float,
        bid_qty: float,
        ask_px: float,
        ask_qty: float,
        *,
        synced: bool = True,
    ) -> float:
        """Ingest one L1 state and return the window sum (0 before the first ``e_n``)."""
        event = self._acc.observe_l1(bid_px, bid_qty, ask_px, ask_qty, synced=synced)
        if event is not None:
            self._events.append((ts_ns, event))
        cutoff = ts_ns - self.window_ns
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()
        return float(sum(value for _ts, value in self._events))
