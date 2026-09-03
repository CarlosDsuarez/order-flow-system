"""L2 (price-level) limit order book maintained from snapshots and incremental deltas.

Phase-1 caveat: prices are ``float`` dictionary keys. This is safe because exchanges send
prices as decimal strings and ``float("100.50")`` is deterministic, so a delta always
addresses the same key as the snapshot. Arithmetic on prices (mid, spread) is subject to
normal floating-point error; the roadmap migrates to integer ticks.
"""

from __future__ import annotations

from itertools import islice
from typing import TYPE_CHECKING, NamedTuple

import numpy as np
from sortedcontainers import SortedDict

from order_flow.ingestion.events import BookSnapshot, PriceLevel
from order_flow.orderbook.errors import EmptyBookError, SequenceGapError

if TYPE_CHECKING:
    import numpy.typing as npt

    from order_flow.ingestion.events import BookDelta


class BookArrays(NamedTuple):
    """Top-of-book arrays, best level first, NaN-padded when the book is shallower."""

    bid_px: npt.NDArray[np.float64]
    bid_qty: npt.NDArray[np.float64]
    ask_px: npt.NDArray[np.float64]
    ask_qty: npt.NDArray[np.float64]


class OrderBook:
    """Price-level order book with O(log n) updates and best-level access.

    Sequence handling on :meth:`apply_delta` (exchange-agnostic form of the Binance rules):

    * a delta whose ``prev_final_update_id`` equals the book's ``last_update_id`` is
      contiguous and applied;
    * right after a snapshot, a delta with ``final_update_id < last_update_id`` is stale
      and ignored (returns ``False``) and the first applied delta must bracket the
      snapshot id (``first_update_id <= last_update_id <= final_update_id``);
    * anything else raises :class:`SequenceGapError`.
    """

    def __init__(self, exchange: str = "", symbol: str = "") -> None:
        self.exchange = exchange
        self.symbol = symbol
        self._bids: SortedDict[float, float] = SortedDict()
        self._asks: SortedDict[float, float] = SortedDict()
        self.last_update_id: int | None = None
        # Sequence protocol: False until the first accepted delta of this snapshot epoch.
        self._seq_ready = False
        # Metrics trust: True after a snapshot, False when empty or a gap forces resync.
        self._is_synced = False
        self.ts_event_ns = 0
        self.ts_recv_ns = 0

    # ----------------------------------------------------------------- state
    @property
    def is_synced(self) -> bool:
        """``True`` after a snapshot has been applied and no sequence gap is pending.

        Distinct from the internal first-delta bracketing flag. Metrics must skip
        reads while this is ``False``. The feed calls :meth:`mark_unsynced` on a
        detected gap / disconnect; :meth:`apply_delta` also clears it before raising
        :class:`SequenceGapError`.
        """
        return self._is_synced

    @property
    def last_update_ts_ns(self) -> int:
        """``ts_event_ns`` of the last applied snapshot or delta."""
        return self.ts_event_ns

    def mark_unsynced(self) -> None:
        """Flip the metrics flag off until the next :meth:`apply_snapshot` (resync)."""
        self._is_synced = False

    @property
    def is_empty(self) -> bool:
        """``True`` when neither side has levels."""
        return not self._bids and not self._asks

    @property
    def n_levels(self) -> tuple[int, int]:
        """Number of (bid, ask) price levels currently in the book."""
        return len(self._bids), len(self._asks)

    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        """Replace the whole book with ``snapshot`` (levels with ``qty <= 0`` are skipped)."""
        self._bind_instrument(snapshot.exchange, snapshot.symbol)
        self._bids.clear()
        self._asks.clear()
        for level in snapshot.bids:
            self._set_level(self._bids, level)
        for level in snapshot.asks:
            self._set_level(self._asks, level)
        self.last_update_id = snapshot.last_update_id
        self._seq_ready = False
        self._is_synced = True
        self.ts_event_ns = snapshot.ts_event_ns
        self.ts_recv_ns = snapshot.ts_recv_ns

    def apply_delta(self, delta: BookDelta) -> bool:
        """Apply an incremental update; return ``False`` if it was stale and ignored.

        Raises:
            EmptyBookError: If no snapshot has been applied yet.
            SequenceGapError: If update-id continuity is broken (resync required).
        """
        if self.last_update_id is None:
            msg = "apply_snapshot() must be called before apply_delta()"
            raise EmptyBookError(msg)
        self._bind_instrument(delta.exchange, delta.symbol)
        if delta.prev_final_update_id != self.last_update_id:
            if self._seq_ready:
                msg = (
                    f"gap: expected prev_final_update_id={self.last_update_id}, "
                    f"got {delta.prev_final_update_id} "
                    f"(first={delta.first_update_id}, final={delta.final_update_id})"
                )
                self._is_synced = False
                raise SequenceGapError(msg)
            if delta.final_update_id < self.last_update_id:
                return False
            if not (delta.first_update_id <= self.last_update_id <= delta.final_update_id):
                msg = (
                    f"first delta [{delta.first_update_id}, {delta.final_update_id}] does not "
                    f"bracket snapshot id {self.last_update_id}"
                )
                self._is_synced = False
                raise SequenceGapError(msg)
        for level in delta.bids:
            self._set_level(self._bids, level)
        for level in delta.asks:
            self._set_level(self._asks, level)
        self.last_update_id = delta.final_update_id
        self._seq_ready = True
        self.ts_event_ns = delta.ts_event_ns
        self.ts_recv_ns = delta.ts_recv_ns
        return True

    # ----------------------------------------------------------------- queries
    def best_bid(self) -> PriceLevel | None:
        """Highest bid level, or ``None`` if the bid side is empty."""
        if not self._bids:
            return None
        price, qty = self._bids.peekitem(-1)
        return PriceLevel(price, qty)

    def best_ask(self) -> PriceLevel | None:
        """Lowest ask level, or ``None`` if the ask side is empty."""
        if not self._asks:
            return None
        price, qty = self._asks.peekitem(0)
        return PriceLevel(price, qty)

    def mid_price(self) -> float:
        """``(Pb + Pa) / 2``."""
        bid, ask = self._top()
        return (bid.price + ask.price) / 2.0

    def spread(self) -> float:
        """``Pa - Pb`` (negative when the book is crossed)."""
        bid, ask = self._top()
        return ask.price - bid.price

    def microprice(self) -> float:
        """Size-weighted mid ``(Pa * Qb + Pb * Qa) / (Qa + Qb)``.

        See ``docs/math/microprice.md``.
        """
        bid, ask = self._top()
        return (ask.price * bid.qty + bid.price * ask.qty) / (ask.qty + bid.qty)

    def imbalance(self, levels: int = 1) -> float:
        """Depth imbalance ``(Qb - Qa) / (Qb + Qa)`` over the top ``levels`` of each side.

        Ranges from ``-1`` (only asks) to ``+1`` (only bids).

        Raises:
            EmptyBookError: If both sides are empty.
        """
        bids, asks = self.depth(levels)
        qty_bid = sum(level.qty for level in bids)
        qty_ask = sum(level.qty for level in asks)
        total = qty_bid + qty_ask
        if total <= 0:
            msg = "imbalance is undefined for an empty book"
            raise EmptyBookError(msg)
        return (qty_bid - qty_ask) / total

    def depth(self, levels: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """Top ``levels`` bids (descending price) and asks (ascending price)."""
        if levels < 1:
            msg = "levels must be >= 1"
            raise ValueError(msg)
        bids = [
            PriceLevel(price, self._bids[price])
            for price in islice(self._bids.irange(reverse=True), levels)
        ]
        asks = [PriceLevel(price, self._asks[price]) for price in islice(self._asks, levels)]
        return bids, asks

    def depth_at_level(self, n: int) -> tuple[PriceLevel | None, PriceLevel | None]:
        """The n-th best bid and ask (1-indexed; level 1 is the BBO).

        Missing sides return ``None``. ``n < 1`` raises ``ValueError``.
        """
        if n < 1:
            msg = "n must be >= 1 (1-indexed; level 1 is the BBO)"
            raise ValueError(msg)
        bids, asks = self.depth(n)
        bid = bids[n - 1] if len(bids) >= n else None
        ask = asks[n - 1] if len(asks) >= n else None
        return bid, ask

    def snapshot(self) -> BookSnapshot:
        """Serializable full-book snapshot for Parquet (every in-memory level).

        Raises:
            EmptyBookError: If no snapshot has ever been applied.
        """
        if self.last_update_id is None:
            msg = "snapshot() requires an applied snapshot"
            raise EmptyBookError(msg)
        n_bid, n_ask = self.n_levels
        if n_bid == 0 and n_ask == 0:
            bids: list[PriceLevel] = []
            asks: list[PriceLevel] = []
        else:
            bids, asks = self.depth(max(n_bid, n_ask, 1))
            bids = bids[:n_bid]
            asks = asks[:n_ask]
        return BookSnapshot(
            exchange=self.exchange,
            symbol=self.symbol,
            ts_event_ns=self.ts_event_ns,
            ts_recv_ns=self.ts_recv_ns,
            last_update_id=self.last_update_id,
            bids=tuple(bids),
            asks=tuple(asks),
        )

    def is_crossed(self) -> bool:
        """``True`` when ``Pb >= Pa`` (locked or crossed), which signals a corrupted book."""
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid.price >= ask.price

    def to_arrays(self, levels: int) -> BookArrays:
        """Top-``levels`` prices/quantities as float64 arrays for the metrics layer."""
        bids, asks = self.depth(levels)
        bid_px = np.full(levels, np.nan, dtype=np.float64)
        bid_qty = np.full(levels, np.nan, dtype=np.float64)
        ask_px = np.full(levels, np.nan, dtype=np.float64)
        ask_qty = np.full(levels, np.nan, dtype=np.float64)
        for i, level in enumerate(bids):
            bid_px[i], bid_qty[i] = level.price, level.qty
        for i, level in enumerate(asks):
            ask_px[i], ask_qty[i] = level.price, level.qty
        return BookArrays(bid_px, bid_qty, ask_px, ask_qty)

    # ----------------------------------------------------------------- internals
    def _top(self) -> tuple[PriceLevel, PriceLevel]:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            msg = "both sides of the book must have at least one level"
            raise EmptyBookError(msg)
        return bid, ask

    def _bind_instrument(self, exchange: str, symbol: str) -> None:
        if not self.exchange:
            self.exchange = exchange
        if not self.symbol:
            self.symbol = symbol
        if (exchange, symbol) != (self.exchange, self.symbol):
            msg = f"event for {exchange}:{symbol} applied to book {self.exchange}:{self.symbol}"
            raise ValueError(msg)

    @staticmethod
    def _set_level(side: SortedDict[float, float], level: PriceLevel) -> None:
        if level.qty <= 0:
            side.pop(level.price, None)
        else:
            side[level.price] = level.qty
