"""Pure reconstruction protocol for Binance USD-M Futures L2 depth.

Official wording retrieved 2026-09-02 from Binance USD-M *Diff. Book Depth Streams*
("How to manage a local order book correctly"):

    4. Drop any event where u is < lastUpdateId in the snapshot.
    5. The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
    6. While listening to the stream, each new event's pu should be equal to the
       previous event's u, otherwise initialize the process from step 3.

Sources:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
    https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly

This is **not** the spot rule ``U <= lastUpdateId+1 AND u >= lastUpdateId+1``.
USD-M Futures also does **not** publish a book checksum; :func:`compare_top_levels`
is the honesty substitute (periodic REST top-N vs the local book).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError

if TYPE_CHECKING:
    from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel

BACKOFF_INITIAL_S: float = 0.5
BACKOFF_CAP_S: float = 30.0
BACKOFF_FACTOR: float = 2.0
JITTER_FRAC: float = 0.25
DEFAULT_MAX_BUFFER: int = 5_000
QTY_MATCH_TOL: float = 1e-9


class UniformRng(Protocol):
    """Anything with ``uniform(a, b)`` — ``random.Random`` satisfies it."""

    def uniform(self, a: float, b: float) -> float:
        """Draw a float in ``[a, b]``."""
        ...


class DepthSequenceValidator:
    """Stateful implementation of Binance USD-M local-order-book sequence rules.

    Given ``lastUpdateId`` of a REST snapshot:

    1. events with ``u < lastUpdateId`` are stale and are dropped;
    2. the first applied event must satisfy ``U <= lastUpdateId AND u >= lastUpdateId``
       (official futures wording; **not** the spot ``lastUpdateId+1`` rule);
    3. every later event must satisfy ``pu == u`` of the previously applied event.

    Violations of 2 or 3 raise :class:`SequenceGapError`: the caller must fetch a new
    snapshot and start over with a fresh validator.
    """

    def __init__(self, snapshot_last_update_id: int) -> None:
        self._last_update_id = snapshot_last_update_id
        self._synced = False

    @property
    def last_update_id(self) -> int:
        """``u`` of the last applied event (or the snapshot id before the first one)."""
        return self._last_update_id

    @property
    def synced(self) -> bool:
        """``True`` once the first bracketing event has been accepted."""
        return self._synced

    def validate(self, delta: BookDelta) -> bool:
        """Return ``True`` if ``delta`` must be applied, ``False`` if it is stale.

        Raises:
            SequenceGapError: If continuity is broken and a resync is required.
        """
        first, final, prev = (
            delta.first_update_id,
            delta.final_update_id,
            delta.prev_final_update_id,
        )
        if self._synced:
            if prev != self._last_update_id:
                msg = (
                    f"gap: expected pu={self._last_update_id}, got pu={prev} (U={first}, u={final})"
                )
                raise SequenceGapError(msg)
        elif final < self._last_update_id:
            return False
        elif not (first <= self._last_update_id <= final):
            msg = (
                f"first event U={first}, u={final} does not bracket "
                f"lastUpdateId={self._last_update_id}"
            )
            raise SequenceGapError(msg)
        else:
            self._synced = True
        self._last_update_id = final
        return True


class DepthDecision(StrEnum):
    """What the caller should do with a depth event."""

    BUFFER = "buffer"
    DROP_STALE = "drop_stale"
    APPLY = "apply"
    RESYNC = "resync"


@dataclass(frozen=True, slots=True)
class HonestyReport:
    """Top-N comparison of a local book against a REST snapshot (no venue checksum)."""

    compared: int
    mismatches: int
    max_qty_discrepancy: float
    last_update_id_local: int | None
    last_update_id_rest: int

    @property
    def matches(self) -> int:
        """Levels whose price exists locally with quantity within :data:`QTY_MATCH_TOL`."""
        return self.compared - self.mismatches


class DepthSynchronizer:
    """Buffers diffs until a REST snapshot, then applies the official futures rules.

    The synchronizer owns sequence/resync *policy*. :class:`~order_flow.orderbook.book.OrderBook`
    still refuses a delta that would corrupt it; this object decides when to drop, apply,
    or abandon the epoch and fetch a new snapshot.

    Never applies a gap event. After :meth:`on_disconnect` the next applied delta requires
    a fresh :meth:`install_snapshot` — in-flight buffers are not trusted across a dropped socket.
    """

    def __init__(self, *, max_buffer: int = DEFAULT_MAX_BUFFER) -> None:
        self.max_buffer = max_buffer
        self._buffer: list[BookDelta] = []
        self._validator: DepthSequenceValidator | None = None

    @property
    def awaiting_snapshot(self) -> bool:
        """``True`` until :meth:`install_snapshot` (or again after a gap / disconnect)."""
        return self._validator is None

    @property
    def buffered(self) -> int:
        """Number of diffs held because no snapshot is active yet."""
        return len(self._buffer)

    @property
    def last_update_id(self) -> int | None:
        """Snapshot id, or ``u`` of the last applied event; ``None`` if unsynchronised."""
        if self._validator is None:
            return None
        return self._validator.last_update_id

    @property
    def synced(self) -> bool:
        """``True`` once the first bracketing event of this epoch has been accepted."""
        return self._validator is not None and self._validator.synced

    def on_disconnect(self) -> None:
        """Drop buffer and snapshot state. The next epoch needs a new REST snapshot."""
        self._buffer.clear()
        self._validator = None

    def decide(self, delta: BookDelta) -> DepthDecision:
        """Classify ``delta``: buffer, drop as stale, apply, or demand a resync.

        A ``RESYNC`` decision never applies the gap event. Subsequent diffs are buffered
        until the caller installs a new snapshot.
        """
        if self._validator is None:
            if len(self._buffer) < self.max_buffer:
                self._buffer.append(delta)
            return DepthDecision.BUFFER
        try:
            apply = self._validator.validate(delta)
        except SequenceGapError:
            self._validator = None
            self._buffer.clear()
            return DepthDecision.RESYNC
        if apply:
            return DepthDecision.APPLY
        return DepthDecision.DROP_STALE

    def install_snapshot(self, last_update_id: int) -> list[tuple[BookDelta, DepthDecision]]:
        """Start a new epoch from REST ``lastUpdateId`` and replay the held buffer.

        Returns one decision per previously buffered delta, in arrival order. If a gap
        appears mid-replay the gap event is ``RESYNC`` (not applied) and leftovers stay
        buffered for the next snapshot.
        """
        self._validator = DepthSequenceValidator(last_update_id)
        pending = self._buffer
        self._buffer = []
        results: list[tuple[BookDelta, DepthDecision]] = []
        for delta in pending:
            results.append((delta, self.decide(delta)))
        return results


def observed_latency_ns(ts_recv_ns: int, ts_event_ns: int) -> int:
    """``ts_recv_ns - ts_event_ns`` (exchange event time vs local receive time)."""
    return ts_recv_ns - ts_event_ns


def reconnect_backoff(
    attempt: int,
    *,
    initial: float = BACKOFF_INITIAL_S,
    cap: float = BACKOFF_CAP_S,
    factor: float = BACKOFF_FACTOR,
) -> float:
    """Monotonic exponential backoff: ``initial * factor**attempt``, capped at ``cap``.

    ``attempt`` is 0-based so the first delay is ``initial`` (0.5s, 1s, 2s, …, 30s).
    """
    exponent = max(attempt, 0)
    return min(initial * (factor**exponent), cap)


def reconnect_delay(
    attempt: int,
    *,
    initial: float = BACKOFF_INITIAL_S,
    cap: float = BACKOFF_CAP_S,
    jitter_frac: float = JITTER_FRAC,
    rng: UniformRng | None = None,
) -> float:
    """:func:`reconnect_backoff` plus ``[0, jitter_frac * base]`` extra, still capped."""
    base = reconnect_backoff(attempt, initial=initial, cap=cap)
    generator: UniformRng = random.Random() if rng is None else rng  # noqa: S311
    jitter = generator.uniform(0.0, jitter_frac * base)
    return min(base + jitter, cap)


def compare_top_levels(book: OrderBook, rest: BookSnapshot, *, levels: int = 20) -> HonestyReport:
    """Compare local top-``levels`` quantities against a REST snapshot, keyed by price.

    Binance USD-M Futures does not expose a depth checksum. This is the honesty
    substitute: REST top-N is the reference; a missing local price counts as a
    mismatch with discrepancy equal to the REST quantity.
    """
    local_bids, local_asks = book.depth(levels)
    rest_book = OrderBook()
    rest_book.apply_snapshot(rest)
    rest_bids, rest_asks = rest_book.depth(levels)

    compared = 0
    mismatches = 0
    max_disc = 0.0
    for local_side, rest_side in ((local_bids, rest_bids), (local_asks, rest_asks)):
        side_compared, side_mismatches, side_disc = _compare_side(local_side, rest_side)
        compared += side_compared
        mismatches += side_mismatches
        max_disc = max(max_disc, side_disc)
    return HonestyReport(
        compared=compared,
        mismatches=mismatches,
        max_qty_discrepancy=max_disc,
        last_update_id_local=book.last_update_id,
        last_update_id_rest=rest.last_update_id,
    )


def _compare_side(local: list[PriceLevel], rest: list[PriceLevel]) -> tuple[int, int, float]:
    local_qty = {level.price: level.qty for level in local}
    compared = 0
    mismatches = 0
    max_disc = 0.0
    for level in rest:
        compared += 1
        local_q = local_qty.get(level.price)
        if local_q is None:
            mismatches += 1
            max_disc = max(max_disc, level.qty)
            continue
        disc = abs(local_q - level.qty)
        max_disc = max(max_disc, disc)
        if disc > QTY_MATCH_TOL:
            mismatches += 1
    return compared, mismatches, max_disc
