"""Rebuild an :class:`~order_flow.orderbook.book.OrderBook` from a Parquet capture.

Algorithm: load the latest snapshot with ``ts_event_ns <= T`` (ties broken by
``last_update_id``), then apply every delta with ``snapshot_ts < ts_event_ns <= T``
in ``(ts_event_ns, final_update_id)`` order.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from order_flow.ingestion.events import BookDelta, BookSnapshot
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError
from order_flow.storage.parquet import deltas_from_frame, read_events, snapshots_from_frame

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    import numpy.typing as npt


class ReconstructionError(Exception):
    """Capture is missing a snapshot at or before ``T``, or replay hit a sequence gap."""


@dataclass(frozen=True, slots=True)
class BookStream:
    """Book events a replay must apply, in order, plus what was left out."""

    events: tuple[BookSnapshot | BookDelta, ...]
    n_periodic_skipped: int
    n_off_chain_deltas: int


@dataclass(frozen=True, slots=True)
class _SnapshotRef:
    """What ordering needs from a snapshot; its levels are built only if it is kept."""

    last_update_id: int
    ts_event_ns: int
    ts_recv_ns: int
    row: int


def _uid(event: _SnapshotRef | BookDelta) -> int:
    return event.final_update_id if isinstance(event, BookDelta) else event.last_update_id


def _restamp(events: list[_SnapshotRef | BookDelta]) -> list[_SnapshotRef | BookDelta]:
    """Pull timestamps back so both clocks strictly increase along the replay order.

    The engines re-sort by time. A REST snapshot's ``E`` / receive time is the response,
    often later than the diff that follows it in update-id order; it is moved to 1 ns
    before that diff (the book it describes existed by then). In-order stamps are kept.
    """
    out = list(events)
    next_event = next_recv = None
    for index in range(len(out) - 1, -1, -1):
        event = out[index]
        ts_event, ts_recv = event.ts_event_ns, event.ts_recv_ns
        if next_event is not None and ts_event >= next_event:
            ts_event = next_event - 1
        if next_recv is not None and ts_recv >= next_recv:
            ts_recv = next_recv - 1
        if (ts_event, ts_recv) != (event.ts_event_ns, event.ts_recv_ns):
            out[index] = replace(event, ts_event_ns=ts_event, ts_recv_ns=ts_recv)
        next_event, next_recv = ts_event, ts_recv
    return out


def _select(
    refs: list[_SnapshotRef], deltas: list[BookDelta]
) -> tuple[list[_SnapshotRef | BookDelta], int, int]:
    """Replay order across resyncs; see :func:`book_stream`."""
    ordered: list[_SnapshotRef | BookDelta] = sorted(
        [*deltas, *refs], key=lambda e: (_uid(e), not isinstance(e, BookDelta))
    )
    kept: list[_SnapshotRef | BookDelta] = []
    periodic = off_chain = 0
    last_u: int | None = None
    bracketing = False
    index = 0
    while index < len(ordered):
        event = ordered[index]
        index += 1
        if not isinstance(event, BookDelta):
            if event.last_update_id == last_u:
                periodic += 1
                continue
            kept.append(event)
            last_u, bracketing = event.last_update_id, True
            continue
        # Same rule as OrderBook.apply_delta: ``pu`` continues the chain, or the first
        # delta after a snapshot brackets its id (futures protocol).
        continues = last_u is not None and (
            event.prev_final_update_id == last_u
            or (bracketing and event.first_update_id <= last_u <= event.final_update_id)
        )
        if not continues:
            upcoming = ordered[index] if index < len(ordered) else None
            if isinstance(upcoming, _SnapshotRef) and upcoming.last_update_id == _uid(event):
                ordered[index - 1], ordered[index] = upcoming, event  # REST first, then diff
                index -= 1
                continue
            # Update ids only grow, so once a delta breaks the chain no later delta can
            # continue ``last_u``: all are dropped until the next snapshot resets it.
            off_chain += 1
            continue
        kept.append(event)
        last_u, bracketing = event.final_update_id, False
    return _restamp(kept), periodic, off_chain


def _stream(
    refs: list[_SnapshotRef],
    deltas: list[BookDelta],
    build: Callable[[list[int]], list[BookSnapshot]],
) -> BookStream:
    kept, periodic, off_chain = _select(refs, deltas)
    snapshot_refs = [event for event in kept if isinstance(event, _SnapshotRef)]
    built = iter(build([ref.row for ref in snapshot_refs]))
    events: list[BookSnapshot | BookDelta] = []
    for event in kept:
        if isinstance(event, BookDelta):
            events.append(event)
            continue
        snapshot = next(built)
        events.append(replace(snapshot, ts_event_ns=event.ts_event_ns, ts_recv_ns=event.ts_recv_ns))
    return BookStream(
        events=tuple(events), n_periodic_skipped=periodic, n_off_chain_deltas=off_chain
    )


def book_stream(snapshots: list[BookSnapshot], deltas: list[BookDelta]) -> BookStream:
    """Order snapshots and deltas for replay, across resyncs, without stale state.

    Walks update ids, not event time: a REST snapshot's ``E`` can be later than the
    diff that brackets it. At equal ids a delta that continues the chain goes first
    and the snapshot is a periodic copy of that book (skipped); a delta that does not
    continue it follows a REST resync snapshot, which goes first (the diff may carry
    levels deeper than the REST depth). After a delta that breaks the chain, deltas
    are dropped until the next snapshot. Timestamps are then made monotonic
    (:func:`_restamp`).
    """
    refs = [
        _SnapshotRef(snap.last_update_id, snap.ts_event_ns, snap.ts_recv_ns, row)
        for row, snap in enumerate(snapshots)
    ]
    return _stream(refs, deltas, lambda rows: [snapshots[row] for row in rows])


def book_stream_from_capture(root: Path, *, exchange: str, symbol: str) -> BookStream:
    """:func:`book_stream` over a capture, building only the snapshots it applies.

    Periodic snapshots (thousands of levels, one every few seconds) are almost all
    skipped; materializing them first made multi-day captures slow and memory-bound.
    """
    frame = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    refs = [
        _SnapshotRef(int(uid), int(ts_event), int(ts_recv), row)
        for row, (uid, ts_event, ts_recv) in enumerate(
            zip(frame["last_update_id"], frame["ts_event_ns"], frame["ts_recv_ns"], strict=True)
        )
    ]
    deltas = deltas_from_frame(read_events(root, "book_delta", exchange=exchange, symbol=symbol))
    return _stream(refs, deltas, lambda rows: snapshots_from_frame(frame[rows]))


@dataclass(frozen=True, slots=True)
class AlignedBook:
    """Book rebuilt at the first stored delta with ``u >= rest_id``.

    ``batches_replayed`` counts applied stored deltas (0 when a snapshot sits
    exactly on ``rest_id``). Compare with :func:`sync.compare_top_levels`.
    """

    book: OrderBook
    aligned_u: int
    batches_replayed: int


def reconstruct_book_at_update_id(
    root: Path,
    rest_id: int,
    *,
    exchange: str,
    symbol: str,
) -> AlignedBook:
    """Return the tape state at the first stored ``u >= rest_id`` (inclusive).

    Snapshot: latest stored with ``last_update_id <= rest_id``. Then stored
    deltas with ``final_update_id`` in ``(snap_id, ...]`` in ``final_update_id``
    order until the first ``u >= rest_id`` (Futures bracketing via
    :meth:`OrderBook.apply_delta`).

    Raises:
        ReconstructionError: If no snapshot exists at or before ``rest_id``,
            the stored chain ends before ``rest_id``, or replay hits a
            sequence gap (e.g. a resync hole: align is impossible, not a mismatch).
    """
    snapshots = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    eligible = snapshots.filter(pl.col("last_update_id") <= rest_id)
    if eligible.height == 0:
        msg = f"no snapshot with last_update_id <= {rest_id} for {exchange}:{symbol}"
        raise ReconstructionError(msg)
    chosen = eligible.sort(["last_update_id", "ts_event_ns"]).tail(1)
    snapshot = snapshots_from_frame(chosen)[0]
    if snapshot.last_update_id == rest_id:
        book = OrderBook()
        book.apply_snapshot(snapshot)
        return AlignedBook(book=book, aligned_u=rest_id, batches_replayed=0)

    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    subsequent = deltas.filter(pl.col("final_update_id") > snapshot.last_update_id).sort(
        "final_update_id"
    )
    book = OrderBook()
    book.apply_snapshot(snapshot)
    for batches, delta in enumerate(deltas_from_frame(subsequent), start=1):
        try:
            book.apply_delta(delta)
        except SequenceGapError as exc:
            msg = (
                f"sequence gap while replaying delta u={delta.final_update_id} towards R={rest_id}"
            )
            raise ReconstructionError(msg) from exc
        if delta.final_update_id >= rest_id:
            return AlignedBook(book=book, aligned_u=delta.final_update_id, batches_replayed=batches)
    msg = f"stored chain ends at u={book.last_update_id}, before R={rest_id}"
    raise ReconstructionError(msg)


def reconstruct_book(
    root: Path,
    ts_ns: int,
    *,
    exchange: str,
    symbol: str,
) -> OrderBook:
    """Return the book as it was at event time ``ts_ns`` (inclusive).

    Raises:
        ReconstructionError: If no snapshot exists at or before ``ts_ns``, or a
            stored delta cannot be applied.
    """
    snapshots = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    if snapshots.height == 0:
        msg = f"no snapshots under {root} for {exchange}:{symbol}"
        raise ReconstructionError(msg)
    eligible = snapshots.filter(pl.col("ts_event_ns") <= ts_ns)
    if eligible.height == 0:
        msg = f"no snapshot with ts_event_ns <= {ts_ns} for {exchange}:{symbol}"
        raise ReconstructionError(msg)
    chosen = eligible.sort(["ts_event_ns", "last_update_id"]).tail(1)
    snapshot = snapshots_from_frame(chosen)[0]
    book = OrderBook()
    book.apply_snapshot(snapshot)

    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    if deltas.height == 0:
        return book
    subsequent = deltas.filter(
        (pl.col("ts_event_ns") > snapshot.ts_event_ns) & (pl.col("ts_event_ns") <= ts_ns)
    ).sort(["ts_event_ns", "final_update_id"])
    for delta in deltas_from_frame(subsequent):
        try:
            book.apply_delta(delta)
        except SequenceGapError as exc:
            msg = f"sequence gap while replaying delta u={delta.final_update_id} at T={ts_ns}"
            raise ReconstructionError(msg) from exc
    return book


@dataclass(frozen=True, slots=True)
class L1Tick:
    """Synced best bid/ask after a snapshot or delta. ``new_epoch`` starts after resync."""

    ts_event_ns: int
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float
    epoch: int
    new_epoch: bool


def _l1_tick(book: OrderBook, epoch: int, new_epoch: bool) -> L1Tick | None:
    bid, ask = book.best_bid(), book.best_ask()
    if bid is None or ask is None or bid.qty <= 0 or ask.qty <= 0:
        return None
    return L1Tick(
        ts_event_ns=book.ts_event_ns,
        bid_px=bid.price,
        bid_qty=bid.qty,
        ask_px=ask.price,
        ask_qty=ask.qty,
        epoch=epoch,
        new_epoch=new_epoch,
    )


def _has_valid_l1(book: OrderBook) -> bool:
    bid, ask = book.best_bid(), book.best_ask()
    return bid is not None and ask is not None and bid.qty > 0 and ask.qty > 0


def _iter_synced_books(
    root: Path, *, exchange: str, symbol: str
) -> Iterator[tuple[OrderBook, int, bool]]:
    """Yield ``(book, epoch, new_epoch)`` after each replayed snapshot/delta with valid L1."""
    stream = book_stream_from_capture(root, exchange=exchange, symbol=symbol)
    book = OrderBook()
    epoch = 0
    yielded = False
    for event in stream.events:
        if isinstance(event, BookSnapshot):
            book.apply_snapshot(event)
            if yielded:
                epoch += 1
            if _has_valid_l1(book):
                yield book, epoch, True
                yielded = True
            continue
        book.apply_delta(event)
        if _has_valid_l1(book):
            yield book, epoch, False
            yielded = True


def iter_l1_ticks(root: Path, *, exchange: str, symbol: str) -> list[L1Tick]:
    """Replay snapshots+deltas in event time and collect synced L1 states.

    Periodic snapshots whose ``last_update_id`` matches the live book are skipped.
    A snapshot with a new id is treated as a resync (new epoch). Gap deltas mark the
    book unsynced until the next snapshot.
    """
    ticks: list[L1Tick] = []
    for book, epoch, new_epoch in _iter_synced_books(root, exchange=exchange, symbol=symbol):
        tick = _l1_tick(book, epoch, new_epoch)
        if tick is not None:
            ticks.append(tick)
    return ticks


@dataclass(frozen=True, slots=True)
class LmTick:
    """Synced top-``M`` prices/sizes after a snapshot or delta. Missing levels are NaN."""

    ts_event_ns: int
    bid_px: npt.NDArray[np.float64]
    bid_qty: npt.NDArray[np.float64]
    ask_px: npt.NDArray[np.float64]
    ask_qty: npt.NDArray[np.float64]
    epoch: int
    new_epoch: bool


def iter_lm_ticks(root: Path, *, exchange: str, symbol: str, levels: int = 5) -> list[LmTick]:
    """Same replay as :func:`iter_l1_ticks`, sampling ``OrderBook.to_arrays(levels)``.

    Arrays are copied immediately because the live book mutates on the next event.
    """
    if levels < 1:
        msg = "levels must be >= 1"
        raise ValueError(msg)
    ticks: list[LmTick] = []
    for book, epoch, new_epoch in _iter_synced_books(root, exchange=exchange, symbol=symbol):
        arrays = book.to_arrays(levels)
        ticks.append(
            LmTick(
                ts_event_ns=book.ts_event_ns,
                bid_px=np.asarray(arrays.bid_px, dtype=np.float64).copy(),
                bid_qty=np.asarray(arrays.bid_qty, dtype=np.float64).copy(),
                ask_px=np.asarray(arrays.ask_px, dtype=np.float64).copy(),
                ask_qty=np.asarray(arrays.ask_qty, dtype=np.float64).copy(),
                epoch=epoch,
                new_epoch=new_epoch,
            )
        )
    return ticks
