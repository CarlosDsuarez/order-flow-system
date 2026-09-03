"""Rebuild an :class:`~order_flow.orderbook.book.OrderBook` from a Parquet capture.

Algorithm: load the latest snapshot with ``ts_event_ns <= T`` (ties broken by
``last_update_id``), then apply every delta with ``snapshot_ts < ts_event_ns <= T``
in ``(ts_event_ns, final_update_id)`` order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from order_flow.ingestion.events import BookDelta, BookSnapshot
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError
from order_flow.storage.parquet import deltas_from_frame, read_events, snapshots_from_frame

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    import numpy.typing as npt


class ReconstructionError(Exception):
    """Capture is missing a snapshot at or before ``T``, or replay hit a sequence gap."""


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
    """Yield ``(book, epoch, new_epoch)`` after each synced snapshot/delta with valid L1."""
    snapshots = snapshots_from_frame(
        read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    )
    deltas = deltas_from_frame(read_events(root, "book_delta", exchange=exchange, symbol=symbol))
    merged: list[tuple[int, int, int, BookSnapshot | BookDelta]] = []
    merged.extend((snap.ts_event_ns, snap.last_update_id, 1, snap) for snap in snapshots)
    merged.extend((delta.ts_event_ns, delta.final_update_id, 0, delta) for delta in deltas)
    merged.sort()
    book = OrderBook()
    epoch = 0
    yielded = False
    for _ts, _uid, _kind, event in merged:
        if isinstance(event, BookSnapshot):
            if book.last_update_id is not None and event.last_update_id == book.last_update_id:
                continue
            book.apply_snapshot(event)
            if book.last_update_id is not None and yielded:
                epoch += 1
            if _has_valid_l1(book):
                yield book, epoch, True
                yielded = True
            continue
        if book.last_update_id is None:
            continue
        try:
            applied = book.apply_delta(event)
        except SequenceGapError:
            book.mark_unsynced()
            continue
        if not applied or not book.is_synced:
            continue
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
