"""Parquet persistence of market events.

Layout::

    <root>/snapshots/exchange=<exchange>/symbol=<symbol>/date=<YYYY-MM-DD>/*.parquet
    <root>/deltas/exchange=.../symbol=.../date=.../*.parquet
    <root>/trades/exchange=.../symbol=.../date=.../*.parquet

Dataclass ``EVENT_TYPE`` stays ``book_snapshot`` / ``book_delta`` / ``trade``; directories
use the shorter names above. ``date`` is the UTC calendar day of ``ts_event_ns``.
Timestamps are int64 nanoseconds since the Unix epoch, L2 levels are
``list<struct<price: f64, qty: f64>>`` and the trade aggressor is stored as
``aggressor_sign`` (+1 buyer-initiated, -1 seller-initiated). Snapshots persist the
**full in-memory book** (typically REST ``limit=1000`` levels per side).
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import threading
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal, NamedTuple, TypeVar

import polars as pl

from order_flow.ingestion.events import BookDelta, BookSnapshot, EventType, PriceLevel, Side, Trade
from order_flow.utils.time import ns_to_datetime

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from order_flow.ingestion.events import MarketEvent

ParquetCompression = Literal["zstd", "snappy", "lz4", "gzip", "uncompressed"]

_PART_RE: Final = re.compile(r"part-(\d+)\.parquet")


class LowDiskSpaceError(OSError):
    """Free space under the writer's floor: refuse to flush rather than fill the disk."""


def _fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


LEVELS_DTYPE: Final = pl.List(pl.Struct({"price": pl.Float64(), "qty": pl.Float64()}))

_COMMON_FIELDS: Final[dict[str, pl.DataType]] = {
    "exchange": pl.String(),
    "symbol": pl.String(),
    "ts_event_ns": pl.Int64(),
    "ts_recv_ns": pl.Int64(),
}

BOOK_SNAPSHOT_SCHEMA: Final = pl.Schema(
    {**_COMMON_FIELDS, "last_update_id": pl.Int64(), "bids": LEVELS_DTYPE, "asks": LEVELS_DTYPE}
)
BOOK_DELTA_SCHEMA: Final = pl.Schema(
    {
        **_COMMON_FIELDS,
        "first_update_id": pl.Int64(),
        "final_update_id": pl.Int64(),
        "prev_final_update_id": pl.Int64(),
        "bids": LEVELS_DTYPE,
        "asks": LEVELS_DTYPE,
    }
)
TRADE_SCHEMA: Final = pl.Schema(
    {
        **_COMMON_FIELDS,
        "trade_id": pl.Int64(),
        "price": pl.Float64(),
        "qty": pl.Float64(),
        "aggressor_sign": pl.Int8(),
    }
)
SCHEMAS: Final[dict[EventType, pl.Schema]] = {
    "book_snapshot": BOOK_SNAPSHOT_SCHEMA,
    "book_delta": BOOK_DELTA_SCHEMA,
    "trade": TRADE_SCHEMA,
}

PARTITION_DIR: Final[dict[EventType, str]] = {
    "book_snapshot": "snapshots",
    "book_delta": "deltas",
    "trade": "trades",
}

_E = TypeVar("_E", BookSnapshot, BookDelta, Trade)


# --------------------------------------------------------------------------- frames
def _levels_to_rows(levels: tuple[PriceLevel, ...]) -> list[dict[str, float]]:
    return [{"price": level.price, "qty": level.qty} for level in levels]


def snapshots_to_frame(events: Sequence[BookSnapshot]) -> pl.DataFrame:
    """Build a :data:`BOOK_SNAPSHOT_SCHEMA` frame."""
    return pl.DataFrame(
        {
            "exchange": [e.exchange for e in events],
            "symbol": [e.symbol for e in events],
            "ts_event_ns": [e.ts_event_ns for e in events],
            "ts_recv_ns": [e.ts_recv_ns for e in events],
            "last_update_id": [e.last_update_id for e in events],
            "bids": [_levels_to_rows(e.bids) for e in events],
            "asks": [_levels_to_rows(e.asks) for e in events],
        },
        schema=BOOK_SNAPSHOT_SCHEMA,
    )


def deltas_to_frame(events: Sequence[BookDelta]) -> pl.DataFrame:
    """Build a :data:`BOOK_DELTA_SCHEMA` frame."""
    return pl.DataFrame(
        {
            "exchange": [e.exchange for e in events],
            "symbol": [e.symbol for e in events],
            "ts_event_ns": [e.ts_event_ns for e in events],
            "ts_recv_ns": [e.ts_recv_ns for e in events],
            "first_update_id": [e.first_update_id for e in events],
            "final_update_id": [e.final_update_id for e in events],
            "prev_final_update_id": [e.prev_final_update_id for e in events],
            "bids": [_levels_to_rows(e.bids) for e in events],
            "asks": [_levels_to_rows(e.asks) for e in events],
        },
        schema=BOOK_DELTA_SCHEMA,
    )


def trades_to_frame(events: Sequence[Trade]) -> pl.DataFrame:
    """Build a :data:`TRADE_SCHEMA` frame."""
    return pl.DataFrame(
        {
            "exchange": [e.exchange for e in events],
            "symbol": [e.symbol for e in events],
            "ts_event_ns": [e.ts_event_ns for e in events],
            "ts_recv_ns": [e.ts_recv_ns for e in events],
            "trade_id": [e.trade_id for e in events],
            "price": [e.price for e in events],
            "qty": [e.qty for e in events],
            "aggressor_sign": [e.aggressor.sign for e in events],
        },
        schema=TRADE_SCHEMA,
    )


def _levels_from_cell(cell: object) -> tuple[PriceLevel, ...]:
    if cell is None or isinstance(cell, (str, bytes)) or not isinstance(cell, Iterable):
        return ()
    levels: list[PriceLevel] = []
    for row in cell:
        if not isinstance(row, Mapping):
            continue
        levels.append(PriceLevel(float(row["price"]), float(row["qty"])))
    return tuple(levels)


def snapshots_from_frame(frame: pl.DataFrame) -> list[BookSnapshot]:
    """Inverse of :func:`snapshots_to_frame`."""
    events: list[BookSnapshot] = []
    for row in frame.iter_rows(named=True):
        events.append(
            BookSnapshot(
                exchange=str(row["exchange"]),
                symbol=str(row["symbol"]),
                ts_event_ns=int(row["ts_event_ns"]),
                ts_recv_ns=int(row["ts_recv_ns"]),
                last_update_id=int(row["last_update_id"]),
                bids=_levels_from_cell(row["bids"]),
                asks=_levels_from_cell(row["asks"]),
            )
        )
    return events


def deltas_from_frame(frame: pl.DataFrame) -> list[BookDelta]:
    """Inverse of :func:`deltas_to_frame`."""
    events: list[BookDelta] = []
    for row in frame.iter_rows(named=True):
        events.append(
            BookDelta(
                exchange=str(row["exchange"]),
                symbol=str(row["symbol"]),
                ts_event_ns=int(row["ts_event_ns"]),
                ts_recv_ns=int(row["ts_recv_ns"]),
                first_update_id=int(row["first_update_id"]),
                final_update_id=int(row["final_update_id"]),
                prev_final_update_id=int(row["prev_final_update_id"]),
                bids=_levels_from_cell(row["bids"]),
                asks=_levels_from_cell(row["asks"]),
            )
        )
    return events


def trades_from_frame(frame: pl.DataFrame) -> list[Trade]:
    """Inverse of :func:`trades_to_frame`."""
    events: list[Trade] = []
    for row in frame.iter_rows(named=True):
        events.append(
            Trade(
                exchange=str(row["exchange"]),
                symbol=str(row["symbol"]),
                ts_event_ns=int(row["ts_event_ns"]),
                ts_recv_ns=int(row["ts_recv_ns"]),
                trade_id=int(row["trade_id"]),
                price=float(row["price"]),
                qty=float(row["qty"]),
                aggressor=Side.from_sign(int(row["aggressor_sign"])),
            )
        )
    return events


def _utc_date(ts_ns: int) -> str:
    return ns_to_datetime(ts_ns).date().isoformat()


# --------------------------------------------------------------------------- writer
class _Batch(NamedTuple):
    snapshots: list[BookSnapshot]
    deltas: list[BookDelta]
    trades: list[Trade]


class ParquetWriter:
    """Buffered :class:`~order_flow.storage.base.EventSink` writing partitioned Parquet.

    Events are grouped by type and UTC date; each :meth:`flush` appends one new
    ``part-<n>.parquet`` file per (type, date) partition. Use as a context manager to
    guarantee the final flush.

    Inside an event loop, pass ``auto_flush=False`` and call :meth:`flush_async`: the disk
    write then runs in a worker thread instead of stalling the WebSocket pump. A failed
    flush raises and its batch is not retried.
    """

    def __init__(
        self,
        root: Path,
        exchange: str,
        symbol: str,
        *,
        buffer_size: int = 10_000,
        compression: ParquetCompression = "zstd",
        auto_flush: bool = True,
        min_free_bytes: int = 0,
    ) -> None:
        if buffer_size < 1:
            msg = "buffer_size must be >= 1"
            raise ValueError(msg)
        self.root = Path(root)
        self.exchange = exchange
        self.symbol = symbol
        self.buffer_size = buffer_size
        self.compression: ParquetCompression = compression
        self.auto_flush = auto_flush
        self.min_free_bytes = min_free_bytes
        # Serialises disk writes: two in-flight flushes would pick the same part index.
        self._write_lock = threading.Lock()
        self._snapshots: list[BookSnapshot] = []
        self._deltas: list[BookDelta] = []
        self._trades: list[Trade] = []
        self._pending = 0

    @property
    def pending(self) -> int:
        """Number of buffered, not yet persisted events."""
        return self._pending

    def write(self, events: Sequence[MarketEvent]) -> None:
        """Buffer ``events``; with ``auto_flush`` flushes once ``buffer_size`` is reached.

        Raises:
            ValueError: If an event belongs to a different exchange/symbol.
        """
        for event in events:
            if event.exchange != self.exchange or event.symbol != self.symbol:
                msg = (
                    f"event for {event.exchange}:{event.symbol} written to sink "
                    f"{self.exchange}:{self.symbol}"
                )
                raise ValueError(msg)
            if isinstance(event, BookSnapshot):
                self._snapshots.append(event)
            elif isinstance(event, BookDelta):
                self._deltas.append(event)
            else:
                self._trades.append(event)
        self._pending += len(events)
        if self.auto_flush and self._pending >= self.buffer_size:
            self.flush()

    def flush(self) -> None:
        """Write every buffered event to its partition.

        Raises:
            LowDiskSpaceError: Free space under ``min_free_bytes``; the buffer is kept.
        """
        self._check_free_space()
        self._write_batch(self._take_batch())

    async def flush_async(self) -> None:
        """Hand the buffer off and write it from a worker thread (same checks as flush)."""
        self._check_free_space()
        batch = self._take_batch()
        await asyncio.to_thread(self._write_batch, batch)

    def _check_free_space(self) -> None:
        if not self._pending or self.min_free_bytes <= 0:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(self.root).free
        if free < self.min_free_bytes:
            msg = (
                f"only {free / 1e9:.2f} GB free under {self.root}, below the "
                f"{self.min_free_bytes / 1e9:.2f} GB floor; not flushing"
            )
            raise LowDiskSpaceError(msg)

    def _take_batch(self) -> _Batch:
        batch = _Batch(self._snapshots, self._deltas, self._trades)
        self._snapshots, self._deltas, self._trades = [], [], []
        self._pending = 0
        return batch

    def _write_batch(self, batch: _Batch) -> None:
        with self._write_lock:
            self._write_partitions("book_snapshot", batch.snapshots, snapshots_to_frame)
            self._write_partitions("book_delta", batch.deltas, deltas_to_frame)
            self._write_partitions("trade", batch.trades, trades_to_frame)

    def close(self) -> None:
        """Flush; kept for :class:`~order_flow.storage.base.EventSink` symmetry."""
        self.flush()

    def __enter__(self) -> ParquetWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _write_partitions(
        self,
        event_type: EventType,
        events: Sequence[_E],
        to_frame: Callable[[Sequence[_E]], pl.DataFrame],
    ) -> None:
        if not events:
            return
        by_date: defaultdict[str, list[_E]] = defaultdict(list)
        for event in events:
            by_date[_utc_date(event.ts_event_ns)].append(event)
        for date, group in sorted(by_date.items()):
            directory = self.partition_dir(event_type, date)
            directory.mkdir(parents=True, exist_ok=True)
            final = directory / f"part-{_next_part_index(directory):05d}.parquet"
            # Write beside the target, then rename: a crash mid-write never leaves a
            # truncated ``part-*.parquet`` that would break every scan of the partition.
            tmp = directory / f".{final.name}.tmp"
            try:
                to_frame(group).write_parquet(tmp, compression=self.compression)
                _fsync(tmp)  # data on disk before the name points at it
                os.replace(tmp, final)
            finally:
                tmp.unlink(missing_ok=True)
            if os.name == "posix":
                _fsync(directory)  # and the rename itself

    def partition_dir(self, event_type: EventType, date: str) -> Path:
        """Directory holding ``event_type`` files for ``date`` (``YYYY-MM-DD``)."""
        return (
            self.root
            / PARTITION_DIR[event_type]
            / f"exchange={self.exchange}"
            / f"symbol={self.symbol}"
            / f"date={date}"
        )


def _next_part_index(directory: Path) -> int:
    """One past the highest ``part-<n>``, so a deleted part never causes an overwrite."""
    indices = [int(m.group(1)) for p in directory.iterdir() if (m := _PART_RE.fullmatch(p.name))]
    return max(indices, default=-1) + 1


# --------------------------------------------------------------------------- reader
def capture_dirs(root: Path) -> list[Path]:
    """Capture directories under ``root``: itself if it holds one, else its runs, sorted.

    ``scripts/capture_loop.sh`` writes one directory per run (``BASE/SYMBOL/<UTC start>/``).
    Pointing any reader at ``BASE/SYMBOL`` reads every run as one tape: runs join like
    resyncs (each starts from a REST snapshot; update ids keep growing across sessions).
    """
    root = Path(root)

    def holds_capture(path: Path) -> bool:
        return any((path / kind).is_dir() for kind in PARTITION_DIR.values())

    if holds_capture(root) or not root.is_dir():
        return [root]
    return sorted(path for path in root.iterdir() if path.is_dir() and holds_capture(path))


def part_files(
    root: Path,
    event_type: EventType,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
) -> list[Path]:
    """Every ``part-*.parquet`` of ``event_type`` under ``root`` (all runs), sorted."""
    pattern = "/".join(
        (
            PARTITION_DIR[event_type],
            f"exchange={exchange or '*'}",
            f"symbol={symbol or '*'}",
            f"date={date or '*'}",
            "*.parquet",
        )
    )
    return sorted(path for run in capture_dirs(root) for path in run.glob(pattern))


def scan_events(
    root: Path,
    event_type: EventType,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
) -> pl.LazyFrame:
    """Lazily scan every partition matching the filters (``None`` = any).

    Returns an empty frame with the right schema when nothing matches.
    """
    files = part_files(root, event_type, exchange=exchange, symbol=symbol, date=date)
    if not files:
        return pl.DataFrame(schema=SCHEMAS[event_type]).lazy()
    return pl.scan_parquet(files)


def read_events(
    root: Path,
    event_type: EventType,
    *,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
) -> pl.DataFrame:
    """Eager counterpart of :func:`scan_events`, sorted by ``ts_event_ns``."""
    return (
        scan_events(root, event_type, exchange=exchange, symbol=symbol, date=date)
        .sort("ts_event_ns")
        .collect()
    )
