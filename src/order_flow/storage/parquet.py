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

from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal, TypeVar

import polars as pl

from order_flow.ingestion.events import BookDelta, BookSnapshot, EventType, PriceLevel, Side, Trade
from order_flow.utils.time import ns_to_datetime

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from order_flow.ingestion.events import MarketEvent

ParquetCompression = Literal["zstd", "snappy", "lz4", "gzip", "uncompressed"]

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
class ParquetWriter:
    """Buffered :class:`~order_flow.storage.base.EventSink` writing partitioned Parquet.

    Events are grouped by type and UTC date; each :meth:`flush` appends one new
    ``part-<n>.parquet`` file per (type, date) partition. Use as a context manager to
    guarantee the final flush.
    """

    def __init__(
        self,
        root: Path,
        exchange: str,
        symbol: str,
        *,
        buffer_size: int = 10_000,
        compression: ParquetCompression = "zstd",
    ) -> None:
        if buffer_size < 1:
            msg = "buffer_size must be >= 1"
            raise ValueError(msg)
        self.root = Path(root)
        self.exchange = exchange
        self.symbol = symbol
        self.buffer_size = buffer_size
        self.compression: ParquetCompression = compression
        self._snapshots: list[BookSnapshot] = []
        self._deltas: list[BookDelta] = []
        self._trades: list[Trade] = []
        self._pending = 0

    @property
    def pending(self) -> int:
        """Number of buffered, not yet persisted events."""
        return self._pending

    def write(self, events: Sequence[MarketEvent]) -> None:
        """Buffer ``events``; flushes automatically once ``buffer_size`` is reached.

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
        if self._pending >= self.buffer_size:
            self.flush()

    def flush(self) -> None:
        """Write every buffered event to its partition."""
        self._write_partitions("book_snapshot", self._snapshots, snapshots_to_frame)
        self._write_partitions("book_delta", self._deltas, deltas_to_frame)
        self._write_partitions("trade", self._trades, trades_to_frame)
        self._snapshots.clear()
        self._deltas.clear()
        self._trades.clear()
        self._pending = 0

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
            part = sum(1 for _ in directory.glob("part-*.parquet"))
            to_frame(group).write_parquet(
                directory / f"part-{part:05d}.parquet", compression=self.compression
            )

    def partition_dir(self, event_type: EventType, date: str) -> Path:
        """Directory holding ``event_type`` files for ``date`` (``YYYY-MM-DD``)."""
        return (
            self.root
            / PARTITION_DIR[event_type]
            / f"exchange={self.exchange}"
            / f"symbol={self.symbol}"
            / f"date={date}"
        )


# --------------------------------------------------------------------------- reader
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
    pattern = "/".join(
        (
            PARTITION_DIR[event_type],
            f"exchange={exchange or '*'}",
            f"symbol={symbol or '*'}",
            f"date={date or '*'}",
            "*.parquet",
        )
    )
    files = sorted(Path(root).glob(pattern))
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
