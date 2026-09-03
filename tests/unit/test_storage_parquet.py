"""ParquetWriter / read_events round trip in a temporary directory."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from order_flow.ingestion.events import EventType, Side
from order_flow.storage.base import EventSink
from order_flow.storage.parquet import (
    BOOK_DELTA_SCHEMA,
    PARTITION_DIR,
    TRADE_SCHEMA,
    ParquetWriter,
    read_events,
    scan_events,
    snapshots_from_frame,
    trades_from_frame,
)
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

DAY_NS = 86_400 * 1_000_000_000


def partition(root: Path, event_type: EventType, date: str = "2024-09-02") -> Path:
    kind = PARTITION_DIR[event_type]
    return root / kind / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}" / f"date={date}"


def test_partition_layout_uses_snapshots_deltas_trades() -> None:
    assert PARTITION_DIR["book_snapshot"] == "snapshots"
    assert PARTITION_DIR["book_delta"] == "deltas"
    assert PARTITION_DIR["trade"] == "trades"


def test_round_trip_all_event_types(tmp_path: Path) -> None:
    snapshot = make_snapshot(last_update_id=100)
    deltas = [
        make_delta(
            101, 105, 100, bids=((100.0, 12.0),), asks=((101.0, 0.0),), ts_event_ns=T0_NS + 1
        ),
        make_delta(
            106, 110, 105, bids=(), asks=((102.5, 4.0), (103.0, 1.0)), ts_event_ns=T0_NS + 2
        ),
    ]
    trades = [
        make_trade(1, 100.5, 0.25, Side.BUY, ts_event_ns=T0_NS + 3),
        make_trade(2, 100.4, 0.75, Side.SELL, ts_event_ns=T0_NS + 4),
    ]
    writer = ParquetWriter(tmp_path, EXCHANGE, SYMBOL)
    assert isinstance(writer, EventSink)
    with writer:
        writer.write([snapshot, *deltas, *trades])
        assert writer.pending == 5
    assert writer.pending == 0

    for event_type in ("book_snapshot", "book_delta", "trade"):
        assert (partition(tmp_path, event_type) / "part-00000.parquet").is_file()

    snapshots = read_events(tmp_path, "book_snapshot", exchange=EXCHANGE, symbol=SYMBOL)
    assert snapshots.height == 1
    assert snapshots["last_update_id"].to_list() == [100]
    assert snapshots["bids"].to_list()[0] == [
        {"price": 100.0, "qty": 10.0},
        {"price": 99.0, "qty": 5.0},
    ]

    delta_frame = read_events(tmp_path, "book_delta")
    assert delta_frame.schema == BOOK_DELTA_SCHEMA
    assert delta_frame["final_update_id"].to_list() == [105, 110]
    assert delta_frame["prev_final_update_id"].to_list() == [100, 105]
    assert delta_frame["asks"].to_list()[0] == [{"price": 101.0, "qty": 0.0}]
    assert delta_frame["bids"].to_list()[1] == []
    assert delta_frame["ts_event_ns"].to_list() == [T0_NS + 1, T0_NS + 2]

    trade_frame = read_events(tmp_path, "trade", date="2024-09-02")
    assert trade_frame.schema == TRADE_SCHEMA
    assert trade_frame["trade_id"].to_list() == [1, 2]
    assert trade_frame["aggressor_sign"].to_list() == [1, -1]
    assert trade_frame["price"].to_list() == [100.5, 100.4]
    restored = trades_from_frame(trade_frame)
    assert restored[0].trade_id == 1
    assert restored[1].aggressor is Side.SELL
    snap_objs = snapshots_from_frame(
        read_events(tmp_path, "book_snapshot", exchange=EXCHANGE, symbol=SYMBOL)
    )
    assert snap_objs[0].last_update_id == 100


def test_auto_flush_and_part_numbering(tmp_path: Path) -> None:
    writer = ParquetWriter(tmp_path, EXCHANGE, SYMBOL, buffer_size=2)
    writer.write([make_trade(1, 1.0, 1.0, Side.BUY)])
    assert writer.pending == 1
    writer.write([make_trade(2, 1.0, 1.0, Side.BUY)])  # reaches buffer_size -> flush
    assert writer.pending == 0
    writer.write([make_trade(3, 1.0, 1.0, Side.SELL)])
    writer.close()
    files = sorted(p.name for p in partition(tmp_path, "trade").glob("*.parquet"))
    assert files == ["part-00000.parquet", "part-00001.parquet"]
    assert read_events(tmp_path, "trade")["trade_id"].to_list() == [1, 2, 3]


def test_events_are_partitioned_by_utc_date(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(
            [
                make_trade(1, 1.0, 1.0, Side.BUY, ts_event_ns=T0_NS),
                make_trade(2, 1.0, 1.0, Side.BUY, ts_event_ns=T0_NS + DAY_NS),
            ]
        )
    assert (partition(tmp_path, "trade", "2024-09-02") / "part-00000.parquet").is_file()
    assert (partition(tmp_path, "trade", "2024-09-03") / "part-00000.parquet").is_file()
    assert read_events(tmp_path, "trade", date="2024-09-03")["trade_id"].to_list() == [2]
    assert read_events(tmp_path, "trade")["trade_id"].to_list() == [1, 2]


def test_scan_events_is_lazy_and_handles_missing_partitions(tmp_path: Path) -> None:
    lazy = scan_events(tmp_path, "book_delta", exchange="nowhere")
    assert isinstance(lazy, pl.LazyFrame)
    frame = lazy.collect()
    assert frame.height == 0
    assert frame.schema == BOOK_DELTA_SCHEMA
    assert read_events(tmp_path, "trade").height == 0


def test_writer_rejects_other_instruments(tmp_path: Path) -> None:
    writer = ParquetWriter(tmp_path, EXCHANGE, SYMBOL)
    with pytest.raises(ValueError, match="ETHUSDT"):
        writer.write([make_trade(1, 1.0, 1.0, Side.BUY, symbol="ETHUSDT")])
    with pytest.raises(ValueError, match="buffer_size"):
        ParquetWriter(tmp_path, EXCHANGE, SYMBOL, buffer_size=0)


def test_flush_without_events_creates_nothing(tmp_path: Path) -> None:
    ParquetWriter(tmp_path, EXCHANGE, SYMBOL).flush()
    assert list(tmp_path.iterdir()) == []
