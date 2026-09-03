"""Convert hive Parquet LOB events into hftbacktest structured arrays.

Callers: pytest. Affected API: ``order_flow.backtest.hft_adapter``. A missing
DEPTH_SNAPSHOT on the first book, a qty-0 trade that is not dropped, or an
unexplained event-count gap must fail these tests. Does not import
``hftbacktest`` or ``nautilus_trader``.
"""

from __future__ import annotations

import ast
import inspect
from typing import TYPE_CHECKING, Any

import numpy as np
import pytest

from order_flow.backtest.hft_adapter import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EVENT_DTYPE,
    EXCH_EVENT,
    KIND_MASK,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    capture_to_hft_feed,
    hftbacktest_available,
)
from order_flow.ingestion.events import Side
from order_flow.storage.parquet import ParquetWriter
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

NS = 1_000_000


def _kind(ev: int) -> int:
    return int(ev) & KIND_MASK


def _kind_mask(evs: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    return evs.astype(np.uint64) & np.uint64(KIND_MASK)


def test_adapter_module_does_not_import_engines() -> None:
    import order_flow.backtest.hft_adapter as mod  # noqa: PLC0415

    tree = ast.parse(inspect.getsource(mod))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module.split(".", maxsplit=1)[0])
    assert "nautilus_trader" not in imported
    assert "hftbacktest" not in imported
    assert "nautilus" not in imported


def test_hftbacktest_available_is_bool() -> None:
    assert isinstance(hftbacktest_available(), bool)


def test_event_dtype_matches_hftbacktest_layout() -> None:
    assert EVENT_DTYPE.names == (
        "ev",
        "exch_ts",
        "local_ts",
        "px",
        "qty",
        "order_id",
        "ival",
        "fval",
    )
    assert EVENT_DTYPE["ev"] == np.dtype("u8")
    assert EVENT_DTYPE["exch_ts"] == np.dtype("i8")
    assert EVENT_DTYPE["local_ts"] == np.dtype("i8")
    assert EVENT_DTYPE.alignment > 1


def test_first_snapshot_becomes_initial_depth_snapshot(tmp_path: Path) -> None:
    snap = make_snapshot(
        last_update_id=100,
        bids=((100.0, 10.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS,
    )
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.conservation_gap() == 0
    assert feed.n_snapshots_in == 1
    assert feed.n_initial_snapshots == 1
    assert len(feed.initial_snapshot) == 4
    kinds = {_kind(int(row["ev"])) for row in feed.initial_snapshot}
    assert kinds == {DEPTH_SNAPSHOT_EVENT}
    bids = feed.initial_snapshot[feed.initial_snapshot["ev"] & BUY_EVENT == BUY_EVENT]
    asks = feed.initial_snapshot[feed.initial_snapshot["ev"] & SELL_EVENT == SELL_EVENT]
    assert sorted(bids["px"].tolist()) == [99.0, 100.0]
    assert sorted(asks["px"].tolist()) == [101.0, 102.0]
    assert np.all(feed.initial_snapshot["ev"] & EXCH_EVENT == EXCH_EVENT)
    assert np.all(feed.initial_snapshot["ev"] & LOCAL_EVENT == LOCAL_EVENT)


def test_qty_zero_snapshot_levels_are_dropped(tmp_path: Path) -> None:
    snap = make_snapshot(
        last_update_id=1,
        bids=((100.0, 1.0), (99.0, 0.0)),
        asks=((101.0, 1.0),),
        ts_event_ns=T0_NS,
    )
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    prices = set(feed.initial_snapshot["px"].tolist())
    assert 99.0 not in prices
    assert feed.n_qty0_snapshot_levels_dropped == 1
    assert feed.conservation_gap() == 0


def test_qty_zero_delta_is_depth_delete_not_dropped(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    delta = make_delta(
        101,
        105,
        100,
        bids=((100.0, 0.0),),
        asks=((101.0, 4.0),),
        ts_event_ns=T0_NS + NS,
    )
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, delta])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    depth = feed.data[_kind_mask(feed.data["ev"]) == DEPTH_EVENT]
    by_px = {float(row["px"]): row for row in depth}
    assert float(by_px[100.0]["qty"]) == 0.0
    assert float(by_px[101.0]["qty"]) == 4.0
    assert int(by_px[100.0]["ev"]) & BUY_EVENT == BUY_EVENT
    assert int(by_px[101.0]["ev"]) & SELL_EVENT == SELL_EVENT
    assert feed.n_feed_depth_events == 2
    assert feed.conservation_gap() == 0


def test_trade_flags_follow_aggressor_and_qty_zero_is_dropped(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    buy = make_trade(1, 101.0, 0.5, Side.BUY, ts_event_ns=T0_NS + NS)
    zero = make_trade(2, 101.0, 0.0, Side.SELL, ts_event_ns=T0_NS + 2 * NS)
    sell = make_trade(3, 100.0, 0.001, Side.SELL, ts_event_ns=T0_NS + 3 * NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, buy, zero, sell])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    trades = feed.data[_kind_mask(feed.data["ev"]) == TRADE_EVENT]
    assert len(trades) == 2
    assert feed.n_qty0_trades_dropped == 1
    assert feed.n_trades_in == 3
    sides = sorted(
        "buy" if int(row["ev"]) & BUY_EVENT == BUY_EVENT else "sell" for row in trades
    )
    assert sides == ["buy", "sell"]
    assert feed.conservation_gap() == 0


def test_periodic_snapshot_with_same_update_id_is_skipped(tmp_path: Path) -> None:
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    delta = make_delta(
        95,
        105,
        90,
        bids=((100.0, 12.0),),
        ts_event_ns=T0_NS + NS,
    )
    periodic = make_snapshot(
        last_update_id=105,
        bids=((100.0, 12.0), (99.0, 5.0)),
        asks=((101.0, 8.0), (102.0, 3.0)),
        ts_event_ns=T0_NS + 2 * NS,
    )
    trade = make_trade(1, 101.0, 0.01, Side.SELL, ts_event_ns=T0_NS + NS + 1)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, delta, periodic, trade])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.n_snapshots_in == 2
    assert feed.n_initial_snapshots == 1
    assert feed.n_snapshots_skipped == 1
    assert feed.n_resync_snapshots == 0
    assert not np.any(_kind_mask(feed.data["ev"]) == DEPTH_CLEAR_EVENT)
    assert not np.any(_kind_mask(feed.data["ev"]) == DEPTH_SNAPSHOT_EVENT)
    assert feed.n_feed_trade_events == 1
    assert feed.conservation_gap() == 0


def test_resync_snapshot_emits_clear_then_snapshot(tmp_path: Path) -> None:
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    delta = make_delta(
        101,
        105,
        100,
        bids=((100.0, 12.0),),
        ts_event_ns=T0_NS + NS,
    )
    resync = make_snapshot(
        last_update_id=300,
        bids=((100.5, 1.0),),
        asks=((100.6, 1.0),),
        ts_event_ns=T0_NS + 3 * NS,
    )
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, delta, resync])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.n_resync_snapshots == 1
    kinds = [_kind(int(row["ev"])) for row in feed.data]
    assert DEPTH_CLEAR_EVENT in kinds
    assert DEPTH_SNAPSHOT_EVENT in kinds
    clears = feed.data[_kind_mask(feed.data["ev"]) == DEPTH_CLEAR_EVENT]
    snaps = feed.data[_kind_mask(feed.data["ev"]) == DEPTH_SNAPSHOT_EVENT]
    assert len(clears) == 2
    assert len(snaps) == 2
    assert set(snaps["px"].tolist()) == {100.5, 100.6}
    assert feed.conservation_gap() == 0


def test_resync_skips_side_with_no_positive_levels(tmp_path: Path) -> None:
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    delta = make_delta(
        101,
        105,
        100,
        bids=((100.0, 12.0),),
        ts_event_ns=T0_NS + NS,
    )
    resync = make_snapshot(
        last_update_id=300,
        bids=(),
        asks=((100.6, 1.0),),
        ts_event_ns=T0_NS + 3 * NS,
    )
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([rest, delta, resync])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.conservation_gap() == 0
    clears = feed.data[_kind_mask(feed.data["ev"]) == DEPTH_CLEAR_EVENT]
    snaps = feed.data[_kind_mask(feed.data["ev"]) == DEPTH_SNAPSHOT_EVENT]
    assert len(clears) == 1
    assert len(snaps) == 1
    assert float(snaps[0]["px"]) == 100.6


def test_empty_delta_is_documented_drop(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    empty = make_delta(101, 102, 100, ts_event_ns=T0_NS + NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, empty])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.n_empty_deltas_dropped == 1
    assert feed.n_feed_depth_events == 0
    assert feed.conservation_gap() == 0


def test_local_ts_is_strictly_after_exch_ts_when_recv_is_behind(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    early = make_trade(1, 101.0, 0.01, Side.BUY, ts_event_ns=T0_NS + NS)
    object.__setattr__(early, "ts_recv_ns", T0_NS + NS - 50 * NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, early])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    trades = feed.data[_kind_mask(feed.data["ev"]) == TRADE_EVENT]
    assert int(trades[0]["local_ts"]) > int(trades[0]["exch_ts"])
    assert np.all(feed.data["local_ts"] > feed.data["exch_ts"])
    assert np.all(feed.initial_snapshot["local_ts"] > feed.initial_snapshot["exch_ts"])
    assert feed.conservation_gap() == 0


def test_trades_precede_depth_at_the_same_exchange_timestamp(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    ts = T0_NS + NS
    delta = make_delta(101, 105, 100, bids=((100.0, 12.0),), ts_event_ns=ts)
    trade = make_trade(1, 100.0, 0.01, Side.SELL, ts_event_ns=ts)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, delta, trade])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    kinds = [_kind(int(row["ev"])) for row in feed.data]
    assert kinds[0] == TRADE_EVENT
    assert kinds[1] == DEPTH_EVENT
    assert feed.conservation_gap() == 0


def test_unexplained_drop_fails_conservation(tmp_path: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([snap])
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    broken = feed.__class__(
        **{**{field: getattr(feed, field) for field in feed.__dataclass_fields__}, "n_trades_in": 9}
    )
    assert broken.conservation_gap() != 0


def test_missing_snapshots_raise(tmp_path: Path) -> None:
    trade = make_trade(1, 101.0, 0.01, Side.BUY, ts_event_ns=T0_NS)
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write([trade])
    with pytest.raises(ValueError, match="no book snapshots"):
        capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
