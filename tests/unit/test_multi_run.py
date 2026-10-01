"""A continuous capture is BASE/SYMBOL/<UTC start>/ per run; readers take BASE/SYMBOL.

Runs join like resyncs: each starts from a REST snapshot and Binance update ids keep
growing across sessions, so the concatenation is one tape with one epoch per run.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from order_flow.backtest.conversion import BookOp, capture_to_ops
from order_flow.ingestion.events import Side
from order_flow.ingestion.instruments import (
    read_instrument_spec,
    resolve_capture_spec,
    write_instrument_spec,
)
from order_flow.orderbook.book import OrderBook
from order_flow.storage.parquet import ParquetWriter, capture_dirs, read_events
from order_flow.storage.quality import check_capture
from order_flow.storage.reconstruct import iter_l1_ticks
from order_flow.storage.report import capture_stats
from tests.helpers import (
    DOGE,
    EXCHANGE,
    T0_NS,
    doge_spec,
    make_delta,
    make_snapshot,
    make_trade,
)

if TYPE_CHECKING:
    from pathlib import Path

    from order_flow.ingestion.events import MarketEvent

HOUR = 3_600 * 1_000_000_000
MS = 1_000_000


def record_run(base: Path, name: str, *, start_u: int, t0: int) -> Path:
    """One run as capture_loop.sh leaves it: REST snapshot, deltas, trades, a spec."""
    run = base / name
    book = OrderBook(exchange=EXCHANGE, symbol=DOGE)
    rest = make_snapshot(
        last_update_id=start_u,
        bids=((0.12344, 5_000.0),),
        asks=((0.12346, 5_000.0),),
        ts_event_ns=t0,
        symbol=DOGE,
    )
    first = make_delta(
        start_u - 2,
        start_u + 5,
        start_u - 3,
        bids=((0.12345, 100.0),),
        ts_event_ns=t0 + MS,
        symbol=DOGE,
    )
    book.apply_snapshot(rest)
    book.apply_delta(first)
    events: list[MarketEvent] = [
        rest,
        first,
        book.snapshot(),
        make_trade(start_u, 0.12346, 10.0, Side.BUY, ts_event_ns=t0 + 2 * MS, symbol=DOGE),
    ]
    with ParquetWriter(run, EXCHANGE, DOGE) as writer:
        writer.write(events)
    write_instrument_spec(run, doge_spec())
    return run


@pytest.fixture
def two_runs(tmp_path: Path) -> Path:
    base = tmp_path / DOGE
    record_run(base, "20260927T000000Z", start_u=100, t0=T0_NS)
    record_run(base, "20260928T000000Z", start_u=900, t0=T0_NS + 24 * HOUR)
    (base / "launchd.log").write_text("agent log, not a run\n")
    return base


def test_capture_dirs_expands_a_base_into_its_runs(two_runs: Path) -> None:
    assert [run.name for run in capture_dirs(two_runs)] == [
        "20260927T000000Z",
        "20260928T000000Z",
    ]
    single = two_runs / "20260927T000000Z"
    assert capture_dirs(single) == [single]


def test_read_events_joins_every_run(two_runs: Path) -> None:
    trades = read_events(two_runs, "trade", exchange=EXCHANGE, symbol=DOGE)
    assert trades["trade_id"].to_list() == [100, 900]


def test_capture_stats_count_bytes_of_every_run(two_runs: Path) -> None:
    stats = capture_stats(two_runs, exchange=EXCHANGE, symbol=DOGE)
    single = capture_stats(two_runs / "20260927T000000Z", exchange=EXCHANGE, symbol=DOGE)
    assert stats.n_trades == 2
    assert stats.bytes_total > single.bytes_total > 0


def test_replays_see_one_epoch_per_run(two_runs: Path) -> None:
    ticks = iter_l1_ticks(two_runs, exchange=EXCHANGE, symbol=DOGE)
    assert sorted({tick.epoch for tick in ticks}) == [0, 1]
    batches, _ = capture_to_ops(two_runs, exchange=EXCHANGE, symbol=DOGE, tick=0.00001)
    assert [batch[0].action for batch in batches].count(BookOp.CLEAR) == 2


def test_integrity_check_spans_runs(two_runs: Path) -> None:
    quality = check_capture(two_runs, exchange=EXCHANGE, symbol=DOGE)
    assert quality.ok, quality.issues
    assert quality.n_epochs == 2
    assert quality.n_off_grid_prices == 0  # the runs' common spec was found


def test_spec_is_shared_by_the_runs(two_runs: Path) -> None:
    assert read_instrument_spec(two_runs) == doge_spec()
    assert resolve_capture_spec(two_runs, DOGE) == doge_spec()


def test_spec_change_between_runs_is_an_error(two_runs: Path) -> None:
    # A tick-size change mid-series cannot be replayed on one grid.
    changed = doge_spec().to_dict() | {"tick_size": "0.000001"}
    later = two_runs / "20260928T000000Z"
    (later / "instrument.json").unlink()
    write_instrument_spec(later, type(doge_spec()).from_dict(changed))
    with pytest.raises(ValueError, match="differ"):
        read_instrument_spec(two_runs)
    assert Decimal(changed["tick_size"]) != doge_spec().tick_size
