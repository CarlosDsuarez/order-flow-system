"""check_capture: integrity of a recorded tape before anyone trusts a replay of it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from order_flow.ingestion.events import Side
from order_flow.ingestion.instruments import write_instrument_spec
from order_flow.orderbook.book import OrderBook
from order_flow.storage.parquet import ParquetWriter
from order_flow.storage.quality import check_capture
from tests.helpers import (
    DOGE,
    EXCHANGE,
    SYMBOL,
    T0_NS,
    doge_spec,
    make_delta,
    make_snapshot,
    make_trade,
)

if TYPE_CHECKING:
    from pathlib import Path

    from order_flow.ingestion.events import MarketEvent

NS = 1_000_000


def record(root: Path, events: list[MarketEvent], *, symbol: str = SYMBOL) -> None:
    with ParquetWriter(root, EXCHANGE, symbol) as writer:
        writer.write(events)


def clean_tape() -> list[MarketEvent]:
    """REST snapshot, two contiguous deltas, a periodic snapshot as record_l2 writes it."""
    rest = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    first = make_delta(95, 105, 90, bids=((100.0, 12.0),), ts_event_ns=T0_NS + NS)
    second = make_delta(106, 110, 105, asks=((101.0, 4.0),), ts_event_ns=T0_NS + 2 * NS)
    book = OrderBook(exchange=EXCHANGE, symbol=SYMBOL)
    book.apply_snapshot(rest)
    book.apply_delta(first)
    book.apply_delta(second)
    trades = [
        make_trade(1, 100.0, 0.5, Side.SELL, ts_event_ns=T0_NS + NS),
        make_trade(2, 101.0, 0.2, Side.BUY, ts_event_ns=T0_NS + 2 * NS),
    ]
    return [rest, first, second, book.snapshot(), *trades]


def test_clean_capture_has_no_issues(tmp_path: Path) -> None:
    record(tmp_path, clean_tape())
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.ok
    assert quality.issues == ()
    assert (quality.n_snapshots, quality.n_deltas, quality.n_trades) == (2, 2, 2)
    assert quality.n_epochs == 1
    assert quality.n_chain_breaks == 0


def test_resync_through_a_new_snapshot_is_not_a_break(tmp_path: Path) -> None:
    resync = make_snapshot(last_update_id=300, ts_event_ns=T0_NS + 5 * NS)
    after = make_delta(299, 305, 290, bids=((100.0, 1.0),), ts_event_ns=T0_NS + 6 * NS)
    record(tmp_path, [*clean_tape(), resync, after])
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.ok
    assert quality.n_epochs == 2


def test_resync_snapshot_stamped_after_its_bracketing_delta_is_not_a_break(
    tmp_path: Path,
) -> None:
    # Seen in data/live-btcusdt-45min: the REST snapshot's E is the response time, so it
    # can be later than the E of the diff that brackets its lastUpdateId. Ordering by
    # event time then puts the diff first and fakes a break; update ids do not lie.
    rest = make_snapshot(last_update_id=300, ts_event_ns=T0_NS + 6 * NS)
    bracketing = make_delta(299, 305, 290, bids=((100.0, 1.0),), ts_event_ns=T0_NS + 5 * NS)
    record(tmp_path, [*clean_tape(), rest, bracketing])
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.ok, quality.issues
    assert quality.n_epochs == 2


def test_missing_delta_without_resync_is_a_chain_break(tmp_path: Path) -> None:
    orphan = make_delta(120, 125, 118, bids=((100.0, 3.0),), ts_event_ns=T0_NS + 5 * NS)
    record(tmp_path, [*clean_tape(), orphan])
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert not quality.ok
    assert quality.n_chain_breaks == 1
    assert any("pu" in issue for issue in quality.issues)


def test_crossed_snapshot_and_duplicate_trade_ids_are_issues(tmp_path: Path) -> None:
    crossed = make_snapshot(
        last_update_id=400,
        bids=((101.0, 1.0),),
        asks=((100.0, 1.0),),
        ts_event_ns=T0_NS + 9 * NS,
    )
    duplicate = make_trade(2, 101.0, 0.2, Side.BUY, ts_event_ns=T0_NS + 9 * NS)
    record(tmp_path, [*clean_tape(), crossed, duplicate])
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.n_crossed_snapshots == 1
    assert quality.n_duplicate_trade_ids == 1
    assert len(quality.issues) == 2


def test_zero_qty_trades_are_a_warning_not_an_issue(tmp_path: Path) -> None:
    zero = make_trade(3, 101.0, 0.0, Side.BUY, ts_event_ns=T0_NS + 3 * NS)
    record(tmp_path, [*clean_tape(), zero])
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.ok
    assert quality.n_zero_qty_trades == 1
    assert any("qty 0" in warning for warning in quality.warnings)


def test_off_grid_prices_are_flagged_against_the_recorded_spec(tmp_path: Path) -> None:
    # DOGE tick is 1e-5: 0.123455 sits half a tick off the grid (tick-size change, or a
    # capture paired with the wrong instrument.json).
    rest = make_snapshot(
        last_update_id=100,
        bids=((0.12344, 5_000.0),),
        asks=((0.12346, 5_000.0),),
        ts_event_ns=T0_NS,
        symbol=DOGE,
    )
    trade = make_trade(1, 0.123455, 100.0, Side.BUY, ts_event_ns=T0_NS + NS, symbol=DOGE)
    record(tmp_path, [rest, trade], symbol=DOGE)
    write_instrument_spec(tmp_path, doge_spec())
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=DOGE)
    assert quality.n_off_grid_prices == 1
    assert not quality.ok


def test_without_a_spec_the_grid_is_not_checked(tmp_path: Path) -> None:
    record(tmp_path, clean_tape())
    quality = check_capture(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert quality.n_off_grid_prices is None
    assert any("instrument.json" in warning for warning in quality.warnings)
