"""Optional hftbacktest 2.4.4 checks: synthetic capture conservation + engine smoke.

Skipped unless ``hftbacktest`` is installed (``uv sync --extra hftbacktest``) or
``RUN_HFTBACKTEST=1``. User: queue-position-aware backtest via hftbacktest;
marker skip unless extra installed (like nautilus).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.backtest.hft_adapter import capture_to_hft_feed
from order_flow.ingestion.events import Side
from order_flow.storage.parquet import ParquetWriter
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

NS = 1_000_000


def test_converted_feed_conservation_before_engine(tmp_path: Path) -> None:
    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(
            [
                make_snapshot(last_update_id=100, ts_event_ns=T0_NS),
                make_delta(
                    101,
                    105,
                    100,
                    bids=((100.0, 12.0),),
                    ts_event_ns=T0_NS + NS,
                ),
                make_trade(1, 100.0, 0.5, Side.SELL, ts_event_ns=T0_NS + 2 * NS),
                make_trade(2, 101.0, 0.0, Side.BUY, ts_event_ns=T0_NS + 3 * NS),
            ]
        )
    feed = capture_to_hft_feed(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert feed.conservation_gap() == 0
    assert feed.n_qty0_trades_dropped == 1
    assert feed.n_feed_trade_events == 1


@pytest.mark.hftbacktest
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::UserWarning")
def test_engine_smoke_on_synthetic_capture(tmp_path: Path) -> None:
    from order_flow.backtest.hft_runner import run_ofi_mm_hftbacktest

    with ParquetWriter(tmp_path, EXCHANGE, SYMBOL) as writer:
        writer.write(
            [
                make_snapshot(last_update_id=100, ts_event_ns=T0_NS),
                make_delta(
                    101,
                    105,
                    100,
                    bids=((100.0, 12.0),),
                    ts_event_ns=T0_NS + NS,
                ),
                make_trade(1, 100.0, 0.5, Side.SELL, ts_event_ns=T0_NS + 2 * NS),
                make_trade(2, 101.0, 0.5, Side.BUY, ts_event_ns=T0_NS + 3 * NS),
                make_delta(
                    106,
                    110,
                    105,
                    asks=((101.0, 4.0),),
                    ts_event_ns=T0_NS + 4 * NS,
                ),
            ]
        )
    maker = run_ofi_mm_hftbacktest(tmp_path, cross_spread=False)
    assert maker.n_public_trades == 2
    assert maker.conservation_gap == 0
    assert maker.hftbacktest_version
    assert "PowerProbQueueFunc" in maker.queue_model
    assert maker.cross_spread is False
    cross = run_ofi_mm_hftbacktest(tmp_path, cross_spread=True)
    assert cross.cross_spread is True
