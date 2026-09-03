"""Short live record + Parquet replay (skipped unless RUN_INTEGRATION=1)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from order_flow.ingestion.binance_futures import EXCHANGE, BinanceFuturesFeed
from order_flow.ingestion.events import BookDelta, BookSnapshot, Trade
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError
from order_flow.storage.parquet import ParquetWriter
from order_flow.storage.reconstruct import reconstruct_book

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

SMOKE_SECONDS = 15.0


@pytest.mark.asyncio
async def test_short_record_replay_matches_live_book(tmp_path: Path) -> None:
    feed = BinanceFuturesFeed("BTCUSDT")
    book = OrderBook(exchange=EXCHANGE, symbol=feed.symbol)
    n_delta = 0
    with ParquetWriter(tmp_path, EXCHANGE, feed.symbol, buffer_size=500) as writer:
        await feed.start()
        try:
            deadline = asyncio.get_running_loop().time() + SMOKE_SECONDS
            while asyncio.get_running_loop().time() < deadline:
                try:
                    event = await asyncio.wait_for(feed.queue.get(), timeout=1.0)
                except TimeoutError:
                    continue
                if isinstance(event, BookSnapshot):
                    book.apply_snapshot(event)
                elif isinstance(event, BookDelta):
                    try:
                        book.apply_delta(event)
                    except SequenceGapError:
                        book.mark_unsynced()
                    else:
                        n_delta += 1
                elif isinstance(event, Trade):
                    pass
                writer.write([event])
            if book.is_synced:
                writer.write([book.snapshot()])
        finally:
            await feed.stop()

    assert n_delta >= 1
    assert book.last_update_id is not None
    rebuilt = reconstruct_book(
        tmp_path, book.last_update_ts_ns, exchange=EXCHANGE, symbol=feed.symbol
    )
    assert rebuilt.best_bid() == book.best_bid()
    assert rebuilt.best_ask() == book.best_ask()
    assert not rebuilt.is_crossed()
    assert rebuilt.spread() > 0
