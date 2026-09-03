"""Live smoke test against Binance USD-M Futures public streams (RUN_INTEGRATION=1)."""

from __future__ import annotations

import asyncio
from contextlib import aclosing

import pytest

from order_flow.ingestion.binance_futures import BinanceFuturesFeed
from order_flow.ingestion.events import BookDelta, BookSnapshot, Trade
from order_flow.orderbook.book import OrderBook

pytestmark = pytest.mark.integration


async def test_stream_applies_live_deltas() -> None:
    feed = BinanceFuturesFeed("BTCUSDT")
    book = OrderBook()
    n_deltas = 0
    n_trades = 0
    try:
        async with asyncio.timeout(20), aclosing(feed.stream()) as events:
            async for event in events:
                if isinstance(event, BookSnapshot):
                    book.apply_snapshot(event)
                elif isinstance(event, BookDelta):
                    assert book.apply_delta(event)
                    n_deltas += 1
                elif isinstance(event, Trade):
                    n_trades += 1
                if n_deltas >= 20:
                    break
    except TimeoutError:
        pass  # slow network: the assertions below decide

    assert n_deltas >= 1
    assert n_trades >= 0
    assert not book.is_crossed()
    assert book.mid_price() > 0
    assert book.n_levels[0] > 0
    assert book.n_levels[1] > 0
