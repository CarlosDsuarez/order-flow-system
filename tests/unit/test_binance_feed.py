"""BinanceFuturesFeed sync/resync loop driven by fake transports (no network)."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import httpx
import pytest

from order_flow.ingestion.base import MarketDataFeed
from order_flow.ingestion.binance_futures import (
    DEFAULT_REST_URL,
    DEFAULT_WS_URL,
    BinanceFuturesFeed,
)
from order_flow.ingestion.events import BookDelta, BookSnapshot, MarketEvent, PriceLevel, Trade

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


class FakeWebSocket:
    """Async-iterable stand-in for ``websockets.asyncio.client.ClientConnection``."""

    def __init__(self, messages: Sequence[str | bytes]) -> None:
        self._messages = list(messages)

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[str | bytes]:
        for message in self._messages:
            yield message


def depth(first: int, final: int, prev: int) -> str:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1,
            "T": 1,
            "s": "BTCUSDT",
            "U": first,
            "u": final,
            "pu": prev,
            "b": [["100.0", "1.0"]],
            "a": [],
        }
    )


def wrapped(payload: str) -> str:
    return json.dumps({"stream": "btcusdt@depth@100ms", "data": json.loads(payload)})


TRADE_MSG = json.dumps(
    {"e": "aggTrade", "E": 2, "T": 2, "s": "BTCUSDT", "a": 7, "p": "100.5", "q": "0.1", "m": False}
)
TRADE_PRINT_MSG = json.dumps(
    {
        "e": "trade",
        "E": 3,
        "T": 3,
        "s": "BTCUSDT",
        "t": 99,
        "p": "100.6",
        "q": "0.2",
        "m": True,
        "X": "MARKET",
    }
)


def snapshot_client(last_update_ids: list[int], calls: list[dict[str, Any]]) -> httpx.AsyncClient:
    """HTTP client whose ``/fapi/v1/depth`` answers with successive ``lastUpdateId`` values."""

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        last_update_id = last_update_ids[min(len(calls) - 1, len(last_update_ids) - 1)]
        return httpx.Response(
            200,
            json={
                "lastUpdateId": last_update_id,
                "E": 3,
                "T": 3,
                "bids": [["99.0", "5.0"]],
                "asks": [["101.0", "5.0"]],
            },
        )

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=DEFAULT_REST_URL)


async def collect(feed: BinanceFuturesFeed) -> list[MarketEvent]:
    return [event async for event in feed.stream()]


def test_feed_satisfies_protocol_and_builds_stream_url() -> None:
    feed = BinanceFuturesFeed("btcusdt")
    assert isinstance(feed, MarketDataFeed)
    assert feed.symbol == "BTCUSDT"
    # @aggTrade on fstream WS is silent (probe 2026-09-02); @trade carries the same ``m`` flag.
    # Put @trade first: depth@100ms/@trade starved the book on this venue.
    assert feed.stream_url == f"{DEFAULT_WS_URL}?streams=btcusdt@trade/btcusdt@depth@100ms"
    assert BinanceFuturesFeed("ETHUSDT", depth_speed="").stream_url.endswith(
        "?streams=ethusdt@trade/ethusdt@depth"
    )


async def test_stream_syncs_filters_and_resyncs_on_gap() -> None:
    messages: list[str | bytes] = [
        depth(90, 95, 85),  # stale: u < 100
        wrapped(depth(98, 102, 95)),  # brackets lastUpdateId=100 -> applied
        TRADE_MSG,
        wrapped(TRADE_PRINT_MSG),
        depth(103, 104, 102).encode(),  # contiguous, as bytes
        depth(110, 112, 108),  # gap (pu != 104) -> resync with snapshot 113
        json.dumps({"e": "markPriceUpdate", "s": "BTCUSDT"}),  # unknown -> ignored
        depth(113, 115, 112),  # brackets new snapshot id 113
        depth(116, 118, 115),
    ]
    calls: list[dict[str, Any]] = []

    @asynccontextmanager
    async def fake_connect(uri: str) -> AsyncIterator[FakeWebSocket]:
        assert "btcusdt@trade" in uri
        assert "btcusdt@depth" in uri
        yield FakeWebSocket(messages)

    feed = BinanceFuturesFeed(
        "BTCUSDT",
        snapshot_limit=500,
        ws_connect=fake_connect,
        http_client=snapshot_client([100, 113], calls),
    )
    events = await collect(feed)

    kinds = [type(event).__name__ for event in events]
    assert kinds == [
        "BookSnapshot",
        "BookDelta",
        "Trade",
        "Trade",
        "BookDelta",
        "BookSnapshot",
        "BookDelta",
        "BookDelta",
    ]
    snapshots = [event for event in events if isinstance(event, BookSnapshot)]
    deltas = [event for event in events if isinstance(event, BookDelta)]
    trades = [event for event in events if isinstance(event, Trade)]
    assert [snapshot.last_update_id for snapshot in snapshots] == [100, 113]
    assert [delta.final_update_id for delta in deltas] == [102, 104, 115, 118]
    assert [trade.trade_id for trade in trades] == [7, 99]
    assert trades[1].aggressor.value == "ask"
    assert calls == [{"symbol": "BTCUSDT", "limit": "500"}] * 2


async def test_fetch_snapshot_raises_on_http_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"code": -1003, "msg": "Too many requests"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=DEFAULT_REST_URL)
    with pytest.raises(httpx.HTTPStatusError):
        await BinanceFuturesFeed("BTCUSDT").fetch_snapshot(client)


def test_raw_depth_url_when_trades_disabled() -> None:
    feed = BinanceFuturesFeed("BTCUSDT", include_trades=False)
    assert feed.stream_url == f"{DEFAULT_WS_URL}?streams=btcusdt@depth@100ms"
    assert feed.depth_stream_url == feed.stream_url
    assert feed.trade_stream_url == f"{DEFAULT_WS_URL}?streams=btcusdt@trade"


async def test_dual_sockets_merges_depth_and_trade() -> None:
    connected: list[str] = []

    @asynccontextmanager
    async def fake_connect(uri: str) -> AsyncIterator[FakeWebSocket]:
        connected.append(uri)
        if "@depth" in uri:
            yield FakeWebSocket([depth(98, 102, 95)])
        else:
            yield FakeWebSocket([TRADE_PRINT_MSG])

    feed = BinanceFuturesFeed(
        "BTCUSDT",
        dual_sockets=True,
        ws_connect=fake_connect,
        http_client=snapshot_client([100], []),
    )
    events = await collect(feed)
    kinds = [type(event).__name__ for event in events]
    assert kinds[0] == "BookSnapshot"
    assert "BookDelta" in kinds
    assert "Trade" in kinds
    assert any("@depth" in url for url in connected)
    assert any("trade" in url for url in connected)
    trades = [event for event in events if isinstance(event, Trade)]
    assert trades[0].trade_id == 99
    assert trades[0].aggressor.value == "ask"


async def test_stream_retries_429_using_retry_after() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"msg": "wait"})
        return httpx.Response(
            200,
            json={
                "lastUpdateId": 100,
                "E": 3,
                "T": 3,
                "bids": [["99.0", "5.0"]],
                "asks": [["101.0", "5.0"]],
            },
        )

    @asynccontextmanager
    async def fake_connect(_uri: str) -> AsyncIterator[FakeWebSocket]:
        yield FakeWebSocket([depth(98, 102, 95)])

    feed = BinanceFuturesFeed(
        "BTCUSDT",
        ws_connect=fake_connect,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url=DEFAULT_REST_URL
        ),
    )
    events = await collect(feed)
    assert calls == 2
    assert feed.stats.rest_429s == 1
    assert isinstance(events[0], BookSnapshot)
    assert feed.stats.snapshots_applied == 1
    assert feed.stats.deltas_applied == 1
    assert feed.queue.qsize() == len(events)


async def test_start_reconnects_after_socket_eof(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "order_flow.ingestion.binance_futures.reconnect_delay", lambda *_a, **_k: 0.0
    )
    connects = 0

    @asynccontextmanager
    async def fake_connect(_uri: str) -> AsyncIterator[FakeWebSocket]:
        nonlocal connects
        connects += 1
        if connects == 1:
            yield FakeWebSocket([depth(98, 102, 95)])
        else:
            yield FakeWebSocket([depth(101, 105, 100)])

    feed = BinanceFuturesFeed(
        "BTCUSDT",
        ws_connect=fake_connect,
        http_client=snapshot_client([100, 103], []),
    )
    await feed.start()
    got: list[MarketEvent] = []
    while len(got) < 4:
        got.append(await asyncio.wait_for(feed.queue.get(), timeout=2.0))
    await feed.stop()
    assert feed.stats.reconnects >= 1
    assert connects >= 2
    assert isinstance(got[0], BookSnapshot)
    assert feed.book.last_update_id is not None


async def test_bad_trade_does_not_kill_depth_pipeline() -> None:
    @asynccontextmanager
    async def fake_connect(_uri: str) -> AsyncIterator[FakeWebSocket]:
        yield FakeWebSocket(
            [
                json.dumps({"e": "aggTrade", "s": "BTCUSDT"}),  # missing fields
                depth(98, 102, 95),
            ]
        )

    feed = BinanceFuturesFeed(
        "BTCUSDT",
        ws_connect=fake_connect,
        http_client=snapshot_client([100], []),
    )
    events = await collect(feed)
    kinds = [type(event).__name__ for event in events]
    assert kinds == ["BookSnapshot", "BookDelta"]
    assert feed.stats.trades == 0
    assert feed.stats.deltas_applied == 1


def test_latency_summary_empty_and_populated() -> None:
    feed = BinanceFuturesFeed("BTCUSDT")
    empty = feed.stats.latency_summary()
    assert empty["count"] == 0.0
    assert empty["mean"] != empty["mean"]  # NaN
    feed.stats.record_latency(10)
    feed.stats.record_latency(20)
    feed.stats.record_latency(30)
    summary = feed.stats.latency_summary()
    assert summary["count"] == 3.0
    assert summary["mean"] == pytest.approx(20.0)
    assert summary["min"] == pytest.approx(10.0)
    assert summary["max"] == pytest.approx(30.0)


def test_feed_marks_book_unsynced_on_gap() -> None:
    feed = BinanceFuturesFeed("BTCUSDT")
    feed.book.apply_snapshot(
        BookSnapshot(
            exchange="binance_futures",
            symbol="BTCUSDT",
            ts_event_ns=1,
            ts_recv_ns=2,
            last_update_id=100,
            bids=(PriceLevel(100.0, 1.0),),
            asks=(PriceLevel(101.0, 1.0),),
        )
    )
    feed._on_gap(
        BookDelta(
            exchange="binance_futures",
            symbol="BTCUSDT",
            ts_event_ns=3,
            ts_recv_ns=4,
            first_update_id=110,
            final_update_id=112,
            prev_final_update_id=108,
            bids=(),
            asks=(),
        )
    )
    assert feed.book.is_synced is False
