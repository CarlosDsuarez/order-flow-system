"""Binance USD-M Futures adapter: parsers, reconstruction, async WebSocket feed.

Public streams need no API key. Official protocol (retrieved 2026-09-02):

* Diff. Book Depth Streams / "How to manage a local order book correctly":
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
* First applied event: ``U <= lastUpdateId AND u >= lastUpdateId`` (NOT spot ``+1``).
* Subsequent: ``pu`` of the current event equals ``u`` of the previous; else resync.
* Aggregate Trade Streams:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
* REST snapshot ``GET /fapi/v1/depth``:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book

The live feed publishes validated events to an ``asyncio.Queue``. It does **not** write
disk / Parquet / ClickHouse; that is a later phase. Combined stream is used so depth
and trades share one connection (one WS per :class:`BinanceFuturesFeed` instance).
On 2026-09-02 ``@aggTrade`` was silent on ``fstream`` WebSocket; the feed subscribes
to ``@trade`` (same ``m`` flag). ``dual_sockets=True`` (the recorder default) opens
raw depth plus a dedicated trade socket because the combined handshake was flaky.
Trade parse failures are logged and skipped — they must not tear down the book pipeline.
"""

from __future__ import annotations

import asyncio
import math
import statistics
from collections import deque
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, AsyncExitStack, suppress
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import httpx
import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from order_flow.ingestion.events import (
    BookDelta,
    BookSnapshot,
    MarketEvent,
    PriceLevel,
    Side,
    Trade,
)
from order_flow.ingestion.sync import (
    DepthDecision,
    DepthSequenceValidator,
    DepthSynchronizer,
    observed_latency_ns,
    reconnect_delay,
)
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError
from order_flow.utils.logging import get_logger
from order_flow.utils.time import ms_to_ns, now_ns

EXCHANGE: Final = "binance_futures"
DEFAULT_WS_URL: Final = "wss://fstream.binance.com/stream"
DEFAULT_RAW_WS_URL: Final = "wss://fstream.binance.com/ws"
DEFAULT_REST_URL: Final = "https://fapi.binance.com"
DEPTH_SNAPSHOT_PATH: Final = "/fapi/v1/depth"
LATENCY_LOG_EVERY: Final = 200
MAX_SNAPSHOT_RETRIES: Final = 8
MAX_LATENCY_SAMPLES: Final = 10_000
HTTP_TOO_MANY_REQUESTS: Final = 429
HTTP_BAD_REQUEST: Final = 400
_WS_SENTINEL: Final = object()

# Documented REST weights for GET /fapi/v1/depth (limit → weight).
DEPTH_LIMIT_WEIGHT: Final[dict[int, int]] = {5: 2, 10: 2, 20: 2, 50: 2, 100: 5, 500: 10, 1000: 20}

WsConnect = Callable[[str], AbstractAsyncContextManager[AsyncIterable[str | bytes]]]
"""Factory returning an async context manager that yields an async iterable of raw messages.

``websockets.asyncio.client.connect`` satisfies it; tests inject fakes.
"""


def _default_ws_connect(url: str) -> AbstractAsyncContextManager[AsyncIterable[str | bytes]]:
    """Public Binance WS connect. Short handshake timeout so a hung path fails over fast."""
    return connect(url, open_timeout=20.0, ping_interval=20.0, ping_timeout=20.0)


async def _merge_raw_streams(
    *streams: AsyncIterator[str | bytes],
) -> AsyncGenerator[str | bytes, None]:
    """Fair-merge several WebSocket iterators until every one has ended."""
    incoming: asyncio.Queue[object] = asyncio.Queue()

    async def pump(messages: AsyncIterator[str | bytes]) -> None:
        try:
            async for raw in messages:
                await incoming.put(raw)
        finally:
            await incoming.put(_WS_SENTINEL)

    pumps = [asyncio.create_task(pump(stream)) for stream in streams]
    finished = 0
    try:
        while finished < len(pumps):
            item = await incoming.get()
            if item is _WS_SENTINEL:
                finished += 1
                continue
            if isinstance(item, (bytes, str)):
                yield item
    finally:
        for task in pumps:
            task.cancel()
        for task in pumps:
            with suppress(asyncio.CancelledError):
                await task


log = get_logger(__name__)

__all__ = [
    "DEFAULT_RAW_WS_URL",
    "DEFAULT_REST_URL",
    "DEFAULT_WS_URL",
    "DEPTH_SNAPSHOT_PATH",
    "EXCHANGE",
    "BinanceFuturesFeed",
    "DepthSequenceValidator",
    "FeedStats",
    "parse_agg_trade",
    "parse_depth_snapshot",
    "parse_depth_update",
    "parse_trade",
    "unwrap_stream_message",
]


# --------------------------------------------------------------------------- parsing
def _levels(raw: Iterable[Sequence[str | float]]) -> tuple[PriceLevel, ...]:
    return tuple(PriceLevel(float(price), float(qty)) for price, qty in raw)


def _event_ts_ns(
    msg: Mapping[str, Any], fallback_ns: int, *, keys: tuple[str, ...] = ("E", "T")
) -> int:
    """Exchange timestamp in ns.

    Depth payloads document ``E`` as event time (ms) and ``T`` as transaction time (ms).
    Latency uses event time ``E`` first; ``T`` is the fallback. Trades pass ``keys=("T", "E")``
    so the trade time is preferred.
    """
    for key in keys:
        value = msg.get(key)
        if value is not None:
            return ms_to_ns(int(value))
    return fallback_ns


def _expect_event_type(msg: Mapping[str, Any], expected: str) -> None:
    actual = msg.get("e", expected)
    if actual != expected:
        msg_text = f"expected event type {expected!r}, got {actual!r}"
        raise ValueError(msg_text)


def unwrap_stream_message(msg: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the payload of a combined-stream envelope ``{"stream": ..., "data": {...}}``.

    Messages that are not wrapped are returned unchanged.
    """
    data = msg.get("data")
    if "stream" in msg and isinstance(data, Mapping):
        return data
    return msg


def parse_depth_update(msg: Mapping[str, Any], *, ts_recv_ns: int | None = None) -> BookDelta:
    """Parse a ``depthUpdate`` message (``<symbol>@depth[@100ms]``) into a :class:`BookDelta`.

    Fields: ``E`` event time (ms), ``T`` transaction time (ms), ``s`` symbol, ``U`` first
    update id, ``u`` final update id, ``pu`` final update id of the previous event,
    ``b``/``a`` bid/ask levels as ``[price, qty]`` decimal strings (``qty == 0`` removes).
    ``ts_event_ns`` is taken from ``E`` (official event time) so latency is
    ``ts_recv_ns - ts_event_ns``.
    """
    _expect_event_type(msg, "depthUpdate")
    recv = now_ns() if ts_recv_ns is None else ts_recv_ns
    return BookDelta(
        exchange=EXCHANGE,
        symbol=str(msg["s"]),
        ts_event_ns=_event_ts_ns(msg, recv, keys=("E", "T")),
        ts_recv_ns=recv,
        first_update_id=int(msg["U"]),
        final_update_id=int(msg["u"]),
        prev_final_update_id=int(msg["pu"]),
        bids=_levels(msg["b"]),
        asks=_levels(msg["a"]),
    )


def _parse_print(msg: Mapping[str, Any], trade_id: int, *, ts_recv_ns: int | None) -> Trade:
    """Shared Trade constructor for ``aggTrade`` and ``trade`` (same ``m`` flag)."""
    recv = now_ns() if ts_recv_ns is None else ts_recv_ns
    return Trade(
        exchange=EXCHANGE,
        symbol=str(msg["s"]),
        ts_event_ns=_event_ts_ns(msg, recv, keys=("T", "E")),
        ts_recv_ns=recv,
        trade_id=trade_id,
        price=float(msg["p"]),
        qty=float(msg["q"]),
        aggressor=Side.SELL if bool(msg["m"]) else Side.BUY,
    )


def parse_agg_trade(msg: Mapping[str, Any], *, ts_recv_ns: int | None = None) -> Trade:
    """Parse an ``aggTrade`` message into a :class:`Trade`.

    Fields: ``a`` aggregate trade id, ``p`` price, ``q`` quantity, ``T`` trade time (ms),
    ``m`` "Is the buyer the market maker?" - when true the seller was the aggressor.
    """
    _expect_event_type(msg, "aggTrade")
    return _parse_print(msg, int(msg["a"]), ts_recv_ns=ts_recv_ns)


def parse_trade(msg: Mapping[str, Any], *, ts_recv_ns: int | None = None) -> Trade:
    """Parse a ``trade`` message (USD-M ``<symbol>@trade``) into a :class:`Trade`.

    Same ``m`` mapping as :func:`parse_agg_trade`. Trade id is field ``t``.
    As of 2026-09-02 the ``@aggTrade`` WebSocket on ``fstream.binance.com`` delivers
    no prints while ``@trade`` does; REST ``/fapi/v1/aggTrades`` still works.
    """
    _expect_event_type(msg, "trade")
    return _parse_print(msg, int(msg["t"]), ts_recv_ns=ts_recv_ns)


def parse_depth_snapshot(
    payload: Mapping[str, Any], symbol: str, *, ts_recv_ns: int | None = None
) -> BookSnapshot:
    """Parse a ``GET /fapi/v1/depth`` response into a :class:`BookSnapshot`.

    Fields: ``lastUpdateId``, ``E`` message output time (ms), ``T`` transaction time (ms),
    ``bids``/``asks`` as ``[price, qty]`` decimal strings.
    """
    recv = now_ns() if ts_recv_ns is None else ts_recv_ns
    return BookSnapshot(
        exchange=EXCHANGE,
        symbol=symbol.upper(),
        ts_event_ns=_event_ts_ns(payload, recv, keys=("E", "T")),
        ts_recv_ns=recv,
        last_update_id=int(payload["lastUpdateId"]),
        bids=_levels(payload["bids"]),
        asks=_levels(payload["asks"]),
    )


# --------------------------------------------------------------------------- stats
def _percentile(samples: Sequence[int], pct: float) -> float:
    if not samples:
        return math.nan
    ordered = sorted(samples)
    rank = pct / 100.0 * (len(ordered) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(ordered[lo])
    weight = rank - lo
    return float(ordered[lo]) * (1.0 - weight) + float(ordered[hi]) * weight


@dataclass
class FeedStats:
    """Counters and latency samples for one :class:`BinanceFuturesFeed` instance."""

    gaps: int = 0
    resyncs: int = 0
    reconnects: int = 0
    snapshots_applied: int = 0
    deltas_applied: int = 0
    trades: int = 0
    rest_429s: int = 0
    rest_errors: int = 0
    latency_samples_ns: list[int] = field(default_factory=list)

    def record_latency(self, sample_ns: int) -> None:
        """Keep a bounded list of ``ts_recv_ns - ts_event_ns`` samples."""
        if len(self.latency_samples_ns) < MAX_LATENCY_SAMPLES:
            self.latency_samples_ns.append(sample_ns)

    def latency_summary(self) -> dict[str, float]:
        """Count / mean / p50 / p99 / min / max in nanoseconds (NaN when empty)."""
        samples = self.latency_samples_ns
        if not samples:
            nan = math.nan
            return {
                "count": 0.0,
                "mean": nan,
                "p50": nan,
                "p99": nan,
                "min": nan,
                "max": nan,
            }
        return {
            "count": float(len(samples)),
            "mean": float(statistics.fmean(samples)),
            "p50": _percentile(samples, 50.0),
            "p99": _percentile(samples, 99.0),
            "min": float(min(samples)),
            "max": float(max(samples)),
        }


# --------------------------------------------------------------------------- feed
class BinanceFuturesFeed:
    """Combined ``depth`` + ``trade`` feed for one USD-M perpetual symbol.

    Depth is mandatory. Trades share the combined stream
    ``wss://fstream.binance.com/stream?streams=<sym>@depth@100ms/<sym>@trade``
    so a single connector uses **one** WebSocket (Binance limits ~300 connections /
    5 minutes / IP). ``@aggTrade`` is documented but was silent on this venue's
    WebSocket on 2026-09-02; ``@trade`` carries the same ``m`` flag. Set
    ``include_trades=False`` for the raw depth URL
    ``wss://fstream.binance.com/ws/<sym>@depth@100ms``.

    ``stream()`` runs one connection (resync-on-gap without reconnecting) and yields
    validated events, also putting them on :attr:`queue`. :meth:`start` wraps that in
    an exponential-backoff reconnect loop. The feed owns :attr:`book`; published events
    are frozen dataclasses — callers must not mutate the book.

    Network I/O is confined to snapshot HTTP and the injected ``ws_connect``.
    """

    exchange: str = EXCHANGE

    def __init__(
        self,
        symbol: str,
        *,
        queue: asyncio.Queue[MarketEvent] | None = None,
        depth_speed: str = "100ms",
        snapshot_limit: int = 1000,
        include_trades: bool = True,
        dual_sockets: bool = False,
        ws_url: str = DEFAULT_WS_URL,
        rest_url: str = DEFAULT_REST_URL,
        timeout_s: float = 10.0,
        max_snapshot_retries: int = MAX_SNAPSHOT_RETRIES,
        ws_connect: WsConnect | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.depth_speed = depth_speed
        self.snapshot_limit = snapshot_limit
        self.include_trades = include_trades
        self.dual_sockets = dual_sockets
        self.ws_url = ws_url
        self.rest_url = rest_url
        self.timeout_s = timeout_s
        self.max_snapshot_retries = max_snapshot_retries
        self._ws_connect: WsConnect = _default_ws_connect if ws_connect is None else ws_connect
        self._http_client = http_client
        self.queue: asyncio.Queue[MarketEvent] = asyncio.Queue() if queue is None else queue
        self.stats = FeedStats()
        self._book = OrderBook(exchange=EXCHANGE, symbol=self.symbol)
        self._sync = DepthSynchronizer()
        self._stopped = False
        self._task: asyncio.Task[None] | None = None
        self._events_since_latency_log = 0

    @property
    def book(self) -> OrderBook:
        """Live reconstructed book. Owned by the feed; do not mutate it."""
        return self._book

    @property
    def stream_url(self) -> str:
        """Combined-stream URL, or raw ``/ws/<symbol>@depth@100ms`` when trades are off."""
        if not self.include_trades:
            return self.depth_stream_url
        # Trade first: `depth@100ms/<sym>@trade` was observed to starve depth (2026-09-02).
        return f"{self.ws_url}?streams={self.symbol.lower()}@trade/{self._depth_stream_name()}"

    def _depth_stream_name(self) -> str:
        sym = self.symbol.lower()
        return f"{sym}@depth@{self.depth_speed}" if self.depth_speed else f"{sym}@depth"

    @property
    def depth_stream_url(self) -> str:
        """Depth URL. Combined ``/stream?streams=`` is more reliable than raw ``/ws/`` here."""
        return f"{DEFAULT_WS_URL}?streams={self._depth_stream_name()}"

    @property
    def trade_stream_url(self) -> str:
        """Dedicated ``@trade`` combined-stream URL (same ``m`` flag as aggTrade)."""
        return f"{DEFAULT_WS_URL}?streams={self.symbol.lower()}@trade"

    async def fetch_snapshot(self, client: httpx.AsyncClient) -> BookSnapshot:
        """Fetch the REST depth snapshot once (``limit`` levels per side). Raises on HTTP errors."""
        response = await client.get(
            DEPTH_SNAPSHOT_PATH, params={"symbol": self.symbol, "limit": self.snapshot_limit}
        )
        response.raise_for_status()
        return parse_depth_snapshot(orjson.loads(response.content), self.symbol)

    async def start(self) -> None:
        """Run the reconnect loop in a background task, publishing to :attr:`queue`."""
        if self._task is not None and not self._task.done():
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_forever(), name=f"binance-feed-{self.symbol}")

    async def stop(self) -> None:
        """Cancel the background task started by :meth:`start`."""
        self._stopped = True
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def stream(self) -> AsyncGenerator[MarketEvent, None]:
        """Connect once, synchronise with a snapshot and yield validated events.

        Sequence gaps resync from a new REST snapshot without dropping the socket.
        Socket close ends the iterator (use :meth:`start` for automatic reconnect).
        """
        async with AsyncExitStack() as stack:
            client = self._http_client
            if client is None:
                client = await stack.enter_async_context(
                    httpx.AsyncClient(base_url=self.rest_url, timeout=self.timeout_s)
                )
            if self.include_trades and self.dual_sockets:
                depth_ws, trade_ws = await asyncio.gather(
                    stack.enter_async_context(self._ws_connect(self.depth_stream_url)),
                    stack.enter_async_context(self._ws_connect(self.trade_stream_url)),
                )
                messages: AsyncIterator[str | bytes] = _merge_raw_streams(
                    aiter(depth_ws), aiter(trade_ws)
                )
            else:
                ws = await stack.enter_async_context(self._ws_connect(self.stream_url))
                messages = aiter(ws)
            async for event in self._synced_events(client, messages):
                yield event

    async def _run_forever(self) -> None:
        attempt = 0
        while True:
            try:
                async for _event in self.stream():
                    pass
                close_reason = "eof"
                close_code: int | None = None
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as exc:
                close_reason = str(exc.reason) or "connection_closed"
                close_code = exc.code
            except Exception as exc:
                close_reason = str(exc)
                close_code = None
                log.error("ws_session_error", symbol=self.symbol, error=str(exc))
            self._sync.on_disconnect()
            self._book.mark_unsynced()
            delay = reconnect_delay(attempt)
            self.stats.reconnects += 1
            log.warning(
                "ws_reconnect",
                symbol=self.symbol,
                attempt=attempt + 1,
                backoff_s=delay,
                close_reason=close_reason,
                close_code=close_code,
            )
            await asyncio.sleep(delay)
            attempt = min(attempt + 1, 16)

    async def _fetch_snapshot_retrying(self, client: httpx.AsyncClient) -> BookSnapshot:
        attempt = 0
        while True:
            response = await client.get(
                DEPTH_SNAPSHOT_PATH,
                params={"symbol": self.symbol, "limit": self.snapshot_limit},
            )
            if response.status_code == HTTP_TOO_MANY_REQUESTS:
                self.stats.rest_429s += 1
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None and retry_after.strip().isdigit():
                    delay = float(retry_after)
                else:
                    delay = reconnect_delay(attempt)
                log.warning(
                    "rest_429",
                    symbol=self.symbol,
                    attempt=attempt + 1,
                    retry_after_s=delay,
                    path=DEPTH_SNAPSHOT_PATH,
                )
                if attempt >= self.max_snapshot_retries:
                    response.raise_for_status()
                await asyncio.sleep(delay)
                attempt += 1
                continue
            if response.status_code >= HTTP_BAD_REQUEST:
                self.stats.rest_errors += 1
                log.error(
                    "rest_error",
                    symbol=self.symbol,
                    status=response.status_code,
                    path=DEPTH_SNAPSHOT_PATH,
                )
                response.raise_for_status()
            return parse_depth_snapshot(orjson.loads(response.content), self.symbol)

    async def _synced_events(
        self, client: httpx.AsyncClient, messages: AsyncIterator[str | bytes]
    ) -> AsyncGenerator[MarketEvent, None]:
        """Buffer WS diffs, apply REST snapshot, then validated deltas/trades.

        On ``pu != previous u``: never apply the gap event; count it; fetch a new snapshot
        while the socket stays up. Official step 6: "initialize the process from step 3".
        """
        incoming: asyncio.Queue[object] = asyncio.Queue()
        pump = asyncio.create_task(self._pump(messages, incoming), name=f"ws-pump-{self.symbol}")
        pending: deque[object] = deque()
        try:
            while not self._stopped:
                snapshot = await self._fetch_snapshot_retrying(client)
                self._drain_nowait(incoming, pending)
                self._book.apply_snapshot(snapshot)
                self._sync.install_snapshot(snapshot.last_update_id)
                self.stats.snapshots_applied += 1
                log.info(
                    "resync_success",
                    symbol=self.symbol,
                    last_update_id=snapshot.last_update_id,
                )
                self._note_latency(snapshot)
                await self.queue.put(snapshot)
                yield snapshot
                ended = False
                while not self._stopped:
                    if not pending:
                        pending.append(await incoming.get())
                    raw = pending.popleft()
                    if raw is _WS_SENTINEL:
                        ended = True
                        break
                    if not isinstance(raw, (bytes, str)):
                        continue
                    outcome = self._handle_raw(raw)
                    if outcome == "gap":
                        self._drain_nowait(incoming, pending)
                        break
                    if outcome is not None:
                        await self.queue.put(outcome)
                        yield outcome
                if ended:
                    return
        finally:
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await pump

    async def _pump(
        self, messages: AsyncIterator[str | bytes], dest: asyncio.Queue[object]
    ) -> None:
        try:
            async for raw in messages:
                await dest.put(raw)
        except ConnectionClosed:
            raise
        finally:
            await dest.put(_WS_SENTINEL)

    @staticmethod
    def _drain_nowait(incoming: asyncio.Queue[object], pending: deque[object]) -> None:
        while True:
            try:
                pending.append(incoming.get_nowait())
            except asyncio.QueueEmpty:
                return

    def _handle_raw(self, raw: str | bytes) -> MarketEvent | Literal["gap"] | None:
        recv = now_ns()
        try:
            data = unwrap_stream_message(orjson.loads(raw))
        except (orjson.JSONDecodeError, TypeError) as exc:
            log.warning("ws_decode_error", symbol=self.symbol, error=str(exc))
            return None
        event_type = data.get("e")
        if event_type == "depthUpdate":
            return self._handle_depth(data, recv)
        if event_type == "aggTrade":
            return self._handle_trade(data, recv, parse_agg_trade)
        if event_type == "trade":
            return self._handle_trade(data, recv, parse_trade)
        return None

    def _handle_depth(
        self, data: Mapping[str, Any], recv: int
    ) -> MarketEvent | Literal["gap"] | None:
        try:
            delta = parse_depth_update(data, ts_recv_ns=recv)
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("depth_parse_error", symbol=self.symbol, error=str(exc))
            return None
        decision = self._sync.decide(delta)
        if decision is DepthDecision.BUFFER or decision is DepthDecision.DROP_STALE:
            return None
        if decision is DepthDecision.RESYNC:
            self._on_gap(delta)
            return "gap"
        try:
            applied = self._book.apply_delta(delta)
        except SequenceGapError as exc:
            self._book.mark_unsynced()
            log.warning(
                "depth_sequence_gap",
                symbol=self.symbol,
                previous_u=self._book.last_update_id,
                pu=delta.prev_final_update_id,
                U=delta.first_update_id,
                u=delta.final_update_id,
                error=str(exc),
            )
            self.stats.gaps += 1
            self.stats.resyncs += 1
            return "gap"
        if not applied:
            return None
        self.stats.deltas_applied += 1
        self._note_latency(delta)
        return delta

    def _handle_trade(
        self,
        data: Mapping[str, Any],
        recv: int,
        parser: Callable[..., Trade],
    ) -> Trade | None:
        try:
            trade = parser(data, ts_recv_ns=recv)
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("trade_parse_error", symbol=self.symbol, error=str(exc))
            return None
        self.stats.trades += 1
        self._note_latency(trade)
        return trade

    def _on_gap(self, delta: BookDelta) -> None:
        self._book.mark_unsynced()
        self.stats.gaps += 1
        self.stats.resyncs += 1
        log.warning(
            "depth_sequence_gap",
            symbol=self.symbol,
            previous_u=self._sync.last_update_id,
            pu=delta.prev_final_update_id,
            U=delta.first_update_id,
            u=delta.final_update_id,
        )
        log.info("resync_start", symbol=self.symbol, reason="sequence_gap")

    def _note_latency(self, event: MarketEvent) -> None:
        sample = observed_latency_ns(event.ts_recv_ns, event.ts_event_ns)
        self.stats.record_latency(sample)
        self._events_since_latency_log += 1
        if self._events_since_latency_log >= LATENCY_LOG_EVERY:
            self._events_since_latency_log = 0
            summary = self.stats.latency_summary()
            log.info("observed_latency", symbol=self.symbol, **summary)
