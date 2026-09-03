"""Run the same OFI-skewed MM economics inside hftbacktest 2.4.4.

Lazy-imports the optional extra. Quotes come from ``maker_quotes`` /
``RollingOfi`` (``OfiAccumulator``); ``e_n`` is not reimplemented. Queue model
is ``ProbQueueModel`` + ``PowerProbQueueFunc(n=2)`` via
``BacktestAsset.power_prob_queue_model(2.0)``. No live execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

import numpy as np

from order_flow.backtest.hft_adapter import capture_to_hft_feed, hftbacktest_available
from order_flow.backtest.quotes import QuotePair, RollingOfi, aggressive_quotes, maker_quotes
from order_flow.backtest.types import Fill, OrderSide, Position
from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE

if TYPE_CHECKING:
    from pathlib import Path

TICK: Final = 0.1
LOT: Final = 0.001
PRICE_EPS: Final = 1e-12
QUEUE_MODEL: Final = "ProbQueueModel+PowerProbQueueFunc(n=2)"


@dataclass(frozen=True, slots=True)
class HftBacktestResult:
    """PnL and fill counters from one hftbacktest run."""

    hftbacktest_version: str
    queue_model: str
    capture: str
    exchange: str
    symbol: str
    duration_ns: int
    n_feed_events: int
    n_public_trades: int
    n_snapshots_in: int
    n_snapshots_skipped: int
    n_resync_snapshots: int
    n_qty0_trades_dropped: int
    conservation_gap: int
    maker_fee: float
    taker_fee: float
    spread_ticks: int
    ofi_threshold: float
    ofi_window_ns: int
    trade_size: float
    max_skew: int
    cross_spread: bool
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    fees: float
    n_submitted: int
    n_canceled: int
    n_rejected: int
    n_fill_events: int
    n_unique_filled: int
    n_maker_fills: int
    n_taker_fills: int
    fill_rate: float
    last_mid: float | None
    last_ofi: float


@dataclass
class _RunStats:
    symbol: str
    n_submitted: int = 0
    n_canceled: int = 0
    n_rejected: int = 0
    n_maker_fills: int = 0
    n_taker_fills: int = 0
    next_oid: int = 1
    live_bid: int | None = None
    live_ask: int | None = None
    live_bid_px: float | None = None
    live_ask_px: float | None = None
    last_mid: float | None = None
    last_ofi: float = 0.0
    filled_ids: set[int] = field(default_factory=set)
    terminal_ids: set[int] = field(default_factory=set)
    position_acct: Position = field(init=False)

    def __post_init__(self) -> None:
        self.position_acct = Position(symbol=self.symbol)


def _as_event_array(arr: np.ndarray[Any, Any], event_dtype: np.dtype[Any]) -> np.ndarray[Any, Any]:
    if arr.dtype == event_dtype:
        return np.ascontiguousarray(arr)
    out = np.empty(arr.shape[0], dtype=event_dtype)
    names = event_dtype.names
    if names is None:
        msg = "hftbacktest event_dtype has no field names"
        raise TypeError(msg)
    for name in names:
        out[name] = arr[name]
    return np.ascontiguousarray(out)


def _order_is_maker(order: Any) -> bool:
    rec = order.arr[0]
    maker = rec["maker"] if hasattr(rec, "dtype") else getattr(rec, "maker", False)
    return bool(maker)


def _harvest(hbt: Any, stats: _RunStats, *, statuses: dict[str, int], buy: int) -> None:
    filled = statuses["FILLED"]
    partial = statuses["PARTIALLY_FILLED"]
    canceled = statuses["CANCELED"]
    rejected = statuses["REJECTED"]
    expired = statuses["EXPIRED"]
    orders = hbt.orders(0)
    values = orders.values()
    while values.has_next():
        order = values.get()
        oid = int(order.order_id)
        status = int(order.status)
        if status in (filled, partial) and oid not in stats.filled_ids:
            qty = float(order.exec_qty)
            if qty > 0:
                stats.filled_ids.add(oid)
                is_maker = _order_is_maker(order)
                if is_maker:
                    stats.n_maker_fills += 1
                else:
                    stats.n_taker_fills += 1
                side = OrderSide.BUY if int(order.side) == buy else OrderSide.SELL
                stats.position_acct.apply_fill(
                    Fill(
                        order_id=str(oid),
                        symbol=stats.symbol,
                        side=side,
                        price=float(order.exec_price),
                        qty=qty,
                        ts_ns=int(order.exch_timestamp),
                        fee=0.0,
                        is_maker=is_maker,
                    )
                )
        if status in (canceled, rejected, expired) and oid not in stats.terminal_ids:
            stats.terminal_ids.add(oid)
            if status == canceled:
                stats.n_canceled += 1
            else:
                stats.n_rejected += 1
    hbt.clear_inactive_orders(0)


def _submit(
    hbt: Any,
    stats: _RunStats,
    *,
    buy: bool,
    price: float,
    qty: float,
    tif: int,
    limit: int,
) -> int | None:
    oid = stats.next_oid
    stats.next_oid += 1
    if buy:
        rc = hbt.submit_buy_order(0, oid, price, qty, tif, limit, True)
    else:
        rc = hbt.submit_sell_order(0, oid, price, qty, tif, limit, True)
    stats.n_submitted += 1
    if rc != 0:
        return None
    return oid


def _replace_side(
    hbt: Any,
    stats: _RunStats,
    *,
    buy: bool,
    price: float,
    qty: float,
    tif: int,
    limit: int,
    statuses: dict[str, int],
) -> tuple[int | None, float | None]:
    live_id = stats.live_bid if buy else stats.live_ask
    live_px = stats.live_bid_px if buy else stats.live_ask_px
    new_st = statuses["NEW"]
    partial = statuses["PARTIALLY_FILLED"]
    if live_id is not None and live_px is not None and abs(live_px - price) < PRICE_EPS:
        order = hbt.orders(0).get(live_id)
        if order is not None and int(order.status) in (new_st, partial):
            return live_id, live_px
    if live_id is not None:
        order = hbt.orders(0).get(live_id)
        if order is not None and order.cancellable:
            hbt.cancel(0, live_id, True)
    new_id = _submit(hbt, stats, buy=buy, price=price, qty=qty, tif=tif, limit=limit)
    if new_id is None:
        return None, None
    return new_id, price


def _requote(
    hbt: Any,
    stats: _RunStats,
    rolling: RollingOfi,
    *,
    spread_ticks: int,
    ofi_threshold: float,
    max_skew: int,
    trade_size: float,
    cross_spread: bool,
    tif: int,
    limit: int,
    statuses: dict[str, int],
) -> None:
    depth = hbt.depth(0)
    best_bid = float(depth.best_bid)
    best_ask = float(depth.best_ask)
    if not np.isfinite(best_bid) or not np.isfinite(best_ask):
        return
    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        return
    bid_qty = float(depth.best_bid_qty)
    ask_qty = float(depth.best_ask_qty)
    mid = (best_bid + best_ask) / 2.0
    stats.last_mid = mid
    ofi = rolling.observe(int(hbt.current_timestamp), best_bid, bid_qty, best_ask, ask_qty)
    stats.last_ofi = ofi
    quotes: QuotePair | None
    if cross_spread:
        quotes = aggressive_quotes(best_bid=best_bid, best_ask=best_ask)
    else:
        quotes = maker_quotes(
            mid=mid,
            tick=TICK,
            spread_ticks=spread_ticks,
            ofi=ofi,
            threshold=ofi_threshold,
            max_skew=max_skew,
            best_bid=best_bid,
            best_ask=best_ask,
        )
    if quotes is None:
        return
    stats.live_bid, stats.live_bid_px = _replace_side(
        hbt,
        stats,
        buy=True,
        price=quotes.bid,
        qty=trade_size,
        tif=tif,
        limit=limit,
        statuses=statuses,
    )
    stats.live_ask, stats.live_ask_px = _replace_side(
        hbt,
        stats,
        buy=False,
        price=quotes.ask,
        qty=trade_size,
        tif=tif,
        limit=limit,
        statuses=statuses,
    )


def run_ofi_mm_hftbacktest(
    root: Path,
    *,
    exchange: str = DEFAULT_EXCHANGE,
    symbol: str = "BTCUSDT",
    maker_fee: float = 0.0002,
    taker_fee: float = 0.0004,
    spread_ticks: int = 2,
    ofi_threshold: float = 5.0,
    ofi_window_ns: int = 1_000_000_000,
    trade_size: float = 0.001,
    max_skew: int = 1,
    cross_spread: bool = False,
) -> HftBacktestResult:
    """Load ``root``, run one HashMapMarketDepthBacktest, return counters.

    Order latency is 0 ns (same as nautilus 1.231 with no ``latency_model``).
    Feed ``local_ts`` is forced strictly after ``exch_ts`` so the engine is valid
    without injecting the capture's clock-skewed recv delay.
    """
    if not hftbacktest_available():
        msg = "hftbacktest is required: uv sync --extra hftbacktest"
        raise ImportError(msg)
    import hftbacktest
    from hftbacktest import (
        BUY,
        GTC,
        GTX,
        LIMIT,
        BacktestAsset,
        HashMapMarketDepthBacktest,
    )
    from hftbacktest.order import CANCELED, EXPIRED, FILLED, NEW, PARTIALLY_FILLED, REJECTED
    from hftbacktest.types import event_dtype

    feed = capture_to_hft_feed(root, exchange=exchange, symbol=symbol.upper())
    if feed.conservation_gap() != 0:
        msg = f"hft adapter conservation gap {feed.conservation_gap()} on {root}"
        raise RuntimeError(msg)
    initial = _as_event_array(feed.initial_snapshot, event_dtype)
    data = _as_event_array(feed.data, event_dtype)
    if initial.size == 0:
        msg = f"no initial snapshot in {root}"
        raise ValueError(msg)
    print(
        f"hftbacktest loaded initial={initial.size} incremental={data.size} "
        f"(skipped {feed.n_snapshots_skipped} periodic snapshots, "
        f"dropped {feed.n_qty0_trades_dropped} qty=0 trades) from {root}",
        flush=True,
    )

    asset = (
        BacktestAsset()
        .data([data])
        .initial_snapshot(initial)
        .linear_asset(1.0)
        .constant_order_latency(0, 0)
        .power_prob_queue_model(2.0)
        .no_partial_fill_exchange()
        .trading_value_fee_model(maker_fee, taker_fee)
        .tick_size(TICK)
        .lot_size(LOT)
    )
    hbt = HashMapMarketDepthBacktest([asset])
    stats = _RunStats(symbol=symbol.upper())
    rolling = RollingOfi(window_ns=ofi_window_ns)
    tif = GTC if cross_spread else GTX
    statuses = {
        "NEW": int(NEW),
        "FILLED": int(FILLED),
        "PARTIALLY_FILLED": int(PARTIALLY_FILLED),
        "CANCELED": int(CANCELED),
        "REJECTED": int(REJECTED),
        "EXPIRED": int(EXPIRED),
    }
    prev_l1: tuple[float, float, float, float] | None = None
    try:
        # Construction leaves current_timestamp at int64 max until time advances.
        hbt.elapse(1)
        while int(hbt.elapse(100_000_000)) == 0:
            _harvest(hbt, stats, statuses=statuses, buy=int(BUY))
            depth = hbt.depth(0)
            best_bid = float(depth.best_bid)
            best_ask = float(depth.best_ask)
            if not np.isfinite(best_bid) or not np.isfinite(best_ask):
                continue
            key = (
                best_bid,
                best_ask,
                float(depth.best_bid_qty),
                float(depth.best_ask_qty),
            )
            if key == prev_l1:
                continue
            prev_l1 = key
            _requote(
                hbt,
                stats,
                rolling,
                spread_ticks=spread_ticks,
                ofi_threshold=ofi_threshold,
                max_skew=max_skew,
                trade_size=trade_size,
                cross_spread=cross_spread,
                tif=int(tif),
                limit=int(LIMIT),
                statuses=statuses,
            )
        _harvest(hbt, stats, statuses=statuses, buy=int(BUY))
        state = hbt.state_values(0)
        last_mid = stats.last_mid
        if last_mid is None:
            depth = hbt.depth(0)
            bid = float(depth.best_bid)
            ask = float(depth.best_ask)
            if np.isfinite(bid) and np.isfinite(ask) and bid > 0 and ask > bid:
                last_mid = (bid + ask) / 2.0
                stats.last_mid = last_mid
        fees = float(state.fee)
        n_fill_events = int(state.num_trades)
        mark = last_mid if last_mid is not None else 0.0
        unrealized = stats.position_acct.unrealized_pnl(mark)
        realized = stats.position_acct.realized_pnl
        total = realized + unrealized - fees
        submitted = stats.n_submitted
        fill_rate = (len(stats.filled_ids) / submitted) if submitted else 0.0
        duration = 0
        if data.size:
            duration = int(data["exch_ts"][-1] - data["exch_ts"][0])
        return HftBacktestResult(
            hftbacktest_version=str(hftbacktest.__version__),
            queue_model=QUEUE_MODEL,
            capture=str(root),
            exchange=exchange,
            symbol=symbol.upper(),
            duration_ns=max(0, duration),
            n_feed_events=int(data.size),
            n_public_trades=feed.n_feed_trade_events,
            n_snapshots_in=feed.n_snapshots_in,
            n_snapshots_skipped=feed.n_snapshots_skipped,
            n_resync_snapshots=feed.n_resync_snapshots,
            n_qty0_trades_dropped=feed.n_qty0_trades_dropped,
            conservation_gap=feed.conservation_gap(),
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            spread_ticks=spread_ticks,
            ofi_threshold=ofi_threshold,
            ofi_window_ns=ofi_window_ns,
            trade_size=trade_size,
            max_skew=max_skew,
            cross_spread=cross_spread,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            total_pnl=total,
            fees=fees,
            n_submitted=submitted,
            n_canceled=stats.n_canceled,
            n_rejected=stats.n_rejected,
            n_fill_events=n_fill_events,
            n_unique_filled=len(stats.filled_ids),
            n_maker_fills=stats.n_maker_fills,
            n_taker_fills=stats.n_taker_fills,
            fill_rate=fill_rate,
            last_mid=stats.last_mid,
            last_ofi=stats.last_ofi,
        )
    finally:
        hbt.close()
