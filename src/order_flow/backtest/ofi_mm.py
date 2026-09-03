"""Nautilus Strategy: one-lot OFI-skewed quotes (pipeline test, not an edge).

Classes are built inside :func:`load_ofi_mm` so importing this module does not
require ``nautilus_trader``. Positive rolling OFI skews quotes up.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from order_flow.backtest.quotes import QuotePair, RollingOfi, aggressive_quotes, maker_quotes


@dataclass
class OfiMmStats:
    """Counters the runner reads after ``engine.run()``."""

    n_submitted: int = 0
    n_canceled: int = 0
    n_rejected: int = 0
    n_fill_events: int = 0
    n_unique_filled: int = 0
    n_maker_fills: int = 0
    n_taker_fills: int = 0
    last_mid: float | None = None
    last_ofi: float = 0.0


def load_ofi_mm() -> tuple[type[Any], type[Any]]:
    """Return ``(OfiMmConfig, OfiMmStrategy)`` bound to the installed nautilus API."""
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.model.enums import BookType, LiquiditySide, OrderSide, TimeInForce
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.trading.strategy import Strategy

    class OfiMmConfig(StrategyConfig, frozen=True):  # type: ignore[misc,call-arg]
        instrument_id: InstrumentId
        trade_size: float = 0.001
        spread_ticks: int = 2
        ofi_threshold: float = 5.0
        ofi_window_ns: int = 1_000_000_000
        max_skew: int = 1
        tick: float = 0.1
        cross_spread: bool = False

    class OfiMmStrategy(Strategy):  # type: ignore[misc]
        """One bid + one ask, cancel/replace when the target price moves."""

        def __init__(self, config: OfiMmConfig) -> None:
            super().__init__(config)
            self.stats = OfiMmStats()
            self._rolling: RollingOfi | None = None
            self._filled_oids: set[str] = set()
            self.instrument: Any = None

        def on_start(self) -> None:
            self.instrument = self.cache.instrument(self.config.instrument_id)
            if self.instrument is None:
                self.log.error(f"missing instrument {self.config.instrument_id}")
                self.stop()
                return
            self._rolling = RollingOfi(window_ns=self.config.ofi_window_ns)
            self.subscribe_order_book_deltas(self.instrument.id, BookType.L2_MBP)

        def on_order_book_deltas(self, deltas: Any) -> None:
            del deltas
            self._requote()

        def on_order_filled(self, event: Any) -> None:
            self.stats.n_fill_events += 1
            oid = str(event.client_order_id)
            if oid not in self._filled_oids:
                self._filled_oids.add(oid)
                self.stats.n_unique_filled += 1
            if event.liquidity_side == LiquiditySide.MAKER:
                self.stats.n_maker_fills += 1
            elif event.liquidity_side == LiquiditySide.TAKER:
                self.stats.n_taker_fills += 1

        def on_order_canceled(self, event: Any) -> None:
            del event
            self.stats.n_canceled += 1

        def on_order_rejected(self, event: Any) -> None:
            del event
            self.stats.n_rejected += 1

        def on_stop(self) -> None:
            if self.instrument is not None:
                self.cancel_all_orders(self.instrument.id)

        def _requote(self) -> None:
            if self.instrument is None or self._rolling is None:
                return
            book = self.cache.order_book(self.instrument.id)
            if book is None:
                return
            bid_px = book.best_bid_price()
            ask_px = book.best_ask_price()
            bid_qty = book.best_bid_size()
            ask_qty = book.best_ask_size()
            if bid_px is None or ask_px is None or bid_qty is None or ask_qty is None:
                return
            best_bid = float(bid_px)
            best_ask = float(ask_px)
            mid = (best_bid + best_ask) / 2.0
            self.stats.last_mid = mid
            ofi = self._rolling.observe(
                self.clock.timestamp_ns(),
                best_bid,
                float(bid_qty),
                best_ask,
                float(ask_qty),
            )
            self.stats.last_ofi = ofi
            quotes: QuotePair | None
            if self.config.cross_spread:
                quotes = aggressive_quotes(best_bid=best_bid, best_ask=best_ask)
            else:
                quotes = maker_quotes(
                    mid=mid,
                    tick=self.config.tick,
                    spread_ticks=self.config.spread_ticks,
                    ofi=ofi,
                    threshold=self.config.ofi_threshold,
                    max_skew=self.config.max_skew,
                    best_bid=best_bid,
                    best_ask=best_ask,
                )
            if quotes is None:
                self.cancel_all_orders(self.instrument.id)
                return
            self._replace_side(OrderSide.BUY, quotes.bid)
            self._replace_side(OrderSide.SELL, quotes.ask)

        def _replace_side(self, side: Any, price: float) -> None:
            target = self.instrument.make_price(price)
            qty = self.instrument.make_qty(self.config.trade_size)
            kept = False
            for order in self.cache.orders_open(instrument_id=self.instrument.id):
                if order.side != side:
                    continue
                if order.price == target:
                    kept = True
                    continue
                if not order.is_pending_cancel:
                    self.cancel_order(order)
            if kept:
                return
            for order in self.cache.orders_inflight(instrument_id=self.instrument.id):
                if order.side == side:
                    return
            order = self.order_factory.limit(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=qty,
                price=target,
                time_in_force=TimeInForce.GTC,
                post_only=not self.config.cross_spread,
            )
            self.submit_order(order)
            self.stats.n_submitted += 1

    return OfiMmConfig, OfiMmStrategy
