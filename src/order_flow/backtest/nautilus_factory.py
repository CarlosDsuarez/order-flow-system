"""Thin constructors that turn :mod:`conversion` objects into nautilus 1.231 types.

Import of ``nautilus_trader`` is deferred so ``uv sync`` without ``--extra backtest``
still imports this module. Unit tests inject a fake ``models`` namespace.
"""

from __future__ import annotations

import importlib.util
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from order_flow.backtest.conversion import BookOp, ConvertedDelta, ConvertedTrade
from order_flow.ingestion.events import Side

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.instruments import InstrumentSpec

VENUE = "BINANCE"


def instrument_id_for(spec: InstrumentSpec) -> str:
    """``<SYMBOL>-PERP.BINANCE``."""
    return f"{spec.symbol}-PERP.{VENUE}"


def _fixed(value: Decimal) -> str:
    """Shortest fixed-point string (``0.000010`` -> ``0.00001``) for ``from_str``."""
    return format(value.normalize(), "f")


def nautilus_available() -> bool:
    """``True`` when the optional ``backtest`` extra is installed."""
    return importlib.util.find_spec("nautilus_trader") is not None


def _models() -> Any:
    if not nautilus_available():
        msg = "nautilus_trader is required: uv sync --extra backtest"
        raise ImportError(msg)
    return _load_models()


def _load_models() -> Any:  # pragma: no cover - executed only with --extra backtest
    from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas, TradeTick
    from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
    from nautilus_trader.model.identifiers import TradeId
    from nautilus_trader.model.objects import Price, Quantity

    return SimpleNamespace(
        BookOrder=BookOrder,
        OrderBookDelta=OrderBookDelta,
        OrderBookDeltas=OrderBookDeltas,
        TradeTick=TradeTick,
        AggressorSide=AggressorSide,
        BookAction=BookAction,
        OrderSide=OrderSide,
        TradeId=TradeId,
        Price=Price,
        Quantity=Quantity,
    )


def perpetual(
    spec: InstrumentSpec,
    *,
    maker_fee: Decimal,
    taker_fee: Decimal,
    ts_event: int = 0,
    ts_init: int = 0,
) -> Any:
    """Linear USDT-margined perpetual on ``spec``'s tick / lot grid for venue BINANCE."""
    if not nautilus_available():
        msg = "nautilus_trader is required: uv sync --extra backtest"
        raise ImportError(msg)
    return _load_perpetual(
        spec,
        maker_fee=maker_fee,
        taker_fee=taker_fee,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def _load_perpetual(
    spec: InstrumentSpec,
    *,
    maker_fee: Decimal,
    taker_fee: Decimal,
    ts_event: int,
    ts_init: int,
) -> Any:  # pragma: no cover - executed only with --extra backtest
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CryptoPerpetual
    from nautilus_trader.model.objects import Currency, Money, Price, Quantity

    # Unknown codes (DOGE, 1000PEPE, ...) become crypto currencies with precision 8.
    base = Currency.from_str(spec.base_asset, strict=False)
    quote = Currency.from_str(spec.quote_asset, strict=False)
    price_prec, size_prec = spec.price_precision, spec.size_precision
    return CryptoPerpetual(
        instrument_id=InstrumentId(symbol=Symbol(f"{spec.symbol}-PERP"), venue=Venue(VENUE)),
        raw_symbol=Symbol(spec.symbol),
        base_currency=base,
        quote_currency=quote,
        settlement_currency=quote,
        is_inverse=False,
        price_precision=price_prec,
        size_precision=size_prec,
        price_increment=Price.from_str(_fixed(spec.tick_size)),
        size_increment=Quantity.from_str(_fixed(spec.lot_size)),
        max_quantity=Quantity(float(spec.max_qty), size_prec),
        min_quantity=Quantity(float(spec.min_qty), size_prec),
        max_notional=None,
        min_notional=Money(float(spec.min_notional), quote),
        max_price=Price(float(spec.max_price), price_prec),
        min_price=Price(float(spec.min_price), price_prec),
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.025"),
        maker_fee=maker_fee,
        taker_fee=taker_fee,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def to_order_book_deltas(
    ops: Sequence[ConvertedDelta],
    instrument_id: Any,
    *,
    price_precision: int,
    size_precision: int,
    models: Any | None = None,
) -> Any:
    """Materialize one nautilus ``OrderBookDeltas`` batch from converted ops."""
    if not ops:
        msg = "ops must be non-empty"
        raise ValueError(msg)
    m = models if models is not None else _models()
    action_map = {
        BookOp.ADD: m.BookAction.ADD,
        BookOp.UPDATE: m.BookAction.UPDATE,
        BookOp.DELETE: m.BookAction.DELETE,
        BookOp.CLEAR: m.BookAction.CLEAR,
    }
    items: list[Any] = []
    for op in ops:
        if op.action is BookOp.CLEAR:
            items.append(
                m.OrderBookDelta.clear(instrument_id, op.sequence, op.ts_event_ns, op.ts_init_ns)
            )
            continue
        if op.order is None:
            continue  # non-CLEAR without a payload is dropped
        side = m.OrderSide.BUY if op.order.side == "bid" else m.OrderSide.SELL
        order = m.BookOrder(
            side,
            m.Price(op.order.price, price_precision),
            m.Quantity(op.order.size, size_precision),
            op.order.order_id,
        )
        items.append(
            m.OrderBookDelta(
                instrument_id,
                action_map[op.action],
                order,
                op.flags,
                op.sequence,
                op.ts_event_ns,
                op.ts_init_ns,
            )
        )
    if not items:
        msg = "ops produced no deltas"
        raise ValueError(msg)
    return m.OrderBookDeltas(instrument_id, items)


def to_trade_tick(
    trade: ConvertedTrade,
    instrument_id: Any,
    *,
    price_precision: int,
    size_precision: int,
    models: Any | None = None,
) -> Any:
    """Materialize one nautilus ``TradeTick``."""
    m = models if models is not None else _models()
    aggressor = m.AggressorSide.BUYER if trade.aggressor is Side.BUY else m.AggressorSide.SELLER
    return m.TradeTick(
        instrument_id,
        m.Price(trade.price, price_precision),
        m.Quantity(trade.qty, size_precision),
        aggressor,
        m.TradeId(str(trade.trade_id)),
        trade.ts_event_ns,
        trade.ts_init_ns,
    )
