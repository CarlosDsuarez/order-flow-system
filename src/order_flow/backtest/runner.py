"""Run the OFI-skewed MM strategy inside nautilus 1.231 ``BacktestEngine``.

Lazy-imports nautilus so ``uv sync`` without ``--extra backtest`` still imports
this module. Simulated fills never edit the historical book; see
``docs/backtest_limitations.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from order_flow.backtest.conversion import capture_to_ops
from order_flow.backtest.nautilus_factory import (
    INSTRUMENT_ID,
    PRICE_PRECISION,
    SIZE_PRECISION,
    TICK_SIZE,
    btcusdt_perp,
    to_order_book_deltas,
    to_trade_tick,
)
from order_flow.backtest.ofi_mm import OfiMmStats, load_ofi_mm
from order_flow.ingestion.binance_futures import EXCHANGE as DEFAULT_EXCHANGE

if TYPE_CHECKING:
    from pathlib import Path

STARTING_USDT = 100_000.0
DEFAULT_LEVERAGE = Decimal("1")
TICK = float(TICK_SIZE)


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """PnL and fill counters from one engine run."""

    nautilus_version: str
    capture: str
    exchange: str
    symbol: str
    instrument_id: str
    duration_ns: int
    n_book_batches: int
    n_public_trades: int
    maker_fee: float
    taker_fee: float
    spread_ticks: int
    ofi_threshold: float
    ofi_window_ns: int
    trade_size: float
    max_skew: int
    cross_spread: bool
    starting_balance: float
    ending_balance: float
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


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if hasattr(value, "as_double"):
        return float(value.as_double())
    return float(value)


def _sum_commissions(account: Any) -> float:
    commissions = account.commissions()
    if isinstance(commissions, dict):
        return sum(_as_float(item) for item in commissions.values())
    return sum(_as_float(item) for item in commissions)


def materialize_capture(
    root: Path,
    instrument_id: Any,
    *,
    exchange: str,
    symbol: str,
    tick: float = TICK,
) -> tuple[list[Any], list[Any]]:
    """Convert a hive capture into nautilus ``OrderBookDeltas`` + ``TradeTick`` lists."""
    batches, trades = capture_to_ops(root, exchange=exchange, symbol=symbol, tick=tick)
    books = [
        to_order_book_deltas(
            ops,
            instrument_id,
            price_precision=PRICE_PRECISION,
            size_precision=SIZE_PRECISION,
        )
        for ops in batches
    ]
    ticks = [
        to_trade_tick(
            trade,
            instrument_id,
            price_precision=PRICE_PRECISION,
            size_precision=SIZE_PRECISION,
        )
        for trade in trades
    ]
    return books, ticks


def run_ofi_mm_backtest(
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
    starting_usdt: float = STARTING_USDT,
    leverage: Decimal = DEFAULT_LEVERAGE,
) -> BacktestResult:
    """Load ``root``, run one engine, dispose it, return counters.

    Venue is L2_MBP with ``trade_execution``, ``queue_position`` and
    ``liquidity_consumption`` on. No ``latency_model``. Risk engine is bypassed
    so cancel/replace on every depth update is not rate-limited.
    """
    import nautilus_trader
    from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.backtest.models import MakerTakerFeeModel
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.model.currencies import USDT
    from nautilus_trader.model.enums import AccountType, BookType, OmsType
    from nautilus_trader.model.identifiers import Venue
    from nautilus_trader.model.objects import Money
    from nautilus_trader.risk.config import RiskEngineConfig

    instrument = btcusdt_perp(maker_fee=Decimal(str(maker_fee)), taker_fee=Decimal(str(taker_fee)))
    books, ticks = materialize_capture(
        root, instrument.id, exchange=exchange, symbol=symbol.upper()
    )
    if not books:
        msg = f"no book data in {root}"
        raise ValueError(msg)
    print(
        f"loaded {len(books)} book batches and {len(ticks)} trades from {root}",
        flush=True,
    )

    ts_first = int(books[0].ts_event)
    ts_last = int(books[-1].ts_event)
    if ticks:
        ts_last = max(ts_last, int(ticks[-1].ts_event))
        ts_first = min(ts_first, int(ticks[0].ts_event))

    config = BacktestEngineConfig(
        trader_id="BACKTEST-001",
        logging=LoggingConfig(bypass_logging=True),
        run_analysis=False,
        risk_engine=RiskEngineConfig(bypass=True),
    )
    engine = BacktestEngine(config=config)
    ofi_config_cls, ofi_strategy_cls = load_ofi_mm()
    strategy = ofi_strategy_cls(
        ofi_config_cls(
            instrument_id=instrument.id,
            trade_size=trade_size,
            spread_ticks=spread_ticks,
            ofi_threshold=ofi_threshold,
            ofi_window_ns=ofi_window_ns,
            max_skew=max_skew,
            tick=TICK,
            cross_spread=cross_spread,
        )
    )
    try:
        engine.add_venue(
            venue=Venue("BINANCE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money(starting_usdt, USDT)],
            base_currency=USDT,
            default_leverage=leverage,
            fee_model=MakerTakerFeeModel(),
            book_type=BookType.L2_MBP,
            trade_execution=True,
            queue_position=True,
            liquidity_consumption=True,
        )
        engine.add_instrument(instrument)
        engine.add_data(books, sort=False)
        if ticks:
            engine.add_data(ticks, sort=False)
        engine.sort_data()
        engine.add_strategy(strategy)
        engine.run()

        stats: OfiMmStats = strategy.stats
        instrument_id = instrument.id
        realized = _as_float(engine.portfolio.realized_pnl(instrument_id))
        unrealized = _as_float(engine.portfolio.unrealized_pnl(instrument_id))
        total = _as_float(engine.portfolio.total_pnl(instrument_id))
        account = engine.portfolio.account(Venue("BINANCE"))
        ending = _as_float(account.balance_total(USDT))
        fees = _sum_commissions(account)
        submitted = stats.n_submitted
        fill_rate = (stats.n_unique_filled / submitted) if submitted else 0.0
        return BacktestResult(
            nautilus_version=str(nautilus_trader.__version__),
            capture=str(root),
            exchange=exchange,
            symbol=symbol.upper(),
            instrument_id=INSTRUMENT_ID,
            duration_ns=max(0, ts_last - ts_first),
            n_book_batches=len(books),
            n_public_trades=len(ticks),
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            spread_ticks=spread_ticks,
            ofi_threshold=ofi_threshold,
            ofi_window_ns=ofi_window_ns,
            trade_size=trade_size,
            max_skew=max_skew,
            cross_spread=cross_spread,
            starting_balance=starting_usdt,
            ending_balance=ending,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            total_pnl=total,
            fees=fees,
            n_submitted=submitted,
            n_canceled=stats.n_canceled,
            n_rejected=stats.n_rejected,
            n_fill_events=stats.n_fill_events,
            n_unique_filled=stats.n_unique_filled,
            n_maker_fills=stats.n_maker_fills,
            n_taker_fills=stats.n_taker_fills,
            fill_rate=fill_rate,
            last_mid=stats.last_mid,
            last_ofi=stats.last_ofi,
        )
    finally:
        engine.dispose()
