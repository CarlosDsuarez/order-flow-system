"""Per-symbol price / quantity grid (tick, lot, bounds) recorded with every capture.

Prices stay ``float`` in events, the book and Parquet: parsing the exchange's decimal
string is deterministic, so the book is exact. What breaks on altcoins is anything that
assumes BTCUSDT's grid (tick 0.1, lot 0.001): nautilus precisions, hftbacktest tick/lot,
order sizes. :class:`InstrumentSpec` carries the real grid; fields are :class:`Decimal`
so the exchange's strings round-trip exactly.

Exchanges change tick sizes over time, so the spec is written next to the Parquet at
capture time (``instrument.json``) instead of being looked up when a backtest runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, fields
from decimal import Decimal
from pathlib import Path
from typing import Final

INSTRUMENT_FILE: Final = "instrument.json"

_DECIMAL_FIELDS: Final = (
    "tick_size",
    "lot_size",
    "min_price",
    "max_price",
    "min_qty",
    "max_qty",
    "min_notional",
)


def _decimals(value: Decimal) -> int:
    """Digits after the point once trailing zeros are dropped (``0.10`` -> 1, ``10`` -> 0)."""
    exponent = value.normalize().as_tuple().exponent  # finite: see __post_init__
    return max(0, -int(exponent))


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Trading grid of one instrument as published by the exchange."""

    exchange: str
    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    lot_size: Decimal
    min_price: Decimal
    max_price: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal

    def __post_init__(self) -> None:
        for name in _DECIMAL_FIELDS:
            if not getattr(self, name).is_finite():
                msg = f"{name} must be finite"
                raise ValueError(msg)
        for name in ("tick_size", "lot_size"):
            if getattr(self, name) <= 0:
                msg = f"{name} must be > 0, got {getattr(self, name)}"
                raise ValueError(msg)

    @property
    def price_precision(self) -> int:
        """Decimal places of the tick grid (nautilus ``price_precision``)."""
        return _decimals(self.tick_size)

    @property
    def size_precision(self) -> int:
        """Decimal places of the lot grid (nautilus ``size_precision``)."""
        return _decimals(self.lot_size)

    @property
    def tick(self) -> float:
        return float(self.tick_size)

    @property
    def lot(self) -> float:
        return float(self.lot_size)

    def to_dict(self) -> dict[str, str]:
        """JSON-safe form; decimals as fixed-point strings (never ``1E-7``)."""
        out: dict[str, str] = {}
        for item in fields(self):
            value = getattr(self, item.name)
            out[item.name] = format(value, "f") if isinstance(value, Decimal) else value
        return out

    @classmethod
    def from_dict(cls, payload: dict[str, str]) -> InstrumentSpec:
        return cls(
            exchange=payload["exchange"],
            symbol=payload["symbol"],
            base_asset=payload["base_asset"],
            quote_asset=payload["quote_asset"],
            **{name: Decimal(payload[name]) for name in _DECIMAL_FIELDS},
        )


#: BTCUSDT grid the backtests hardcoded before specs were recorded. Every capture made
#: without ``instrument.json`` is BTCUSDT, so this keeps old results reproducible.
LEGACY_BTCUSDT_SPEC: Final = InstrumentSpec(
    exchange="binance_futures",
    symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    tick_size=Decimal("0.1"),
    lot_size=Decimal("0.001"),
    min_price=Decimal("0.1"),
    max_price=Decimal("1000000.0"),
    min_qty=Decimal("0.001"),
    max_qty=Decimal("1000"),
    min_notional=Decimal("10"),
)


def write_instrument_spec(root: Path, spec: InstrumentSpec) -> Path:
    """Write ``<root>/instrument.json``."""
    path = Path(root) / INSTRUMENT_FILE
    path.write_text(json.dumps(spec.to_dict(), indent=2) + "\n", encoding="utf-8")
    return path


def read_instrument_spec(root: Path) -> InstrumentSpec | None:
    """Spec recorded with the capture at ``root``, or ``None`` if there is none."""
    path = Path(root) / INSTRUMENT_FILE
    if not path.is_file():
        return None
    return InstrumentSpec.from_dict(json.loads(path.read_text(encoding="utf-8")))


def resolve_capture_spec(root: Path, symbol: str) -> InstrumentSpec:
    """Spec to replay ``symbol`` from the capture at ``root``.

    Raises:
        ValueError: The recorded spec belongs to another symbol.
        FileNotFoundError: No recorded spec and ``symbol`` is not legacy BTCUSDT.
    """
    wanted = symbol.upper()
    spec = read_instrument_spec(root)
    if spec is not None:
        if spec.symbol != wanted:
            msg = f"{Path(root) / INSTRUMENT_FILE} is for {spec.symbol}, not {wanted}"
            raise ValueError(msg)
        return spec
    if wanted == LEGACY_BTCUSDT_SPEC.symbol:
        return LEGACY_BTCUSDT_SPEC
    msg = (
        f"{Path(root) / INSTRUMENT_FILE} not found: {wanted} captures need the "
        "instrument spec that scripts/record_l2.py records"
    )
    raise FileNotFoundError(msg)
