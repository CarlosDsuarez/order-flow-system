"""InstrumentSpec: per-symbol tick / lot grid persisted with each capture."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from order_flow.ingestion.instruments import (
    LEGACY_BTCUSDT_SPEC,
    InstrumentSpec,
    read_instrument_spec,
    resolve_capture_spec,
    write_instrument_spec,
)

if TYPE_CHECKING:
    from pathlib import Path


def pepe() -> InstrumentSpec:
    """1000PEPEUSDT as published by ``GET /fapi/v1/exchangeInfo`` (2026-09-26)."""
    return InstrumentSpec(
        exchange="binance_futures",
        symbol="1000PEPEUSDT",
        base_asset="1000PEPE",
        quote_asset="USDT",
        tick_size=Decimal("0.0000001"),
        lot_size=Decimal("1"),
        min_price=Decimal("0.0000001"),
        max_price=Decimal("200"),
        min_qty=Decimal("1"),
        max_qty=Decimal("800000000"),
        min_notional=Decimal("5"),
    )


def with_grid(tick: str, lot: str) -> InstrumentSpec:
    return InstrumentSpec.from_dict({**pepe().to_dict(), "tick_size": tick, "lot_size": lot})


@pytest.mark.parametrize(
    ("tick", "lot", "price_precision", "size_precision"),
    [
        ("0.10", "0.001", 1, 3),  # Binance pads BTCUSDT's tick as "0.10"
        ("0.0000001", "1", 7, 0),
        ("0.000010", "1", 5, 0),
        ("10", "0.1", 0, 1),
    ],
)
def test_precisions_follow_the_normalized_grid(
    tick: str, lot: str, price_precision: int, size_precision: int
) -> None:
    spec = with_grid(tick, lot)
    assert spec.price_precision == price_precision
    assert spec.size_precision == size_precision


def test_float_views_match_the_decimal_grid() -> None:
    spec = pepe()
    assert spec.tick == 1e-7
    assert spec.lot == 1.0


def test_rejects_non_positive_grid() -> None:
    with pytest.raises(ValueError, match="tick_size"):
        with_grid("0", "1")
    with pytest.raises(ValueError, match="lot_size"):
        with_grid("0.1", "-1")


def test_dict_round_trip_keeps_exact_decimals() -> None:
    spec = pepe()
    payload = spec.to_dict()
    assert payload["tick_size"] == "0.0000001"
    assert InstrumentSpec.from_dict(payload) == spec


def test_capture_round_trip(tmp_path: Path) -> None:
    write_instrument_spec(tmp_path, pepe())
    assert read_instrument_spec(tmp_path) == pepe()
    assert read_instrument_spec(tmp_path / "missing") is None


def test_resolve_prefers_the_captured_spec(tmp_path: Path) -> None:
    write_instrument_spec(tmp_path, pepe())
    assert resolve_capture_spec(tmp_path, "1000PEPEUSDT") == pepe()


def test_resolve_rejects_a_spec_for_another_symbol(tmp_path: Path) -> None:
    write_instrument_spec(tmp_path, pepe())
    with pytest.raises(ValueError, match="DOGEUSDT"):
        resolve_capture_spec(tmp_path, "DOGEUSDT")


def test_resolve_falls_back_to_legacy_btcusdt_for_old_captures(tmp_path: Path) -> None:
    # Captures recorded before instrument.json existed are all BTCUSDT; the legacy spec
    # is exactly the grid the backtests hardcoded, so old results stay reproducible.
    spec = resolve_capture_spec(tmp_path, "btcusdt")
    assert spec is LEGACY_BTCUSDT_SPEC
    assert (spec.tick_size, spec.lot_size) == (Decimal("0.1"), Decimal("0.001"))
    assert (spec.price_precision, spec.size_precision) == (1, 3)


def test_resolve_refuses_to_guess_other_symbols(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"instrument\.json"):
        resolve_capture_spec(tmp_path, "DOGEUSDT")
