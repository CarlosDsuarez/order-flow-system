"""Shared fixtures: synthetic L1 series, realistic Binance payloads and integration gating."""

from __future__ import annotations

import importlib.util
import os
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
import pytest

if TYPE_CHECKING:
    import numpy.typing as npt


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip gated markers unless the matching env var / extra is present."""
    if os.environ.get("RUN_INTEGRATION") != "1":
        skip_integration = pytest.mark.skip(
            reason="integration tests need RUN_INTEGRATION=1 (network access)"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    has_nautilus = importlib.util.find_spec("nautilus_trader") is not None
    if os.environ.get("RUN_NAUTILUS") != "1" and not has_nautilus:
        skip_nautilus = pytest.mark.skip(
            reason="nautilus tests need `uv sync --extra backtest` or RUN_NAUTILUS=1"
        )
        for item in items:
            if "nautilus" in item.keywords:
                item.add_marker(skip_nautilus)

    has_hftbacktest = importlib.util.find_spec("hftbacktest") is not None
    if os.environ.get("RUN_HFTBACKTEST") != "1" and not has_hftbacktest:
        skip_hft = pytest.mark.skip(
            reason="hftbacktest tests need `uv sync --extra hftbacktest` or RUN_HFTBACKTEST=1"
        )
        for item in items:
            if "hftbacktest" in item.keywords:
                item.add_marker(skip_hft)


class L1Series(NamedTuple):
    """Top-of-book series with hand-computed OFI expectations."""

    bid_px: npt.NDArray[np.float64]
    bid_qty: npt.NDArray[np.float64]
    ask_px: npt.NDArray[np.float64]
    ask_qty: npt.NDArray[np.float64]
    expected_ofi_events: npt.NDArray[np.float64]


@pytest.fixture
def l1_series() -> L1Series:
    """Four L1 states.

    n=1: bid qty 10->12 at same price (+2), ask unchanged (0)          -> e = 2
    n=2: bid price up with qty 5 (+5), ask qty 8->6 at same price (+2) -> e = 7
    n=3: bid unchanged (0), ask price down with qty 4 (-4)             -> e = -4
    """
    return L1Series(
        bid_px=np.array([100.0, 100.0, 100.5, 100.5]),
        bid_qty=np.array([10.0, 12.0, 5.0, 5.0]),
        ask_px=np.array([101.0, 101.0, 101.0, 100.8]),
        ask_qty=np.array([8.0, 8.0, 6.0, 4.0]),
        expected_ofi_events=np.array([2.0, 7.0, -4.0]),
    )


@pytest.fixture
def depth_update_msg() -> dict[str, Any]:
    """Realistic ``<symbol>@depth@100ms`` payload (USD-M futures)."""
    return {
        "e": "depthUpdate",
        "E": 1725235200123,
        "T": 1725235200120,
        "s": "BTCUSDT",
        "U": 1027025,
        "u": 1027030,
        "pu": 1027024,
        "b": [["60000.10", "1.250"], ["59999.90", "0"]],
        "a": [["60000.20", "0.800"]],
    }


@pytest.fixture
def agg_trade_msg() -> dict[str, Any]:
    """Realistic ``<symbol>@aggTrade`` payload where the buyer is the maker."""
    return {
        "e": "aggTrade",
        "E": 1725235200456,
        "s": "BTCUSDT",
        "a": 5933014,
        "p": "60000.20",
        "q": "0.005",
        "f": 100,
        "l": 105,
        "T": 1725235200450,
        "m": True,
    }


@pytest.fixture
def depth_snapshot_payload() -> dict[str, Any]:
    """Realistic ``GET /fapi/v1/depth`` response."""
    return {
        "lastUpdateId": 1027024,
        "E": 1725235200000,
        "T": 1725235199990,
        "bids": [["60000.00", "2.000"], ["59999.90", "1.500"]],
        "asks": [["60000.10", "1.000"], ["60000.20", "0.500"]],
    }
