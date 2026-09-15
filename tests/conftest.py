"""Shared fixtures: synthetic L1 series, realistic Binance payloads and integration gating."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
import pytest

if TYPE_CHECKING:
    import numpy.typing as npt


def is_full_suite_args(args: list[str]) -> bool:
    """True when pytest was invoked without a subset path.

    ``--cov-fail-under=80`` is only meaningful on the whole package. Running one
    live file (or any other subset) covers a slice of ``order_flow`` and would
    otherwise fail after the test itself passed.
    """
    paths = [arg for arg in args if arg and not arg.startswith("-")]
    if not paths:
        return True
    return all(Path(path).name == "tests" for path in paths)


def _disable_coverage(namespace: object, plugin: object | None) -> None:
    """Subset runs must not print a 20% report or overwrite coverage.xml."""
    namespace.no_cov = True  # type: ignore[attr-defined]
    namespace.cov_fail_under = 0  # type: ignore[attr-defined]
    if plugin is None:
        return
    plugin.options.no_cov = True  # type: ignore[attr-defined]
    plugin.options.cov_fail_under = 0  # type: ignore[attr-defined]
    plugin._disabled = True  # type: ignore[attr-defined]


def pytest_load_initial_conftests(
    early_config: pytest.Config,
    args: list[str],
) -> None:
    """pytest-cov registers during this hook; disable it for subset paths."""
    if is_full_suite_args([str(arg) for arg in args]):
        return
    _disable_coverage(
        early_config.known_args_namespace,
        early_config.pluginmanager.getplugin("_cov"),
    )


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    """Disable coverage when pytest was given a file/dir subset."""
    invocation = [str(arg) for arg in config.invocation_params.args]
    collected = [str(arg) for arg in config.args]
    if is_full_suite_args(invocation) and is_full_suite_args(collected):
        return
    _disable_coverage(config.option, config.pluginmanager.getplugin("_cov"))


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
