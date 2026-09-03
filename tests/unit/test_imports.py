"""The package, every subpackage and every module import without errors."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import pytest

import order_flow

SUBPACKAGES = ("ingestion", "orderbook", "storage", "metrics", "backtest", "utils")


def test_version() -> None:
    assert order_flow.__version__ == "0.1.0"


@pytest.mark.parametrize("name", SUBPACKAGES)
def test_subpackage_imports(name: str) -> None:
    module = importlib.import_module(f"order_flow.{name}")
    assert module.__name__ == f"order_flow.{name}"


def test_every_module_imports() -> None:
    package_dir = Path(order_flow.__file__).parent
    names = [info.name for info in pkgutil.walk_packages([str(package_dir)], prefix="order_flow.")]
    assert "order_flow.ingestion.binance_futures" in names
    assert "order_flow.metrics.vpin" in names
    for name in names:
        importlib.import_module(name)
