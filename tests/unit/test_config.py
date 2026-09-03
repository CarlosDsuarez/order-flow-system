"""Settings read environment variables, keep secrets masked and cache the singleton."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import SecretStr

from order_flow.utils.config import Settings, get_settings

if TYPE_CHECKING:
    from collections.abc import Iterator

ENV_VARS = (
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BYBIT_API_KEY",
    "OKX_PASSPHRASE",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_PASSWORD",
    "QUESTDB_ILP_PORT",
    "DATA_DIR",
    "LOG_LEVEL",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.binance_api_key is None
    assert settings.okx_passphrase is None
    assert settings.clickhouse_host == "localhost"
    assert settings.clickhouse_port == 8123
    assert settings.clickhouse_password is None
    assert settings.questdb_ilp_port == 9009
    assert settings.questdb_pg_port == 8812
    assert settings.data_dir == Path("data")
    assert settings.log_level == "INFO"


def test_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "key-123")
    monkeypatch.setenv("BINANCE_API_SECRET", "secret-456")
    monkeypatch.setenv("OKX_PASSPHRASE", "hunter2")
    monkeypatch.setenv("CLICKHOUSE_PORT", "9000")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "pw")
    monkeypatch.setenv("DATA_DIR", "/srv/order-flow/data")
    monkeypatch.setenv("LOG_LEVEL", "debug")

    settings = Settings(_env_file=None)
    assert isinstance(settings.binance_api_key, SecretStr)
    assert settings.binance_api_key.get_secret_value() == "key-123"
    assert "key-123" not in repr(settings)
    assert str(settings.binance_api_key) == "**********"
    assert isinstance(settings.binance_api_secret, SecretStr)
    assert settings.binance_api_secret.get_secret_value() == "secret-456"
    assert isinstance(settings.okx_passphrase, SecretStr)
    assert isinstance(settings.clickhouse_password, SecretStr)
    assert settings.clickhouse_port == 9000
    assert settings.data_dir == Path("/srv/order-flow/data")
    assert settings.log_level == "debug"


def test_unknown_variables_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOMETHING_UNRELATED", "1")
    assert Settings(_env_file=None).log_level == "INFO"


def test_get_settings_is_cached() -> None:
    first = get_settings()
    assert first is get_settings()
    get_settings.cache_clear()
    assert get_settings() is not first
