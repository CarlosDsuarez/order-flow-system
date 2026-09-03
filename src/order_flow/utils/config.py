"""Application settings loaded from environment variables and an optional ``.env`` file."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration.

    Values are read from environment variables (case-insensitive) and, if present,
    from a ``.env`` file in the current working directory. Unknown variables are
    ignored so a shared ``.env`` can hold unrelated entries.

    Public market-data WebSocket streams need **no** credentials; the exchange keys
    below are only relevant for private endpoints (execution), which are out of
    scope for phase 1.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Exchange credentials (optional, unused by public streams).
    binance_api_key: SecretStr | None = None
    binance_api_secret: SecretStr | None = None
    bybit_api_key: SecretStr | None = None
    bybit_api_secret: SecretStr | None = None
    okx_api_key: SecretStr | None = None
    okx_api_secret: SecretStr | None = None
    okx_passphrase: SecretStr | None = None

    # ClickHouse (optional extra ``clickhouse``).
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: SecretStr | None = None
    clickhouse_database: str = "order_flow"

    # QuestDB (optional extra ``questdb``).
    questdb_host: str = "localhost"
    questdb_ilp_port: int = 9009
    questdb_pg_port: int = 8812

    # Local paths and logging.
    data_dir: Path = Path("data")
    log_level: str = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings` instance (constructed once)."""
    return Settings()
