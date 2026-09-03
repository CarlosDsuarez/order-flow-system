"""ClickHouse sink skeleton (phase 2). Needs the optional extra: ``uv sync --extra clickhouse``."""

from __future__ import annotations

from importlib.util import find_spec
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.events import MarketEvent

EXTRA_HINT = "ClickHouse support requires the optional dependency: run `uv sync --extra clickhouse`"


def require_driver() -> None:
    """Raise a helpful ``ImportError`` when ``clickhouse_connect`` is not installed."""
    if find_spec("clickhouse_connect") is None:
        raise ImportError(EXTRA_HINT)


class ClickHouseSink:
    """:class:`~order_flow.storage.base.EventSink` backed by ClickHouse (not implemented yet)."""

    def __init__(
        self, *, host: str = "localhost", port: int = 8123, database: str = "order_flow"
    ) -> None:
        require_driver()
        self.host = host
        self.port = port
        self.database = database
        msg = "ClickHouseSink is scheduled for phase 2 (see README roadmap)"
        raise NotImplementedError(msg)

    def write(self, events: Sequence[MarketEvent]) -> None:
        """Not implemented in phase 1."""
        raise NotImplementedError

    def flush(self) -> None:
        """Not implemented in phase 1."""
        raise NotImplementedError

    def close(self) -> None:
        """Not implemented in phase 1."""
        raise NotImplementedError
