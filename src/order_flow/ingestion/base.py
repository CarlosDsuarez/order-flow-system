"""Feed abstraction implemented by every exchange adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from order_flow.ingestion.events import MarketEvent


@runtime_checkable
class MarketDataFeed(Protocol):
    """Asynchronous source of market-data events for one instrument.

    ``stream`` is declared as a regular method returning an async iterator so that
    implementations written as async generators (``async def stream(self): ... yield``)
    satisfy the protocol; that is the intended implementation style.
    """

    exchange: str
    symbol: str

    def stream(self) -> AsyncIterator[MarketEvent]:
        """Yield events in arrival order until the connection closes or the caller stops."""
        ...
