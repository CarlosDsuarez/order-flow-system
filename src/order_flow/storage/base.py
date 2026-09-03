"""Sink abstraction shared by every storage backend."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.events import MarketEvent


@runtime_checkable
class EventSink(Protocol):
    """Destination for market events. Implementations may buffer until :meth:`flush`."""

    def write(self, events: Sequence[MarketEvent]) -> None:
        """Accept a batch of events (may buffer)."""
        ...

    def flush(self) -> None:
        """Persist everything buffered so far."""
        ...

    def close(self) -> None:
        """Flush and release resources."""
        ...
