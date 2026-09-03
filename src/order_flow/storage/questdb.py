"""QuestDB sink skeleton (phase 2). Needs the optional extra: ``uv sync --extra questdb``."""

from __future__ import annotations

from importlib.util import find_spec
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from order_flow.ingestion.events import MarketEvent

EXTRA_HINT = "QuestDB support requires the optional dependency: run `uv sync --extra questdb`"


def require_driver() -> None:
    """Raise a helpful ``ImportError`` when the ``questdb`` client is not installed."""
    if find_spec("questdb") is None:
        raise ImportError(EXTRA_HINT)


class QuestDBSink:
    """:class:`~order_flow.storage.base.EventSink` backed by QuestDB ILP (not implemented yet)."""

    def __init__(self, *, host: str = "localhost", ilp_port: int = 9009) -> None:
        require_driver()
        self.host = host
        self.ilp_port = ilp_port
        msg = "QuestDBSink is scheduled for phase 2 (see README roadmap)"
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
