"""Layer 1 - ingestion: real-time market data from public exchange WebSockets.

Exchange adapters live in their own modules (``binance_futures``; Bybit and OKX in
phase 2) and are imported explicitly to keep this package cheap to import.
"""

from order_flow.ingestion.base import MarketDataFeed
from order_flow.ingestion.events import (
    BookDelta,
    BookSnapshot,
    EventType,
    MarketEvent,
    PriceLevel,
    Side,
    Trade,
)

__all__ = [
    "BookDelta",
    "BookSnapshot",
    "EventType",
    "MarketDataFeed",
    "MarketEvent",
    "PriceLevel",
    "Side",
    "Trade",
]
