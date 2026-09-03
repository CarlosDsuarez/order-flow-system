"""Order flow analysis toolkit for crypto perpetual futures.

Open-source, public-data-only research stack organised in four layers:

1. ``order_flow.ingestion`` - real-time L2/L3 capture over public WebSockets.
2. ``order_flow.storage``   - Parquet (phase 1), ClickHouse / QuestDB (phase 2).
3. ``order_flow.metrics``   - microstructure signals: OFI, MLOFI, VPIN, CVD.
4. ``order_flow.backtest``  - domain types plus ``nautilus_trader`` (extra
   ``backtest``) and ``hftbacktest`` (extra ``hftbacktest``) adapters.

``order_flow.orderbook`` reconstructs the limit order book that feeds layer 3, and
``order_flow.utils`` holds configuration, logging and time helpers.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
