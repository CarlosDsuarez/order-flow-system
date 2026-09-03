"""Layer 2 - storage: persistence of market events.

Phase 1 ships Parquet (:mod:`order_flow.storage.parquet`); ClickHouse and QuestDB sinks
are skeletons that activate with the optional extras in phase 2.
"""

from order_flow.storage.base import EventSink
from order_flow.storage.parquet import ParquetWriter, read_events, scan_events
from order_flow.storage.reconstruct import (
    L1Tick,
    LmTick,
    ReconstructionError,
    iter_l1_ticks,
    iter_lm_ticks,
    reconstruct_book,
)
from order_flow.storage.report import CaptureStats, capture_stats, detect_gaps

__all__ = [
    "CaptureStats",
    "EventSink",
    "L1Tick",
    "LmTick",
    "ParquetWriter",
    "ReconstructionError",
    "capture_stats",
    "detect_gaps",
    "iter_l1_ticks",
    "iter_lm_ticks",
    "read_events",
    "reconstruct_book",
    "scan_events",
]
