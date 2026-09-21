"""Layer 2 - storage: persistence of market events.

Phase 1 ships Parquet (:mod:`order_flow.storage.parquet`); ClickHouse and QuestDB sinks
are skeletons that activate with the optional extras in phase 2.
"""

from order_flow.storage.base import EventSink
from order_flow.storage.parquet import ParquetWriter, read_events, scan_events
from order_flow.storage.probe_audit import run_probe_audit
from order_flow.storage.reconstruct import (
    AlignedBook,
    L1Tick,
    LmTick,
    ReconstructionError,
    iter_l1_ticks,
    iter_lm_ticks,
    reconstruct_book,
    reconstruct_book_at_update_id,
)
from order_flow.storage.report import CaptureStats, capture_stats, detect_gaps

__all__ = [
    "AlignedBook",
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
    "reconstruct_book_at_update_id",
    "run_probe_audit",
    "scan_events",
]
