"""Capture-report helpers: rates, file sizes, event-time / recv-time gaps."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.ingestion.events import Side
from order_flow.storage.parquet import ParquetWriter
from order_flow.storage.report import (
    DEFAULT_GAP_NS,
    capture_stats,
    detect_gaps,
    format_capture_report,
    updates_per_second_histogram,
)
from tests.helpers import EXCHANGE, SYMBOL, T0_NS, make_delta, make_snapshot, make_trade

if TYPE_CHECKING:
    from pathlib import Path

NS_PER_MS = 1_000_000


def _write_gappy_capture(root: Path) -> None:
    snap = make_snapshot(last_update_id=100, ts_event_ns=T0_NS)
    # Contiguous 100 ms, then a 500 ms event-time hole (gap vs 200 ms budget).
    d1 = make_delta(101, 102, 100, ts_event_ns=T0_NS + 100 * NS_PER_MS)
    d2 = make_delta(103, 104, 102, ts_event_ns=T0_NS + 200 * NS_PER_MS)
    d3 = make_delta(105, 106, 104, ts_event_ns=T0_NS + 700 * NS_PER_MS)
    trade = make_trade(1, 100.5, 0.1, Side.BUY, ts_event_ns=T0_NS + 150 * NS_PER_MS)
    with ParquetWriter(root, EXCHANGE, SYMBOL) as writer:
        writer.write([snap, d1, d2, d3, trade])


def test_detect_gaps_flags_event_time_hole(tmp_path: Path) -> None:
    _write_gappy_capture(tmp_path)
    gaps = detect_gaps(tmp_path, exchange=EXCHANGE, symbol=SYMBOL, threshold_ns=DEFAULT_GAP_NS)
    event_gaps = [g for g in gaps if g.clock == "event"]
    assert len(event_gaps) == 1
    assert event_gaps[0].gap_ns == 500 * NS_PER_MS
    assert event_gaps[0].prev_ts_ns == T0_NS + 200 * NS_PER_MS
    assert event_gaps[0].ts_ns == T0_NS + 700 * NS_PER_MS


def test_capture_stats_rates_and_sizes(tmp_path: Path) -> None:
    _write_gappy_capture(tmp_path)
    stats = capture_stats(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert stats.n_snapshots == 1
    assert stats.n_deltas == 3
    assert stats.n_trades == 1
    assert stats.duration_ns == 700 * NS_PER_MS
    assert stats.deltas_per_s == pytest.approx(3 / 0.7)
    assert stats.trades_per_s == pytest.approx(1 / 0.7)
    assert stats.bytes_total > 0
    assert stats.bytes_snapshots > 0
    assert stats.bytes_deltas > 0
    assert stats.bytes_trades > 0
    assert stats.n_event_gaps == 1


def test_format_capture_report_mentions_symbol(tmp_path: Path) -> None:
    _write_gappy_capture(tmp_path)
    stats = capture_stats(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    text = format_capture_report(stats)
    assert "BTCUSDT" in text
    assert "delta" in text.lower()


def test_detect_gaps_empty_capture(tmp_path: Path) -> None:
    assert detect_gaps(tmp_path, exchange=EXCHANGE, symbol=SYMBOL) == []
    stats = capture_stats(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert stats.n_deltas == 0
    assert stats.deltas_per_s == 0.0
    hist = updates_per_second_histogram(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert hist.height == 0


def test_updates_per_second_histogram_counts_seconds(tmp_path: Path) -> None:
    _write_gappy_capture(tmp_path)
    hist = updates_per_second_histogram(tmp_path, exchange=EXCHANGE, symbol=SYMBOL)
    assert hist.height >= 1
    assert int(hist["n"].sum()) == 3
