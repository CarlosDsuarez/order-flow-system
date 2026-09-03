"""Unit tests for live-report formatting (no network)."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from order_flow.ingestion.live import format_live_report_md, live_duration_s, write_live_report

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_live_duration_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BINANCE_LIVE_SECONDS", raising=False)
    assert live_duration_s() == 60.0
    monkeypatch.setenv("BINANCE_LIVE_SECONDS", "15")
    assert live_duration_s() == 15.0


def test_format_and_write_report(tmp_path: Path) -> None:
    report = {
        "date_utc": "2026-09-02 12:00:00Z",
        "symbol": "BTCUSDT",
        "duration_s_requested": 60,
        "duration_s_elapsed": 60.2,
        "gaps": 0,
        "resyncs": 0,
        "reconnects": 0,
        "rest_429s": 0,
        "snapshots_applied": 1,
        "deltas_applied": 400,
        "trades": 50,
        "latency_ns": {
            "count": 451.0,
            "mean": 12_000_000.0,
            "p50": 10_000_000.0,
            "p99": 40_000_000.0,
            "min": 1_000_000.0,
            "max": 50_000_000.0,
        },
        "book_crossed": False,
        "n_levels": (500, 500),
        "honesty_levels": 20,
        "honesty": {
            "compared": 40,
            "matches": 38,
            "mismatches": 2,
            "max_qty_discrepancy": 0.5,
            "last_update_id_local": 10,
            "last_update_id_rest": 9,
        },
        "error": None,
    }
    text = format_live_report_md(report)
    assert "BTCUSDT" in text
    assert "400" in text
    assert "lo bastante honesto" in text
    path = tmp_path / "live-validation.md"
    write_live_report(report, path)
    assert path.read_text(encoding="utf-8") == text


def test_format_report_crossed_and_high_mismatch() -> None:
    crossed = format_live_report_md(
        {"deltas_applied": 10, "book_crossed": True, "honesty": {"compared": 10, "mismatches": 0}}
    )
    assert "cruzado" in crossed
    high = format_live_report_md(
        {
            "deltas_applied": 10,
            "book_crossed": False,
            "honesty": {"compared": 10, "mismatches": 8, "matches": 2},
        }
    )
    assert "Demasiados mismatches" in high
    gapped = format_live_report_md(
        {
            "deltas_applied": 10,
            "book_crossed": False,
            "gaps": 2,
            "honesty": {"compared": 10, "mismatches": 0, "matches": 10},
        }
    )
    assert "resincronizó 2" in gapped
    empty = format_live_report_md({"deltas_applied": 0, "honesty": {}})
    assert "No se aplicaron deltas" in empty
    text = format_live_report_md(
        {
            "symbol": "BTCUSDT",
            "error": "ConnectError: blocked",
            "latency_ns": {"count": 0, "mean": math.nan},
            "honesty": None,
            "deltas_applied": 0,
        }
    )
    assert "no pudo completarse" in text
    assert "ConnectError" in text
