"""Unit tests for live-report formatting + honesty verdicts (no network)."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from order_flow.ingestion.live import (
    HONESTY_MAX_SLACK_BATCHES,
    HONESTY_MISMATCH_WARN,
    MAX_HONESTY_QTY_DISCREPANCY_BTC,
    _conclusion,
    format_live_report_md,
    live_duration_s,
    write_live_report,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

FORBIDDEN = ("carrera residual", "bastante honesto para construir encima")


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
        "book_synced": True,
        "n_levels": (500, 500),
        "honesty_levels": 20,
        "honesty": {
            "compared": 40,
            "matches": 38,
            "mismatches": 2,
            "max_qty_discrepancy": 0.5,
            "last_update_id_local": 10,
            "last_update_id_local_at_compare": 10,
            "last_update_id_rest": 9,
            "last_update_id_aligned": 10,
            "delta_ids": 1,
            "overshoot": 1,
            "stale": False,
            "batches_replayed": 1,
            "mismatch_details": [],
        },
        "error": None,
    }
    text = format_live_report_md(report)
    assert "BTCUSDT" in text
    assert "400" in text
    assert "Veredicto: PASS" in text
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
    assert "Veredicto: FAIL" in high
    gapped = format_live_report_md(
        {
            "deltas_applied": 10,
            "book_crossed": False,
            "gaps": 2,
            "resyncs": 2,
            "honesty": {
                "compared": 10,
                "mismatches": 0,
                "matches": 10,
                "delta_ids": 0,
                "batches_replayed": 0,
            },
        }
    )
    assert "PASS con resyncs" in gapped
    empty = format_live_report_md({"deltas_applied": 0, "honesty": {}})
    assert "no se aplicaron deltas" in empty
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


def _base_honesty(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "compared": 40,
        "matches": 40,
        "mismatches": 0,
        "max_qty_discrepancy": 0.0,
        "last_update_id_local": 110,
        "last_update_id_local_at_compare": 110,
        "last_update_id_rest": 110,
        "last_update_id_aligned": 110,
        "delta_ids": 0,
        "overshoot": 0,
        "stale": False,
        "frozen": True,
        "batches_replayed": 1,
        "mismatch_details": [],
    }
    base.update(overrides)
    return base


def _base_report(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "date_utc": "2026-01-01 00:00:00Z",
        "symbol": "BTCUSDT",
        "duration_s_requested": 60,
        "duration_s_elapsed": 60.0,
        "gaps": 0,
        "resyncs": 0,
        "reconnects": 0,
        "rest_429s": 0,
        "snapshots_applied": 1,
        "deltas_applied": 100,
        "trades": 10,
        "book_crossed": False,
        "book_synced": True,
        "n_levels": (2, 2),
        "honesty_levels": 20,
        "honesty": _base_honesty(),
        "error": None,
        "latency_ns": {},
    }
    base.update(overrides)
    return base


def test_contract_constants() -> None:
    assert HONESTY_MISMATCH_WARN == 0.10
    assert HONESTY_MAX_SLACK_BATCHES == 1
    assert MAX_HONESTY_QTY_DISCREPANCY_BTC == 0.5


def test_verdict_pass_clean() -> None:
    assert _conclusion(_base_report()).startswith("Veredicto: PASS")


def test_verdict_pass_with_resolved_resyncs() -> None:
    text = _conclusion(_base_report(gaps=1, resyncs=1))
    assert text.startswith("Veredicto: PASS con resyncs")


def test_verdict_fail_rate_uses_gte_boundary() -> None:
    text = _conclusion(_base_report(honesty=_base_honesty(mismatches=4, matches=36)))
    assert text.startswith("Veredicto: FAIL")
    assert "4/40" in text


def test_verdict_fail_rate_above_warn() -> None:
    text = _conclusion(_base_report(honesty=_base_honesty(mismatches=10, matches=30)))
    assert text.startswith("Veredicto: FAIL")
    for phrase in FORBIDDEN:
        assert phrase not in text


def test_verdict_fail_qty_discrepancy() -> None:
    honesty = _base_honesty(max_qty_discrepancy=2.525)
    text = _conclusion(_base_report(honesty=honesty))
    assert text.startswith("Veredicto: FAIL")
    assert "2.525" in text


def test_verdict_fail_slack_exceeded() -> None:
    text = _conclusion(_base_report(honesty=_base_honesty(batches_replayed=2)))
    assert text.startswith("Veredicto: FAIL")
    assert "batches_replayed=2" in text


def test_verdict_fail_negative_delta_ids() -> None:
    text = _conclusion(_base_report(honesty=_base_honesty(delta_ids=-3)))
    assert text.startswith("Veredicto: FAIL")


def test_verdict_fail_unresolved_gaps() -> None:
    assert _conclusion(_base_report(gaps=2, resyncs=1)).startswith("Veredicto: FAIL")


def test_verdict_fail_crossed() -> None:
    assert _conclusion(_base_report(book_crossed=True)).startswith("Veredicto: FAIL")


def test_verdict_inconclusive_stale() -> None:
    text = _conclusion(_base_report(honesty=_base_honesty(stale=True)))
    assert text.startswith("Veredicto: INCONCLUSIVE")


def test_verdict_inconclusive_without_deltas() -> None:
    assert _conclusion(_base_report(deltas_applied=0)).startswith("Veredicto: INCONCLUSIVE")


def test_verdict_error() -> None:
    assert _conclusion(_base_report(error="boom")).startswith("Veredicto: ERROR")


def test_no_euphemisms_outside_clean_pass() -> None:
    cases = [
        _base_report(honesty=_base_honesty(mismatches=10, matches=30)),
        _base_report(honesty=_base_honesty(max_qty_discrepancy=2.525)),
        _base_report(book_crossed=True),
        _base_report(honesty=_base_honesty(batches_replayed=5)),
    ]
    for report in cases:
        text = _conclusion(report)
        assert text.startswith("Veredicto: FAIL")
        for phrase in FORBIDDEN:
            assert phrase not in text


def test_format_renders_mismatch_details_table() -> None:
    honesty = _base_honesty(
        mismatches=2,
        matches=2,
        max_qty_discrepancy=5.0,
        mismatch_details=[
            {
                "side": "bid",
                "price": 100.0,
                "qty_local": 5.0,
                "qty_rest": 10.0,
                "abs_diff": 5.0,
            },
            {
                "side": "bid",
                "price": 99.0,
                "qty_local": None,
                "qty_rest": 5.0,
                "abs_diff": 5.0,
            },
        ],
    )
    md = format_live_report_md(_base_report(honesty=honesty))
    assert "| side | price | qty_local | qty_rest | abs_diff |" in md
    assert "| bid | 100.0 | 5.0 | 10.0 | 5.0 |" in md
    assert "| bid | 99.0 | — | 5.0 | 5.0 |" in md
    assert "delta_ids" in md
    assert "batches_replayed" in md


def test_format_conclusion_section_uses_verdict() -> None:
    md = format_live_report_md(_base_report())
    assert "## Conclusión" in md
    assert "Veredicto: PASS" in md
