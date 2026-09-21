"""60s honesty run against real Binance USD-M (skipped unless RUN_INTEGRATION=1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from order_flow.ingestion.live import (
    HONESTY_MAX_SLACK_BATCHES,
    HONESTY_MISMATCH_WARN,
    MAX_HONESTY_QTY_DISCREPANCY_BTC,
    format_live_report_md,
    live_duration_s,
    run_live_validation,
    write_live_report,
)

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_PATH = REPO_ROOT / "docs" / "ingestion" / "live-validation.md"


async def test_binance_live_l2_honesty() -> None:
    duration = live_duration_s()
    report = await run_live_validation(symbol="BTCUSDT", duration_s=duration, honesty_levels=20)
    print(json.dumps(report, indent=2, default=str))
    write_live_report(report, REPORT_PATH)
    print(format_live_report_md(report))

    if report.get("error"):
        pytest.fail(f"live run failed: {report['error']}")

    assert report["symbol"] == "BTCUSDT"
    assert report["deltas_applied"] >= 1
    assert report["snapshots_applied"] >= 1
    # Tripleta consistente del mismo instante: lo visto en cola + lo en vuelo
    # al expirar el timeout == lo aplicado al libro (run 2026-09-21: 577+5=582).
    assert report["deltas_in_flight"] >= 0
    assert report["events_seen"]["deltas"] + report["deltas_in_flight"] == report["deltas_applied"]
    # Contrato honesty alineado: cualquier gate roto es FAIL (sin eufemismos).
    assert report["book_crossed"] is False, "crossed book"
    assert report["book_synced"] is True, "book left unsynced"
    assert report["gaps"] == report["resyncs"], f"gaps sin resolver: {report['gaps']=}"
    honesty = report["honesty"]
    assert honesty is not None
    assert honesty["compared"] >= 2
    assert honesty["stale"] is False, f"honesty inconclusive: {honesty}"
    assert honesty["delta_ids"] is not None
    assert honesty["delta_ids"] >= 0
    assert honesty["overshoot"] == honesty["delta_ids"]
    assert honesty["batches_replayed"] <= HONESTY_MAX_SLACK_BATCHES, honesty
    if honesty["compared"]:
        rate = honesty["mismatches"] / honesty["compared"]
        assert rate <= HONESTY_MISMATCH_WARN, f"mismatch_rate={rate}"
    assert honesty["max_qty_discrepancy"] <= MAX_HONESTY_QTY_DISCREPANCY_BTC, (
        f"max_qty_discrepancy={honesty['max_qty_discrepancy']}"
    )
