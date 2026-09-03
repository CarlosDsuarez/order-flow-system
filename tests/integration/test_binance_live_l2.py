"""60s honesty run against real Binance USD-M (skipped unless RUN_INTEGRATION=1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from order_flow.ingestion.live import (
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
    assert report["book_crossed"] is False
    honesty = report["honesty"]
    assert honesty is not None
    assert honesty["compared"] >= 2
    # Residual race can produce a few qty mismatches; a high rate would be a real bug.
    if honesty["compared"]:
        assert honesty["mismatches"] / honesty["compared"] <= 0.5
