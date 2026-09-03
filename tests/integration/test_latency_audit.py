"""Short live latency probe against Binance USD-M (skipped unless RUN_INTEGRATION=1).

The 10_000-event audit is ``scripts/latency_audit.py``; this only checks that the
public WS + ``GET /fapi/v1/time`` path works. No orders.
"""

from __future__ import annotations

import socket

import pytest

from order_flow.ingestion.latency_audit import run_latency_audit

pytestmark = pytest.mark.integration


async def test_live_latency_probe_collects_depth() -> None:
    result = await run_latency_audit(
        symbol="BTCUSDT",
        n_events=8,
        offset_samples=1,
        max_seconds=45.0,
        hostname=socket.gethostname(),
    )
    assert result["n_depth"] >= 1
    assert "depth_raw" in result
    assert "clock_offset" in result
    assert result["clock_offset"]["n"] >= 1
