"""Unit tests for latency-audit helpers. No network: synthetic arrays and frames."""

from __future__ import annotations

import math
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import orjson
import pytest

from order_flow.ingestion.latency_audit import (
    INSTITUTIONAL_BENCHMARKS,
    ClockSample,
    ProbeAccumulator,
    build_audit_results,
    clock_offset_ns,
    combined_stream_url,
    distribution,
    drain_until,
    format_institutional_table,
    format_latency_report_md,
    ingest_raw,
    inter_arrival_ns,
    n_gaps_over,
    ns_to_ms_str,
    offset_summary,
    parse_probe_message,
    percentile,
    raw_and_adjusted_ns,
    results_to_jsonable,
    run_latency_audit,
    sample_server_time,
    sample_server_time_n,
    utc_now_label,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def test_percentile_empty_is_nan() -> None:
    value = percentile([], 50.0)
    assert math.isnan(value)


def test_percentile_rejects_out_of_range() -> None:
    with pytest.raises(ValueError, match="percentile"):
        percentile([1], -0.1)
    with pytest.raises(ValueError, match="percentile"):
        percentile([1], 100.1)


def test_percentile_single_value_all_pct() -> None:
    assert percentile([42], 0.0) == 42.0
    assert percentile([42], 50.0) == 42.0
    assert percentile([42], 100.0) == 42.0


def test_percentile_linear_interpolation_on_even_length() -> None:
    # Sorted [10, 20, 30, 40]; p50 rank = 0.5 * 3 = 1.5 → 25.
    samples = [40, 10, 30, 20]
    assert percentile(samples, 0.0) == 10.0
    assert percentile(samples, 100.0) == 40.0
    assert percentile(samples, 50.0) == pytest.approx(25.0)
    assert percentile(samples, 25.0) == pytest.approx(17.5)


def test_percentile_p99_and_p999_on_synthetic_10k() -> None:
    samples = list(range(10_000))  # 0..9999
    # rank = pct/100 * 9999
    assert percentile(samples, 50.0) == pytest.approx(4999.5)
    assert percentile(samples, 90.0) == pytest.approx(8999.1)
    assert percentile(samples, 99.0) == pytest.approx(9899.01)
    assert percentile(samples, 99.9) == pytest.approx(9989.001)


def test_distribution_empty() -> None:
    summary = distribution([])
    assert summary["n"] == 0.0
    for key in ("min", "p50", "p90", "p99", "p99_9", "max", "mean"):
        assert math.isnan(summary[key])


def test_distribution_populated_keys_and_order_stats() -> None:
    samples = [100, 200, 300, 400, 500]
    summary = distribution(samples)
    assert summary["n"] == 5.0
    assert summary["min"] == 100.0
    assert summary["max"] == 500.0
    assert summary["mean"] == pytest.approx(300.0)
    assert summary["p50"] == pytest.approx(percentile(samples, 50.0))
    assert summary["p90"] == pytest.approx(percentile(samples, 90.0))
    assert summary["p99"] == pytest.approx(percentile(samples, 99.0))
    assert summary["p99_9"] == pytest.approx(percentile(samples, 99.9))


def test_clock_offset_negative_when_local_behind_server() -> None:
    local_ns = 1_000_000_000
    server_ns = 1_034_000_000  # server 34 ms ahead
    offset = clock_offset_ns(local_ns=local_ns, server_ns=server_ns)
    assert offset == -34_000_000
    assert offset < 0


def test_raw_latency_can_be_negative_clock_skew() -> None:
    recv_wall = 1_000_000_000
    event = 1_034_000_000
    raw, adjusted = raw_and_adjusted_ns(
        recv_wall_ns=recv_wall, event_ns=event, offset_ns=-34_000_000
    )
    assert raw == -34_000_000
    assert adjusted == 0


def test_adjusted_subtracts_offset_not_abs() -> None:
    # Local 10 ms ahead of exchange: offset = +10 ms. Raw 50 ms includes that lead.
    raw, adjusted = raw_and_adjusted_ns(recv_wall_ns=50_000_000, event_ns=0, offset_ns=10_000_000)
    assert raw == 50_000_000
    assert adjusted == 40_000_000


def test_inter_arrival_and_gap_count() -> None:
    ts = [0, 100_000_000, 200_000_000, 800_000_000]
    gaps = inter_arrival_ns(ts)
    assert gaps == [100_000_000, 100_000_000, 600_000_000]
    assert n_gaps_over(gaps, 250_000_000) == 1
    assert inter_arrival_ns([]) == []
    assert inter_arrival_ns([1]) == []


def test_parse_depth_and_trade_combined_envelope() -> None:
    depth = {
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate",
            "E": 1_000,
            "T": 999,
            "s": "BTCUSDT",
            "U": 10,
            "u": 12,
            "pu": 9,
            "b": [["1", "1"]],
            "a": [["2", "1"]],
        },
    }
    trade = {
        "e": "trade",
        "E": 2_000,
        "T": 1_999,
        "s": "BTCUSDT",
        "t": 7,
        "p": "1",
        "q": "0.1",
        "m": False,
    }
    d = parse_probe_message(orjson.dumps(depth), recv_wall_ns=1_500_000_000, recv_mono_ns=100)
    t = parse_probe_message(orjson.dumps(trade), recv_wall_ns=2_500_000_000, recv_mono_ns=200)
    assert d is not None
    assert d.kind == "depth"
    assert d.ts_exchange_e_ns == 1_000_000_000
    assert d.ts_exchange_t_ns == 999_000_000
    assert d.final_update_id == 12
    assert d.prev_final_update_id == 9
    assert t is not None
    assert t.kind == "trade"
    assert t.ts_exchange_e_ns == 2_000_000_000
    assert t.ts_exchange_t_ns == 1_999_000_000


def test_parse_skips_unknown_and_bad_json() -> None:
    assert parse_probe_message(b"not-json", recv_wall_ns=1, recv_mono_ns=1) is None
    assert (
        parse_probe_message(orjson.dumps({"e": "bookTicker"}), recv_wall_ns=1, recv_mono_ns=1)
        is None
    )


def test_accumulator_counts_sequence_gaps_on_pu() -> None:
    acc = ProbeAccumulator()
    frames = [
        {
            "e": "depthUpdate",
            "E": 1000,
            "T": 999,
            "s": "BTCUSDT",
            "U": 10,
            "u": 12,
            "pu": 9,
            "b": [],
            "a": [],
        },
        {
            "e": "depthUpdate",
            "E": 1100,
            "T": 1099,
            "s": "BTCUSDT",
            "U": 13,
            "u": 15,
            "pu": 12,
            "b": [],
            "a": [],
        },
        {
            "e": "depthUpdate",
            "E": 1200,
            "T": 1199,
            "s": "BTCUSDT",
            "U": 20,
            "u": 22,
            "pu": 18,  # gap: expected pu == 15
            "b": [],
            "a": [],
        },
        {
            "e": "trade",
            "E": 1201,
            "T": 1200,
            "s": "BTCUSDT",
            "t": 1,
            "p": "1",
            "q": "1",
            "m": True,
        },
    ]
    for i, payload in enumerate(frames):
        ingest_raw(
            acc,
            orjson.dumps(payload),
            recv_wall_ns=2_000_000_000 + i,
            recv_mono_ns=i,
        )
    assert acc.n_depth == 3
    assert acc.n_trade == 1
    assert acc.sequence_gaps == 1
    assert acc.decode_errors == 0
    ingest_raw(acc, b"{", recv_wall_ns=1, recv_mono_ns=1)
    assert acc.decode_errors == 1


def test_institutional_table_cites_public_sources() -> None:
    table = format_institutional_table()
    assert "2026-09-02" in table
    assert "Rithmic" in table
    assert "Databento" in table
    assert "Nanoconda" in table or "CME" in table
    assert "100 ms" in table or "100ms" in table
    assert len(INSTITUTIONAL_BENCHMARKS) >= 3
    for row in INSTITUTIONAL_BENCHMARKS:
        assert row["url"]
        assert row["retrieved"] == "2026-09-02"


def test_report_flags_negative_raw_as_clock_skew_not_colo() -> None:
    md = format_latency_report_md(
        {
            "date_utc": "2026-09-02 00:00:00Z",
            "hostname": "test.local",
            "machine_context": "red de investigación doméstica, no colocación",
            "symbol": "BTCUSDT",
            "n_depth": 10_000,
            "n_trade": 100,
            "duration_s": 1000.0,
            "reconnects": 0,
            "sequence_gaps": 0,
            "clock_offset": {
                "n": 5.0,
                "mean_ns": -34_000_000.0,
                "min_ns": -36_000_000.0,
                "p50_ns": -34_000_000.0,
                "p90_ns": -32_000_000.0,
                "p99_ns": -31_000_000.0,
                "p99_9_ns": -30_500_000.0,
                "max_ns": -30_000_000.0,
                "mean_rtt_ns": 80_000_000.0,
                "max_rtt_ns": 120_000_000.0,
            },
            "depth_raw": {
                "n": 10_000.0,
                "min": -40_000_000.0,
                "p50": -34_000_000.0,
                "p90": -20_000_000.0,
                "p99": -10_000_000.0,
                "p99_9": -5_000_000.0,
                "max": 1_000_000.0,
                "mean": -33_000_000.0,
            },
            "depth_adjusted": {
                "n": 10_000.0,
                "min": 1_000_000.0,
                "p50": 5_000_000.0,
                "p90": 20_000_000.0,
                "p99": 40_000_000.0,
                "p99_9": 80_000_000.0,
                "max": 100_000_000.0,
                "mean": 8_000_000.0,
            },
            "trade_raw": distribution([]),
            "trade_adjusted": distribution([]),
        }
    )
    assert "BTCUSDT" in md
    assert "reloj" in md.lower() or "clock" in md.lower()
    lowered = md.lower()
    assert "superluminal" not in lowered
    assert "no es más rápido que colo" in lowered or "no es ventaja frente a colo" in lowered
    assert "p99.9" in md or "p99,9" in md


def test_results_to_jsonable_replaces_nan(tmp_path: Path) -> None:
    payload = results_to_jsonable({"empty": distribution([]), "ok": 1})
    empty = payload["empty"]
    assert isinstance(empty, dict)
    assert empty["n"] == 0.0
    assert empty["mean"] is None
    text = orjson.dumps(payload).decode()
    assert "NaN" not in text
    out = tmp_path / "x.json"
    out.write_bytes(orjson.dumps(payload))
    assert out.exists()


def test_invalid_transaction_time_does_not_drop_event() -> None:
    msg = {"e": "trade", "E": 1000, "T": "nope", "s": "BTCUSDT"}
    event = parse_probe_message(orjson.dumps(msg), recv_wall_ns=1, recv_mono_ns=1)
    assert event is not None
    assert event.kind == "trade"
    assert event.ts_exchange_t_ns is None


def test_parse_rejects_non_object_and_missing_e() -> None:
    assert parse_probe_message(orjson.dumps([1, 2]), recv_wall_ns=1, recv_mono_ns=1) is None
    missing = orjson.dumps({"e": "depthUpdate"})
    assert parse_probe_message(missing, recv_wall_ns=1, recv_mono_ns=1) is None
    acc = ProbeAccumulator()
    ingest_raw(acc, orjson.dumps([1]), recv_wall_ns=1, recv_mono_ns=1)
    assert acc.decode_errors == 1


def test_clock_sample_offset_and_rtt() -> None:
    sample = ClockSample(
        local_before_ns=1_000,
        server_time_ns=1_034_000,
        local_after_ns=2_000,
        phase="start",
    )
    assert sample.offset_ns == 2_000 - 1_034_000
    assert sample.rtt_ns == 1_000
    assert sample.midpoint_offset_ns == 1_500 - 1_034_000


def test_offset_summary_empty_and_populated() -> None:
    empty = offset_summary([])
    assert empty["n"] == 0.0
    assert math.isnan(empty["mean_ns"])
    samples = [
        ClockSample(0, 100, 10, phase="start"),
        ClockSample(20, 100, 40, phase="end"),
    ]
    filled = offset_summary(samples)
    assert filled["n"] == 2.0
    assert filled["max_rtt_ns"] == 20.0


def test_ns_to_ms_str_and_stream_url() -> None:
    assert ns_to_ms_str(math.nan) == "n/a"
    assert ns_to_ms_str(1_500_000.0) == "1.500"
    assert "btcusdt@trade" in combined_stream_url("BTCUSDT")
    assert "btcusdt@depth@100ms" in combined_stream_url("BTCUSDT")
    assert "Z" in utc_now_label()


def test_build_audit_results_from_accumulator() -> None:
    acc = ProbeAccumulator()
    ingest_raw(
        acc,
        orjson.dumps(
            {
                "e": "depthUpdate",
                "E": 1000,
                "T": 999,
                "s": "BTCUSDT",
                "U": 1,
                "u": 2,
                "pu": 0,
                "b": [],
                "a": [],
            }
        ),
        recv_wall_ns=1_100_000_000,
        recv_mono_ns=10,
    )
    ingest_raw(
        acc,
        orjson.dumps(
            {
                "e": "depthUpdate",
                "E": 1100,
                "T": 1099,
                "s": "BTCUSDT",
                "U": 3,
                "u": 4,
                "pu": 2,
                "b": [],
                "a": [],
            }
        ),
        recv_wall_ns=1_200_000_000,
        recv_mono_ns=20,
    )
    offsets = [ClockSample(0, 0, 5_000_000, phase="start")]
    payload = build_audit_results(
        acc=acc,
        offset_samples=offsets,
        reconnects=1,
        duration_s=12.5,
        symbol="btcusdt",
        hostname="test.local",
        date_utc="2026-09-02 00:00:00Z",
        n_depth_requested=10_000,
        streams=["btcusdt@depth@100ms"],
    )
    assert payload["n_depth"] == 2
    assert payload["reconnects"] == 1
    assert payload["symbol"] == "BTCUSDT"
    assert payload["depth_raw"]["n"] == 2.0
    md = format_latency_report_md(payload)
    assert "12.5" in md


async def test_drain_until_stops_at_n_depth() -> None:
    async def frames() -> object:
        for i in range(5):
            yield orjson.dumps(
                {
                    "e": "depthUpdate",
                    "E": 1000 + i,
                    "T": 999 + i,
                    "s": "BTCUSDT",
                    "U": i + 1,
                    "u": i + 1,
                    "pu": i,
                    "b": [],
                    "a": [],
                }
            )
            yield orjson.dumps(
                {
                    "e": "trade",
                    "E": 2000 + i,
                    "T": 1999 + i,
                    "s": "BTCUSDT",
                    "t": i,
                    "p": "1",
                    "q": "1",
                    "m": False,
                }
            )

    acc = ProbeAccumulator()
    seen: list[int] = []

    async def progress(n: int) -> None:
        seen.append(n)

    await drain_until(frames(), acc, n_depth=3, on_progress=progress)  # type: ignore[arg-type]
    assert acc.n_depth == 3
    assert acc.n_trade >= 2
    assert seen[-1] == 3


async def test_sample_server_time_uses_mock_http() -> None:
    ticks = iter([1_000, 4_000, 10_000, 13_000])

    def wall() -> int:
        return next(ticks)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/fapi/v1/time")
        return httpx.Response(200, content=orjson.dumps({"serverTime": 2}))

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        transport=transport, base_url="https://fapi.binance.com"
    ) as client:
        one = await sample_server_time(client, wall=wall, phase="start")
        many = await sample_server_time_n(client, n=1, wall=wall, phase="end")
    assert one.server_time_ns == 2_000_000
    assert one.offset_ns == 4_000 - 2_000_000
    assert one.phase == "start"
    assert len(many) == 1
    assert many[0].phase == "end"


def _depth_frame(i: int) -> bytes:
    return orjson.dumps(
        {
            "e": "depthUpdate",
            "E": 1_000 + i,
            "T": 999 + i,
            "s": "BTCUSDT",
            "U": i + 1,
            "u": i + 1,
            "pu": i,
            "b": [],
            "a": [],
        }
    )


def _time_client() -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/time" in str(request.url)
        return httpx.Response(200, content=orjson.dumps({"serverTime": 1_000}))

    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://fapi.binance.com",
    )


async def test_run_latency_audit_fake_ws_and_reconnect() -> None:
    batches = [[_depth_frame(0)], [_depth_frame(1), _depth_frame(2)]]
    calls: list[str] = []

    @asynccontextmanager
    async def fake_connect(url: str) -> AsyncIterator[AsyncIterator[bytes]]:
        calls.append(url)
        idx = min(len(calls) - 1, len(batches) - 1)

        async def gen() -> AsyncIterator[bytes]:
            for frame in batches[idx]:
                yield frame

        yield gen()

    async def no_sleep(_: float) -> None:
        return None

    async with _time_client() as client:
        result = await run_latency_audit(
            symbol="BTCUSDT",
            n_events=3,
            include_trades=False,
            offset_samples=1,
            http_client=client,
            ws_connect=fake_connect,
            sleep=no_sleep,
            hostname="test.local",
            max_seconds=30.0,
        )
    assert result["n_depth"] == 3
    assert result["reconnects"] == 1
    assert "depth@100ms" in calls[0]
    assert "@trade" not in calls[0]


async def test_run_latency_audit_rejects_bad_n() -> None:
    with pytest.raises(ValueError, match="n_events"):
        await run_latency_audit(n_events=0)


async def test_run_latency_audit_timeout_without_frames() -> None:
    @asynccontextmanager
    async def empty_connect(_url: str) -> AsyncIterator[AsyncIterator[bytes]]:
        async def gen() -> AsyncIterator[bytes]:
            empty: tuple[bytes, ...] = ()
            for frame in empty:
                yield frame  # pragma: no cover

        yield gen()

    async def no_sleep(_: float) -> None:
        return None

    async with _time_client() as client:
        result = await run_latency_audit(
            n_events=5,
            offset_samples=1,
            http_client=client,
            ws_connect=empty_connect,
            sleep=no_sleep,
            max_seconds=0.0,
            max_reconnects=0,
            hostname="test.local",
        )
    assert result["n_depth"] == 0


async def test_run_latency_audit_retries_handshake_timeout() -> None:
    calls = {"n": 0}

    @asynccontextmanager
    async def flaky(_url: str) -> AsyncIterator[AsyncIterator[bytes]]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("timed out during opening handshake")

        async def gen() -> AsyncIterator[bytes]:
            for i in range(3):
                yield _depth_frame(i)

        yield gen()

    async def no_sleep(_: float) -> None:
        return None

    async with _time_client() as client:
        result = await run_latency_audit(
            n_events=3,
            include_trades=False,
            offset_samples=1,
            http_client=client,
            ws_connect=flaky,
            sleep=no_sleep,
            hostname="test.local",
            max_seconds=30.0,
        )
    assert result["n_depth"] == 3
    assert result["reconnects"] == 1
    assert calls["n"] == 2


def test_cli_help_exposes_symbol_and_n_events() -> None:
    repo = Path(__file__).resolve().parents[2]
    proc = subprocess.run(  # noqa: S603
        [sys.executable, str(repo / "scripts" / "latency_audit.py"), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    assert "--n-events" in proc.stdout
    assert "--symbol" in proc.stdout
    assert "BTCUSDT" in proc.stdout


async def test_drain_until_accepts_str_frames_and_agg_trade() -> None:
    async def frames() -> AsyncIterator[str]:
        yield orjson.dumps(
            {"e": "aggTrade", "E": 2, "T": 2, "s": "BTCUSDT", "a": 9, "p": "1", "q": "1", "m": True}
        ).decode()
        yield orjson.dumps(
            {
                "e": "depthUpdate",
                "E": 1,
                "T": 1,
                "s": "BTCUSDT",
                "U": 1,
                "u": 1,
                "pu": 0,
                "b": [],
                "a": [],
            }
        ).decode()

    acc = ProbeAccumulator()
    await drain_until(frames(), acc, n_depth=1)
    assert acc.n_depth == 1
    assert acc.n_trade == 1


def test_parse_depth_missing_ids_and_bad_event_time() -> None:
    assert (
        parse_probe_message(
            orjson.dumps({"e": "depthUpdate", "E": 1, "s": "BTCUSDT"}),
            recv_wall_ns=1,
            recv_mono_ns=1,
        )
        is None
    )
    assert (
        parse_probe_message(
            orjson.dumps({"e": "trade", "E": "x"}),
            recv_wall_ns=1,
            recv_mono_ns=1,
        )
        is None
    )
