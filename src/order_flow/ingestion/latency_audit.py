"""Latency-audit math, probe parsing, and report formatting.

Pure helpers for ``scripts/latency_audit.py``. No order placement. Public market-data
frames only. Wall-clock comparisons to exchange UTC **include NTP / OS clock offset**;
``time.perf_counter_ns`` is for inter-arrival only.

Clock offset convention (mandatory): ``offset_ns = local - server``. Negative offset
means the local clock is **behind** Binance. Raw ``recv_wall - E`` can then be
negative; that is skew, not superluminal processing. Adjusted delay is
``recv_wall - E - offset``. Residual uncertainty: NTP, HTTP RTT, and path asymmetry.
"""

from __future__ import annotations

import asyncio
import math
import statistics
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Literal, TypedDict

import httpx
import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from order_flow.ingestion.sync import reconnect_delay
from order_flow.utils.logging import get_logger
from order_flow.utils.time import NS_PER_MS, NS_PER_S, ms_to_ns

DEFAULT_SYMBOL: Final = "BTCUSDT"
DEFAULT_N_EVENTS: Final = 10_000
DEFAULT_REST_URL: Final = "https://fapi.binance.com"
DEFAULT_WS_URL: Final = "wss://fstream.binance.com/stream"
SERVER_TIME_PATH: Final = "/fapi/v1/time"
GAP_THRESHOLD_NS: Final = 250_000_000  # 250 ms; depth@100ms already buckets at 100 ms
OFFSET_SAMPLES_PER_PHASE: Final = 5
RETRIEVED: Final = "2026-09-02"
DEFAULT_HTTP_TIMEOUT_S: Final = 10.0
DEFAULT_MAX_RECONNECTS: Final = 32
MACHINE_CONTEXT: Final = "red de investigación doméstica, no colocación"

log = get_logger(__name__)

WsConnect = Callable[[str], AbstractAsyncContextManager[AsyncIterable[str | bytes]]]

ProbeKind = Literal["depth", "trade"]


class BenchmarkRow(TypedDict):
    """One public institutional / vendor latency citation."""

    source: str
    figure: str
    notes: str
    url: str
    retrieved: str


INSTITUTIONAL_BENCHMARKS: Final[tuple[BenchmarkRow, ...]] = (
    {
        "source": "Databento (marketing cuantitativo)",
        "figure": "p90 42 µs (cross-connect) / 590 µs (internet) hasta la aplicación; "
        "mediana 6.1 µs handoff→envío en el gateway",
        "notes": "Cifras de producto, no un SLA de Binance. Orden de magnitud colo vs internet.",
        "url": "https://databento.com/live",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Databento dedicated connectivity (marketing)",
        "figure": "p90 42.4 µs cross-connect 10G/25G; internet 0.5+ ms; interconnect cloud 1.7+ ms",
        "notes": "Tabla de arquitectura propia. Marketing, pero con números explícitos.",
        "url": "https://databento.com/docs/architecture/dedicated-connectivity-guide",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Nanoconda — CME MDP3 vs iLink (empírico colocated)",
        "figure": "MD latency mediana 265.7 µs; MSGW 203.1 µs "
        "(exchange sending time - transaction time)",
        "notes": "Medición con timestamps CME, no reloj local. Colo Aurora, no retail.",
        "url": "https://nanoconda.com/blog/cme-trade-summary-vs-private-fills/",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Rithmic R|API suite (marketing de vendor)",
        "figure": "Diamond API tick-to-trade típico <250 µs (colo); R|API+ / Protocol <1 ms",
        "notes": "Especificación comercial. No es Binance. CQG no publica µs comparables.",
        "url": "https://www.rithmic.com/products/api-suite",
        "retrieved": RETRIEVED,
    },
    {
        "source": "CQG Client APIs (marketing)",
        "figure": "«the CQG API introduces only one millisecond for data round-trip»",
        "notes": "Folleto, no hop de matching engine. Sin spec pública en µs.",
        "url": "https://www.cqg.com/products/cqg-apis/client-apis",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Binance USD-M Diff. Book Depth (oficial)",
        "figure": "Update speed 250 ms / 500 ms / 100 ms (`@depth@100ms`)",
        "notes": "El libro que alimenta OFI ya está discretizado a 100 ms. Eso puede dominar "
        "cualquier last-mile de unos pocos ms.",
        "url": "https://developers.binance.com/docs/derivatives/usds-margined-futures/"
        "websocket-market-streams/Diff-Book-Depth-Streams",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Binance Developer Community (anecdótico, no SLA)",
        "figure": "WS API USD-M RTT ~6 ms vs Spot ~1.6 ms (un usuario, 2024-07)",
        "notes": "Hilo de foro. Infra de proximidad, no hogar. No generalizar.",
        "url": "https://dev.binance.vision/t/usd-m-futures-high-websocket-api-latency/21511",
        "retrieved": RETRIEVED,
    },
    {
        "source": "Jane Street engineering blog (orden de magnitud)",
        "figure": "Sistemas de trading que responden en «far less than» 250 µs "
        "(el intervalo de un profiler muestral)",
        "notes": "No es un SLA. Sitúa el tick-to-trade colo en cientos de µs o menos.",
        "url": "https://blog.janestreet.com/magic-trace/",
        "retrieved": RETRIEVED,
    },
)


@dataclass(frozen=True)
class ProbeEvent:
    """One public WS frame with exchange and local timestamps."""

    kind: ProbeKind
    ts_exchange_e_ns: int
    ts_exchange_t_ns: int | None
    ts_recv_wall_ns: int
    ts_recv_mono_ns: int
    first_update_id: int | None = None
    final_update_id: int | None = None
    prev_final_update_id: int | None = None


@dataclass
class ProbeAccumulator:
    """In-order depth + trade frames. Sequence gaps use ``pu != previous u``."""

    depth: list[ProbeEvent] = field(default_factory=list)
    trades: list[ProbeEvent] = field(default_factory=list)
    sequence_gaps: int = 0
    decode_errors: int = 0
    last_depth_u: int | None = None

    @property
    def n_depth(self) -> int:
        """Number of ``depthUpdate`` frames ingested."""
        return len(self.depth)

    @property
    def n_trade(self) -> int:
        """Number of ``trade`` / ``aggTrade`` frames ingested."""
        return len(self.trades)


@dataclass(frozen=True)
class ClockSample:
    """One ``GET /fapi/v1/time`` round-trip.

    ``offset_ns`` uses recv local time minus ``serverTime`` (``local - server``).
    ``midpoint_offset_ns`` is the NTP-style midpoint; residual error is ~RTT/2
    under path asymmetry.
    """

    local_before_ns: int
    server_time_ns: int
    local_after_ns: int
    phase: str = "unspecified"

    @property
    def offset_ns(self) -> int:
        """``local_after - serverTime``. Negative ⇒ local clock behind Binance."""
        return clock_offset_ns(local_ns=self.local_after_ns, server_ns=self.server_time_ns)

    @property
    def midpoint_offset_ns(self) -> int:
        """``((before + after) / 2) - serverTime``."""
        mid = (self.local_before_ns + self.local_after_ns) // 2
        return clock_offset_ns(local_ns=mid, server_ns=self.server_time_ns)

    @property
    def rtt_ns(self) -> int:
        """HTTP round-trip bound (includes parse)."""
        return self.local_after_ns - self.local_before_ns


_PCT_MAX: Final = 100.0
_MIN_GAPS: Final = 2


def percentile(samples: Sequence[int] | Sequence[float], pct: float) -> float:
    """Linear-interpolated percentile. Empty → NaN. ``pct`` in ``[0, 100]``."""
    if pct < 0.0 or pct > _PCT_MAX:
        msg = f"percentile must be in [0, 100], got {pct}"
        raise ValueError(msg)
    if not samples:
        return math.nan
    ordered = sorted(samples)
    rank = pct / 100.0 * (len(ordered) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(ordered[lo])
    weight = rank - lo
    return float(ordered[lo]) * (1.0 - weight) + float(ordered[hi]) * weight


def distribution(samples: Sequence[int] | Sequence[float]) -> dict[str, float]:
    """``n, min, p50, p90, p99, p99_9, max, mean``. Empty → n=0 and NaNs."""
    if not samples:
        nan = math.nan
        return {
            "n": 0.0,
            "min": nan,
            "p50": nan,
            "p90": nan,
            "p99": nan,
            "p99_9": nan,
            "max": nan,
            "mean": nan,
        }
    return {
        "n": float(len(samples)),
        "min": float(min(samples)),
        "p50": percentile(samples, 50.0),
        "p90": percentile(samples, 90.0),
        "p99": percentile(samples, 99.0),
        "p99_9": percentile(samples, 99.9),
        "max": float(max(samples)),
        "mean": float(statistics.fmean(samples)),
    }


def clock_offset_ns(*, local_ns: int, server_ns: int) -> int:
    """``offset = local - server``. Negative means the local clock is behind."""
    return local_ns - server_ns


def raw_and_adjusted_ns(*, recv_wall_ns: int, event_ns: int, offset_ns: int) -> tuple[int, int]:
    """Raw ``recv_wall - E`` and offset-adjusted ``recv_wall - E - offset``.

    Adjusted estimates one-way network + parse delay. It is **not** a colocated
    hop and still carries NTP / asymmetry error.
    """
    raw = recv_wall_ns - event_ns
    return raw, raw - offset_ns


def inter_arrival_ns(timestamps: Sequence[int]) -> list[int]:
    """Positive diffs between consecutive timestamps. Empty if ``len < 2``."""
    if len(timestamps) < _MIN_GAPS:
        return []
    return [timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))]


def n_gaps_over(intervals: Sequence[int], threshold_ns: int) -> int:
    """Count intervals strictly greater than ``threshold_ns``."""
    return sum(1 for gap in intervals if gap > threshold_ns)


def _unwrap(msg: Mapping[str, Any]) -> Mapping[str, Any]:
    data = msg.get("data")
    if "stream" in msg and isinstance(data, Mapping):
        return data
    return msg


def _optional_ms_to_ns(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return ms_to_ns(value)
    if isinstance(value, str):
        try:
            return ms_to_ns(int(value))
        except ValueError:
            return None
    return None


def parse_probe_message(
    raw: bytes | str, *, recv_wall_ns: int, recv_mono_ns: int
) -> ProbeEvent | None:
    """Parse a combined-stream or raw depth/trade frame. Unknown / bad JSON → None."""
    try:
        loaded: Any = orjson.loads(raw)
    except (orjson.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(loaded, Mapping):
        return None
    return probe_event_from_mapping(loaded, recv_wall_ns=recv_wall_ns, recv_mono_ns=recv_mono_ns)


def probe_event_from_mapping(
    msg: Mapping[str, Any], *, recv_wall_ns: int, recv_mono_ns: int
) -> ProbeEvent | None:
    """Build a :class:`ProbeEvent` from an already-decoded mapping."""
    payload = _unwrap(msg)
    event_type = payload.get("e")
    event_ms = payload.get("E")
    if event_ms is None:
        return None
    try:
        e_ns = ms_to_ns(int(event_ms))
    except (TypeError, ValueError):
        return None
    t_ns = _optional_ms_to_ns(payload.get("T"))
    if event_type == "depthUpdate":
        try:
            return ProbeEvent(
                kind="depth",
                ts_exchange_e_ns=e_ns,
                ts_exchange_t_ns=t_ns,
                ts_recv_wall_ns=recv_wall_ns,
                ts_recv_mono_ns=recv_mono_ns,
                first_update_id=int(payload["U"]),
                final_update_id=int(payload["u"]),
                prev_final_update_id=int(payload["pu"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
    if event_type in {"trade", "aggTrade"}:
        return ProbeEvent(
            kind="trade",
            ts_exchange_e_ns=e_ns,
            ts_exchange_t_ns=t_ns,
            ts_recv_wall_ns=recv_wall_ns,
            ts_recv_mono_ns=recv_mono_ns,
        )
    return None


def ingest_raw(
    acc: ProbeAccumulator,
    raw: bytes | str,
    *,
    recv_wall_ns: int,
    recv_mono_ns: int,
) -> ProbeEvent | None:
    """Decode one frame into ``acc``. JSON failures increment ``decode_errors``."""
    try:
        loaded: Any = orjson.loads(raw)
    except (orjson.JSONDecodeError, TypeError, ValueError):
        acc.decode_errors += 1
        return None
    if not isinstance(loaded, Mapping):
        acc.decode_errors += 1
        return None
    event = probe_event_from_mapping(loaded, recv_wall_ns=recv_wall_ns, recv_mono_ns=recv_mono_ns)
    if event is None:
        return None
    if event.kind == "depth":
        prev_u = acc.last_depth_u
        if (
            prev_u is not None
            and event.prev_final_update_id is not None
            and event.prev_final_update_id != prev_u
        ):
            acc.sequence_gaps += 1
        if event.final_update_id is not None:
            acc.last_depth_u = event.final_update_id
        acc.depth.append(event)
    else:
        acc.trades.append(event)
    return event


async def drain_until(
    messages: AsyncIterator[bytes | str],
    acc: ProbeAccumulator,
    *,
    n_depth: int,
    wall: Callable[[], int] = time.time_ns,
    mono: Callable[[], int] = time.perf_counter_ns,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
) -> None:
    """Read ``messages`` until ``acc.n_depth >= n_depth`` (or the iterator ends)."""
    async for raw in messages:
        payload = raw.encode() if isinstance(raw, str) else raw
        ingest_raw(acc, payload, recv_wall_ns=wall(), recv_mono_ns=mono())
        if on_progress is not None:
            await on_progress(acc.n_depth)
        if acc.n_depth >= n_depth:
            return


async def sample_server_time(
    client: httpx.AsyncClient,
    *,
    path: str = SERVER_TIME_PATH,
    wall: Callable[[], int] = time.time_ns,
    phase: str = "unspecified",
) -> ClockSample:
    """One ``GET /fapi/v1/time``. ``offset = local_after - serverTime``."""
    before = wall()
    response = await client.get(path)
    after = wall()
    response.raise_for_status()
    payload: Any = orjson.loads(response.content)
    server_ms = int(payload["serverTime"])
    return ClockSample(
        local_before_ns=before,
        server_time_ns=ms_to_ns(server_ms),
        local_after_ns=after,
        phase=phase,
    )


async def sample_server_time_n(
    client: httpx.AsyncClient,
    *,
    n: int = OFFSET_SAMPLES_PER_PHASE,
    phase: str = "unspecified",
    wall: Callable[[], int] = time.time_ns,
) -> list[ClockSample]:
    """``n`` consecutive server-time samples."""
    samples: list[ClockSample] = []
    for _ in range(n):
        samples.append(await sample_server_time(client, wall=wall, phase=phase))
    return samples


def offset_summary(samples: Sequence[ClockSample]) -> dict[str, float]:
    """Distribution of ``local - server`` plus RTT bounds."""
    if not samples:
        empty = distribution([])
        return {
            **{f"{k}_ns" if k != "n" else k: v for k, v in empty.items()},
            "mean_rtt_ns": math.nan,
            "max_rtt_ns": math.nan,
            "mean_midpoint_offset_ns": math.nan,
        }
    offsets = [s.offset_ns for s in samples]
    rtts = [s.rtt_ns for s in samples]
    mid = [s.midpoint_offset_ns for s in samples]
    dist = distribution(offsets)
    return {
        "n": dist["n"],
        "mean_ns": dist["mean"],
        "min_ns": dist["min"],
        "p50_ns": dist["p50"],
        "p90_ns": dist["p90"],
        "p99_ns": dist["p99"],
        "p99_9_ns": dist["p99_9"],
        "max_ns": dist["max"],
        "mean_rtt_ns": float(statistics.fmean(rtts)),
        "max_rtt_ns": float(max(rtts)),
        "mean_midpoint_offset_ns": float(statistics.fmean(mid)),
    }


def event_latencies(events: Sequence[ProbeEvent], offset_ns: int) -> tuple[list[int], list[int]]:
    """Raw and adjusted ``recv_wall - E`` series for one event kind."""
    raws: list[int] = []
    adjs: list[int] = []
    for event in events:
        raw, adj = raw_and_adjusted_ns(
            recv_wall_ns=event.ts_recv_wall_ns,
            event_ns=event.ts_exchange_e_ns,
            offset_ns=offset_ns,
        )
        raws.append(raw)
        adjs.append(adj)
    return raws, adjs


def _replace_nan(value: object) -> object:
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Mapping):
        return {str(k): _replace_nan(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_nan(item) for item in value]
    return value


def results_to_jsonable(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Replace NaN with ``None`` so orjson/JSON do not emit non-standard NaN."""
    replaced = _replace_nan(dict(payload))
    if not isinstance(replaced, dict):
        msg = "results payload must be a mapping"
        raise TypeError(msg)
    return replaced


def ns_to_ms_str(ns: float, *, digits: int = 3) -> str:
    """Format nanoseconds as milliseconds, or ``n/a`` for NaN."""
    if isinstance(ns, float) and math.isnan(ns):
        return "n/a"
    return f"{ns / NS_PER_MS:.{digits}f}"


def format_institutional_table() -> str:
    """Markdown comparison table (Spanish headers, public citations)."""
    lines = [
        "| Fuente | Cifra | Notas | URL | Recuperado |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in INSTITUTIONAL_BENCHMARKS:
        notes = row["notes"].replace("|", "/")
        figure = row["figure"].replace("|", "/")
        lines.append(
            f"| {row['source']} | {figure} | {notes} | {row['url']} | {row['retrieved']} |"
        )
    return "\n".join(lines)


def _dist_row(label: str, dist: Mapping[str, float]) -> str:
    return (
        f"| {label} | {int(dist.get('n', 0))} | {ns_to_ms_str(float(dist.get('min', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('p50', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('p90', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('p99', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('p99_9', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('max', math.nan)))} | "
        f"{ns_to_ms_str(float(dist.get('mean', math.nan)))} |"
    )


def _has_negative_raw(dist: Mapping[str, float]) -> bool:
    minimum = float(dist.get("min", math.nan))
    return not math.isnan(minimum) and minimum < 0.0


def _ms_key(blob: object, key: str) -> str:
    if not isinstance(blob, Mapping):
        return ns_to_ms_str(math.nan)
    return ns_to_ms_str(float(blob.get(key, math.nan)))


def format_latency_report_md(report: Mapping[str, Any]) -> str:
    """Spanish markdown sidecar for one audit run on this machine."""
    clock = report.get("clock_offset") or {}
    depth_raw = report.get("depth_raw") or distribution([])
    depth_adj = report.get("depth_adjusted") or distribution([])
    trade_raw = report.get("trade_raw") or distribution([])
    trade_adj = report.get("trade_adjusted") or distribution([])
    negative_note = ""
    if _has_negative_raw(depth_raw) or _has_negative_raw(trade_raw):
        negative_note = (
            "\n**Latencia cruda negativa:** el reloj local va **atrás** del `E` de Binance "
            "(offset `local - server` negativo). **No es ventaja frente a colo** y no es "
            "procesamiento más rápido que la luz. Se reporta cruda y ajustada por offset; "
            "el ajuste estima red+parse con incertidumbre residual (NTP, RTT HTTP, asimetría).\n"
        )
    rtt_mean = _ms_key(clock, "mean_rtt_ns")
    rtt_max = _ms_key(clock, "max_rtt_ns")
    offset_uncertainty = (
        f"RTT medio HTTP a `/fapi/v1/time`: {rtt_mean} ms; máx {rtt_max} ms. "
        "La incertidumbre residual del offset es del orden de RTT/2 por asimetría de ruta, "
        "más el error de NTP del OS. Exchange-local en wall clock **incluye** ese offset."
    )
    streams = report.get("streams") or ["<symbol>@depth@100ms"]
    stream_s = ", ".join(str(s) for s in streams)
    lines = [
        "# Auditoría de latencia — Binance USD-M (esta máquina)",
        "",
        "Sonda de market data **pública**. No envía órdenes. Reloj de evento: campo `E` (ms).",
        "",
        f"- **Fecha (UTC):** {report.get('date_utc', '')}",
        f"- **Host:** `{report.get('hostname', '')}`",
        f"- **Contexto:** {report.get('machine_context', 'red de investigación, no colocación')}",
        f"- **Símbolo:** `{report.get('symbol', DEFAULT_SYMBOL)}`",
        f"- **Streams:** `{stream_s}`",
        f"- **n depth pedido:** {report.get('n_depth_requested', report.get('n_depth', ''))}",
        f"- **n depth recibido:** {report.get('n_depth', 0)}",
        f"- **n trade recibido:** {report.get('n_trade', 0)}",
        f"- **Duración:** {float(report.get('duration_s', 0) or 0):.1f} s",
        f"- **Reconnects WS:** {report.get('reconnects', 0)}",
        f"- **Gaps de secuencia (`pu != u` previo):** {report.get('sequence_gaps', 0)}",
        f"- **Errores de decode:** {report.get('decode_errors', 0)}",
        "",
        "## Offset de reloj (`offset = local - server`)",
        "",
        "Muestras `GET https://fapi.binance.com/fapi/v1/time` (`serverTime`) al inicio, "
        "a mitad y al final. `offset_ns = local_after_ns - serverTime_ns`. Offset negativo "
        "= reloj local detrás de Binance. El mid-point NTP se reporta como control; "
        "el ajuste de latencia usa la media del offset `local - server`.",
        "",
        f"- n muestras: {int(clock.get('n') or 0)}",
        f"- mean offset: {_ms_key(clock, 'mean_ns')} ms",
        f"- min / p50 / p90 / p99 / p99.9 / max offset: "
        f"{_ms_key(clock, 'min_ns')} / {_ms_key(clock, 'p50_ns')} / "
        f"{_ms_key(clock, 'p90_ns')} / {_ms_key(clock, 'p99_ns')} / "
        f"{_ms_key(clock, 'p99_9_ns')} / {_ms_key(clock, 'max_ns')} ms",
        f"- mean midpoint offset: {_ms_key(clock, 'mean_midpoint_offset_ns')} ms",
        f"- {offset_uncertainty}",
        negative_note,
        "## Distribución de latencia (ms)",
        "",
        "Cruda: `recv_wall - E`. Ajustada: `recv_wall - E - offset`. "
        "`E` es event time oficial (ms). `T` (transaction time) se registra pero no define "
        "la latencia. `time.time_ns()` vs UTC del exchange; `time.perf_counter_ns()` solo "
        "para inter-llegada local.",
        "",
        "| Serie | n | min | p50 | p90 | p99 | p99.9 | max | mean |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        _dist_row("depth cruda", depth_raw),
        _dist_row("depth ajustada", depth_adj),
        _dist_row("trade cruda", trade_raw),
        _dist_row("trade ajustada", trade_adj),
        "",
        "## Inter-llegada y huecos",
        "",
        f"- Umbral de hueco: {report.get('gap_threshold_ms', GAP_THRESHOLD_NS / NS_PER_MS):.0f} ms",
        f"- Huecos event-time `E` (depth): {report.get('depth_gaps_exchange', 0)}",
        f"- Huecos local monotonic (depth): {report.get('depth_gaps_local', 0)}",
        f"- Inter-llegada `E` depth p50/p99: "
        f"{_ms_key(report.get('depth_interarrival_exchange'), 'p50')} / "
        f"{_ms_key(report.get('depth_interarrival_exchange'), 'p99')} ms",
        f"- Inter-llegada local depth p50/p99: "
        f"{_ms_key(report.get('depth_interarrival_local'), 'p50')} / "
        f"{_ms_key(report.get('depth_interarrival_local'), 'p99')} ms",
        "",
        "## Comparación institucional (órdenes de magnitud)",
        "",
        format_institutional_table(),
        "",
        "Esta máquina es **red de investigación / hogar, no colo**. Comparar p99 / p99.9 "
        "ajustados (no la media) contra decenas-cientos de **µs** en colo y contra el "
        "throttle oficial de **100 ms** del depth. Un p99 de decenas de ms es ~100-1000x "
        "un hop colocated típico de market data.",
        "",
        "## Cómo repetir",
        "",
        "```bash",
        "uv run python scripts/latency_audit.py --symbol BTCUSDT --n-events 10000",
        "```",
        "",
    ]
    return "\n".join(lines)


def utc_now_label() -> str:
    """UTC timestamp label ``YYYY-MM-DD HH:MM:SSZ``."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def combined_stream_url(symbol: str, *, ws_url: str = DEFAULT_WS_URL) -> str:
    """Trade-first combined URL (depth@100ms + trade). Public, no API key."""
    sym = symbol.lower()
    return f"{ws_url}?streams={sym}@trade/{sym}@depth@100ms"


def build_audit_results(
    *,
    acc: ProbeAccumulator,
    offset_samples: Sequence[ClockSample],
    reconnects: int,
    duration_s: float,
    symbol: str,
    hostname: str,
    date_utc: str,
    n_depth_requested: int,
    streams: Sequence[str],
    gap_threshold_ns: int = GAP_THRESHOLD_NS,
    machine_context: str = "red de investigación doméstica, no colocación",
) -> dict[str, Any]:
    """Assemble the JSON-serialisable audit payload from an accumulator."""
    clock = offset_summary(offset_samples)
    offset = 0 if math.isnan(clock["mean_ns"]) else round(clock["mean_ns"])
    depth_raw, depth_adj = event_latencies(acc.depth, offset)
    trade_raw, trade_adj = event_latencies(acc.trades, offset)
    e_times = [e.ts_exchange_e_ns for e in acc.depth]
    mono_times = [e.ts_recv_mono_ns for e in acc.depth]
    e_gaps = inter_arrival_ns(e_times)
    local_gaps = inter_arrival_ns(mono_times)
    return {
        "date_utc": date_utc,
        "hostname": hostname,
        "machine_context": machine_context,
        "symbol": symbol.upper(),
        "streams": list(streams),
        "n_depth_requested": n_depth_requested,
        "n_depth": acc.n_depth,
        "n_trade": acc.n_trade,
        "duration_s": duration_s,
        "reconnects": reconnects,
        "sequence_gaps": acc.sequence_gaps,
        "decode_errors": acc.decode_errors,
        "gap_threshold_ms": gap_threshold_ns / NS_PER_MS,
        "clock_offset": clock,
        "clock_offset_formula": "offset_ns = local_after_ns - serverTime_ns (local - server)",
        "mean_offset_ns": clock["mean_ns"],
        "depth_raw": distribution(depth_raw),
        "depth_adjusted": distribution(depth_adj),
        "trade_raw": distribution(trade_raw),
        "trade_adjusted": distribution(trade_adj),
        "depth_interarrival_exchange": distribution(e_gaps),
        "depth_interarrival_local": distribution(local_gaps),
        "depth_gaps_exchange": n_gaps_over(e_gaps, gap_threshold_ns),
        "depth_gaps_local": n_gaps_over(local_gaps, gap_threshold_ns),
        "institutional_benchmarks": [dict(row) for row in INSTITUTIONAL_BENCHMARKS],
        "duration_ns": int(duration_s * NS_PER_S) if duration_s else 0,
    }


def _default_ws_connect(url: str) -> AbstractAsyncContextManager[AsyncIterable[str | bytes]]:
    """Public Binance WS. No API key. Does not send orders."""
    return connect(url, open_timeout=60.0, ping_interval=20.0, ping_timeout=20.0)


def depth_stream_url(
    symbol: str, *, ws_url: str = DEFAULT_WS_URL, include_trades: bool = True
) -> str:
    """Public combined-stream URL. Trade-first when trades are included."""
    if include_trades:
        return combined_stream_url(symbol, ws_url=ws_url)
    return f"{ws_url}?streams={symbol.lower()}@depth@100ms"


def _default_max_seconds(n_events: int) -> float:
    """Safety cap: depth@100ms is ~10 msgs/s → 1000 s for 10k, plus slack."""
    return max(float(n_events) * 0.3 + 120.0, 180.0)


async def _collect_with_reconnect(
    *,
    connect_ws: WsConnect,
    url: str,
    acc: ProbeAccumulator,
    n_events: int,
    on_progress: Callable[[int], Awaitable[None]],
    sleep: Callable[[float], Awaitable[None]],
    cap_s: float,
    t0: float,
    max_reconnects: int,
) -> int:
    """Drain public WS frames until ``n_events`` depth messages or caps."""
    reconnects = 0
    while acc.n_depth < n_events:
        if time.perf_counter() - t0 >= cap_s:
            log.warning("latency_audit_timeout", n_depth=acc.n_depth, n_events=n_events)
            return reconnects
        if reconnects > max_reconnects:
            log.warning("latency_audit_max_reconnects", reconnects=reconnects)
            return reconnects
        try:
            async with connect_ws(url) as ws:
                await drain_until(aiter(ws), acc, n_depth=n_events, on_progress=on_progress)
        except ConnectionClosed as exc:
            log.warning("latency_audit_ws_closed", code=exc.code, reason=str(exc.reason))
        except (TimeoutError, OSError) as exc:
            log.warning("latency_audit_ws_error", error=repr(exc))
        if acc.n_depth >= n_events:
            return reconnects
        delay = reconnect_delay(reconnects)
        reconnects += 1
        log.warning("latency_audit_reconnect", attempt=reconnects, backoff_s=delay)
        await sleep(delay)
    return reconnects


async def run_latency_audit(
    *,
    symbol: str = DEFAULT_SYMBOL,
    n_events: int = DEFAULT_N_EVENTS,
    rest_url: str = DEFAULT_REST_URL,
    ws_url: str = DEFAULT_WS_URL,
    include_trades: bool = True,
    offset_samples: int = OFFSET_SAMPLES_PER_PHASE,
    gap_threshold_ns: int = GAP_THRESHOLD_NS,
    timeout_s: float = DEFAULT_HTTP_TIMEOUT_S,
    max_reconnects: int = DEFAULT_MAX_RECONNECTS,
    max_seconds: float | None = None,
    hostname: str = "unknown",
    machine_context: str = MACHINE_CONTEXT,
    ws_connect: WsConnect | None = None,
    http_client: httpx.AsyncClient | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """Collect ``n_events`` public depth frames and clock-offset samples.

    Does **not** place orders. Market-data WebSocket + ``GET /fapi/v1/time`` only.
    """
    if n_events < 1:
        msg = "n_events must be >= 1"
        raise ValueError(msg)
    connect_ws = _default_ws_connect if ws_connect is None else ws_connect
    url = depth_stream_url(symbol, ws_url=ws_url, include_trades=include_trades)
    cap_s = _default_max_seconds(n_events) if max_seconds is None else max_seconds
    acc = ProbeAccumulator()
    offsets: list[ClockSample] = []
    mid_sampled = False
    streams = [f"{symbol.lower()}@depth@100ms"]
    if include_trades:
        streams.append(f"{symbol.lower()}@trade")
    t0 = time.perf_counter()
    date_utc = utc_now_label()
    last_logged = 0

    async with AsyncExitStack() as stack:
        client = http_client
        if client is None:
            client = await stack.enter_async_context(
                httpx.AsyncClient(base_url=rest_url, timeout=timeout_s)
            )
        offsets.extend(await sample_server_time_n(client, n=offset_samples, phase="start"))

        async def on_progress(n_depth: int) -> None:
            nonlocal mid_sampled, last_logged
            step = max(n_events // 10, 1)
            if n_depth >= last_logged + step:
                last_logged = n_depth
                log.info("latency_audit_progress", n_depth=n_depth, n_events=n_events)
            if mid_sampled or n_depth < max(n_events // 2, 1):
                return
            mid_sampled = True
            offsets.extend(await sample_server_time_n(client, n=offset_samples, phase="mid"))

        reconnects = await _collect_with_reconnect(
            connect_ws=connect_ws,
            url=url,
            acc=acc,
            n_events=n_events,
            on_progress=on_progress,
            sleep=sleep,
            cap_s=cap_s,
            t0=t0,
            max_reconnects=max_reconnects,
        )

        offsets.extend(await sample_server_time_n(client, n=offset_samples, phase="end"))

    duration_s = time.perf_counter() - t0
    return build_audit_results(
        acc=acc,
        offset_samples=offsets,
        reconnects=reconnects,
        duration_s=duration_s,
        symbol=symbol,
        hostname=hostname,
        date_utc=date_utc,
        n_depth_requested=n_events,
        streams=streams,
        gap_threshold_ns=gap_threshold_ns,
        machine_context=machine_context,
    )
