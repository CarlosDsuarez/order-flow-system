"""Live honesty run against Binance USD-M Futures (no disk writes).

Used by ``scripts/validate_live_l2.py`` and the integration test. The feed publishes
to an in-memory queue only; Parquet is out of scope for this helper.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import orjson

from order_flow.ingestion.binance_futures import (
    DEFAULT_REST_URL,
    DEPTH_SNAPSHOT_PATH,
    BinanceFuturesFeed,
    parse_depth_snapshot,
)
from order_flow.ingestion.events import BookDelta, BookSnapshot, Trade
from order_flow.ingestion.sync import HonestyReport, compare_top_levels
from order_flow.utils.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path

log = get_logger(__name__)

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_DURATION_S = 60.0
DEFAULT_HONESTY_LEVELS = 20
CATCH_UP_TIMEOUT_S = 5.0
HONESTY_MISMATCH_WARN = 0.25


def live_duration_s(default: float = DEFAULT_DURATION_S) -> float:
    """``BINANCE_LIVE_SECONDS`` env override used by the integration test and CLI."""
    raw = os.environ.get("BINANCE_LIVE_SECONDS")
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


async def _honesty_snapshot(feed: BinanceFuturesFeed, *, levels: int) -> HonestyReport:
    """REST top-N vs local book after catching up on ``lastUpdateId``.

    Method: fetch ``GET /fapi/v1/depth`` while the feed is still applying; wait until
    ``book.last_update_id >= snapshot.lastUpdateId`` (or ``CATCH_UP_TIMEOUT_S``). Then
    compare top-``levels`` quantities keyed by price.

    Residual race: Binance has no depth checksum. One or more ``@depth@100ms`` diffs may
    land between the wait and the comparison, so a handful of quantity mismatches does
    not by itself prove a corrupt book. A large mismatch rate or a crossed book does.
    """
    async with httpx.AsyncClient(base_url=feed.rest_url, timeout=feed.timeout_s) as client:
        response = await client.get(
            DEPTH_SNAPSHOT_PATH, params={"symbol": feed.symbol, "limit": feed.snapshot_limit}
        )
        response.raise_for_status()
        rest = parse_depth_snapshot(orjson.loads(response.content), feed.symbol)
    deadline = time.monotonic() + CATCH_UP_TIMEOUT_S
    while time.monotonic() < deadline:
        local_id = feed.book.last_update_id
        if local_id is not None and local_id >= rest.last_update_id:
            break
        await asyncio.sleep(0.05)
    return compare_top_levels(feed.book, rest, levels=levels)


async def run_live_validation(
    *,
    symbol: str = DEFAULT_SYMBOL,
    duration_s: float | None = None,
    honesty_levels: int = DEFAULT_HONESTY_LEVELS,
) -> dict[str, Any]:
    """Run the feed for ``duration_s`` seconds and return a structured honesty report."""
    seconds = DEFAULT_DURATION_S if duration_s is None else duration_s
    feed = BinanceFuturesFeed(symbol)
    started = time.monotonic()
    n_snap = 0
    n_delta = 0
    n_trade = 0
    honesty: HonestyReport | None = None
    error: str | None = None
    try:
        await feed.start()
        async with asyncio.timeout(seconds):
            while True:
                event = await feed.queue.get()
                if isinstance(event, BookSnapshot):
                    n_snap += 1
                elif isinstance(event, BookDelta):
                    n_delta += 1
                elif isinstance(event, Trade):
                    n_trade += 1
    except TimeoutError:
        pass
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        log.error("live_validation_failed", symbol=symbol, error=error)
    elapsed = time.monotonic() - started
    try:
        honesty = await _honesty_snapshot(feed, levels=honesty_levels)
    except Exception as exc:
        if error is None:
            error = f"honesty: {type(exc).__name__}: {exc}"
        log.error("honesty_check_failed", symbol=symbol, error=str(exc))
    await feed.stop()
    latency = feed.stats.latency_summary()
    crossed = feed.book.is_crossed()
    honesty_dict: dict[str, Any] | None
    if honesty is None:
        honesty_dict = None
    else:
        honesty_dict = {
            "compared": honesty.compared,
            "matches": honesty.matches,
            "mismatches": honesty.mismatches,
            "max_qty_discrepancy": honesty.max_qty_discrepancy,
            "last_update_id_local": honesty.last_update_id_local,
            "last_update_id_rest": honesty.last_update_id_rest,
        }
    return {
        "date_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "symbol": feed.symbol,
        "duration_s_requested": seconds,
        "duration_s_elapsed": elapsed,
        "gaps": feed.stats.gaps,
        "resyncs": feed.stats.resyncs,
        "reconnects": feed.stats.reconnects,
        "rest_429s": feed.stats.rest_429s,
        "snapshots_applied": feed.stats.snapshots_applied,
        "deltas_applied": feed.stats.deltas_applied,
        "trades": feed.stats.trades,
        "events_seen": {"snapshots": n_snap, "deltas": n_delta, "trades": n_trade},
        "latency_ns": latency,
        "book_crossed": crossed,
        "n_levels": feed.book.n_levels,
        "honesty_levels": honesty_levels,
        "honesty": honesty_dict,
        "error": error,
        "venue_checksum": False,
        "rest_url": DEFAULT_REST_URL,
    }


def format_live_report_md(report: dict[str, Any]) -> str:
    """Spanish markdown report for ``docs/ingestion/live-validation.md``."""
    honesty = report.get("honesty") or {}
    latency = report.get("latency_ns") or {}
    error = report.get("error")
    status = "bloqueado / no ejecutado" if error else "ejecutado"
    lines = [
        "# Validación en vivo — Binance USD-M L2",
        "",
        "Resultados de un run de honestidad contra **Binance USD-M Futures** en esta máquina.",
        "No es un SLA universal: latencia y reconexiones dependen de la red local.",
        "",
        f"- **Fecha (UTC):** {report.get('date_utc', '—')}",
        f"- **Símbolo:** `{report.get('symbol', 'BTCUSDT')}`",
        f"- **Duración pedida:** {report.get('duration_s_requested', 60)} s",
        f"- **Duración transcurrida:** {report.get('duration_s_elapsed', 0):.1f} s",
        f"- **Estado:** {status}",
        "",
        "## Contadores",
        "",
        f"- Gaps de secuencia (`pu != u` previo): **{report.get('gaps', 0)}**",
        f"- Resyncs (nuevo snapshot REST): **{report.get('resyncs', 0)}**",
        f"- Reconexiones WS: **{report.get('reconnects', 0)}**",
        f"- HTTP 429: **{report.get('rest_429s', 0)}**",
        f"- Snapshots aplicados: **{report.get('snapshots_applied', 0)}**",
        f"- Deltas aplicados: **{report.get('deltas_applied', 0)}**",
        f"- Trades (aggTrade): **{report.get('trades', 0)}**",
        "",
        "## Latencia observada (`ts_recv_ns - ts_event_ns`)",
        "",
        "El reloj de evento es el campo oficial `E` (event time, ms) del payload `depthUpdate`.",
        "",
        f"- n = {int(latency.get('count') or 0)}",
        f"- mean = {_ns_to_ms(latency.get('mean'))} ms",
        f"- p50 = {_ns_to_ms(latency.get('p50'))} ms",
        f"- p99 = {_ns_to_ms(latency.get('p99'))} ms",
        f"- min = {_ns_to_ms(latency.get('min'))} ms",
        f"- max = {_ns_to_ms(latency.get('max'))} ms",
        "",
        "## Checksum / comparación REST",
        "",
        "Binance USD-M **no publica checksum** del libro. Sustituto: snapshot REST",
        f"`GET /fapi/v1/depth` vs top-{report.get('honesty_levels', 20)} local",
        "(por precio). Carrera residual: 1-N diffs de 100 ms pueden caer entre el catch-up",
        "de `lastUpdateId` y la copia de niveles.",
        "",
    ]
    if honesty:
        lines.extend(
            [
                f"- lastUpdateId local: `{honesty.get('last_update_id_local')}`",
                f"- lastUpdateId REST: `{honesty.get('last_update_id_rest')}`",
                f"- Niveles comparados: **{honesty.get('compared', 0)}**",
                f"- Coincidencias: **{honesty.get('matches', 0)}**",
                f"- Mismatches: **{honesty.get('mismatches', 0)}**",
                f"- Máxima discrepancia de qty: **{honesty.get('max_qty_discrepancy', 0)}**",
                f"- Libro cruzado: **{report.get('book_crossed', False)}**",
                f"- Niveles (bid, ask): `{report.get('n_levels')}`",
                "",
            ]
        )
    else:
        lines.extend(["- Comparación no disponible.", ""])
    if error:
        lines.extend(["## Error", "", "```", str(error), "```", ""])
    lines.extend(["## Conclusión", "", _conclusion(report), ""])
    lines.extend(
        [
            "## Cómo repetir",
            "",
            "```bash",
            "RUN_INTEGRATION=1 uv run pytest tests/integration/test_binance_live_l2.py -v -s",
            "# o",
            "uv run python scripts/validate_live_l2.py --symbol BTCUSDT --seconds 60",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def write_live_report(report: dict[str, Any], path: Path) -> None:
    """Write :func:`format_live_report_md` to ``path``, creating parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_live_report_md(report), encoding="utf-8")


def _ns_to_ms(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "-"
    number = float(value)
    if math.isnan(number):
        return "-"
    return f"{number / 1_000_000.0:.3f}"


def _conclusion(report: dict[str, Any]) -> str:
    if report.get("error"):
        return (
            "La corrida **no pudo completarse** (ver error). El conector está implementado "
            "y el test existe; hay que re-ejecutar cuando la red o Binance estén disponibles."
        )
    honesty = report.get("honesty") or {}
    mismatches = int(honesty.get("mismatches") or 0)
    compared = int(honesty.get("compared") or 0)
    crossed = bool(report.get("book_crossed"))
    gaps = int(report.get("gaps") or 0)
    deltas = int(report.get("deltas_applied") or 0)
    mismatch_rate = (mismatches / compared) if compared else 1.0
    if deltas < 1:
        return "No se aplicaron deltas; no hay evidencia suficiente de que el pipeline sea honesto."
    if crossed:
        return (
            "El libro local quedó **cruzado**. No es lo bastante honesto para construir encima "
            "sin investigar el resync."
        )
    if mismatch_rate > HONESTY_MISMATCH_WARN:
        return (
            f"Demasiados mismatches de top-N ({mismatches}/{compared}). Puede ser carrera residual "
            "o un libro corrupto; repetir el run antes de fiarse."
        )
    if gaps:
        return (
            f"El pipeline aplicó {deltas} deltas y resincronizó {gaps} gap(s) según el "
            "protocolo oficial. La comparación REST es aceptable para investigación; "
            "no es un SLA de producción."
        )
    return (
        f"El pipeline aplicó {deltas} deltas sin gaps de secuencia y la comparación REST top-N "
        f"quedó en {mismatches}/{compared} mismatches (carrera residual esperada). "
        "**Es lo bastante honesto para construir la siguiente capa encima**, con la salvedad "
        "de que esto es la red de esta máquina, no un SLA."
    )
