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
from order_flow.ingestion.sync import (
    MAX_MISMATCH_DETAILS,
    HonestyReport,
    compare_top_levels,
)
from order_flow.orderbook.errors import SequenceGapError
from order_flow.utils.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path

log = get_logger(__name__)

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_DURATION_S = 60.0
DEFAULT_HONESTY_LEVELS = 20
CATCH_UP_TIMEOUT_S = 5.0
HONESTY_MISMATCH_WARN = 0.10
#: Holgura máxima del replay alineado, en batches (deltas `@depth@100ms`).
#: El replay para en el primer `u >= rest.lastUpdateId`; como cada batch es
#: atómico, el overshoot es necesariamente < 1 batch (0 si `u == R` exacto).
#: Medir en IDs sería flaky: un batch cubre cientos/miles de IDs en BTCUSDT.
HONESTY_MAX_SLACK_BATCHES = 1
#: Backstop de corrupción para BTCUSDT (no dust: minQty 0.001; muy por debajo
#: del run corrupto observado de 2.525). La comparación alineada debería ser
#: ~exacta; tunable por símbolo.
MAX_HONESTY_QTY_DISCREPANCY_BTC = 0.5


def live_duration_s(default: float = DEFAULT_DURATION_S) -> float:
    """``BINANCE_LIVE_SECONDS`` env override used by the integration test and CLI."""
    raw = os.environ.get("BINANCE_LIVE_SECONDS")
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def split_window_counters(*, queued: int, applied_total: int) -> dict[str, int]:
    """Desglose ventana/total para el reporte (una sola fuente de verdad).

    - ``queued``: deltas consumidos de la cola durante la ventana (no se infla
      con nada posterior: es el contador de la ventana).
    - ``applied_total``: mutaciones al libro hasta el cierre de la ventana.
    - ``in_flight``: aplicados y/o encolados pero no consumidos al expirar el
      timeout (p.ej. 582 - 577 = 5 en el run 2026-09-21). Todo delta aplicado
      se encola exactamente una vez, así que ``applied_total >= queued``;
      el ``max`` es solo defensivo.
    """
    return {
        "queued": queued,
        "applied_total": applied_total,
        "in_flight": max(0, applied_total - queued),
    }


async def _fetch_rest_snapshot(feed: BinanceFuturesFeed) -> BookSnapshot:
    """Un ``GET /fapi/v1/depth`` parseado como :class:`BookSnapshot`."""
    async with httpx.AsyncClient(base_url=feed.rest_url, timeout=feed.timeout_s) as client:
        response = await client.get(
            DEPTH_SNAPSHOT_PATH, params={"symbol": feed.symbol, "limit": feed.snapshot_limit}
        )
        response.raise_for_status()
        return parse_depth_snapshot(orjson.loads(response.content), feed.symbol)


async def _fetch_rest_newer_than(
    feed: BinanceFuturesFeed, frozen_id: int
) -> tuple[BookSnapshot, bool]:
    """Snapshot REST con ``lastUpdateId > frozen_id`` o ``(rest, True)`` si stale.

    Un solo reintento fresco: si REST sigue más viejo que el freeze, comparar
    sería mirar al pasado y se reporta ``stale`` (no concluyente, no mismatch).
    """
    rest = await _fetch_rest_snapshot(feed)
    if rest.last_update_id <= frozen_id:
        rest = await _fetch_rest_snapshot(feed)
        if rest.last_update_id <= frozen_id:
            log.warning(
                "honesty_stale_rest",
                symbol=feed.symbol,
                frozen_id=frozen_id,
                rest_id=rest.last_update_id,
            )
            return rest, True
    return rest, False


async def _honesty_snapshot(feed: BinanceFuturesFeed, *, levels: int) -> HonestyReport:
    """REST top-N vs copia congelada del libro, alineada por ``lastUpdateId``.

    Método (copia+replay, mundo parado para el libro vivo):

    1. Congelar la mutación (el pump WS sigue bufferizando) y copiar el libro
       en ``L0``.
    2. ``GET /fapi/v1/depth`` → ``R``. Si ``R.lastUpdateId <= L0`` el snapshot
       REST es más viejo que el freeze: un reintento fresco; si sigue viejo se
       reporta ``stale=True`` (no concluyente, no es mismatch).
    3. Re-jugar los deltas bufferizados **sobre la copia** hasta el primer
       ``u >= R.lastUpdateId`` (regla Futures ``U <= R <= u`` vía
       ``OrderBook.apply_delta``). Registrar ``delta_ids = L_alin - R`` (≈0).
    4. Comparar la copia alineada contra ``R``. Timeout o gap durante el replay
       → ``stale=True`` (no concluyente).
    5. Unfreeze (la ventana se descarta: el helper detiene el feed después).

    Binance no publica checksum de libro; esto elimina la carrera del método
    anterior (comparar el libro vivo sin pausar el feed).
    """
    feed.freeze_for_honesty()
    try:
        frozen_id = feed.book.last_update_id
        if frozen_id is None:
            msg = "honesty: book never synced (no snapshot applied)"
            raise RuntimeError(msg)
        book_copy = feed.copy_book()
        rest, stale_rest = await _fetch_rest_newer_than(feed, frozen_id)
        if stale_rest:
            return compare_top_levels(
                book_copy,
                rest,
                levels=levels,
                last_update_id_aligned=frozen_id,
                delta_ids=frozen_id - rest.last_update_id,
                stale=True,
                batches_replayed=0,
            )
        seen: list[BookDelta] = []
        applied_idx = 0
        deadline = time.monotonic() + CATCH_UP_TIMEOUT_S
        while True:
            aligned_id = book_copy.last_update_id
            if aligned_id is not None and aligned_id >= rest.last_update_id:
                break
            if time.monotonic() >= deadline:
                log.warning(
                    "honesty_catch_up_timeout",
                    symbol=feed.symbol,
                    frozen_id=frozen_id,
                    rest_id=rest.last_update_id,
                    aligned_id=aligned_id,
                )
                return compare_top_levels(
                    book_copy,
                    rest,
                    levels=levels,
                    last_update_id_aligned=aligned_id,
                    stale=True,
                    batches_replayed=applied_idx,
                )
            seen.extend(feed.drain_frozen_deltas())
            progressed = False
            while applied_idx < len(seen):
                delta = seen[applied_idx]
                applied_idx += 1
                try:
                    book_copy.apply_delta(delta)
                except SequenceGapError as exc:
                    log.warning(
                        "honesty_replay_gap", symbol=feed.symbol, error=str(exc), stale=True
                    )
                    return compare_top_levels(
                        book_copy,
                        rest,
                        levels=levels,
                        last_update_id_aligned=book_copy.last_update_id,
                        stale=True,
                        batches_replayed=applied_idx,
                    )
                progressed = True
                if (
                    book_copy.last_update_id is not None
                    and book_copy.last_update_id >= rest.last_update_id
                ):
                    break
            if not progressed:
                await asyncio.sleep(0.05)
        aligned_id = book_copy.last_update_id
        if aligned_id is None:
            msg = "honesty: aligned copy lost last_update_id"
            raise RuntimeError(msg)
        return compare_top_levels(
            book_copy,
            rest,
            levels=levels,
            last_update_id_aligned=aligned_id,
            delta_ids=aligned_id - rest.last_update_id,
            batches_replayed=applied_idx,
        )
    finally:
        leftover = feed.unfreeze_for_honesty()
        if leftover:
            log.info("honesty_unfreeze", symbol=feed.symbol, discarded_deltas=len(leftover))


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
    # Foto de contadores al cierre de la ventana, ANTES de honesty: el feed
    # sigue corriendo unos ms hasta el freeze, pero `stats.deltas_applied`
    # solo crece ahí (libro), mientras `n_delta` ya quedó fijo (cola). La
    # diferencia es `in_flight` (aplicados/encolados no consumidos al timeout),
    # no descarte ni corrupción. Durante honesty el freeze congela los stats.
    window = split_window_counters(queued=n_delta, applied_total=feed.stats.deltas_applied)
    try:
        # El feed sigue corriendo durante honesty a propósito: el pump WS debe
        # seguir recibiendo para que el replay alcance rest.lastUpdateId. Los
        # contadores no derivan porque el freeze congela libro, validador y
        # stats (depth+trades).
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
            "last_update_id_local_at_compare": honesty.last_update_id_local,
            "last_update_id_rest": honesty.last_update_id_rest,
            "last_update_id_aligned": honesty.last_update_id_aligned,
            "delta_ids": honesty.delta_ids,
            "overshoot": honesty.overshoot,
            "stale": honesty.stale,
            "frozen": True,
            "batches_replayed": honesty.batches_replayed,
            "mismatch_details": [
                {
                    "side": detail.side,
                    "price": detail.price,
                    "qty_local": detail.qty_local,
                    "qty_rest": detail.qty_rest,
                    "abs_diff": detail.abs_diff,
                }
                for detail in honesty.mismatch_details
            ],
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
        # Tripleta del mismo instante (cierre de ventana): queued + in_flight
        # == applied_total. `feed.stats` no se relee aquí porque entre la foto
        # y el freeze el feed aún aplica algún delta suelto.
        "deltas_applied": window["applied_total"],
        "deltas_in_flight": window["in_flight"],
        "trades": feed.stats.trades,
        "events_seen": {"snapshots": n_snap, "deltas": n_delta, "trades": n_trade},
        "latency_ns": latency,
        "book_crossed": crossed,
        "book_synced": feed.book.is_synced,
        "n_levels": feed.book.n_levels,
        "honesty_levels": honesty_levels,
        "honesty": honesty_dict,
        "error": error,
        "venue_checksum": False,
        "rest_url": DEFAULT_REST_URL,
    }


def _format_mismatch_details(details: list[dict[str, Any]]) -> list[str]:
    """Tabla markdown con el detalle por mismatch (ya recortado a cap 20)."""
    if not details:
        return []
    lines = [
        "### Detalle de mismatches (cap 20)",
        "",
        "| side | price | qty_local | qty_rest | abs_diff |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in details[:MAX_MISMATCH_DETAILS]:
        qty_local = item.get("qty_local")
        qty_local_txt = "—" if qty_local is None else f"{qty_local}"
        lines.append(
            f"| {item.get('side')} | {item.get('price')} | {qty_local_txt} | "
            f"{item.get('qty_rest')} | {item.get('abs_diff')} |"
        )
    lines.append("")
    return lines


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
        f"- Deltas aplicados al libro: **{report.get('deltas_applied', 0)}**",
        "- Deltas vistos en cola (ventana): "
        f"**{(report.get('events_seen') or {}).get('deltas', 0)}**",
        "- Deltas en vuelo al cerrar la ventana (aplicados/encolados no "
        f"consumidos): **{report.get('deltas_in_flight', 0)}**",
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
        f"`GET /fapi/v1/depth` vs top-{report.get('honesty_levels', 20)} de una copia",
        "congelada del libro local, alineada por `lastUpdateId` (replay de los diffs",
        "bufferizados durante el freeze hasta `u >= rest.lastUpdateId`).",
        "",
    ]
    if honesty:
        lines.extend(
            [
                "- lastUpdateId local al comparar: "
                f"`{honesty.get('last_update_id_local_at_compare')}`",
                f"- lastUpdateId REST: `{honesty.get('last_update_id_rest')}`",
                f"- lastUpdateId alineado: `{honesty.get('last_update_id_aligned')}`",
                f"- delta_ids (`alineado - REST`, debe ser ~0): `{honesty.get('delta_ids')}`",
                "- batches_replayed "
                f"(≤ {HONESTY_MAX_SLACK_BATCHES}): `{honesty.get('batches_replayed')}`",
                f"- Stale / no concluyente: **{honesty.get('stale', False)}**",
                f"- Niveles comparados: **{honesty.get('compared', 0)}**",
                f"- Coincidencias: **{honesty.get('matches', 0)}**",
                f"- Mismatches: **{honesty.get('mismatches', 0)}**",
                f"- Máxima discrepancia de qty: **{honesty.get('max_qty_discrepancy', 0)}**",
                f"- Libro cruzado: **{report.get('book_crossed', False)}**",
                f"- Niveles (bid, ask): `{report.get('n_levels')}`",
                "",
            ]
        )
        lines.extend(_format_mismatch_details(honesty.get("mismatch_details") or []))
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


def _honesty_verdict(
    honesty: dict[str, Any],
    *,
    crossed: bool,
    gaps: int,
    resyncs: int,
    deltas: int,
) -> str:
    """Veredicto literal del honesty check (sin eufemismos)."""
    mismatches = int(honesty.get("mismatches") or 0)
    compared = int(honesty.get("compared") or 0)
    max_disc = float(honesty.get("max_qty_discrepancy") or 0.0)
    stale = bool(honesty.get("stale"))
    delta_ids = honesty.get("delta_ids")
    batches = int(honesty.get("batches_replayed") or 0)
    mismatch_rate = (mismatches / compared) if compared else 1.0
    if deltas < 1:
        verdict = (
            "Veredicto: INCONCLUSIVE — no se aplicaron deltas; no hay evidencia "
            "de que el pipeline sea honesto."
        )
    elif stale:
        verdict = (
            "Veredicto: INCONCLUSIVE — comparación REST stale / no concluyente "
            "(REST más viejo que el freeze, timeout de catch-up o gap en el replay)."
        )
    elif crossed:
        verdict = "Veredicto: FAIL — libro local cruzado; investigar el resync."
    elif mismatch_rate >= HONESTY_MISMATCH_WARN:
        verdict = (
            f"Veredicto: FAIL — {mismatches}/{compared} mismatches de top-N "
            f"(tasa {mismatch_rate:.2%} ≥ warn {HONESTY_MISMATCH_WARN:.2%}) con "
            "comparación congelada y alineada: libro corrupto o regresión."
        )
    elif max_disc > MAX_HONESTY_QTY_DISCREPANCY_BTC:
        verdict = (
            f"Veredicto: FAIL — discrepancia máxima de qty {max_disc} BTC > "
            f"{MAX_HONESTY_QTY_DISCREPANCY_BTC} BTC con comparación alineada."
        )
    elif delta_ids is None or delta_ids < 0 or batches > HONESTY_MAX_SLACK_BATCHES:
        verdict = (
            "Veredicto: FAIL — alineación fuera de holgura "
            f"(delta_ids={delta_ids}, batches_replayed={batches})."
        )
    elif gaps != resyncs:
        verdict = f"Veredicto: FAIL — {gaps} gaps con {resyncs} resyncs: hay gaps sin resolver."
    elif gaps:
        verdict = (
            f"Veredicto: PASS con resyncs — {deltas} deltas aplicados, {gaps} gap(s) "
            "resincronizados según protocolo; honesty top-N dentro de umbrales "
            "(red local, no SLA)."
        )
    else:
        verdict = (
            f"Veredicto: PASS — {deltas} deltas sin gaps y honesty top-N "
            f"{mismatches}/{compared} dentro de umbrales (red local, no SLA)."
        )
    return verdict


def _conclusion(report: dict[str, Any]) -> str:
    if report.get("error"):
        return (
            "Veredicto: ERROR — la corrida no pudo completarse (ver error). "
            "Re-ejecutar cuando la red o Binance estén disponibles."
        )
    honesty = report.get("honesty") or {}
    return _honesty_verdict(
        honesty,
        crossed=bool(report.get("book_crossed")),
        gaps=int(report.get("gaps") or 0),
        resyncs=int(report.get("resyncs") or 0),
        deltas=int(report.get("deltas_applied") or 0),
    )
