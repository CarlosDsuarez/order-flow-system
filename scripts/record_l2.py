"""CLI: record Binance USD-M L2 (+ aggTrade) to Parquet.

The feed itself never requires disk. This orchestrator subscribes to ``feed.queue``,
applies events to a local :class:`~order_flow.orderbook.book.OrderBook`, and writes
snapshots / deltas / trades via :class:`~order_flow.storage.parquet.ParquetWriter`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import orjson

from order_flow.ingestion.binance_futures import (
    DEFAULT_REST_URL,
    DEPTH_SNAPSHOT_PATH,
    EXCHANGE,
    BinanceFuturesFeed,
    parse_depth_snapshot,
)
from order_flow.ingestion.events import BookDelta, BookSnapshot, MarketEvent, Trade
from order_flow.ingestion.latency_audit import sample_server_time
from order_flow.ingestion.live import CATCH_UP_TIMEOUT_S, DEFAULT_HONESTY_LEVELS
from order_flow.ingestion.sync import compare_top_levels
from order_flow.orderbook.book import OrderBook
from order_flow.orderbook.errors import SequenceGapError
from order_flow.storage.parquet import ParquetWriter, read_events, snapshots_from_frame
from order_flow.storage.reconstruct import reconstruct_book
from order_flow.storage.report import capture_stats, format_capture_report
from order_flow.utils.logging import configure_logging, get_logger

log = get_logger(__name__)

DEFAULT_CAPTURE_S = 300.0
DEFAULT_SNAPSHOT_INTERVAL_S = 1.0
DEFAULT_FLUSH_INTERVAL_S = 2.0
DEFAULT_BUFFER_SIZE = 2_000


def capture_seconds(default: float = DEFAULT_CAPTURE_S) -> float:
    """``CAPTURE_SECONDS`` env override (phase-3 live capture)."""
    raw = os.environ.get("CAPTURE_SECONDS")
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record Binance USD-M Futures L2 depth (+ aggTrade) to partitioned Parquet. "
            "The feed never writes disk; this script is the orchestrator."
        )
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="USD-M perpetual symbol")
    parser.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="Capture duration (default: CAPTURE_SECONDS or 300)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Parquet root. If omitted, events stay in memory (feed never needs disk).",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=DEFAULT_SNAPSHOT_INTERVAL_S,
        help="Seconds between full in-memory LOB snapshots (0 disables periodic snapshots)",
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=DEFAULT_FLUSH_INTERVAL_S,
        help="Seconds between Parquet flushes (in addition to buffer_size)",
    )
    parser.add_argument("--buffer-size", type=int, default=DEFAULT_BUFFER_SIZE)
    parser.add_argument("--honesty-levels", type=int, default=DEFAULT_HONESTY_LEVELS)
    parser.add_argument(
        "--rest-probe-every",
        type=float,
        default=0.0,
        help=(
            "Seconds between REST /fapi/v1/depth probes written to rest_probes.jsonl "
            "(0 disables; audit sidecar, does not change Parquet schema)"
        ),
    )
    parser.add_argument(
        "--rest-probe-levels",
        type=int,
        default=DEFAULT_HONESTY_LEVELS,
        help="Top-N bid/ask levels stored in each REST probe (default: honesty-levels)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional markdown report path (Spanish)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def _apply(book: OrderBook, event: MarketEvent) -> None:
    if isinstance(event, BookSnapshot):
        book.apply_snapshot(event)
    elif isinstance(event, BookDelta):
        try:
            book.apply_delta(event)
        except SequenceGapError:
            book.mark_unsynced()


def _bbo(book: OrderBook) -> tuple[object, object]:
    return book.best_bid(), book.best_ask()


async def _honesty_vs_rest(
    feed: BinanceFuturesFeed, book: OrderBook, *, levels: int
) -> dict[str, Any]:
    """REST top-N vs the recorder book. Residual race: diffs may land during the GET."""
    async with httpx.AsyncClient(base_url=feed.rest_url, timeout=feed.timeout_s) as client:
        response = await client.get(
            DEPTH_SNAPSHOT_PATH, params={"symbol": feed.symbol, "limit": feed.snapshot_limit}
        )
        response.raise_for_status()
        rest = parse_depth_snapshot(orjson.loads(response.content), feed.symbol)
    deadline = time.monotonic() + CATCH_UP_TIMEOUT_S
    while time.monotonic() < deadline:
        local_id = book.last_update_id
        if local_id is not None and local_id >= rest.last_update_id:
            break
        await asyncio.sleep(0.05)
    report = compare_top_levels(book, rest, levels=levels)
    return {
        "compared": report.compared,
        "matches": report.matches,
        "mismatches": report.mismatches,
        "max_qty_discrepancy": report.max_qty_discrepancy,
        "last_update_id_local": report.last_update_id_local,
        "last_update_id_rest": report.last_update_id_rest,
        "book_crossed": book.is_crossed(),
        "is_synced": book.is_synced,
    }


def _mid_snapshot_consistency(root: Path, exchange: str, symbol: str) -> dict[str, Any]:
    """Replay at a mid-capture snapshot timestamp and compare BBO to that snapshot."""
    snaps = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    if snaps.height == 0:
        return {"ok": False, "reason": "no snapshots"}
    mid_idx = snaps.height // 2
    mid_ts = int(snaps["ts_event_ns"][mid_idx])
    rebuilt = reconstruct_book(root, mid_ts, exchange=exchange, symbol=symbol)
    nearest = (
        snaps.filter(snaps["ts_event_ns"] <= mid_ts).sort(["ts_event_ns", "last_update_id"]).tail(1)
    )
    expected = OrderBook()
    expected.apply_snapshot(snapshots_from_frame(nearest)[0])
    ok = _bbo(rebuilt) == _bbo(expected) and not rebuilt.is_crossed()
    bid, ask = rebuilt.best_bid(), rebuilt.best_ask()
    spread = (ask.price - bid.price) if bid is not None and ask is not None else None
    return {
        "ok": ok,
        "mid_ts_event_ns": mid_ts,
        "n_snapshots": snaps.height,
        "rebuilt_last_update_id": rebuilt.last_update_id,
        "spread": spread,
        "crossed": rebuilt.is_crossed(),
    }


def _end_reconstruct_match(
    root: Path, book: OrderBook, exchange: str, symbol: str
) -> dict[str, Any]:
    ts = book.last_update_ts_ns
    rebuilt = reconstruct_book(root, ts, exchange=exchange, symbol=symbol)
    n_bid, n_ask = book.n_levels
    levels = max(n_bid, n_ask, 1)
    live_depth = book.depth(min(levels, 5))
    replay_depth = rebuilt.depth(min(levels, 5))
    live_bid = live_depth[0][0].price if live_depth[0] else None
    live_ask = live_depth[1][0].price if live_depth[1] else None
    replay_bid = replay_depth[0][0].price if replay_depth[0] else None
    replay_ask = replay_depth[1][0].price if replay_depth[1] else None
    return {
        "ok": _bbo(rebuilt) == _bbo(book) and live_depth == replay_depth,
        "ts_event_ns": ts,
        "live_last_update_id": book.last_update_id,
        "replay_last_update_id": rebuilt.last_update_id,
        "live_bbo": [live_bid, live_ask],
        "replay_bbo": [replay_bid, replay_ask],
    }


def _probe_levels(snapshot: BookSnapshot, levels: int) -> dict[str, Any]:
    """Top-N price/qty pairs from a REST snapshot (audit sidecar, not Parquet)."""
    n = max(levels, 0)
    return {
        "bids": [[lvl.price, lvl.qty] for lvl in snapshot.bids[:n]],
        "asks": [[lvl.price, lvl.qty] for lvl in snapshot.asks[:n]],
    }


async def _fetch_rest_snapshot(feed: BinanceFuturesFeed) -> BookSnapshot:
    async with httpx.AsyncClient(base_url=feed.rest_url, timeout=feed.timeout_s) as client:
        response = await client.get(
            DEPTH_SNAPSHOT_PATH, params={"symbol": feed.symbol, "limit": feed.snapshot_limit}
        )
        response.raise_for_status()
        return parse_depth_snapshot(orjson.loads(response.content), feed.symbol)


async def _append_clock_offset(
    path: Path, feed: BinanceFuturesFeed, phase: str, *, n: int = 1
) -> None:
    """Append ``n`` GET /fapi/v1/time samples to clock_offsets.jsonl."""
    async with httpx.AsyncClient(base_url=feed.rest_url, timeout=feed.timeout_s) as client:
        for i in range(n):
            label = phase if n == 1 else f"{phase}_{i}"
            sample = await sample_server_time(client, phase=label)
            record = {
                "phase": label,
                "ts_local_before_ns": sample.local_before_ns,
                "ts_local_after_ns": sample.local_after_ns,
                "server_time_ns": sample.server_time_ns,
                "offset_ns": sample.offset_ns,
                "midpoint_offset_ns": sample.midpoint_offset_ns,
                "rtt_ns": sample.rtt_ns,
            }
            with path.open("a", encoding="utf-8") as handle:
                handle.write(orjson.dumps(record).decode() + "\n")


async def _rest_probe_loop(
    feed: BinanceFuturesFeed,
    out: Path,
    *,
    interval_s: float,
    levels: int,
    stop: asyncio.Event,
) -> None:
    """Write REST depth probes + clock offsets until ``stop`` is set."""
    probes_path = out / "rest_probes.jsonl"
    clock_path = out / "clock_offsets.jsonl"
    n = 0
    while not stop.is_set():
        phase = "start" if n == 0 else f"probe_{n}"
        try:
            await _append_clock_offset(clock_path, feed, phase, n=5 if n == 0 else 1)
            rest = await _fetch_rest_snapshot(feed)
            payload = {
                "ts_local_ns": time.time_ns(),
                "last_update_id": rest.last_update_id,
                "symbol": feed.symbol,
                "n_levels": levels,
                **_probe_levels(rest, levels),
            }
            with probes_path.open("a", encoding="utf-8") as handle:
                handle.write(orjson.dumps(payload).decode() + "\n")
            n += 1
            log.info(
                "rest_probe_written",
                last_update_id=rest.last_update_id,
                n=n,
                path=str(probes_path),
            )
        except Exception as exc:
            log.warning("rest_probe_failed", error=f"{type(exc).__name__}: {exc}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            continue


def _format_live_capture_md(meta: dict[str, Any]) -> str:
    stats_md = str(meta.get("capture_report") or "")
    honesty = meta.get("honesty") or {}
    end = meta.get("end_reconstruct") or {}
    mid = meta.get("mid_reconstruct") or {}
    compared = honesty.get("compared") or 0
    mismatch_rate = (honesty.get("mismatches", 0) / compared) if compared else None
    conclusion = "Captura utilizable para reconstruir el libro desde Parquet."
    if meta.get("error"):
        conclusion = f"La captura no terminó limpiamente: {meta['error']}"
    elif end.get("ok") is False:
        conclusion = "La reconstrucción al final NO coincidió con el libro en vivo."
    elif honesty and compared and mismatch_rate is not None and mismatch_rate > 0.5:
        conclusion = "Demasiados mismatches vs REST; revisar el protocolo o la carrera residual."
    lines = [
        "# Captura L2 en vivo (Parquet)",
        "",
        f"- Fecha UTC: {meta.get('date_utc')}",
        f"- Símbolo: `{meta.get('symbol')}`",
        f"- Duración pedida: {meta.get('duration_s_requested')} s "
        f"(elapsed {meta.get('duration_s_elapsed')} s)",
        f"- Directorio: `{meta.get('out')}`",
        f"- Intervalo de snapshots periódicos: {meta.get('snapshot_interval_s')} s",
        "",
        "## Volumen",
        "",
        stats_md,
        "",
        f"- Snapshots REST (cola): {meta.get('n_rest_snapshots')}",
        f"- Snapshots periódicos del LOB: {meta.get('n_periodic_snapshots')}",
        f"- Deltas aplicadas (cola): {meta.get('n_deltas')}",
        f"- Trades (cola): {meta.get('n_trades')}",
        f"- Gaps / resyncs / reconnects: {meta.get('gaps')} / {meta.get('resyncs')} / "
        f"{meta.get('reconnects')}",
        "",
        "## Reconstrucción",
        "",
        f"- Replay al final vs libro en vivo: {'OK' if end.get('ok') else end}",
        f"- Replay a mitad de captura vs snapshot persistido: {'OK' if mid.get('ok') else mid}",
        "",
        "## Honestidad vs REST (carrera residual)",
        "",
        "Binance USD-M no publica checksum de profundidad. Tras el GET `/fapi/v1/depth` "
        "pueden llegar diffs `@depth@100ms` antes de comparar. Un puñado de mismatches de "
        "cantidad no prueba un libro corrupto; un libro cruzado o una tasa alta sí.",
        "",
        f"- compared / matches / mismatches: {honesty.get('compared')} / "
        f"{honesty.get('matches')} / {honesty.get('mismatches')}",
        f"- max |Δqty|: {honesty.get('max_qty_discrepancy')}",
        f"- lastUpdateId local / REST: {honesty.get('last_update_id_local')} / "
        f"{honesty.get('last_update_id_rest')}",
        f"- libro cruzado: {honesty.get('book_crossed')}",
        "",
        "## Conclusión",
        "",
        conclusion,
        "",
    ]
    return "\n".join(lines)


async def _record(
    symbol: str,
    seconds: float,
    out: Path | None,
    *,
    snapshot_interval: float,
    flush_interval: float,
    buffer_size: int,
    honesty_levels: int,
    report_path: Path | None,
    rest_probe_every: float = 0.0,
    rest_probe_levels: int = DEFAULT_HONESTY_LEVELS,
) -> int:
    feed = BinanceFuturesFeed(symbol, dual_sockets=False)
    book = OrderBook(exchange=EXCHANGE, symbol=feed.symbol)
    writer: ParquetWriter | None = None
    if out is not None:
        out.mkdir(parents=True, exist_ok=True)
        writer = ParquetWriter(out, EXCHANGE, feed.symbol, buffer_size=buffer_size)
    n_snap = n_delta = n_trade = n_periodic = 0
    honesty: dict[str, Any] | None = None
    started = time.monotonic()
    error: str | None = None
    probe_stop = asyncio.Event()
    probe_task: asyncio.Task[None] | None = None
    try:
        await feed.start()
        if rest_probe_every > 0 and out is not None:
            probe_task = asyncio.create_task(
                _rest_probe_loop(
                    feed,
                    out,
                    interval_s=rest_probe_every,
                    levels=rest_probe_levels,
                    stop=probe_stop,
                ),
                name="rest-probe",
            )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        next_snap = loop.time() + snapshot_interval if snapshot_interval > 0 else float("inf")
        next_flush = loop.time() + flush_interval if flush_interval > 0 else float("inf")
        while loop.time() < deadline:
            event: MarketEvent | None
            try:
                event = feed.queue.get_nowait()
            except asyncio.QueueEmpty:
                # Always await: a timeout<=0 busy loop starves the feed task (no WS progress).
                timeout = min(deadline, next_snap, next_flush) - loop.time()
                try:
                    event = await asyncio.wait_for(feed.queue.get(), timeout=max(timeout, 0.05))
                except TimeoutError:
                    event = None
            if event is not None:
                _apply(book, event)
                if writer is not None:
                    writer.write([event])
                if isinstance(event, BookSnapshot):
                    n_snap += 1
                elif isinstance(event, BookDelta):
                    n_delta += 1
                elif isinstance(event, Trade):
                    n_trade += 1
            now = loop.time()
            if now >= next_snap:
                if writer is not None and snapshot_interval > 0 and book.is_synced:
                    writer.write([book.snapshot()])
                    n_periodic += 1
                if snapshot_interval > 0:
                    next_snap = now + snapshot_interval
            if writer is not None and now >= next_flush:
                writer.flush()
                next_flush = now + flush_interval
        if writer is not None and book.is_synced:
            honesty = await _honesty_vs_rest(feed, book, levels=honesty_levels)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        probe_stop.set()
        if probe_task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(probe_task), timeout=20.0)
            except (TimeoutError, Exception):
                probe_task.cancel()
                with suppress(asyncio.CancelledError):
                    await probe_task
        if rest_probe_every > 0 and out is not None:
            clock_path = out / "clock_offsets.jsonl"
            with suppress(Exception):
                await _append_clock_offset(clock_path, feed, "end", n=5)
                rest = await _fetch_rest_snapshot(feed)
                payload = {
                    "ts_local_ns": time.time_ns(),
                    "last_update_id": rest.last_update_id,
                    "symbol": feed.symbol,
                    "n_levels": rest_probe_levels,
                    **_probe_levels(rest, rest_probe_levels),
                    "phase": "end",
                }
                with (out / "rest_probes.jsonl").open("a", encoding="utf-8") as handle:
                    handle.write(orjson.dumps(payload).decode() + "\n")
        await feed.stop()
        if writer is not None:
            writer.close()
    elapsed = time.monotonic() - started
    print(
        f"symbol={feed.symbol} snapshots={n_snap} periodic={n_periodic} "
        f"deltas={n_delta} trades={n_trade} "
        f"gaps={feed.stats.gaps} resyncs={feed.stats.resyncs} reconnects={feed.stats.reconnects}"
    )
    meta: dict[str, Any] = {
        "date_utc": datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "symbol": feed.symbol,
        "exchange": EXCHANGE,
        "duration_s_requested": seconds,
        "duration_s_elapsed": round(elapsed, 3),
        "out": str(out) if out is not None else None,
        "snapshot_interval_s": snapshot_interval,
        "n_rest_snapshots": n_snap,
        "n_periodic_snapshots": n_periodic,
        "n_deltas": n_delta,
        "n_trades": n_trade,
        "gaps": feed.stats.gaps,
        "resyncs": feed.stats.resyncs,
        "reconnects": feed.stats.reconnects,
        "latency_ns": feed.stats.latency_summary(),
        "dual_sockets": feed.dual_sockets,
        "honesty": honesty,
        "error": error,
        "rest_url": DEFAULT_REST_URL,
    }
    if out is not None:
        if book.last_update_id is not None:
            meta["end_reconstruct"] = _end_reconstruct_match(out, book, EXCHANGE, feed.symbol)
            meta["mid_reconstruct"] = _mid_snapshot_consistency(out, EXCHANGE, feed.symbol)
        stats = capture_stats(out, exchange=EXCHANGE, symbol=feed.symbol)
        meta["capture_report"] = format_capture_report(stats)
        meta["bytes_total"] = stats.bytes_total
        meta["deltas_per_s"] = stats.deltas_per_s
        meta["trades_per_s"] = stats.trades_per_s
        (out / "capture_meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")
        print(meta["capture_report"])
        print(f"end_reconstruct={meta.get('end_reconstruct')}")
        print(f"mid_reconstruct={meta.get('mid_reconstruct')}")
        print(f"honesty={honesty}")
        if report_path is not None:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(_format_live_capture_md(meta), encoding="utf-8")
            print(f"wrote {report_path}")
    return 1 if error else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    seconds = capture_seconds() if args.seconds is None else args.seconds
    return asyncio.run(
        _record(
            args.symbol,
            seconds,
            args.out,
            snapshot_interval=args.snapshot_interval,
            flush_interval=args.flush_interval,
            buffer_size=args.buffer_size,
            honesty_levels=args.honesty_levels,
            report_path=args.report,
            rest_probe_every=args.rest_probe_every,
            rest_probe_levels=args.rest_probe_levels,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
