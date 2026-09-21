"""Offline alignment audit of REST probes vs a Parquet tape.

For each probe in ``rest_probes.jsonl`` (written by ``record_l2.py
--rest-probe-every``): rebuild the tape book at the first stored ``u >=
probe.lastUpdateId`` (:func:`reconstruct_book_at_update_id`, Futures
bracketing) and run :func:`sync.compare_top_levels` on those two aligned
states. Unalignable probes (gap / short chain) are `stale` rows and never
count as mismatches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import orjson

from order_flow.ingestion.events import BookSnapshot, PriceLevel
from order_flow.ingestion.sync import compare_top_levels
from order_flow.storage.reconstruct import (
    ReconstructionError,
    reconstruct_book_at_update_id,
)
from order_flow.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

log = get_logger(__name__)


def _probe_levels(pairs: Sequence[Sequence[float]] | None) -> tuple[PriceLevel, ...]:
    """Rehydrate ``[[price, qty], ...]`` probe levels (audit sidecar, not Parquet)."""
    levels: list[PriceLevel] = []
    for price, qty in pairs or []:
        levels.append(PriceLevel(float(price), float(qty)))
    return tuple(levels)


def probe_snapshot(probe: dict[str, Any], *, exchange: str, symbol: str) -> BookSnapshot:
    """Rehydrate one ``rest_probes.jsonl`` row as a :class:`BookSnapshot`."""
    ts_ns = int(probe["ts_local_ns"])
    return BookSnapshot(
        exchange=exchange,
        symbol=str(probe.get("symbol", symbol)),
        ts_event_ns=ts_ns,
        ts_recv_ns=ts_ns,
        last_update_id=int(probe["last_update_id"]),
        bids=_probe_levels(probe.get("bids")),
        asks=_probe_levels(probe.get("asks")),
    )


def audit_probes(
    *,
    tape: Path,
    probes_path: Path,
    out_path: Path,
    exchange: str,
    symbol: str,
    levels: int,
) -> list[dict[str, Any]]:
    """Align every probe and append one JSON row per probe to ``out_path``."""
    rows: list[dict[str, Any]] = []
    with probes_path.open("rb") as handle:
        lines = handle.readlines()
    with out_path.open("w", encoding="utf-8") as out:
        for index, raw in enumerate(lines):
            probe = orjson.loads(raw)
            rest_id = int(probe["last_update_id"])
            try:
                aligned = reconstruct_book_at_update_id(
                    tape, rest_id, exchange=exchange, symbol=symbol
                )
            except ReconstructionError as exc:
                row: dict[str, Any] = {
                    "t_ns": int(probe["ts_local_ns"]),
                    "probe": index,
                    "probe_last_update_id": rest_id,
                    "stale": True,
                    "error": str(exc),
                }
                log.warning("probe_unalignable", probe=index, rest_id=rest_id, error=str(exc))
            else:
                probe_snap = probe_snapshot(probe, exchange=exchange, symbol=symbol)
                report = compare_top_levels(aligned.book, probe_snap, levels=levels)
                rate = report.mismatches / report.compared if report.compared else 1.0
                row = {
                    "t_ns": int(probe["ts_local_ns"]),
                    "probe": index,
                    "probe_last_update_id": rest_id,
                    "stale": False,
                    "aligned_u": aligned.aligned_u,
                    "delta_ids": aligned.aligned_u - rest_id,
                    "batches_replayed": aligned.batches_replayed,
                    "compared": report.compared,
                    "mismatches": report.mismatches,
                    "mismatch_rate": rate,
                    "max_qty_discrepancy": report.max_qty_discrepancy,
                }
                log.info(
                    "probe_aligned",
                    probe=index,
                    rest_id=rest_id,
                    mismatch_rate=rate,
                    max_disc=report.max_qty_discrepancy,
                )
            rows.append(row)
            out.write(orjson.dumps(row).decode() + "\n")
            print(
                f"probe={row['probe']} t={row['t_ns']} "
                f"stale={row['stale']} "
                f"rate={row.get('mismatch_rate', '-')} "
                f"maxdq={row.get('max_qty_discrepancy', '-')}"
            )
    return rows


def run_probe_audit(
    *,
    tape: Path,
    probes_path: Path,
    out_path: Path,
    exchange: str,
    symbol: str,
    levels: int,
    warn: float,
) -> int:
    """Audit probes; return 1 when any alignable probe exceeds ``warn`` rate.

    A breach is a failure, not "residual race".
    """
    rows = audit_probes(
        tape=tape,
        probes_path=probes_path,
        out_path=out_path,
        exchange=exchange,
        symbol=symbol,
        levels=levels,
    )
    breached = [row for row in rows if not row["stale"] and float(row["mismatch_rate"]) > warn]
    if breached:
        print(f"FAIL: {len(breached)}/{len(rows)} probes exceed warn {warn}")
        return 1
    print(f"PASS: {len(rows)} probes within warn {warn}")
    return 0
