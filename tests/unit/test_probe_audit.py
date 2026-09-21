"""Offline probe audit: aligned series + exit-code gate (sin red)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import orjson

from order_flow.storage.parquet import deltas_to_frame, snapshots_to_frame
from order_flow.storage.probe_audit import run_probe_audit
from tests.helpers import T0_NS, make_delta, make_snapshot

if TYPE_CHECKING:
    from pathlib import Path

EXCHANGE = "binance_futures"
SYMBOL = "BTCUSDT"
DAY = "2024-09-02"


def _write_tape(root: Path) -> None:
    snap_dir = root / "snapshots" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}" / f"date={DAY}"
    delta_dir = root / "deltas" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}" / f"date={DAY}"
    snap_dir.mkdir(parents=True)
    delta_dir.mkdir(parents=True)
    snapshots_to_frame([make_snapshot(100, ts_event_ns=T0_NS)]).write_parquet(
        snap_dir / "snap.parquet"
    )
    d1 = make_delta(98, 105, 97, bids=((100.0, 5.0),), ts_event_ns=T0_NS + 1)
    d2 = make_delta(106, 110, 105, bids=((100.0, 7.0),), ts_event_ns=T0_NS + 2)
    deltas_to_frame([d1, d2]).write_parquet(delta_dir / "delta.parquet")


def _probe_row(
    rest_id: int, bids: list[list[float]], asks: list[list[float]], ts_ns: int
) -> dict[str, Any]:
    return {
        "ts_local_ns": ts_ns,
        "last_update_id": rest_id,
        "symbol": SYMBOL,
        "n_levels": 2,
        "bids": bids,
        "asks": asks,
    }


def _write_probes(path: Path) -> None:
    asks = [[101.0, 8.0], [102.0, 3.0]]
    rows = [
        # Alineado exacto en R=100 → rate 0.
        _probe_row(100, [[100.0, 10.0], [99.0, 5.0]], asks, T0_NS + 10),
        # 2 diffs por delante (R=110, qty final 7.0) → rate 0 tras replay.
        _probe_row(110, [[100.0, 7.0], [99.0, 5.0]], asks, T0_NS + 20),
        # qty inventada en R=110 → 1/4 = 0.25 > warn.
        _probe_row(110, [[100.0, 999.0], [99.0, 5.0]], asks, T0_NS + 30),
        # R más allá de la cinta → stale, nunca mismatch.
        _probe_row(9999, [[100.0, 7.0], [99.0, 5.0]], asks, T0_NS + 40),
    ]
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(orjson.dumps(row).decode() + "\n")


def _run(tmp_path: Path, warn: float) -> tuple[int, list[dict[str, Any]]]:
    _write_tape(tmp_path)
    probes = tmp_path / "rest_probes.jsonl"
    out = tmp_path / "probe_alignment.jsonl"
    _write_probes(probes)
    code = run_probe_audit(
        tape=tmp_path,
        probes_path=probes,
        out_path=out,
        exchange=EXCHANGE,
        symbol=SYMBOL,
        levels=2,
        warn=warn,
    )
    rows = [orjson.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    return code, rows


def test_breach_returns_1_with_series(tmp_path: Path) -> None:
    code, rows = _run(tmp_path, warn=0.10)
    assert code == 1
    assert [row["probe"] for row in rows] == [0, 1, 2, 3]
    assert rows[0]["mismatch_rate"] == 0.0
    assert rows[0]["batches_replayed"] == 0
    assert rows[1]["mismatch_rate"] == 0.0
    assert rows[1]["aligned_u"] == 110
    assert rows[1]["batches_replayed"] == 2
    assert rows[2]["mismatch_rate"] == 0.25
    assert rows[2]["max_qty_discrepancy"] == 992.0
    assert rows[3]["stale"] is True
    assert "mismatch_rate" not in rows[3]


def test_clean_series_returns_0(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    probes = tmp_path / "rest_probes.jsonl"
    out = tmp_path / "probe_alignment.jsonl"
    asks = [[101.0, 8.0], [102.0, 3.0]]
    with probes.open("w", encoding="utf-8") as handle:
        handle.write(
            orjson.dumps(_probe_row(100, [[100.0, 10.0], [99.0, 5.0]], asks, T0_NS)).decode() + "\n"
        )
    code = run_probe_audit(
        tape=tmp_path,
        probes_path=probes,
        out_path=out,
        exchange=EXCHANGE,
        symbol=SYMBOL,
        levels=2,
        warn=0.10,
    )
    assert code == 0
