"""`reconstruct_book_at_update_id`: alineación por ID sobre Parquet sintético (sin red)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from order_flow.storage.parquet import deltas_to_frame, snapshots_to_frame
from order_flow.storage.reconstruct import (
    ReconstructionError,
    reconstruct_book_at_update_id,
)
from tests.helpers import T0_NS, make_delta, make_snapshot

if TYPE_CHECKING:
    from pathlib import Path

EXCHANGE = "binance_futures"
SYMBOL = "BTCUSDT"
DAY = "2024-09-02"


def _write_tape(root: Path) -> None:
    """Snapshot@100 + cadena contigua 105 → 110 (bid 100: 10 → 5 → 7)."""
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


def _aligned_bid(root: Path, rest_id: int) -> tuple[float, int, int]:
    aligned = reconstruct_book_at_update_id(root, rest_id, exchange=EXCHANGE, symbol=SYMBOL)
    bids, _ = aligned.book.depth(1)
    return bids[0].qty, aligned.aligned_u, aligned.batches_replayed


def test_exact_snapshot_id_needs_no_replay(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    qty, aligned_u, batches = _aligned_bid(tmp_path, 100)
    assert (qty, aligned_u, batches) == (10.0, 100, 0)


def test_two_diffs_ahead_align_to_rest_id(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    qty, aligned_u, batches = _aligned_bid(tmp_path, 110)
    assert (qty, aligned_u, batches) == (7.0, 110, 2)


def test_rest_inside_batch_overshoots_one_chain(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    qty, aligned_u, batches = _aligned_bid(tmp_path, 108)
    assert (qty, aligned_u, batches) == (7.0, 110, 2)


def test_gap_in_chain_is_unalignable_not_mismatch(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    delta_dir = tmp_path / "deltas" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}" / f"date={DAY}"
    bad = make_delta(106, 110, 999, bids=((100.0, 7.0),), ts_event_ns=T0_NS + 2)
    d1 = make_delta(98, 105, 97, bids=((100.0, 5.0),), ts_event_ns=T0_NS + 1)
    deltas_to_frame([d1, bad]).write_parquet(delta_dir / "delta.parquet")
    with pytest.raises(ReconstructionError, match="sequence gap"):
        reconstruct_book_at_update_id(tmp_path, 110, exchange=EXCHANGE, symbol=SYMBOL)


def test_no_snapshot_at_or_before_rest_id(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    with pytest.raises(ReconstructionError, match="no snapshot"):
        reconstruct_book_at_update_id(tmp_path, 50, exchange=EXCHANGE, symbol=SYMBOL)


def test_chain_ending_before_rest_id(tmp_path: Path) -> None:
    _write_tape(tmp_path)
    with pytest.raises(ReconstructionError, match="ends at"):
        reconstruct_book_at_update_id(tmp_path, 200, exchange=EXCHANGE, symbol=SYMBOL)
