"""Integrity checks of a recorded capture, before anyone trusts a replay of it.

``reconstruct`` and the backtest adapters skip what they cannot apply (a delta that
breaks the ``pu`` chain just marks the book unsynced), so a damaged tape still replays
quietly. :func:`check_capture` counts those problems instead:

* **issues** (the tape is not trustworthy): deltas whose ``pu`` does not continue the
  previous ``u`` without a resync snapshot in between, crossed snapshots, duplicate
  ``trade_id``, prices off the recorded tick grid (e.g. a tick-size change mid-capture);
* **warnings**: ``qty == 0`` trades (Binance prints them; replays drop them) and a
  missing ``instrument.json`` (grid not checked).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Final

import numpy as np
import polars as pl

from order_flow.ingestion.instruments import read_instrument_spec
from order_flow.storage.parquet import read_events

if TYPE_CHECKING:
    from pathlib import Path

#: A price is on the grid when it is within this fraction of a tick of a multiple.
GRID_TOLERANCE_TICKS: Final = 1e-6


@dataclass(frozen=True, slots=True)
class CaptureQuality:
    """Counters plus human-readable ``issues`` (fatal) and ``warnings``."""

    exchange: str
    symbol: str
    n_snapshots: int
    n_deltas: int
    n_trades: int
    n_epochs: int
    n_chain_breaks: int
    n_crossed_snapshots: int
    n_duplicate_trade_ids: int
    n_zero_qty_trades: int
    n_off_grid_prices: int | None
    issues: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "ok": self.ok}


def _chain(snapshots: pl.DataFrame, deltas: pl.DataFrame) -> tuple[int, int]:
    """``(epochs, breaks)`` of the recorded delta chain, in update-id order.

    Only applied deltas are recorded, so walking them by ``u`` must see ``pu`` equal to
    the previous ``u``. A jump is a resync, and is explained, when the delta brackets
    some snapshot id (``U <= lastUpdateId <= u``); otherwise it is a break. Event time
    is not used: a REST snapshot's ``E`` is its response time and can be later than
    the ``E`` of the diff that brackets it.
    """
    snapshot_ids = np.unique(snapshots["last_update_id"].to_numpy())
    ordered = deltas.sort("final_update_id")
    epochs = breaks = 0
    last_u: int | None = None
    for first, u, pu in zip(
        ordered["first_update_id"],
        ordered["final_update_id"],
        ordered["prev_final_update_id"],
        strict=True,
    ):
        if last_u is None or pu != last_u:
            index = int(np.searchsorted(snapshot_ids, first))
            if index < snapshot_ids.size and snapshot_ids[index] <= u:
                epochs += 1
            else:
                breaks += 1
        last_u = u
    return epochs, breaks


def _level_prices(frame: pl.DataFrame, side: str) -> pl.Series:
    prices = pl.col(side).explode(empty_as_null=True).struct.field("price")
    return frame.select(prices).to_series().drop_nulls()


def _crossed(snapshots: pl.DataFrame) -> int:
    best = snapshots.select(
        bid=pl.col("bids").list.eval(pl.element().struct.field("price")).list.max(),
        ask=pl.col("asks").list.eval(pl.element().struct.field("price")).list.min(),
    )
    return int(best.filter(pl.col("bid") >= pl.col("ask")).height)


def _off_grid(prices: list[pl.Series], tick: float) -> int:
    values = np.concatenate([series.to_numpy() for series in prices]) / tick
    return int(np.count_nonzero(np.abs(values - np.round(values)) > GRID_TOLERANCE_TICKS))


def check_capture(root: Path, *, exchange: str, symbol: str) -> CaptureQuality:
    """Integrity counters for one ``exchange`` / ``symbol`` under ``root``."""
    snapshots = read_events(root, "book_snapshot", exchange=exchange, symbol=symbol)
    deltas = read_events(root, "book_delta", exchange=exchange, symbol=symbol)
    trades = read_events(root, "trade", exchange=exchange, symbol=symbol)

    epochs, breaks = _chain(snapshots, deltas)
    crossed = _crossed(snapshots)
    duplicates = trades.height - trades["trade_id"].n_unique()
    zero_qty = int((trades["qty"] <= 0).sum())
    spec = read_instrument_spec(root)
    off_grid: int | None = None
    if spec is not None:
        prices = [
            _level_prices(frame, side) for frame in (snapshots, deltas) for side in ("bids", "asks")
        ]
        off_grid = _off_grid([*prices, trades["price"]], spec.tick)

    issues: list[str] = []
    if breaks:
        issues.append(
            f"{breaks} delta(s) whose pu does not continue the previous u "
            "(no resync snapshot in between)"
        )
    if crossed:
        issues.append(f"{crossed} snapshot(s) with best bid >= best ask")
    if duplicates:
        issues.append(f"{duplicates} duplicate trade_id(s)")
    if off_grid and spec is not None:
        issues.append(f"{off_grid} price(s) off the {spec.tick_size} tick grid")
    warnings: list[str] = []
    if zero_qty:
        warnings.append(f"{zero_qty} trade(s) with qty 0 (dropped by the replays)")
    if spec is None:
        warnings.append("no instrument.json: tick grid not checked")

    return CaptureQuality(
        exchange=exchange,
        symbol=symbol,
        n_snapshots=snapshots.height,
        n_deltas=deltas.height,
        n_trades=trades.height,
        n_epochs=epochs,
        n_chain_breaks=breaks,
        n_crossed_snapshots=crossed,
        n_duplicate_trade_ids=duplicates,
        n_zero_qty_trades=zero_qty,
        n_off_grid_prices=off_grid,
        issues=tuple(issues),
        warnings=tuple(warnings),
    )
