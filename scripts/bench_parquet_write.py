"""Small streaming-write bench: polars vs pyarrow for nested L2 delta rows."""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

N_ROWS = 8_000
LEVELS_PER_SIDE = 40


def _payload(n: int) -> dict[str, Any]:
    bids = [
        [{"price": 100.0 - i * 0.1, "qty": 1.0} for i in range(LEVELS_PER_SIDE)] for _ in range(n)
    ]
    asks = [
        [{"price": 100.1 + i * 0.1, "qty": 1.0} for i in range(LEVELS_PER_SIDE)] for _ in range(n)
    ]
    return {
        "exchange": ["binance_futures"] * n,
        "symbol": ["BTCUSDT"] * n,
        "ts_event_ns": list(range(n)),
        "ts_recv_ns": list(range(n)),
        "first_update_id": list(range(n)),
        "final_update_id": list(range(n)),
        "prev_final_update_id": list(range(-1, n - 1)),
        "bids": bids,
        "asks": asks,
    }


def _polars_write(path: Path, data: dict[str, Any]) -> float:
    schema = {
        "exchange": pl.String(),
        "symbol": pl.String(),
        "ts_event_ns": pl.Int64(),
        "ts_recv_ns": pl.Int64(),
        "first_update_id": pl.Int64(),
        "final_update_id": pl.Int64(),
        "prev_final_update_id": pl.Int64(),
        "bids": pl.List(pl.Struct({"price": pl.Float64(), "qty": pl.Float64()})),
        "asks": pl.List(pl.Struct({"price": pl.Float64(), "qty": pl.Float64()})),
    }
    t0 = time.perf_counter()
    pl.DataFrame(data, schema=schema).write_parquet(path, compression="zstd")
    return time.perf_counter() - t0


def _pyarrow_write(path: Path, data: dict[str, Any]) -> float:
    level = pa.list_(pa.struct([("price", pa.float64()), ("qty", pa.float64())]))
    table = pa.table(
        {
            "exchange": pa.array(data["exchange"], pa.string()),
            "symbol": pa.array(data["symbol"], pa.string()),
            "ts_event_ns": pa.array(data["ts_event_ns"], pa.int64()),
            "ts_recv_ns": pa.array(data["ts_recv_ns"], pa.int64()),
            "first_update_id": pa.array(data["first_update_id"], pa.int64()),
            "final_update_id": pa.array(data["final_update_id"], pa.int64()),
            "prev_final_update_id": pa.array(data["prev_final_update_id"], pa.int64()),
            "bids": pa.array(data["bids"], level),
            "asks": pa.array(data["asks"], level),
        }
    )
    t0 = time.perf_counter()
    pq.write_table(table, path, compression="zstd")
    return time.perf_counter() - t0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="polars vs pyarrow Parquet write bench")
    parser.add_argument("--rows", type=int, default=N_ROWS)
    parser.add_argument("--out", type=Path, default=Path(tempfile.gettempdir()) / "of_pq_bench")
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    data = _payload(args.rows)
    _polars_write(args.out / "warmup_polars.parquet", {k: v[:200] for k, v in data.items()})
    _pyarrow_write(args.out / "warmup_arrow.parquet", {k: v[:200] for k, v in data.items()})
    p_path = args.out / "polars.parquet"
    a_path = args.out / "pyarrow.parquet"
    p_s = _polars_write(p_path, data)
    a_s = _pyarrow_write(a_path, data)
    p_bytes = p_path.stat().st_size
    a_bytes = a_path.stat().st_size
    print(f"rows={args.rows} nested_levels/side={LEVELS_PER_SIDE} compression=zstd")
    print(f"polars  {p_s:.4f}s  {args.rows / p_s:,.0f} rows/s  file={p_bytes:,} bytes")
    print(f"pyarrow {a_s:.4f}s  {args.rows / a_s:,.0f} rows/s  file={a_bytes:,} bytes")
    print(
        "Note: this benches one-shot DataFrame/Table writes (our writer flushes a new "
        "part file per buffer). pyarrow Dataset row-group append is a different API; "
        "we persist by adding part-NNNNN.parquet files, which both libraries do equally."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
