"""Microbench: SortedDict vs bisect+list for a ~1000-level L2 book.

Not a production data structure. Used to decide whether Python + SortedDict meets
the Binance ``@depth@100ms`` budget (a few thousand level ops/s, bursts of 50-200
level changes per event).
"""

from __future__ import annotations

import argparse
import random
import statistics
import sys
import time
from bisect import bisect_left

from sortedcontainers import SortedDict

N_LEVELS = 1000
N_OPS = 20_000
N_BURST_EVENTS = 2_000
BURST_WIDTH = 100
SEED = 7


class BisectSide:
    """Ascending unique prices with parallel qty list (honest CPython alternative)."""

    def __init__(self) -> None:
        self.prices: list[float] = []
        self.qty: list[float] = []

    def __len__(self) -> int:
        return len(self.prices)

    def set(self, price: float, qty: float) -> None:
        idx = bisect_left(self.prices, price)
        if qty <= 0:
            if idx < len(self.prices) and self.prices[idx] == price:
                del self.prices[idx]
                del self.qty[idx]
            return
        if idx < len(self.prices) and self.prices[idx] == price:
            self.qty[idx] = qty
            return
        self.prices.insert(idx, price)
        self.qty.insert(idx, qty)

    def best_ask(self) -> float | None:
        return self.prices[0] if self.prices else None

    def best_bid(self) -> float | None:
        return self.prices[-1] if self.prices else None


def _percentile(samples: list[float], pct: float) -> float:
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    rank = pct / 100.0 * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def _fill_sorted(bids: SortedDict[float, float], asks: SortedDict[float, float], n: int) -> None:
    mid = 100_000.0
    for i in range(n):
        bids[mid - 0.1 * (i + 1)] = 1.0 + i * 0.01
        asks[mid + 0.1 * (i + 1)] = 1.0 + i * 0.01


def _fill_bisect(bids: BisectSide, asks: BisectSide, n: int) -> None:
    mid = 100_000.0
    for i in range(n):
        bids.set(mid - 0.1 * (i + 1), 1.0 + i * 0.01)
        asks.set(mid + 0.1 * (i + 1), 1.0 + i * 0.01)


def _ops_payload(n_ops: int, n_levels: int, rng: random.Random) -> list[tuple[str, float, float]]:
    mid = 100_000.0
    ops: list[tuple[str, float, float]] = []
    for _ in range(n_ops):
        kind = rng.choice(("insert", "update", "delete"))
        side = rng.choice(("bid", "ask"))
        offset = rng.randint(1, n_levels)
        price = mid - 0.1 * offset if side == "bid" else mid + 0.1 * offset
        qty = 0.0 if kind == "delete" else rng.random() * 5.0 + 0.01
        if kind == "insert":
            price = price + rng.choice((-0.05, 0.05, 0.15, -0.15))
        ops.append((side, price, qty))
    return ops


def _time_sorted(ops: list[tuple[str, float, float]]) -> tuple[float, list[float]]:
    bids: SortedDict[float, float] = SortedDict()
    asks: SortedDict[float, float] = SortedDict()
    _fill_sorted(bids, asks, N_LEVELS)
    lat: list[float] = []
    t0 = time.perf_counter()
    for side, price, qty in ops:
        t1 = time.perf_counter_ns()
        book = bids if side == "bid" else asks
        if qty <= 0:
            book.pop(price, None)
        else:
            book[price] = qty
        if side == "bid":
            _ = book.peekitem(-1) if book else None
        else:
            _ = book.peekitem(0) if book else None
        lat.append(time.perf_counter_ns() - t1)
    elapsed = time.perf_counter() - t0
    return elapsed, lat


def _time_bisect(ops: list[tuple[str, float, float]]) -> tuple[float, list[float]]:
    bids = BisectSide()
    asks = BisectSide()
    _fill_bisect(bids, asks, N_LEVELS)
    lat: list[float] = []
    t0 = time.perf_counter()
    for side, price, qty in ops:
        t1 = time.perf_counter_ns()
        book = bids if side == "bid" else asks
        book.set(price, qty)
        _ = book.best_bid() if side == "bid" else book.best_ask()
        lat.append(time.perf_counter_ns() - t1)
    elapsed = time.perf_counter() - t0
    return elapsed, lat


def _time_burst_sorted(n_events: int, width: int, rng: random.Random) -> float:
    bids: SortedDict[float, float] = SortedDict()
    asks: SortedDict[float, float] = SortedDict()
    _fill_sorted(bids, asks, N_LEVELS)
    t0 = time.perf_counter()
    for _ in range(n_events):
        for _step in range(width):
            price = 100_000.0 - 0.1 * rng.randint(1, N_LEVELS)
            qty = rng.random() * 3.0
            if rng.random() < 0.1:
                bids.pop(price, None)
            else:
                bids[price] = qty
        _ = bids.peekitem(-1)
    return time.perf_counter() - t0


def _fmt(name: str, elapsed: float, n: int, lat_ns: list[float] | None) -> str:
    ops_s = n / elapsed if elapsed else float("inf")
    p99_us = _percentile(lat_ns, 99.0) / 1_000.0 if lat_ns else float("nan")
    mean_us = (statistics.fmean(lat_ns) / 1_000.0) if lat_ns else float("nan")
    return f"{name:28s}  {ops_s:12.0f} ops/s  p99={p99_us:8.2f} µs  mean={mean_us:7.2f} µs"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Order book data-structure microbench")
    parser.parse_args(argv)
    rng = random.Random(SEED)  # noqa: S311
    ops = _ops_payload(N_OPS, N_LEVELS, rng)
    _time_sorted(ops[:1000])
    _time_bisect(ops[:1000])
    s_elapsed, s_lat = _time_sorted(ops)
    b_elapsed, b_lat = _time_bisect(ops)
    burst = _time_burst_sorted(N_BURST_EVENTS, BURST_WIDTH, rng)
    burst_level_ops = N_BURST_EVENTS * BURST_WIDTH
    print(f"book size ≈ {N_LEVELS} levels/side, seed={SEED}")
    print(_fmt("SortedDict insert/update/del", s_elapsed, N_OPS, s_lat))
    print(_fmt("bisect+list insert/update/del", b_elapsed, N_OPS, b_lat))
    print(
        f"{'SortedDict burst 100 lvls/evt':28s}  {burst_level_ops / burst:12.0f} level-ops/s  "
        f"({N_BURST_EVENTS} events x {BURST_WIDTH} levels in {burst:.4f}s)"
    )
    print(
        "Budget: @depth@100ms ~ 10 events/s x 50-200 levels ~ 500-2000 level-ops/s; "
        "single-level 1k-10k/s."
    )
    faster = "SortedDict" if s_elapsed <= b_elapsed else "bisect+list"
    print(f"Winner on mixed ops: {faster}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
