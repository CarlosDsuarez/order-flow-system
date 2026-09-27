"""Property tests: book_stream replays any tape record_l2 could have written.

The simulator models the exchange update by update (ids consecutive), aggregates
updates into depth diffs (``U..u``, ``pu``, final value per level), and replays what
the recorder does: sync on a REST snapshot limited to ``REST_DEPTH`` levels, taken at any
id inside a diff (so the bracketing diff can share its ``u``) and stamped with a random
lag (often later than that diff); apply the bracketing diff and the rest; lose diffs at
random and resync; write periodic copies of its own book. Two bugs in this code path
shipped before it existed (stale levels after a resync; deep levels lost when REST and
its diff share ``u``).
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field

from hypothesis import given, settings
from hypothesis import strategies as st

from order_flow.ingestion.events import BookDelta, BookSnapshot, PriceLevel
from order_flow.orderbook.book import OrderBook
from order_flow.storage.reconstruct import book_stream
from tests.helpers import EXCHANGE, SYMBOL, T0_NS

MS = 1_000_000
BID_PRICES = tuple(90.0 + i for i in range(10))
ASK_PRICES = tuple(101.0 + i for i in range(10))
REST_DEPTH = 3  # far below the book's width, like Binance's limit=1000 vs 7 000 levels
FIRST_ID = 1_000

Key = tuple[str, float]
Book = dict[Key, float]


@dataclass
class Tape:
    snapshots: list[BookSnapshot] = field(default_factory=list)
    deltas: list[BookDelta] = field(default_factory=list)
    #: (u, recorder book) at every periodic snapshot; the replay must match each one.
    periodic: list[tuple[int, Book]] = field(default_factory=list)
    final: Book = field(default_factory=dict)


def _apply(book: Book, changes: dict[Key, float]) -> None:
    for key, qty in changes.items():
        if qty > 0:
            book[key] = qty
        else:
            book.pop(key, None)


def _side(book: Book, side: str, depth: int | None) -> tuple[PriceLevel, ...]:
    levels = sorted(
        ((price, qty) for (s, price), qty in book.items() if s == side),
        reverse=side == "bid",
    )
    return tuple(PriceLevel(price, qty) for price, qty in levels[:depth])


def _snapshot(book: Book, u: int, ts: int, *, depth: int | None = None) -> BookSnapshot:
    return BookSnapshot(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        ts_event_ns=ts,
        ts_recv_ns=ts + MS,
        last_update_id=u,
        bids=_side(book, "bid", depth),
        asks=_side(book, "ask", depth),
    )


@dataclass(frozen=True)
class _Diff:
    first: int
    final: int
    changes: dict[Key, float]
    ts: int


updates_st = st.lists(
    st.tuples(
        st.sampled_from(["bid", "ask"]),
        st.integers(0, len(BID_PRICES) - 1),
        st.sampled_from([0.0, 1.0, 2.5, 7.0]),
    ),
    min_size=12,
    max_size=150,
)


@st.composite
def tapes(draw: st.DrawFn) -> Tape:
    raw = draw(updates_st)
    updates: list[tuple[Key, float]] = [
        ((side, (BID_PRICES if side == "bid" else ASK_PRICES)[i]), qty) for side, i, qty in raw
    ]
    # Exchange: a full initial book, then one update per id.
    initial: Book = {("bid", p): 3.0 for p in BID_PRICES} | {("ask", p): 3.0 for p in ASK_PRICES}

    def state_at(update_id: int) -> Book:
        book = dict(initial)
        _apply(book, dict(updates[: update_id - FIRST_ID + 1]))
        return book

    diffs: list[_Diff] = []
    start = 0
    while start < len(updates):
        size = draw(st.integers(1, 5))
        chunk = updates[start : start + size]
        diffs.append(
            _Diff(
                first=FIRST_ID + start,
                final=FIRST_ID + start + len(chunk) - 1,
                changes=dict(chunk),  # the diff carries each level's final value
                ts=T0_NS + len(diffs) * 100 * MS,
            )
        )
        start += size

    tape = Tape()
    book: Book = {}
    last_u = -1
    synced = False
    j = draw(st.integers(0, min(2, len(diffs) - 1)))
    while j < len(diffs):
        if not synced:
            # REST snapshot at any id from "just before diff j" to diff j's u.
            rest_id = draw(st.integers(diffs[j].first - 1, diffs[j].final))
            lag = draw(st.integers(0, 350)) * MS  # REST E = response time: often late
            rest = _snapshot(state_at(rest_id), rest_id, diffs[j].ts + lag, depth=REST_DEPTH)
            tape.snapshots.append(rest)
            book = {("bid", lvl.price): lvl.qty for lvl in rest.bids}
            book |= {("ask", lvl.price): lvl.qty for lvl in rest.asks}
            last_u, synced = rest_id, True
            if rest_id < diffs[j].final and draw(st.booleans()):
                tape.snapshots.append(_snapshot(book, last_u, diffs[j].ts))
                tape.periodic.append((last_u, dict(book)))
        diff = diffs[j]
        j += 1
        if draw(st.integers(0, 9)) == 0 and j < len(diffs):
            # Lost on the wire: the next diff's pu will not match; the feed resyncs
            # from a later REST and drops everything received before it.
            synced = False
            j = draw(st.integers(j, min(j + 3, len(diffs) - 1)))
            continue
        tape.deltas.append(
            BookDelta(
                exchange=EXCHANGE,
                symbol=SYMBOL,
                ts_event_ns=diff.ts,
                ts_recv_ns=diff.ts + MS,
                first_update_id=diff.first,
                final_update_id=diff.final,
                prev_final_update_id=diff.first - 1,
                bids=tuple(PriceLevel(k[1], q) for k, q in diff.changes.items() if k[0] == "bid"),
                asks=tuple(PriceLevel(k[1], q) for k, q in diff.changes.items() if k[0] == "ask"),
            )
        )
        _apply(book, diff.changes)
        last_u = diff.final
        if draw(st.booleans()):
            tape.snapshots.append(_snapshot(book, last_u, diff.ts))
            tape.periodic.append((last_u, dict(book)))
    tape.final = book
    return tape


def _replay(events: tuple[BookSnapshot | BookDelta, ...]) -> list[tuple[int, Book]]:
    """Book after each replayed event, keyed by its update id."""
    book: Book = {}
    states: list[tuple[int, Book]] = []
    for event in events:
        if isinstance(event, BookSnapshot):
            book = {("bid", lvl.price): lvl.qty for lvl in event.bids if lvl.qty > 0}
            book |= {("ask", lvl.price): lvl.qty for lvl in event.asks if lvl.qty > 0}
            states.append((event.last_update_id, dict(book)))
        else:
            _apply(book, {("bid", lvl.price): lvl.qty for lvl in event.bids})
            _apply(book, {("ask", lvl.price): lvl.qty for lvl in event.asks})
            states.append((event.final_update_id, dict(book)))
    return states


@settings(max_examples=300, deadline=None)
@given(tapes())
def test_replay_matches_the_recorder_book_at_every_periodic_snapshot(tape: Tape) -> None:
    stream = book_stream(tape.snapshots, tape.deltas)
    states = _replay(stream.events)
    ids = [u for u, _ in states]
    for u, expected in tape.periodic:
        assert states[bisect_right(ids, u) - 1][1] == expected
    assert states[-1][1] == tape.final


@settings(max_examples=300, deadline=None)
@given(tapes())
def test_stream_is_valid_for_order_book_and_time_ordered(tape: Tape) -> None:
    stream = book_stream(tape.snapshots, tape.deltas)
    book = OrderBook(exchange=EXCHANGE, symbol=SYMBOL)
    for event in stream.events:  # SequenceGapError here would break reconstruct
        if isinstance(event, BookSnapshot):
            book.apply_snapshot(event)
        else:
            assert book.apply_delta(event)
    stamps = [event.ts_event_ns for event in stream.events]
    received = [event.ts_recv_ns for event in stream.events]
    assert stamps == sorted(stamps)  # engines re-sort by time
    assert received == sorted(received)
    n_snapshots = sum(isinstance(event, BookSnapshot) for event in stream.events)
    assert n_snapshots + stream.n_periodic_skipped == len(tape.snapshots)
    assert len(stream.events) - n_snapshots + stream.n_off_chain_deltas == len(tape.deltas)
