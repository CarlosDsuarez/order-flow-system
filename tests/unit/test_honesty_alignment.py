"""Freeze + replay alineado por ``lastUpdateId`` de ``_honesty_snapshot`` (sin red).

El método viejo comparaba el libro vivo sin pausar el feed: cualquier diff de
``@depth@100ms`` entre el catch-up y la comparación era un mismatch espurio
(run 2026-09-21: 10/40 con protocolo sano). Estos tests prueban con un feed
sintético que el método nuevo congela el vivo, re-juega lo bufferizado sobre
una copia hasta ``rest.lastUpdateId`` y compara esa copia.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from order_flow.ingestion import live
from order_flow.ingestion.binance_futures import BinanceFuturesFeed
from order_flow.ingestion.sync import MAX_MISMATCH_DETAILS, compare_top_levels
from order_flow.orderbook.book import OrderBook
from tests.helpers import T0_NS, make_snapshot

if TYPE_CHECKING:
    from order_flow.ingestion.events import BookSnapshot


def _raw_depth(
    first: int,
    final: int,
    prev: int,
    bids: tuple[tuple[float, float], ...] = (),
    asks: tuple[tuple[float, float], ...] = (),
) -> dict[str, Any]:
    """Payload ``depthUpdate`` mínimo para ``_handle_depth``."""
    return {
        "e": "depthUpdate",
        "E": 1_725_235_200_123,
        "T": 1_725_235_200_120,
        "s": "BTCUSDT",
        "U": first,
        "u": final,
        "pu": prev,
        "b": [[f"{price:.1f}", f"{qty:.3f}"] for price, qty in bids],
        "a": [[f"{price:.1f}", f"{qty:.3f}"] for price, qty in asks],
    }


def _synced_feed() -> BinanceFuturesFeed:
    """Feed con snapshot@100 + un delta aplicado (libro en 105, bid 100x5.0)."""
    feed = BinanceFuturesFeed("BTCUSDT")
    feed._book.apply_snapshot(make_snapshot(100))
    feed._sync.install_snapshot(100)
    applied = feed._handle_depth(_raw_depth(98, 105, 97, bids=((100.0, 5.0),)), T0_NS)
    assert applied is not None
    assert feed.book.last_update_id == 105
    assert feed.stats.deltas_applied == 1
    return feed


async def test_freeze_replay_reaches_rest_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    """3 diffs caen durante el GET; el replay los aplica a la copia → 0 mismatches."""
    feed = _synced_feed()
    rest = make_snapshot(110, bids=((100.0, 7.0), (99.0, 5.0)))

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        assert f is feed
        assert f.honesty_frozen
        f._handle_depth(_raw_depth(106, 107, 105, bids=((98.0, 1.0),)), T0_NS)
        f._handle_depth(_raw_depth(108, 109, 107, asks=((103.0, 2.0),)), T0_NS)
        f._handle_depth(_raw_depth(110, 110, 109, bids=((100.0, 7.0),)), T0_NS)
        return rest

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert report.stale is False
    assert report.last_update_id_aligned == 110
    assert report.overshoot == 0
    assert (report.compared, report.mismatches) == (4, 0)
    # El libro vivo no se movió, los contadores no derivaron y se hizo unfreeze.
    assert feed.book.last_update_id == 105
    assert feed.stats.deltas_applied == 1
    assert feed.honesty_frozen is False
    # Lo que el método viejo habría reportado (vivo sin pausar vs REST nuevo).
    old_style = compare_top_levels(feed.book, rest, levels=2)
    assert old_style.mismatches == 1


async def test_overshoot_of_one_batch_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    """REST cae dentro de un batch: overshoot>0 y solo ese batch puede discrepar."""
    feed = _synced_feed()
    rest = make_snapshot(108)  # estado pre-delta (bid 100x5.0)

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        f._handle_depth(_raw_depth(106, 110, 105, bids=((100.0, 7.0),)), T0_NS)
        return rest

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert report.stale is False
    assert report.last_update_id_aligned == 110
    assert report.overshoot == 2
    assert (report.compared, report.mismatches) == (4, 1)
    assert feed.honesty_frozen is False


async def test_stale_when_rest_is_older_than_freeze(monkeypatch: pytest.MonkeyPatch) -> None:
    """REST más viejo que el freeze dos veces → un reintento y reporte stale."""
    feed = _synced_feed()
    calls = 0

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        nonlocal calls
        calls += 1
        return make_snapshot(100)

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert calls == 2
    assert report.stale is True
    assert report.overshoot == 5  # L0=105 - R=100
    assert feed.honesty_frozen is False
    assert feed.book.last_update_id == 105


async def test_local_two_diffs_ahead_matches_100pct(monkeypatch: pytest.MonkeyPatch) -> None:
    """El caso pedido: 2 diffs por delante del REST → compare alineado 100% top-N."""
    feed = _synced_feed()
    rest = make_snapshot(110, bids=((100.0, 7.0), (99.0, 5.0)))

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        # Ambos diffs tocan el top-N (100: 5.0→6.0→7.0); REST(110) ya trae el final.
        f._handle_depth(_raw_depth(106, 108, 105, bids=((100.0, 6.0),)), T0_NS)
        f._handle_depth(_raw_depth(109, 110, 108, bids=((100.0, 7.0),)), T0_NS)
        return rest

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert report.stale is False
    assert report.last_update_id_aligned == 110
    assert report.delta_ids == report.overshoot == 0
    assert (report.compared, report.mismatches) == (4, 0)
    assert report.mismatch_details == ()
    assert feed.book.last_update_id == 105  # vivo intacto


async def test_timeout_without_catch_up_is_stale_not_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sin diffs que alcancen R antes del timeout → stale, no un falso mismatch."""
    feed = _synced_feed()
    monkeypatch.setattr(live, "CATCH_UP_TIMEOUT_S", 0.3)

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        # Un solo diff insuficiente (llega a 150, REST=200): el replay lo agota,
        # duerme, y el deadline convierte el resto en stale.
        f._handle_depth(_raw_depth(106, 150, 105, bids=((100.0, 6.0),)), T0_NS)
        return make_snapshot(200)

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert report.stale is True
    assert report.delta_ids is None
    assert report.last_update_id_aligned == 150
    assert feed.honesty_frozen is False


async def test_never_synced_book_is_explicit_error() -> None:
    """Libro sin snapshot → RuntimeError explícito (no un compare vacío)."""
    feed = BinanceFuturesFeed("BTCUSDT")
    with pytest.raises(RuntimeError, match="never synced"):
        await live._honesty_snapshot(feed, levels=2)
    assert feed.honesty_frozen is False


async def test_gap_during_replay_is_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    """Diff discontinuo en el buffer → gap en el replay → stale, no mismatch."""
    feed = _synced_feed()

    async def fake_fetch(f: BinanceFuturesFeed) -> BookSnapshot:
        # pu=999 rompe la continuidad (el freeze bufferiza sin validar).
        f._handle_depth(_raw_depth(106, 110, 999, bids=((100.0, 7.0),)), T0_NS)
        return make_snapshot(110, bids=((100.0, 7.0), (99.0, 5.0)))

    monkeypatch.setattr(live, "_fetch_rest_snapshot", fake_fetch)
    report = await live._honesty_snapshot(feed, levels=2)

    assert report.stale is True
    assert feed.honesty_frozen is False


def test_qty_zero_removal_and_missing_level_are_mismatches() -> None:
    """`qty == 0` elimina el nivel y un precio ausente sigue siendo mismatch con detalle."""
    feed = _synced_feed()
    removed = feed._handle_depth(_raw_depth(106, 106, 105, bids=((99.0, 0.0),)), T0_NS)
    assert removed is not None
    assert feed.book.last_update_id == 106
    rest = make_snapshot(106)  # bids 100x10, 99x5; asks iguales al vivo
    report = compare_top_levels(feed.book, rest, levels=2)

    assert (report.compared, report.mismatches) == (4, 2)
    assert report.max_qty_discrepancy == 5.0
    by_price = {(detail.side, detail.price): detail for detail in report.mismatch_details}
    changed = by_price[("bid", 100.0)]
    assert (changed.qty_local, changed.qty_rest, changed.abs_diff) == (5.0, 10.0, 5.0)
    missing = by_price[("bid", 99.0)]
    assert (missing.qty_local, missing.qty_rest, missing.abs_diff) == (None, 5.0, 5.0)


def test_mismatch_details_capped_at_20() -> None:
    """25 niveles discrepantes → 25 mismatches pero detalle recortado a cap 20."""
    local_book = OrderBook(exchange="binance_futures", symbol="BTCUSDT")
    local_book.apply_snapshot(
        make_snapshot(
            1,
            bids=[(100.0 - i, 1.0) for i in range(25)],
            asks=[(101.0 + i, 1.0) for i in range(25)],
        )
    )
    rest = make_snapshot(
        1,
        bids=[(100.0 - i, 2.0) for i in range(25)],
        asks=[(101.0 + i, 1.0) for i in range(25)],
    )
    report = compare_top_levels(local_book, rest, levels=25)

    assert (report.compared, report.mismatches) == (50, 25)
    assert len(report.mismatch_details) == MAX_MISMATCH_DETAILS == 20
    assert all(detail.side == "bid" for detail in report.mismatch_details)
