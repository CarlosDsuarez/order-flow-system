"""Desglose ventana/total de deltas: la ventana nunca se infla (sin red)."""

from __future__ import annotations

from order_flow.ingestion.live import split_window_counters


def test_exact_match_has_no_in_flight() -> None:
    assert split_window_counters(queued=582, applied_total=582) == {
        "queued": 582,
        "applied_total": 582,
        "in_flight": 0,
    }


def test_run_2026_09_21_five_in_flight() -> None:
    """El caso observado: 577 en cola, 582 aplicados → 5 en vuelo, ventana en 577."""
    window = split_window_counters(queued=577, applied_total=582)
    assert window["queued"] == 577
    assert window["applied_total"] == 582
    assert window["in_flight"] == 5


def test_post_window_applies_do_not_inflate_window() -> None:
    """Deltas aplicados tras cortar la ventana solo engordan `in_flight`."""
    before = split_window_counters(queued=577, applied_total=582)
    after = split_window_counters(queued=577, applied_total=590)
    assert after["queued"] == before["queued"] == 577
    assert after["in_flight"] == 13
    assert after["queued"] + after["in_flight"] == after["applied_total"]


def test_negative_is_clamped_to_zero() -> None:
    """Defensivo: `applied_total < queued` no puede pasar (cada delta encolado
    se aplicó exactamente una vez); si pasara, `in_flight` es 0, no negativo."""
    assert split_window_counters(queued=10, applied_total=9)["in_flight"] == 0
