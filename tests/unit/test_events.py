"""Event model semantics: side aliases, sign convention, immutability."""

from __future__ import annotations

import dataclasses

import pytest

from order_flow.ingestion.events import PriceLevel, Side
from tests.helpers import make_delta, make_snapshot, make_trade


def test_side_aliases_and_signs() -> None:
    assert Side.BUY is Side.BID  # type: ignore[comparison-overlap]
    assert Side.SELL is Side.ASK  # type: ignore[comparison-overlap]
    assert Side.BUY.sign == 1
    assert Side.SELL.sign == -1
    assert Side.from_sign(3.5) is Side.BID
    assert Side.from_sign(-1) is Side.ASK


def test_side_from_zero_sign_rejected() -> None:
    with pytest.raises(ValueError, match="non-zero"):
        Side.from_sign(0)


def test_events_are_frozen() -> None:
    level = PriceLevel(1.0, 2.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        level.qty = 3.0  # type: ignore[misc]
    snapshot = make_snapshot()
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.last_update_id = 1  # type: ignore[misc]


def test_event_type_tags() -> None:
    assert make_snapshot().EVENT_TYPE == "book_snapshot"
    assert make_delta(1, 2, 0).EVENT_TYPE == "book_delta"
    assert make_trade(1, 100.0, 1.0, Side.BUY).EVENT_TYPE == "trade"
