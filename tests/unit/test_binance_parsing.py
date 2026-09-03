"""Binance USD-M Futures message parsing and the official depth-sync rules."""

from __future__ import annotations

from typing import Any

import pytest

from order_flow.ingestion.binance_futures import (
    EXCHANGE,
    DepthSequenceValidator,
    parse_agg_trade,
    parse_depth_snapshot,
    parse_depth_update,
    parse_trade,
    unwrap_stream_message,
)
from order_flow.ingestion.events import PriceLevel, Side
from order_flow.orderbook.errors import SequenceGapError
from tests.helpers import make_delta

MS = 1_000_000


def test_parse_depth_update(depth_update_msg: dict[str, Any]) -> None:
    delta = parse_depth_update(depth_update_msg, ts_recv_ns=42)
    assert delta.exchange == EXCHANGE
    assert delta.symbol == "BTCUSDT"
    assert delta.ts_event_ns == 1725235200123 * MS  # event time E (official latency clock)
    assert delta.ts_recv_ns == 42
    assert (delta.first_update_id, delta.final_update_id, delta.prev_final_update_id) == (
        1027025,
        1027030,
        1027024,
    )
    assert delta.bids == (PriceLevel(60000.10, 1.25), PriceLevel(59999.90, 0.0))
    assert delta.asks == (PriceLevel(60000.20, 0.8),)


def test_parse_depth_update_falls_back_to_transaction_time(
    depth_update_msg: dict[str, Any],
) -> None:
    del depth_update_msg["E"]
    assert parse_depth_update(depth_update_msg, ts_recv_ns=1).ts_event_ns == 1725235200120 * MS


def test_futures_validator_rejects_spot_plus_one_first_event() -> None:
    """Spot ``U <= lastUpdateId+1 <= u`` would accept this; futures must not."""
    validator = DepthSequenceValidator(1000)
    with pytest.raises(SequenceGapError, match="does not bracket"):
        validator.validate(make_delta(1001, 1005, 1000))


def test_parse_depth_update_defaults_receive_time(depth_update_msg: dict[str, Any]) -> None:
    delta = parse_depth_update(depth_update_msg)
    assert delta.ts_recv_ns > delta.ts_event_ns


def test_parse_depth_update_rejects_other_event_types(agg_trade_msg: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="depthUpdate"):
        parse_depth_update(agg_trade_msg)


def test_parse_agg_trade_buyer_is_maker_means_sell_aggressor(
    agg_trade_msg: dict[str, Any],
) -> None:
    trade = parse_agg_trade(agg_trade_msg, ts_recv_ns=7)
    assert trade.trade_id == 5933014
    assert trade.price == pytest.approx(60000.2)
    assert trade.qty == pytest.approx(0.005)
    assert trade.ts_event_ns == 1725235200450 * MS
    assert trade.aggressor is Side.SELL


def test_parse_agg_trade_taker_buy(agg_trade_msg: dict[str, Any]) -> None:
    agg_trade_msg["m"] = False
    assert parse_agg_trade(agg_trade_msg, ts_recv_ns=7).aggressor is Side.BUY


def test_parse_trade_buyer_is_maker_means_sell_aggressor() -> None:
    """USD-M ``@trade`` uses ``t`` as trade id; ``m=true`` still means seller aggressor."""
    trade = parse_trade(
        {
            "e": "trade",
            "E": 1725235200456,
            "s": "BTCUSDT",
            "t": 8044220783,
            "p": "60000.20",
            "q": "0.005",
            "T": 1725235200450,
            "m": True,
            "X": "MARKET",
        },
        ts_recv_ns=7,
    )
    assert trade.trade_id == 8044220783
    assert trade.price == pytest.approx(60000.2)
    assert trade.qty == pytest.approx(0.005)
    assert trade.ts_event_ns == 1725235200450 * MS
    assert trade.aggressor is Side.SELL


def test_parse_trade_taker_buy() -> None:
    trade = parse_trade(
        {
            "e": "trade",
            "E": 1,
            "T": 1,
            "s": "BTCUSDT",
            "t": 1,
            "p": "1.0",
            "q": "2.0",
            "m": False,
        },
        ts_recv_ns=3,
    )
    assert trade.aggressor is Side.BUY


def test_parse_trade_rejects_agg_trade_type(agg_trade_msg: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="trade"):
        parse_trade(agg_trade_msg)


def test_parse_depth_snapshot(depth_snapshot_payload: dict[str, Any]) -> None:
    snapshot = parse_depth_snapshot(depth_snapshot_payload, "btcusdt", ts_recv_ns=9)
    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.last_update_id == 1027024
    assert snapshot.ts_event_ns == 1725235200000 * MS  # snapshot output time E
    assert snapshot.bids[0] == PriceLevel(60000.0, 2.0)
    assert snapshot.asks[1] == PriceLevel(60000.2, 0.5)


def test_parse_depth_snapshot_without_timestamps_uses_receive_time() -> None:
    payload = {"lastUpdateId": 5, "bids": [], "asks": []}
    snapshot = parse_depth_snapshot(payload, "BTCUSDT", ts_recv_ns=123)
    assert snapshot.ts_event_ns == 123
    assert snapshot.bids == ()


def test_unwrap_stream_message(depth_update_msg: dict[str, Any]) -> None:
    wrapped = {"stream": "btcusdt@depth@100ms", "data": depth_update_msg}
    assert unwrap_stream_message(wrapped) is depth_update_msg
    assert unwrap_stream_message(depth_update_msg) is depth_update_msg
    assert unwrap_stream_message({"stream": "x", "data": "not-a-dict"}) == {
        "stream": "x",
        "data": "not-a-dict",
    }


class TestDepthSequenceValidator:
    def test_drops_stale_events_then_accepts_bracketing_event(self) -> None:
        validator = DepthSequenceValidator(1000)
        assert validator.validate(make_delta(900, 950, 880)) is False
        assert validator.validate(make_delta(951, 999, 950)) is False
        assert not validator.synced
        assert validator.validate(make_delta(995, 1003, 994)) is True
        assert validator.last_update_id == 1003
        assert validator.synced is True

    def test_first_event_with_u_equal_to_snapshot_id_is_applied(self) -> None:
        validator = DepthSequenceValidator(1000)
        assert validator.validate(make_delta(990, 1000, 989)) is True

    def test_first_event_beyond_snapshot_requires_resync(self) -> None:
        validator = DepthSequenceValidator(1000)
        with pytest.raises(SequenceGapError, match="does not bracket"):
            validator.validate(make_delta(1001, 1005, 1000))

    def test_chained_events_must_match_previous_u(self) -> None:
        validator = DepthSequenceValidator(1000)
        validator.validate(make_delta(995, 1003, 994))
        assert validator.validate(make_delta(1004, 1010, 1003)) is True
        with pytest.raises(SequenceGapError, match="expected pu=1010"):
            validator.validate(make_delta(1012, 1015, 1011))

    def test_duplicate_event_after_sync_is_a_gap(self) -> None:
        validator = DepthSequenceValidator(1000)
        validator.validate(make_delta(995, 1003, 994))
        with pytest.raises(SequenceGapError):
            validator.validate(make_delta(995, 1003, 994))
