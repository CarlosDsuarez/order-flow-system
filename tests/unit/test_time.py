"""Nanosecond epoch helpers."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta, timezone

import pytest

from order_flow.utils.time import datetime_to_ns, ms_to_ns, now_ns, ns_to_datetime, ns_to_ms


def test_now_ns_is_close_to_wall_clock() -> None:
    before = time.time_ns()
    value = now_ns()
    after = time.time_ns()
    assert before <= value <= after


def test_ms_ns_conversions() -> None:
    assert ms_to_ns(1) == 1_000_000
    assert ms_to_ns(1_725_235_200_123) == 1_725_235_200_123_000_000
    assert ns_to_ms(1_725_235_200_123_456_789) == 1_725_235_200_123
    assert ns_to_ms(999_999) == 0


def test_ns_to_datetime_truncates_to_microseconds() -> None:
    dt = ns_to_datetime(1_700_000_000_123_456_789)
    assert dt == datetime(2023, 11, 14, 22, 13, 20, 123_456, tzinfo=UTC)
    assert dt.tzinfo is UTC


def test_datetime_round_trip() -> None:
    dt = datetime(2024, 9, 2, 0, 0, 0, 250_000, tzinfo=UTC)
    ns = datetime_to_ns(dt)
    assert ns == 1_725_235_200_250_000_000
    assert ns_to_datetime(ns) == dt


def test_datetime_to_ns_handles_other_timezones() -> None:
    plus_two = timezone(timedelta(hours=2))
    local = datetime(2024, 9, 2, 2, 0, tzinfo=plus_two)
    assert datetime_to_ns(local) == 1_725_235_200_000_000_000


def test_naive_datetime_rejected() -> None:
    naive = datetime(2024, 9, 2)  # intentionally naive
    with pytest.raises(ValueError, match="timezone-aware"):
        datetime_to_ns(naive)
