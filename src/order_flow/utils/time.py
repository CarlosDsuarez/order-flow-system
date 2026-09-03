"""Epoch timestamp helpers. All event timestamps in this project are integer ns (UTC)."""

from __future__ import annotations

import time as _time
from datetime import UTC, datetime

NS_PER_US = 1_000
NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000


def now_ns() -> int:
    """Current wall-clock time as integer nanoseconds since the Unix epoch."""
    return _time.time_ns()


def ms_to_ns(ms: int) -> int:
    """Convert milliseconds since epoch (exchange convention) to nanoseconds."""
    return ms * NS_PER_MS


def ns_to_ms(ns: int) -> int:
    """Convert nanoseconds since epoch to milliseconds (floor division)."""
    return ns // NS_PER_MS


def ns_to_datetime(ns: int) -> datetime:
    """Convert nanoseconds since epoch to a timezone-aware UTC ``datetime``.

    ``datetime`` only resolves microseconds, so the sub-microsecond part is truncated.
    """
    seconds, remainder_ns = divmod(ns, NS_PER_S)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=remainder_ns // NS_PER_US)


def datetime_to_ns(dt: datetime) -> int:
    """Convert a timezone-aware ``datetime`` to nanoseconds since epoch.

    Raises:
        ValueError: If ``dt`` is naive (no ``tzinfo``), because its epoch offset is ambiguous.
    """
    if dt.tzinfo is None:
        msg = "datetime must be timezone-aware"
        raise ValueError(msg)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = dt - epoch
    return (delta.days * 86_400 + delta.seconds) * NS_PER_S + delta.microseconds * NS_PER_US
