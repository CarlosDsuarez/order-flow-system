"""Exceptions raised by the order book layer."""


class OrderBookError(Exception):
    """Base class for order book errors."""


class SequenceGapError(OrderBookError):
    """Update-id continuity was broken; the book must be rebuilt from a fresh snapshot."""


class EmptyBookError(OrderBookError):
    """The operation needs at least one level on the relevant side(s) of the book."""
