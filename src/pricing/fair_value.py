"""Utilities to compute fair values from observed market data."""

from dataclasses import dataclass
from typing import Iterable


@dataclass
class Quote:
    """Represents a best bid/ask pair."""

    bid: float
    ask: float

    @property
    def mid(self) -> float:
        """Return the mid price."""

        return (self.bid + self.ask) / 2


class FairValueCalculator:
    """Derive fair values using configurable spreads and weights."""

    def __init__(self, spread_buffer_bps: float = 0):
        self.spread_buffer_bps = spread_buffer_bps

    def from_quotes(self, quotes: Iterable[Quote]) -> float:
        """Compute a buffered mid across quotes."""

        mids = [quote.mid for quote in quotes]
        if not mids:
            raise ValueError("No quotes provided for fair value calculation")

        base_fair = sum(mids) / len(mids)
        buffer = base_fair * (self.spread_buffer_bps / 10_000)
        return base_fair + buffer
