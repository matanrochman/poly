"""Track realized and unrealized profit and loss."""

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class PositionPnL:
    """PnL for a single trading symbol."""

    symbol: str
    realized: float = 0.0
    unrealized: float = 0.0

    @property
    def total(self) -> float:
        """Return combined realized and unrealized PnL."""

        return self.realized + self.unrealized


@dataclass
class PnLTracker:
    """Aggregates PnL across all symbols."""

    positions: Dict[str, PositionPnL] = field(default_factory=dict)

    def update_unrealized(self, symbol: str, value: float) -> None:
        """Set the unrealized PnL for a symbol."""

        pnl = self.positions.setdefault(symbol, PositionPnL(symbol))
        pnl.unrealized = value

    def add_realized(self, symbol: str, value: float) -> None:
        """Accumulate realized PnL for a symbol."""

        pnl = self.positions.setdefault(symbol, PositionPnL(symbol))
        pnl.realized += value

    def total_pnl(self) -> float:
        """Return portfolio-level PnL."""

        return sum(position.total for position in self.positions.values())
