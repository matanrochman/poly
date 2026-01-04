"""Basic risk limits and validation."""

from dataclasses import dataclass
from typing import Dict


@dataclass
class RiskLimits:
    """Defines position and loss limits per symbol."""

    max_notional_usd: float
    max_position_sizes: Dict[str, float]
    daily_loss_limit_usd: float

    def validate_position(self, symbol: str, proposed_size: float) -> bool:
        """Return True if the proposed position is within limits."""

        max_size = self.max_position_sizes.get(symbol)
        return max_size is not None and abs(proposed_size) <= max_size

    def validate_loss(self, realized_loss_usd: float) -> bool:
        """Return True when realized loss is under the daily cap."""

        return realized_loss_usd <= self.daily_loss_limit_usd
