"""Inventory and position caps."""

from dataclasses import dataclass
from typing import Dict


@dataclass
class InventoryCaps:
    """Defines and checks per-symbol inventory ceilings."""

    caps: Dict[str, float]

    def within_caps(self, symbol: str, proposed_inventory: float) -> bool:
        """Return True if inventory remains under the configured cap."""

        cap = self.caps.get(symbol)
        return cap is not None and abs(proposed_inventory) <= cap
