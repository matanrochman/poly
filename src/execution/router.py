"""Route orders to venues based on latency and availability."""

from dataclasses import dataclass
from typing import Dict, Iterable, Optional


@dataclass
class RoutePreference:
    """Preferred routing strategy for a symbol."""

    primary: str
    secondary: Optional[str] = None


class ExecutionRouter:
    """Simple router using latency budgets and preferences."""

    def __init__(self, latency_budget_ms: int, preferences: Dict[str, RoutePreference]):
        self.latency_budget_ms = latency_budget_ms
        self.preferences = preferences

    def choose_venue(self, symbol: str, venue_latencies_ms: Dict[str, int]) -> Optional[str]:
        """Select a venue under the latency budget, respecting preferences."""

        preference = self.preferences.get(symbol)
        if preference:
            for venue_name in self._preference_order(preference):
                if self._within_budget(venue_name, venue_latencies_ms):
                    return venue_name
        return self._fastest_within_budget(venue_latencies_ms)

    def _preference_order(self, preference: RoutePreference) -> Iterable[str]:
        if preference.secondary:
            return (preference.primary, preference.secondary)
        return (preference.primary,)

    def _within_budget(self, venue_name: str, venue_latencies_ms: Dict[str, int]) -> bool:
        latency = venue_latencies_ms.get(venue_name)
        return latency is not None and latency <= self.latency_budget_ms

    def _fastest_within_budget(self, venue_latencies_ms: Dict[str, int]) -> Optional[str]:
        eligible = {name: latency for name, latency in venue_latencies_ms.items() if latency <= self.latency_budget_ms}
        if not eligible:
            return None
        return min(eligible, key=eligible.get)
