"""Lightweight metrics sink for application instrumentation."""

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class MetricsSink:
    """Collects counters and gauges for reporting."""

    counters: Dict[str, int] = field(default_factory=dict)
    gauges: Dict[str, float] = field(default_factory=dict)

    def incr(self, name: str, value: int = 1) -> None:
        """Increment a counter."""

        self.counters[name] = self.counters.get(name, 0) + value

    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge value."""

        self.gauges[name] = value
