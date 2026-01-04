"""Polling utilities for periodic REST-based market data collection."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Iterable, List

from .clients import MarketDataClient


@dataclass
class PollingTask:
    """Configuration for a single polling action."""

    symbol: str
    interval: timedelta
    handler: Callable[[str], None]


class PollingClient:
    """Simple polling coordinator for market data snapshots."""

    def __init__(self, client: MarketDataClient):
        self._client = client
        self._tasks: List[PollingTask] = []

    def add_task(self, task: PollingTask) -> None:
        """Register a new polling task."""

        self._tasks.append(task)

    def run_once(self) -> None:
        """Execute a single polling pass across all tasks."""

        for task in self._tasks:
            task.handler(task.symbol)

    def symbols(self) -> Iterable[str]:
        """Expose symbols supported by the underlying client."""

        return self._client.list_symbols()
