"""Orchestrator wiring market data, detection, and execution."""

from __future__ import annotations

from typing import Optional

from src.data.polymarket_client import NormalizedMarketData
from src.execution.polymarket_executor import ExecutionReport, PolymarketExecutor
from src.pricing.market_arbitrage import CompleteSetOpportunity, MarketArbitrageDetector


class ArbitrageOrchestrator:
    """Consume normalized market data, detect arbs, and trigger execution."""

    def __init__(self, detector: MarketArbitrageDetector, executor: PolymarketExecutor) -> None:
        self.detector = detector
        self.executor = executor

    async def handle_message(self, data: NormalizedMarketData) -> Optional[ExecutionReport]:
        """Process a single market data message, executing on detected arbs."""

        opportunity = self._detect(data)
        if not opportunity:
            return None

        market = self.detector.snapshot().get(opportunity.market_id)
        if market is None:
            return None

        return await self.executor.execute_complete_set(opportunity, market)

    def _detect(self, data: NormalizedMarketData) -> Optional[CompleteSetOpportunity]:
        if data.type not in {"order_book", "order_book_snapshot"}:
            return None
        return self.detector.ingest(data)


__all__ = ["ArbitrageOrchestrator"]

