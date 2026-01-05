import unittest

from src.data.polymarket_client import NormalizedMarketData
from src.execution.orchestrator import ArbitrageOrchestrator
from src.execution.order_manager import OrderManager
from src.execution.polymarket_executor import ExecutionConfig, PolymarketExecutor
from src.pricing.market_arbitrage import MarketArbitrageDetector


class StubTradingClient:
    def __init__(self) -> None:
        self.call_count = 0

    async def place_order(
        self,
        market_id: str,
        outcome_id: str,
        side: str,
        size: float,
        limit_price: float | None,
        client_order_id: str,
    ) -> dict:
        self.call_count += 1
        return {"filled": size, "price": limit_price}

    async def mint_complete_set(self, market_id: str, size: float, client_order_id: str) -> dict:
        self.call_count += 1
        return {"minted": size, "price": 1.0}


class NullSnapshotStore:
    def persist_snapshot(self, name: str, payload: bytes, timestamp=None) -> str:  # pragma: no cover - trivial
        return "noop"


BOOK_FIXTURE = [
    NormalizedMarketData(
        market_id="fx1",
        outcome_id="yes",
        bid=0.52,
        ask=0.45,
        size=4,
        last_trade=None,
        fee_bps=0,
        liquidity=None,
        type="order_book",
    ),
    NormalizedMarketData(
        market_id="fx1",
        outcome_id="no",
        bid=0.52,
        ask=0.45,
        size=4,
        last_trade=None,
        fee_bps=0,
        liquidity=None,
        type="order_book",
    ),
]


class OrchestratorIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_handles_book_fixture_in_dry_run(self) -> None:
        detector = MarketArbitrageDetector(min_edge_bps=0.0)
        order_manager = OrderManager()
        client = StubTradingClient()
        executor = PolymarketExecutor(
            client,
            order_manager,
            snapshot_store=NullSnapshotStore(),
            config=ExecutionConfig(dry_run=True),
        )
        orchestrator = ArbitrageOrchestrator(detector, executor)

        report = None
        for message in BOOK_FIXTURE:
            report = await orchestrator.handle_message(message)

        self.assertIsNotNone(report)
        assert report
        self.assertFalse(report.skipped)
        self.assertEqual(2, len(report.orders))
        self.assertEqual(0, client.call_count)
        self.assertEqual(2, len(order_manager.list_orders()))


if __name__ == "__main__":
    unittest.main()

