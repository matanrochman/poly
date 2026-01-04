import unittest

from src.data.polymarket_client import NormalizedMarketData
from src.execution.order_manager import OrderManager
from src.execution.polymarket_executor import ExecutionConfig, PolymarketExecutor
from src.pricing.market_arbitrage import MarketArbitrageDetector
from src.risk.limits import RiskLimits


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


def make_order_book_event(
    market_id: str,
    outcome_id: str,
    bid: float | None,
    ask: float | None,
    size: float,
) -> NormalizedMarketData:
    return NormalizedMarketData(
        market_id=market_id,
        outcome_id=outcome_id,
        bid=bid,
        ask=ask,
        size=size,
        last_trade=None,
        fee_bps=0,
        liquidity=None,
        type="order_book",
    )


class PolymarketExecutorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.order_manager = OrderManager()
        self.snapshot_store = NullSnapshotStore()

    async def test_risk_gate_blocks_when_notional_exceeds_limit(self) -> None:
        detector = MarketArbitrageDetector(min_edge_bps=0.0)
        events = [
            make_order_book_event("risk", "yes", bid=0.25, ask=0.30, size=10),
            make_order_book_event("risk", "no", bid=0.25, ask=0.30, size=10),
        ]
        opportunity = None
        for event in events:
            opportunity = detector.ingest(event)
        assert opportunity is not None
        market = detector.snapshot()["risk"]

        risk_limits = RiskLimits(
            max_notional_usd=5.0,
            max_position_sizes={"risk:yes": 100.0, "risk:no": 100.0},
            daily_loss_limit_usd=1_000.0,
        )
        executor = PolymarketExecutor(
            StubTradingClient(),
            self.order_manager,
            risk_limits=risk_limits,
            snapshot_store=self.snapshot_store,
            config=ExecutionConfig(dry_run=True),
        )

        report = await executor.execute_complete_set(opportunity, market, size=10)
        self.assertTrue(report.skipped)
        self.assertEqual("risk_blocked", report.reason)
        self.assertEqual(0, executor.client.call_count)

    async def test_slippage_threshold_blocks_execution(self) -> None:
        detector = MarketArbitrageDetector(min_edge_bps=0.0)
        events = [
            make_order_book_event("slip", "yes", bid=0.48, ask=0.49, size=5),
            make_order_book_event("slip", "no", bid=0.48, ask=0.49, size=5),
        ]
        opportunity = None
        for event in events:
            opportunity = detector.ingest(event)
        assert opportunity is not None
        market = detector.snapshot()["slip"]

        executor = PolymarketExecutor(
            StubTradingClient(),
            self.order_manager,
            snapshot_store=self.snapshot_store,
            config=ExecutionConfig(max_slippage_pct=0.05, dry_run=True),
        )

        report = await executor.execute_complete_set(opportunity, market, size=5)
        self.assertTrue(report.skipped)
        self.assertEqual("edge_erased", report.reason)
        self.assertEqual(0, executor.client.call_count)

    async def test_dry_run_records_orders_without_network_calls(self) -> None:
        detector = MarketArbitrageDetector(min_edge_bps=0.0)
        events = [
            make_order_book_event("dry", "yes", bid=0.6, ask=0.40, size=3),
            make_order_book_event("dry", "no", bid=0.6, ask=0.40, size=3),
        ]
        opportunity = None
        for event in events:
            opportunity = detector.ingest(event)
        assert opportunity is not None
        market = detector.snapshot()["dry"]

        client = StubTradingClient()
        executor = PolymarketExecutor(
            client,
            self.order_manager,
            snapshot_store=self.snapshot_store,
            config=ExecutionConfig(dry_run=True),
        )

        report = await executor.execute_complete_set(opportunity, market, size=2)
        self.assertFalse(report.skipped)
        self.assertEqual(2, len(report.orders))
        self.assertTrue(all(order.status == "filled" for order in report.orders))
        self.assertTrue(all(order.filled_quantity == 2 for order in report.orders))
        self.assertEqual(0, client.call_count)
        self.assertEqual(2, len(self.order_manager.list_orders()))


if __name__ == "__main__":
    unittest.main()

