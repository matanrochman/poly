import unittest

from src.data.polymarket_client import NormalizedMarketData
from src.pricing.market_arbitrage import MarketArbitrageDetector


class MarketArbitrageDetectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.detector = MarketArbitrageDetector(min_edge_bps=1.0)

    def test_detects_buy_complete_set(self) -> None:
        yes = NormalizedMarketData(
            market_id="m1",
            outcome_id="yes",
            bid=0.52,
            ask=0.45,
            size=10,
            last_trade=None,
            fee_bps=0,
            liquidity=None,
            type="order_book",
        )
        no = NormalizedMarketData(
            market_id="m1",
            outcome_id="no",
            bid=0.52,
            ask=0.45,
            size=12,
            last_trade=None,
            fee_bps=0,
            liquidity=None,
            type="order_book",
        )

        self.assertIsNone(self.detector.ingest(yes))
        opportunity = self.detector.ingest(no)
        self.assertIsNotNone(opportunity)
        assert opportunity  # mypy/pyright hinting
        self.assertEqual("buy_set", opportunity.direction)
        self.assertAlmostEqual(0.10, opportunity.edge, places=3)
        self.assertAlmostEqual(0.90, opportunity.notional, places=2)
        self.assertEqual(10, opportunity.max_size)

    def test_detects_sell_complete_set(self) -> None:
        yes = NormalizedMarketData(
            market_id="m2",
            outcome_id="yes",
            bid=0.55,
            ask=0.9,
            size=7,
            last_trade=None,
            fee_bps=0,
            liquidity=None,
            type="order_book",
        )
        no = NormalizedMarketData(
            market_id="m2",
            outcome_id="no",
            bid=0.55,
            ask=0.9,
            size=6,
            last_trade=None,
            fee_bps=0,
            liquidity=None,
            type="order_book",
        )

        self.assertIsNone(self.detector.ingest(yes))
        opportunity = self.detector.ingest(no)
        self.assertIsNotNone(opportunity)
        assert opportunity
        self.assertEqual("sell_set", opportunity.direction)
        self.assertAlmostEqual(0.10, opportunity.edge, places=3)
        self.assertAlmostEqual(1.10, opportunity.notional, places=2)
        self.assertEqual(6, opportunity.max_size)


if __name__ == "__main__":
    unittest.main()
