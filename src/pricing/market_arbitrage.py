"""Arbitrage detection for same-market Polymarket outcomes.

The goal is to identify complete-set mispricings (e.g., YES+NO notional
departing from $1) while keeping the abstraction flexible enough to extend to
cross- or correlated-market opportunities later. The detector consumes
``NormalizedMarketData`` order book updates and maintains a per-market view of
best bids/asks and available size per outcome.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from src.data.polymarket_client import NormalizedMarketData


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class OutcomeQuote:
    """Top-of-book snapshot for a single outcome."""

    outcome_id: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    size: Optional[float] = None
    fee_bps: Optional[int] = None
    updated_at: datetime = field(default_factory=_now)

    def update_from(self, data: NormalizedMarketData) -> None:
        self.bid = data.bid if data.bid is not None else self.bid
        self.ask = data.ask if data.ask is not None else self.ask
        self.size = data.size if data.size is not None else self.size
        self.fee_bps = data.fee_bps if data.fee_bps is not None else self.fee_bps
        self.updated_at = _now()


@dataclass
class MarketBook:
    """Aggregated order book state across all outcomes in a market."""

    market_id: str
    outcomes: Dict[str, OutcomeQuote] = field(default_factory=dict)
    fee_bps: Optional[int] = None
    last_update: datetime = field(default_factory=_now)

    def update_from(self, data: NormalizedMarketData) -> None:
        outcome_id = data.outcome_id or "default"
        outcome = self.outcomes.setdefault(outcome_id, OutcomeQuote(outcome_id))
        outcome.update_from(data)
        if data.fee_bps is not None:
            self.fee_bps = data.fee_bps
        self.last_update = _now()

    def outcome_quotes(self) -> Iterable[OutcomeQuote]:
        return self.outcomes.values()


@dataclass
class CompleteSetOpportunity:
    """Represents a detected complete-set arbitrage opportunity."""

    market_id: str
    direction: str  # "buy_set" (sum asks < 1) or "sell_set" (sum bids > 1)
    edge: float
    notional: float
    max_size: float
    details: Dict[str, float]


class MarketArbitrageDetector:
    """Detect same-market Polymarket arbitrage (complete-set mispricing).

    Logic:
    - If the sum of best asks (plus fees) across outcomes is < 1, buy the
      complete set.
    - If the sum of best bids (minus fees) across outcomes is > 1, mint/sell
      the complete set.

    The detector is outcome-agnostic and works for two-outcome (YES/NO) and
    multi-outcome markets. It can be extended with correlated-market logic by
    adding additional evaluators that consume the maintained MarketBook state.
    """

    def __init__(self, min_edge_bps: float = 10.0) -> None:
        self.min_edge_bps = min_edge_bps
        self._markets: Dict[str, MarketBook] = {}

    def ingest(self, data: NormalizedMarketData) -> Optional[CompleteSetOpportunity]:
        if data.type not in {"order_book", "order_book_snapshot"}:
            return None
        market = self._markets.setdefault(data.market_id, MarketBook(data.market_id))
        market.update_from(data)
        return self._detect_complete_set_arb(market)

    def _detect_complete_set_arb(self, market: MarketBook) -> Optional[CompleteSetOpportunity]:
        quotes = list(market.outcome_quotes())
        if len(quotes) < 2:
            return None

        fee_multiplier = 1 + (market.fee_bps or 0) / 10_000
        ask_sum = 0.0
        bid_sum = 0.0
        sizes: List[float] = []

        for quote in quotes:
            if quote.ask is not None:
                ask_sum += quote.ask * fee_multiplier
            if quote.bid is not None:
                bid_sum += quote.bid * (2 - fee_multiplier)  # approximate fee-adjusted take-profit
            if quote.size is not None:
                sizes.append(quote.size)

        max_size = min(sizes) if sizes else 0.0
        if max_size <= 0:
            return None

        opportunities: List[CompleteSetOpportunity] = []
        buy_edge = 1.0 - ask_sum
        if self._edge_meets_threshold(buy_edge):
            opportunities.append(
                CompleteSetOpportunity(
                    market_id=market.market_id,
                    direction="buy_set",
                    edge=buy_edge,
                    notional=ask_sum,
                    max_size=max_size,
                    details={"ask_sum": ask_sum, "fee_bps": float(market.fee_bps or 0)},
                )
            )

        sell_edge = bid_sum - 1.0
        if self._edge_meets_threshold(sell_edge):
            opportunities.append(
                CompleteSetOpportunity(
                    market_id=market.market_id,
                    direction="sell_set",
                    edge=sell_edge,
                    notional=bid_sum,
                    max_size=max_size,
                    details={"bid_sum": bid_sum, "fee_bps": float(market.fee_bps or 0)},
                )
            )

        return self._best_opportunity(opportunities)

    def _edge_meets_threshold(self, edge: float) -> bool:
        threshold = self.min_edge_bps / 10_000
        return edge >= threshold

    def _best_opportunity(self, opportunities: List[CompleteSetOpportunity]) -> Optional[CompleteSetOpportunity]:
        if not opportunities:
            return None
        return max(opportunities, key=lambda opp: opp.edge)

    def snapshot(self) -> Dict[str, MarketBook]:
        """Expose current market state for downstream usage or debugging."""

        return self._markets


__all__ = [
    "MarketArbitrageDetector",
    "CompleteSetOpportunity",
    "MarketBook",
    "OutcomeQuote",
]
