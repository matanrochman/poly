"""Execution service for Polymarket complete-set arbitrage flows.

The executor is deliberately conservative: it enforces idempotency, validates
that projected costs still leave positive edge after fees/slippage, and
captures partial fills via the shared :class:`OrderManager`.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Protocol

from src.execution.order_manager import OrderManager, OrderRequest, OrderState
from src.pricing.market_arbitrage import CompleteSetOpportunity, MarketBook, OutcomeQuote


class PolymarketTradingClient(Protocol):
    """Thin protocol describing the minimal trading surface used by the executor."""

    async def place_order(
        self,
        market_id: str,
        outcome_id: str,
        side: str,
        size: float,
        limit_price: Optional[float],
        client_order_id: str,
    ) -> Dict[str, Any]:
        ...

    async def mint_complete_set(self, market_id: str, size: float, client_order_id: str) -> Dict[str, Any]:
        ...


@dataclass
class ExecutionConfig:
    """Runtime parameters for execution safety."""

    max_fee_bps: float = 100.0
    max_slippage_pct: float = 0.01
    timeout_seconds: float = 5.0
    idempotency_ttl_seconds: float = 60.0


@dataclass
class ExecutionReport:
    """Outcome of an execution attempt."""

    orders: List[OrderState] = field(default_factory=list)
    skipped: bool = False
    reason: Optional[str] = None


class PolymarketExecutor:
    """Orchestrates complete-set execution against the Polymarket venue."""

    def __init__(
        self,
        client: PolymarketTradingClient,
        order_manager: OrderManager,
        config: Optional[ExecutionConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.client = client
        self.order_manager = order_manager
        self.config = config or ExecutionConfig()
        self.logger = logger or logging.getLogger(__name__)
        self._recent_opportunities: Dict[str, float] = {}

    async def execute_complete_set(
        self, opportunity: CompleteSetOpportunity, market: MarketBook, size: Optional[float] = None
    ) -> ExecutionReport:
        """Execute a detected opportunity if still profitable."""

        key = self._opportunity_key(opportunity)
        if not self._claim_idempotency(key):
            self.logger.info(
                "Skipping duplicate opportunity for %s", opportunity.market_id,
                extra={"event": "idempotent_skip", "market_id": opportunity.market_id, "direction": opportunity.direction},
            )
            return ExecutionReport(skipped=True, reason="duplicate")

        if not self._edge_survives_costs(opportunity, market):
            self.logger.info(
                "Edge eliminated by projected costs for %s", opportunity.market_id,
                extra={"event": "edge_erased", "market_id": opportunity.market_id, "direction": opportunity.direction},
            )
            return ExecutionReport(skipped=True, reason="edge_erased")

        trade_size = min(size or opportunity.max_size, opportunity.max_size)
        if trade_size <= 0:
            return ExecutionReport(skipped=True, reason="no_size_available")

        if opportunity.direction == "buy_set":
            orders = await self._buy_complete_set(opportunity.market_id, market, trade_size)
        else:
            orders = await self._sell_complete_set(opportunity.market_id, market, trade_size)

        return ExecutionReport(orders=orders, skipped=False)

    async def _buy_complete_set(self, market_id: str, market: MarketBook, size: float) -> List[OrderState]:
        orders: List[OrderState] = []
        for quote in self._iter_outcomes(market, require_field="ask"):
            limit_price = quote.ask * (1 + self.config.max_slippage_pct)
            order_id = self._generate_order_id("buy")
            request = OrderRequest(
                symbol=f"{market_id}:{quote.outcome_id}",
                side="buy",
                order_type="market",
                quantity=size,
                price=limit_price,
            )
            state = await self._submit_order(
                request, order_id, self.client.place_order, market_id, quote.outcome_id, "buy", size, limit_price
            )
            orders.append(state)
        return orders

    async def _sell_complete_set(self, market_id: str, market: MarketBook, size: float) -> List[OrderState]:
        orders: List[OrderState] = []
        mint_id = self._generate_order_id("mint")
        mint_state = await self._submit_order(
            OrderRequest(symbol=market_id, side="buy", order_type="market", quantity=size),
            mint_id,
            self.client.mint_complete_set,
            market_id,
            size,
        )
        orders.append(mint_state)

        for quote in self._iter_outcomes(market, require_field="bid"):
            limit_price = quote.bid * (1 - self.config.max_slippage_pct)
            order_id = self._generate_order_id("sell")
            request = OrderRequest(
                symbol=f"{market_id}:{quote.outcome_id}",
                side="sell",
                order_type="market",
                quantity=size,
                price=limit_price,
            )
            state = await self._submit_order(
                request, order_id, self.client.place_order, market_id, quote.outcome_id, "sell", size, limit_price
            )
            orders.append(state)
        return orders

    async def _submit_order(
        self,
        request: OrderRequest,
        order_id: str,
        func: Any,
        *args: Any,
    ) -> OrderState:
        state = OrderState(order_id=order_id, request=request)
        self.order_manager.record_submission(state)
        try:
            response = await self._call_with_timeout(func, *args, client_order_id=order_id)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Order timed out for %s", request.symbol,
                extra={"event": "order_timeout", "order_id": order_id, "symbol": request.symbol},
            )
            state.status = "timeout"
            return state

        filled = self._extract_filled_quantity(response)
        if filled > 0:
            self.order_manager.update_fill(order_id, filled)
            state = self.order_manager.get_order(order_id)
        return state

    async def _call_with_timeout(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(func):
            return await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout_seconds)
        return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=self.config.timeout_seconds)

    def _edge_survives_costs(self, opportunity: CompleteSetOpportunity, market: MarketBook) -> bool:
        fee_multiplier = 1 + (market.fee_bps or 0) / 10_000
        if opportunity.direction == "buy_set":
            ask_sum = self._sum_field(market, field="ask")
            if ask_sum <= 0:
                return False
            projected = ask_sum * (1 + self.config.max_slippage_pct) * fee_multiplier
            return projected < 1.0

        bid_sum = self._sum_field(market, field="bid")
        if bid_sum <= 0:
            return False
        projected = bid_sum * (1 - self.config.max_slippage_pct) / fee_multiplier
        return projected > 1.0

    def _sum_field(self, market: MarketBook, field: str) -> float:
        total = 0.0
        for quote in self._iter_outcomes(market, require_field=field):
            value = getattr(quote, field)
            if value is not None:
                total += value
        return total

    def _iter_outcomes(self, market: MarketBook, require_field: str) -> Iterable[OutcomeQuote]:
        for quote in market.outcome_quotes():
            value = getattr(quote, require_field)
            if value is None:
                continue
            yield quote

    def _extract_filled_quantity(self, response: Optional[Dict[str, Any]]) -> float:
        if not response:
            return 0.0
        for key in ("filled", "filled_size", "filled_qty", "fill", "filledQuantity", "minted"):
            filled = response.get(key)
            if filled is not None:
                try:
                    return float(filled)
                except (TypeError, ValueError):
                    continue
        return 0.0

    def _claim_idempotency(self, key: str) -> bool:
        now = time.monotonic()
        recent = self._recent_opportunities.get(key)
        if recent and now - recent < self.config.idempotency_ttl_seconds:
            return False
        self._recent_opportunities[key] = now
        return True

    def _opportunity_key(self, opportunity: CompleteSetOpportunity) -> str:
        return f"{opportunity.market_id}:{opportunity.direction}:{round(opportunity.edge, 6)}"

    def _generate_order_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"


__all__ = ["PolymarketExecutor", "ExecutionConfig", "ExecutionReport"]
