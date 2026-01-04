"""Execution service for Polymarket complete-set arbitrage flows.

The executor is deliberately conservative: it enforces idempotency, validates
that projected costs still leave positive edge after fees/slippage, and
captures partial fills via the shared :class:`OrderManager`.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol

from src.execution.hedging import HedgeExecutor
from src.execution.order_manager import OrderManager, OrderRequest, OrderState
from src.infra.persistence import FileSystemBackend, SnapshotStore, SQLiteStorageBackend
from src.pricing.market_arbitrage import CompleteSetOpportunity, MarketBook, OutcomeQuote
from src.risk.inventory import InventoryCaps
from src.risk.limits import RiskLimits
from src.risk.pnl import PnLTracker


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
    max_data_staleness_seconds: float = 10.0
    max_reject_streak: int = 3
    max_hedge_failures: int = 3
    snapshot_backend: str = "file"  # "file" or "sqlite"
    snapshot_path: str = "data/snapshots"
    snapshot_name: str = "risk_state"


@dataclass
class ExecutionReport:
    """Outcome of an execution attempt."""

    orders: List[OrderState] = field(default_factory=list)
    skipped: bool = False
    reason: Optional[str] = None


@dataclass
class Position:
    """Tracks signed quantity and average entry price."""

    symbol: str
    quantity: float = 0.0
    avg_price: float = 0.0


class PolymarketExecutor:
    """Orchestrates complete-set execution against the Polymarket venue."""

    def __init__(
        self,
        client: PolymarketTradingClient,
        order_manager: OrderManager,
        risk_limits: Optional[RiskLimits] = None,
        inventory_caps: Optional[InventoryCaps] = None,
        pnl_tracker: Optional[PnLTracker] = None,
        snapshot_store: Optional[SnapshotStore] = None,
        hedge_executor: Optional[HedgeExecutor] = None,
        config: Optional[ExecutionConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.client = client
        self.order_manager = order_manager
        self.config = config or ExecutionConfig()
        self.risk_limits = risk_limits
        self.inventory_caps = inventory_caps
        self.pnl_tracker = pnl_tracker or PnLTracker()
        self.snapshot_store = snapshot_store or self._default_snapshot_store()
        self.hedge_executor = hedge_executor
        if self.hedge_executor:
            self.hedge_executor.max_failures = self.config.max_hedge_failures
        self.logger = logger or logging.getLogger(__name__)
        self._recent_opportunities: Dict[str, float] = {}
        self._positions: Dict[str, Position] = {}
        self._inventory: Dict[str, float] = {}
        self._reject_streak = 0
        self._halted_reason: Optional[str] = None

    async def execute_complete_set(
        self, opportunity: CompleteSetOpportunity, market: MarketBook, size: Optional[float] = None
    ) -> ExecutionReport:
        """Execute a detected opportunity if still profitable."""

        if self._circuit_open():
            return ExecutionReport(skipped=True, reason=self._halted_reason)

        if self._is_market_stale(market):
            self._trip_circuit("stale_data", market.market_id)
            return ExecutionReport(skipped=True, reason="stale_data")

        if self.hedge_executor and getattr(self.hedge_executor, "circuit_open", False):
            self._trip_circuit("hedge_circuit", market.market_id)
            return ExecutionReport(skipped=True, reason="hedge_circuit")

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

        projected_notional = self._estimate_notional(opportunity, market, trade_size)
        if not self._passes_risk_limits(opportunity, market, trade_size, projected_notional):
            return ExecutionReport(skipped=True, reason="risk_blocked")

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
                request,
                market,
                order_id,
                self.client.place_order,
                market_id,
                quote.outcome_id,
                "buy",
                size,
                limit_price,
            )
            orders.append(state)
        return orders

    async def _sell_complete_set(self, market_id: str, market: MarketBook, size: float) -> List[OrderState]:
        orders: List[OrderState] = []
        mint_id = self._generate_order_id("mint")
        mint_state = await self._submit_order(
            OrderRequest(symbol=market_id, side="buy", order_type="market", quantity=size),
            market,
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
                request,
                market,
                order_id,
                self.client.place_order,
                market_id,
                quote.outcome_id,
                "sell",
                size,
                limit_price,
            )
            orders.append(state)
        return orders

    async def _submit_order(
        self,
        request: OrderRequest,
        market: MarketBook,
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
            self._handle_reject()
            return state

        status = self._extract_status(response)
        if status == "rejected":
            state.status = "rejected"
            self._handle_reject()
            return state

        filled = self._extract_filled_quantity(response)
        if filled > 0:
            self.order_manager.update_fill(order_id, filled)
            state = self.order_manager.get_order(order_id)
            self._record_fill(state.request, filled, response, market)
            self._reject_streak = 0
        return state

    async def _call_with_timeout(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(func):
            return await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout_seconds)
        return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=self.config.timeout_seconds)

    def _default_snapshot_store(self) -> SnapshotStore:
        path = Path(self.config.snapshot_path)
        if self.config.snapshot_backend == "sqlite":
            backend = SQLiteStorageBackend(path)
        else:
            backend = FileSystemBackend(path)
        return SnapshotStore(backend)

    def _is_market_stale(self, market: MarketBook) -> bool:
        age_seconds = (datetime.now(timezone.utc) - market.last_update).total_seconds()
        if age_seconds <= self.config.max_data_staleness_seconds:
            return False
        self.logger.error(
            "Market %s is stale (age=%.2fs)", market.market_id, age_seconds,
            extra={"event": "stale_market", "market_id": market.market_id, "age_seconds": age_seconds},
        )
        return True

    def _circuit_open(self) -> bool:
        return self._halted_reason is not None

    def _trip_circuit(self, reason: str, market_id: Optional[str]) -> None:
        self._halted_reason = reason
        self.logger.error(
            "Execution halted due to %s", reason,
            extra={"event": "circuit_breaker", "reason": reason, "market_id": market_id},
        )

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

    def _estimate_notional(self, opportunity: CompleteSetOpportunity, market: MarketBook, size: float) -> float:
        if opportunity.direction == "buy_set":
            unit = self._sum_field(market, field="ask") * (1 + self.config.max_slippage_pct)
        else:
            unit = self._sum_field(market, field="bid") * (1 - self.config.max_slippage_pct)
        return max(unit, 0.0) * size

    def _passes_risk_limits(
        self, opportunity: CompleteSetOpportunity, market: MarketBook, trade_size: float, projected_notional: float
    ) -> bool:
        if self.risk_limits and projected_notional > self.risk_limits.max_notional_usd:
            self.logger.warning(
                "Projected notional %.2f exceeds max %.2f", projected_notional, self.risk_limits.max_notional_usd,
                extra={
                    "event": "risk_blocked",
                    "market_id": opportunity.market_id,
                    "projected_notional": projected_notional,
                    "max_notional": self.risk_limits.max_notional_usd,
                },
            )
            return False

        if not self._positions_within_limits(opportunity, market, trade_size):
            return False

        if self.risk_limits and not self.risk_limits.validate_loss(self._current_realized_loss()):
            self._trip_circuit("daily_loss_limit", opportunity.market_id)
            return False
        return True

    def _positions_within_limits(
        self, opportunity: CompleteSetOpportunity, market: MarketBook, trade_size: float
    ) -> bool:
        projections = self._projected_inventory(opportunity, market, trade_size)
        for symbol, projected in projections.items():
            if self.inventory_caps and not self.inventory_caps.within_caps(symbol, projected):
                self.logger.warning(
                    "Inventory cap breached for %s (projected=%s)", symbol, projected,
                    extra={"event": "inventory_cap", "symbol": symbol, "projected": projected},
                )
                return False
            if self.risk_limits and not self.risk_limits.validate_position(symbol, projected):
                self.logger.warning(
                    "Risk position limit breached for %s (projected=%s)", symbol, projected,
                    extra={"event": "risk_position_limit", "symbol": symbol, "projected": projected},
                )
                return False
        return True

    def _projected_inventory(
        self, opportunity: CompleteSetOpportunity, market: MarketBook, trade_size: float
    ) -> Dict[str, float]:
        projections: Dict[str, float] = {}
        delta = trade_size if opportunity.direction == "buy_set" else -trade_size
        field = "ask" if opportunity.direction == "buy_set" else "bid"
        for quote in self._iter_outcomes(market, require_field=field):
            symbol = f"{market.market_id}:{quote.outcome_id}"
            current = self._positions.get(symbol, Position(symbol)).quantity
            projections[symbol] = current + delta

        if opportunity.direction == "sell_set":
            mint_symbol = market.market_id
            current_mint = self._positions.get(mint_symbol, Position(mint_symbol)).quantity
            projections[mint_symbol] = current_mint + trade_size
        return projections

    def _current_realized_loss(self) -> float:
        losses = [-p.realized for p in self.pnl_tracker.positions.values() if p.realized < 0]
        return sum(losses)

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

    def _handle_reject(self) -> None:
        self._reject_streak += 1
        if self._reject_streak >= self.config.max_reject_streak:
            self._trip_circuit("reject_streak", None)

    def _record_fill(
        self, request: OrderRequest, filled_quantity: float, response: Optional[Dict[str, Any]], market: MarketBook
    ) -> None:
        symbol = request.symbol
        price = self._extract_fill_price(response, request)
        position = self._positions.get(symbol, Position(symbol))
        updated, realized = self._apply_fill_to_position(position, request.side, filled_quantity, price)
        self._positions[symbol] = updated
        self._inventory[symbol] = updated.quantity

        if realized != 0:
            self.pnl_tracker.add_realized(symbol, realized)

        self._mark_unrealized(symbol, updated, market)
        self._persist_snapshot()

    def _apply_fill_to_position(
        self, position: Position, side: str, quantity: float, price: float
    ) -> tuple[Position, float]:
        realized = 0.0
        remaining = quantity
        new_qty = position.quantity
        avg_price = position.avg_price

        if side == "buy" and new_qty < 0:
            closing = min(remaining, -new_qty)
            realized += (avg_price - price) * closing
            new_qty += closing
            remaining -= closing
        if side == "sell" and new_qty > 0:
            closing = min(remaining, new_qty)
            realized += (price - avg_price) * closing
            new_qty -= closing
            remaining -= closing

        if remaining > 0:
            if side == "buy":
                if new_qty <= 0:
                    new_qty += remaining
                    avg_price = price
                else:
                    cost = (new_qty * avg_price) + (remaining * price)
                    new_qty += remaining
                    avg_price = cost / new_qty
            else:
                if new_qty >= 0:
                    new_qty -= remaining
                    avg_price = price
                else:
                    total_short = abs(new_qty)
                    new_qty -= remaining
                    avg_price = ((total_short * avg_price) + (remaining * price)) / abs(new_qty)

        return Position(position.symbol, new_qty, avg_price), realized

    def _mark_unrealized(self, symbol: str, position: Position, market: MarketBook) -> None:
        mark = self._mark_price(symbol, market)
        if mark is None:
            self.pnl_tracker.update_unrealized(symbol, 0.0)
            return
        unrealized = (mark - position.avg_price) * position.quantity
        self.pnl_tracker.update_unrealized(symbol, unrealized)

    def _mark_price(self, symbol: str, market: MarketBook) -> Optional[float]:
        parts = symbol.split(":")
        if len(parts) != 2 or market.market_id != parts[0]:
            return None
        outcome_id = parts[1]
        quote = market.outcomes.get(outcome_id)
        if not quote:
            return None
        bids_asks = [price for price in (quote.bid, quote.ask) if price is not None]
        if not bids_asks:
            return None
        if quote.bid is not None and quote.ask is not None:
            return (quote.bid + quote.ask) / 2
        return bids_asks[0]

    def _persist_snapshot(self) -> None:
        if not self.snapshot_store:
            return
        payload = {
            "positions": {symbol: {"quantity": pos.quantity, "avg_price": pos.avg_price} for symbol, pos in self._positions.items()},
            "inventory": self._inventory,
            "pnl": {
                symbol: {"realized": pnl.realized, "unrealized": pnl.unrealized, "total": pnl.total}
                for symbol, pnl in self.pnl_tracker.positions.items()
            },
        }
        try:
            self.snapshot_store.persist_snapshot(self.config.snapshot_name, json.dumps(payload).encode("utf-8"))
        except Exception as exc:  # pragma: no cover - defensive persistence guard
            self.logger.warning("Snapshot persistence failed: %s", exc)

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

    def _extract_fill_price(self, response: Optional[Dict[str, Any]], request: OrderRequest) -> float:
        if response:
            for key in ("price", "avg_price", "average_price", "fill_price"):
                price = response.get(key)
                if price is None:
                    continue
                try:
                    return float(price)
                except (TypeError, ValueError):
                    continue
        if request.price is not None:
            return float(request.price)
        return 1.0

    def _extract_status(self, response: Optional[Dict[str, Any]]) -> Optional[str]:
        if not response:
            return None
        status = response.get("status") or response.get("state")
        if status and str(status).lower() in {"reject", "rejected", "error"}:
            return "rejected"
        if response.get("rejected") is True:
            return "rejected"
        return None

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
