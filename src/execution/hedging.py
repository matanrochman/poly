"""Hedging interfaces for secondary venue execution.

The hedging surface is intentionally minimal: callers provide a list of
actions and the executor will submit them via the configured client, falling
back to a no-op implementation so the rest of the pipeline does not need to
gate on availability.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Iterable, List, Optional, Protocol

from src.execution.order_manager import OrderManager, OrderRequest, OrderState, OrderType


@dataclass
class HedgeAction:
    """Instruction to offset exposure on a secondary venue."""

    symbol: str
    side: str
    size: float
    price: Optional[float] = None
    order_type: OrderType = "limit"
    venue: Optional[str] = None


class HedgeTradingClient(Protocol):
    """Minimal surface area required for hedge submission."""

    async def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float],
        order_type: str,
        client_order_id: str,
    ) -> dict:
        ...


class NoopHedgeClient:
    """Placeholder client that records intent without touching a venue."""

    async def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float],
        order_type: str,
        client_order_id: str,
    ) -> dict:
        return {"client_order_id": client_order_id, "submitted": False, "filled": 0.0}


class HedgeExecutor:
    """Submit hedge actions through the configured trading client."""

    def __init__(
        self,
        order_manager: OrderManager,
        client: Optional[HedgeTradingClient] = None,
        logger: Optional[logging.Logger] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.order_manager = order_manager
        self.client = client or NoopHedgeClient()
        self.logger = logger or logging.getLogger(__name__)
        self.timeout_seconds = timeout_seconds

    async def submit_hedges(self, hedge_actions: Iterable[HedgeAction]) -> List[OrderState]:
        """Submit all hedge actions, returning their tracked order states."""

        states: List[OrderState] = []
        for action in hedge_actions:
            if action.size <= 0:
                self.logger.info(
                    "Skipping hedge with non-positive size for %s", action.symbol,
                    extra={"event": "hedge_skipped", "symbol": action.symbol, "size": action.size},
                )
                continue

            state = await self._submit(action)
            states.append(state)
        return states

    async def _submit(self, action: HedgeAction) -> OrderState:
        order_id = self._generate_order_id("hedge")
        request = OrderRequest(
            symbol=action.symbol,
            side=action.side,
            order_type=action.order_type,
            quantity=action.size,
            price=action.price,
        )

        state = OrderState(order_id=order_id, request=request)
        self.order_manager.record_submission(state)

        try:
            response = await asyncio.wait_for(
                self.client.place_order(
                    action.symbol,
                    action.side,
                    action.size,
                    action.price,
                    action.order_type,
                    order_id,
                ),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            self.logger.warning(
                "Hedge order timed out for %s", action.symbol,
                extra={"event": "hedge_timeout", "symbol": action.symbol, "order_id": order_id},
            )
            state.status = "timeout"
            return state

        filled = self._extract_filled_quantity(response)
        if filled > 0:
            self.order_manager.update_fill(order_id, filled)
            state = self.order_manager.get_order(order_id)
        return state

    def _extract_filled_quantity(self, response: Optional[dict]) -> float:
        if not response:
            return 0.0
        for key in ("filled", "filled_size", "filled_qty", "fill", "filledQuantity"):
            filled = response.get(key)
            if filled is None:
                continue
            try:
                return float(filled)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _generate_order_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"


__all__ = [
    "HedgeAction",
    "HedgeTradingClient",
    "HedgeExecutor",
    "NoopHedgeClient",
]
