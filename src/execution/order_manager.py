"""Order management primitives for placing and tracking orders."""

from dataclasses import dataclass, field
from typing import Dict, List, Literal

OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]


@dataclass
class OrderRequest:
    """A request to place an order on a venue."""

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: float | None = None


@dataclass
class OrderState:
    """Tracks the lifecycle of a submitted order."""

    order_id: str
    request: OrderRequest
    status: str = "new"
    filled_quantity: float = 0.0


class OrderManager:
    """Minimal in-memory order state manager."""

    def __init__(self):
        self._orders: Dict[str, OrderState] = {}

    def record_submission(self, state: OrderState) -> None:
        """Persist a newly submitted order state."""

        self._orders[state.order_id] = state

    def update_fill(self, order_id: str, fill_quantity: float) -> None:
        """Apply a fill update to the tracked order."""

        order = self._orders[order_id]
        order.filled_quantity += fill_quantity
        if order.filled_quantity >= order.request.quantity:
            order.status = "filled"
        else:
            order.status = "partial_fill"

    def list_orders(self) -> List[OrderState]:
        """Return the current known orders."""

        return list(self._orders.values())

    def get_order(self, order_id: str) -> OrderState:
        """Fetch a single order by ID."""

        return self._orders[order_id]
