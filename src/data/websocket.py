"""WebSocket subscription helpers for streaming market data."""

from dataclasses import dataclass
from typing import Callable


@dataclass
class WebSocketSubscription:
    """Represents a streaming subscription request."""

    channel: str
    symbol: str
    on_message: Callable[[bytes], None]

    def topic(self) -> str:
        """Return the subscription topic for the channel and symbol."""

        return f"{self.channel}:{self.symbol}"
