"""Client interfaces for interacting with trading venues."""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Protocol


@dataclass
class VenueEndpoint:
    """Connection details for a venue.

    Attributes:
        name: Human readable venue identifier.
        rest_url: Base REST endpoint for HTTP requests.
        websocket_url: WebSocket endpoint for streaming data.
    """

    name: str
    rest_url: str
    websocket_url: str


class MarketDataClient(Protocol):
    """Protocol describing required market data client behavior."""

    endpoint: VenueEndpoint

    def fetch_order_book(self, symbol: str) -> Dict[str, Any]:
        """Return the current order book snapshot for a symbol."""

    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Return the latest ticker data for a symbol."""

    def list_symbols(self) -> Iterable[str]:
        """Return all supported trading symbols for the venue."""
