"""Hedge venue client for REST snapshots, WebSocket increments, and accounts.

The client normalizes both REST snapshots and streaming incremental updates into a
single order book schema. It also exposes minimal account endpoints for balance
retrieval and idempotent order placement using client-generated order IDs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence

import requests
import websockets

from .clients import MarketDataClient, VenueEndpoint


@dataclass
class NormalizedOrderBook:
    """Unified order book representation for Hedge snapshots and updates."""

    symbol: str
    bids: List[Dict[str, float]]
    asks: List[Dict[str, float]]
    maker_rate_bps: Optional[float]
    taker_rate_bps: Optional[float]
    min_size: Optional[float]
    sequence: Optional[int]
    timestamp: Optional[datetime]
    type: str
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the normalized book."""

        payload = asdict(self)
        if self.timestamp:
            payload["timestamp"] = self.timestamp.isoformat()
        return payload


class HedgeClient(MarketDataClient):
    """Client for Hedge REST and WebSocket order books plus basic accounts."""

    def __init__(
        self,
        endpoint: Optional[VenueEndpoint] = None,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.endpoint = endpoint or VenueEndpoint(
            name="hedge",
            rest_url="https://api.hedge.com/v1",
            websocket_url="wss://stream.hedge.com/ws",
        )
        self.api_key = api_key
        self.session = session or requests.Session()
        self.logger = logger or logging.getLogger(__name__)

    # --- Market data: REST -------------------------------------------------
    def fetch_order_book(self, symbol: str) -> Dict[str, Any]:
        """Fetch a level 2 order book snapshot via REST and normalize it."""

        path = f"/markets/{symbol}/orderbook"
        payload = self._rest_get(path)
        book = self._normalize_book(payload or {}, kind="snapshot", symbol=symbol)
        return book.to_dict() if book else {}

    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Return Hedge ticker information for a symbol."""

        path = f"/markets/{symbol}/ticker"
        return self._rest_get(path) or {}

    def list_symbols(self) -> Iterable[str]:
        """Return all tradable symbols available on Hedge."""

        payload = self._rest_get("/markets") or {}
        markets: Sequence[Dict[str, Any]] = payload if isinstance(payload, list) else payload.get("markets", [])
        return [str(mkt.get("symbol")) for mkt in markets if mkt.get("symbol")]

    # --- Market data: WebSocket -------------------------------------------
    async def stream_order_books(self, symbols: Iterable[str]) -> AsyncIterator[NormalizedOrderBook]:
        """Yield normalized incremental updates for the provided symbols."""

        async with websockets.connect(self.endpoint.websocket_url, extra_headers=self._ws_headers()) as ws:
            for symbol in symbols:
                await ws.send(json.dumps({"type": "subscribe", "channel": "book", "symbol": symbol}))

            async for raw in ws:
                message = json.loads(raw)
                data = message.get("data") or message
                channel = message.get("channel") or data.get("channel") or message.get("type")
                if channel not in {"book", "orderbook", "l2"}:
                    continue

                kind = "snapshot" if self._is_snapshot(data) else "incremental"
                normalized = self._normalize_book(data, kind=kind)
                if normalized:
                    yield normalized

    # --- Accounts ---------------------------------------------------------
    def fetch_balances(self) -> Dict[str, Any]:
        """Retrieve account balances."""

        return self._rest_get("/account/balances") or {}

    def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float] = None,
        order_type: str = "limit",
        time_in_force: str = "gtc",
        client_order_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Submit an order with an idempotent client order ID."""

        payload: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "size": size,
            "type": order_type,
            "time_in_force": time_in_force,
            "client_order_id": client_order_id or self._client_order_id(),
        }
        if price is not None:
            payload["price"] = price
        if extra:
            payload.update(extra)

        min_size = self._extract_min_size(extra or {})
        if min_size and size < min_size:
            raise ValueError(f"Order size {size} below min size {min_size} for {symbol}")

        return self._rest_post("/orders", json=payload) or {}

    # --- Normalization helpers -------------------------------------------
    def _normalize_book(self, payload: Dict[str, Any], kind: str, symbol: Optional[str] = None) -> Optional[NormalizedOrderBook]:
        book_symbol = symbol or payload.get("symbol") or payload.get("market")
        if not book_symbol:
            return None

        min_size = self._extract_min_size(payload)
        maker_rate = self._safe_float(
            payload.get("maker_rate")
            or payload.get("maker_fee_bps")
            or payload.get("makerFeeBps")
            or payload.get("makerFee")
        )
        taker_rate = self._safe_float(
            payload.get("taker_rate")
            or payload.get("taker_fee_bps")
            or payload.get("takerFeeBps")
            or payload.get("takerFee")
        )

        bids = self._normalize_levels(payload.get("bids") or payload.get("bid") or payload.get("buy"), min_size)
        asks = self._normalize_levels(payload.get("asks") or payload.get("ask") or payload.get("sell"), min_size)

        sequence = self._safe_int(payload.get("sequence") or payload.get("seq") or payload.get("version"))
        timestamp = self._parse_timestamp(payload.get("timestamp") or payload.get("ts") or payload.get("time"))

        return NormalizedOrderBook(
            symbol=book_symbol,
            bids=bids,
            asks=asks,
            maker_rate_bps=maker_rate,
            taker_rate_bps=taker_rate,
            min_size=min_size,
            sequence=sequence,
            timestamp=timestamp,
            type=kind,
            raw=payload,
        )

    def _normalize_levels(self, side: Any, min_size: Optional[float]) -> List[Dict[str, float]]:
        normalized: List[Dict[str, float]] = []
        if not side:
            return normalized

        levels: Sequence[Any] = side if isinstance(side, Sequence) else []
        for level in levels:
            price, size = self._parse_level(level)
            if price is None or size is None:
                continue
            if min_size is not None and size < min_size:
                continue
            normalized.append({"price": price, "size": size})
        return normalized

    def _parse_level(self, level: Any) -> tuple[Optional[float], Optional[float]]:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return self._safe_float(level[0]), self._safe_float(level[1])
        if isinstance(level, dict):
            return self._safe_float(level.get("price")), self._safe_float(
                level.get("size") or level.get("qty") or level.get("quantity")
            )
        return None, None

    def _extract_min_size(self, payload: Dict[str, Any]) -> Optional[float]:
        if not payload:
            return None

        for key in ("min_size", "minSize", "min_order_size", "minOrderSize", "minimum_size"):
            if key in payload:
                return self._safe_float(payload.get(key))

        filters = payload.get("filters") or payload.get("symbol_filters")
        if isinstance(filters, dict):
            for candidate in (
                filters.get("min_size"),
                filters.get("minSize"),
                filters.get("min_order_size"),
                filters.get("minOrderSize"),
                filters.get("minimum_size"),
            ):
                size = self._safe_float(candidate)
                if size is not None:
                    return size
        if isinstance(filters, list):
            for entry in filters:
                if not isinstance(entry, dict):
                    continue
                size = self._extract_min_size(entry)
                if size is not None:
                    return size
        return None

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            timestamp = value / 1000.0 if value > 1e12 else value
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if isinstance(value, str):
            for parser in (self._parse_iso, self._parse_numeric_str):
                parsed = parser(value)
                if parsed:
                    return parsed
        return None

    def _parse_iso(self, value: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    def _parse_numeric_str(self, value: str) -> Optional[datetime]:
        try:
            numeric = float(value)
        except ValueError:
            return None
        return self._parse_timestamp(numeric)

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _is_snapshot(self, payload: Dict[str, Any]) -> bool:
        message_type = payload.get("type") or payload.get("event")
        return message_type in {"snapshot", "book_snapshot", "l2_snapshot"}

    # --- REST helpers -----------------------------------------------------
    def _rest_get(self, path: str) -> Optional[Dict[str, Any]]:
        url = f"{self.endpoint.rest_url}{path}"
        try:
            response = self.session.get(url, headers=self._headers(), timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - network dependent
            self.logger.warning("GET %s failed: %s", url, exc)
        return None

    def _rest_post(self, path: str, json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.endpoint.rest_url}{path}"
        try:
            response = self.session.post(url, headers=self._headers(), json=json, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - network dependent
            self.logger.warning("POST %s failed: %s", url, exc)
        return None

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _ws_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _client_order_id(self) -> str:
        return uuid.uuid4().hex


__all__ = ["HedgeClient", "NormalizedOrderBook"]
