"""Polymarket WebSocket/REST client with normalized market data schema.

This module wraps Polymarket's market data feeds and normalizes them into an
internal schema used across the ingest pipeline. It supports WebSocket
subscriptions for order book updates, trades, and market metadata while also
exposing REST fallbacks that can be used to resynchronize on sequence gaps or
bootstrap state.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional

import requests
import websockets


@dataclass
class BackoffConfig:
    """Configuration for reconnection backoff."""

    initial: float = 1.0
    maximum: float = 60.0
    factor: float = 2.0
    jitter: float = 0.25


@dataclass
class NormalizedMarketData:
    """Normalized data shape used by downstream consumers."""

    market_id: str
    outcome_id: Optional[str]
    bid: Optional[float]
    ask: Optional[float]
    size: Optional[float]
    last_trade: Optional[float]
    fee_bps: Optional[int]
    liquidity: Optional[float]
    type: str
    sequence: Optional[int] = None
    latency_ms: Optional[float] = None
    lag_seconds: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)


class PolymarketClient:
    """Streaming client for Polymarket order books, trades, and metadata.

    The client exposes an async iterator via :meth:`stream` that yields
    :class:`NormalizedMarketData` objects. When sequence gaps are detected the
    client invokes the REST fallback methods to retrieve a fresh snapshot and
    resumes from the live feed.
    """

    def __init__(
        self,
        websocket_url: str = "wss://feed-external.polymarket.com/ws",
        rest_base_url: str = "https://clob.polymarket.com",
        metadata_base_url: str = "https://gamma-api.polymarket.com",
        order_book_markets: Optional[Iterable[str]] = None,
        trade_markets: Optional[Iterable[str]] = None,
        subscribe_metadata: bool = True,
        backoff: Optional[BackoffConfig] = None,
        metrics_callback: Optional[Callable[[str, Dict[str, float]], None]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.websocket_url = websocket_url
        self.rest_base_url = rest_base_url.rstrip("/")
        self.metadata_base_url = metadata_base_url.rstrip("/")
        self.order_book_markets = set(order_book_markets or [])
        self.trade_markets = set(trade_markets or [])
        self.subscribe_metadata = subscribe_metadata
        self.backoff = backoff or BackoffConfig()
        self.metrics_callback = metrics_callback
        self.logger = logger or logging.getLogger(__name__)

        self._sequence_tracker: Dict[str, int] = {}
        self._running = False

    async def stream(self) -> AsyncIterator[NormalizedMarketData]:
        """Yield normalized market data events with reconnection and backoff."""

        self._running = True
        delay = self.backoff.initial
        while self._running:
            start_time = time.monotonic()
            try:
                async for message in self._consume_once():
                    delay = self.backoff.initial
                    yield message
            except asyncio.CancelledError:
                self._running = False
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.exception("Polymarket stream failure: %s", exc)
            elapsed = time.monotonic() - start_time
            sleep_for = min(delay, self.backoff.maximum) + random.uniform(0, self.backoff.jitter)
            self.logger.info(
                "Reconnecting to Polymarket feed",
                extra={"event": "reconnect", "sleep_seconds": sleep_for, "elapsed_seconds": elapsed},
            )
            await asyncio.sleep(sleep_for)
            delay = min(delay * self.backoff.factor, self.backoff.maximum)

    async def _consume_once(self) -> AsyncIterator[NormalizedMarketData]:
        async with websockets.connect(self.websocket_url, ping_interval=20, ping_timeout=20) as ws:
            await self._send_subscriptions(ws)
            async for raw in ws:
                message = json.loads(raw)
                normalized = self._normalize_message(message)
                if normalized:
                    yield normalized

    async def _send_subscriptions(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send subscriptions for order books, trades, and metadata."""

        for market_id in self.order_book_markets:
            payload = {"type": "subscribe", "channel": "orderbook", "market": market_id}
            await ws.send(json.dumps(payload))
            self.logger.info(
                "Subscribed to orderbook",
                extra={"event": "subscription", "channel": "orderbook", "market_id": market_id},
            )
        for market_id in self.trade_markets:
            payload = {"type": "subscribe", "channel": "trades", "market": market_id}
            await ws.send(json.dumps(payload))
            self.logger.info(
                "Subscribed to trades",
                extra={"event": "subscription", "channel": "trades", "market_id": market_id},
            )
        if self.subscribe_metadata:
            await ws.send(json.dumps({"type": "subscribe", "channel": "markets"}))
            self.logger.info(
                "Subscribed to markets metadata",
                extra={"event": "subscription", "channel": "markets"},
            )

    def _normalize_message(self, message: Dict[str, Any]) -> Optional[NormalizedMarketData]:
        event_type = message.get("type") or message.get("channel")
        data = message.get("data") or message

        if event_type == "orderbook":
            return self._normalize_order_book(data)
        if event_type == "trade" or event_type == "trades":
            return self._normalize_trade(data)
        if event_type in {"market", "markets", "metadata"}:
            return self._normalize_metadata(data)
        return None

    def _normalize_order_book(self, data: Dict[str, Any]) -> Optional[NormalizedMarketData]:
        market_id = str(data.get("market") or data.get("market_id") or "")
        outcome_id = str(data.get("outcome") or data.get("outcome_id") or "") or None
        if not market_id:
            return None
        bid = self._safe_float(data.get("bid"))
        ask = self._safe_float(data.get("ask"))
        size = self._safe_float(data.get("size") or data.get("quantity"))
        fee_bps = self._safe_int(data.get("fee_bps") or data.get("feeBps"))
        liquidity = self._safe_float(data.get("liquidity"))
        sequence = self._safe_int(data.get("sequence") or data.get("seq"))
        latency_ms, lag_seconds = self._timing_metrics(data)

        gap_detected = self._detect_sequence_gap("orderbook", market_id, outcome_id, sequence)
        if gap_detected:
            fallback = self.fetch_order_book_snapshot(market_id, outcome_id)
            if fallback:
                return fallback

        return NormalizedMarketData(
            market_id=market_id,
            outcome_id=outcome_id,
            bid=bid,
            ask=ask,
            size=size,
            last_trade=self._safe_float(data.get("last_trade")),
            fee_bps=fee_bps,
            liquidity=liquidity,
            type="order_book",
            sequence=sequence,
            latency_ms=latency_ms,
            lag_seconds=lag_seconds,
            raw=data,
        )

    def _normalize_trade(self, data: Dict[str, Any]) -> Optional[NormalizedMarketData]:
        market_id = str(data.get("market") or data.get("market_id") or "")
        outcome_id = str(data.get("outcome") or data.get("outcome_id") or "") or None
        if not market_id:
            return None
        price = self._safe_float(data.get("price"))
        size = self._safe_float(data.get("size") or data.get("quantity"))
        fee_bps = self._safe_int(data.get("fee_bps") or data.get("feeBps"))
        liquidity = self._safe_float(data.get("liquidity"))
        sequence = self._safe_int(data.get("sequence") or data.get("seq"))
        latency_ms, lag_seconds = self._timing_metrics(data)

        gap_detected = self._detect_sequence_gap("trade", market_id, outcome_id, sequence)
        if gap_detected:
            fallback = self.fetch_trades_snapshot(market_id, outcome_id)
            if fallback:
                return fallback

        return NormalizedMarketData(
            market_id=market_id,
            outcome_id=outcome_id,
            bid=None,
            ask=None,
            size=size,
            last_trade=price,
            fee_bps=fee_bps,
            liquidity=liquidity,
            type="trade",
            sequence=sequence,
            latency_ms=latency_ms,
            lag_seconds=lag_seconds,
            raw=data,
        )

    def _normalize_metadata(self, data: Dict[str, Any]) -> Optional[NormalizedMarketData]:
        market_id = str(data.get("id") or data.get("market") or data.get("market_id") or "")
        if not market_id:
            return None
        fee_bps = self._safe_int(data.get("fee_bps") or data.get("feeBps"))
        liquidity = self._safe_float(data.get("liquidity"))
        sequence = self._safe_int(data.get("sequence") or data.get("seq"))
        latency_ms, lag_seconds = self._timing_metrics(data)

        return NormalizedMarketData(
            market_id=market_id,
            outcome_id=None,
            bid=None,
            ask=None,
            size=None,
            last_trade=self._safe_float(data.get("last_trade")),
            fee_bps=fee_bps,
            liquidity=liquidity,
            type="metadata",
            sequence=sequence,
            latency_ms=latency_ms,
            lag_seconds=lag_seconds,
            raw=data,
        )

    def fetch_order_book_snapshot(
        self, market_id: str, outcome_id: Optional[str] = None
    ) -> Optional[NormalizedMarketData]:
        """REST fallback for order book snapshots."""

        path = f"/markets/{market_id}/orderbook"
        response = self._rest_get(path)
        if not response:
            return None

        outcome_payload: Dict[str, Any]
        if outcome_id:
            outcome_payload = next(
                (row for row in response.get("outcomes", []) if str(row.get("outcome_id")) == outcome_id),
                {},
            )
        else:
            outcome_payload = response.get("outcomes", [{}])[0]

        bid = self._safe_float(outcome_payload.get("bid"))
        ask = self._safe_float(outcome_payload.get("ask"))
        size = self._safe_float(outcome_payload.get("size"))
        fee_bps = self._safe_int(outcome_payload.get("fee_bps") or response.get("fee_bps"))
        liquidity = self._safe_float(outcome_payload.get("liquidity") or response.get("liquidity"))

        snapshot = NormalizedMarketData(
            market_id=market_id,
            outcome_id=outcome_id,
            bid=bid,
            ask=ask,
            size=size,
            last_trade=self._safe_float(outcome_payload.get("last_trade")),
            fee_bps=fee_bps,
            liquidity=liquidity,
            type="order_book_snapshot",
            raw=response,
        )
        self.logger.info(
            "Recovered orderbook snapshot for %s",
            market_id,
            extra={
                "event": "gap_recovery",
                "channel": "orderbook",
                "market_id": market_id,
                "outcome_id": outcome_id,
            },
        )
        self._emit_metrics(
            "rest_fallback_orderbook",
            {"latency_ms": snapshot.latency_ms or 0.0, "gap_resolved": 1.0},
        )
        return snapshot

    def fetch_trades_snapshot(
        self, market_id: str, outcome_id: Optional[str] = None, limit: int = 1
    ) -> Optional[NormalizedMarketData]:
        """REST fallback for the most recent trade to backfill gaps."""

        path = f"/markets/{market_id}/trades?limit={limit}"
        response = self._rest_get(path)
        if not response:
            return None
        trades: List[Dict[str, Any]] = response if isinstance(response, list) else response.get("trades", [])
        if not trades:
            return None

        trade = trades[0]
        if outcome_id and str(trade.get("outcome_id")) != outcome_id:
            trade = next(
                (item for item in trades if str(item.get("outcome_id")) == outcome_id),
                trade,
            )

        snapshot = NormalizedMarketData(
            market_id=market_id,
            outcome_id=outcome_id,
            bid=None,
            ask=None,
            size=self._safe_float(trade.get("size") or trade.get("quantity")),
            last_trade=self._safe_float(trade.get("price")),
            fee_bps=self._safe_int(trade.get("fee_bps") or trade.get("feeBps")),
            liquidity=self._safe_float(trade.get("liquidity")),
            type="trade_snapshot",
            raw=trade,
        )
        self.logger.info(
            "Recovered trade snapshot for %s",
            market_id,
            extra={
                "event": "gap_recovery",
                "channel": "trades",
                "market_id": market_id,
                "outcome_id": outcome_id,
            },
        )
        self._emit_metrics(
            "rest_fallback_trade",
            {"latency_ms": snapshot.latency_ms or 0.0, "gap_resolved": 1.0},
        )
        return snapshot

    def fetch_market_metadata(self, market_id: str) -> Optional[NormalizedMarketData]:
        """REST metadata lookup for fee and liquidity details."""

        path = f"/markets/{market_id}"
        response = self._rest_get(path, base=self.metadata_base_url)
        if not response:
            return None
        return NormalizedMarketData(
            market_id=market_id,
            outcome_id=None,
            bid=None,
            ask=None,
            size=None,
            last_trade=self._safe_float(response.get("last_trade")),
            fee_bps=self._safe_int(response.get("fee_bps") or response.get("feeBps")),
            liquidity=self._safe_float(response.get("liquidity")),
            type="metadata_snapshot",
            raw=response,
        )

    def _rest_get(self, path: str, base: Optional[str] = None) -> Optional[Dict[str, Any]]:
        base_url = (base or self.rest_base_url).rstrip("/")
        url = f"{base_url}{path}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - network dependent
            self.logger.warning("REST fallback failed for %s: %s", url, exc)
        return None

    def _detect_sequence_gap(
        self, event_type: str, market_id: str, outcome_id: Optional[str], sequence: Optional[int]
    ) -> bool:
        if sequence is None:
            return False
        key = f"{event_type}:{market_id}:{outcome_id or '*'}"
        previous = self._sequence_tracker.get(key)
        self._sequence_tracker[key] = sequence
        if previous is None:
            return False
        if sequence == previous + 1:
            return False

        gap = sequence - previous - 1
        metrics = {"gap": float(gap), "sequence": float(sequence)}
        self.logger.warning(
            "Sequence gap detected for %s (prev=%s, curr=%s)", key, previous, sequence,
            extra={"event": "sequence_gap", "key": key, "previous": previous, "current": sequence, "gap": gap},
        )
        self._emit_metrics("sequence_gap", metrics)
        return True

    def _timing_metrics(self, data: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
        timestamp = data.get("timestamp") or data.get("ts") or data.get("time")
        parsed = self._parse_timestamp(timestamp)
        if not parsed:
            return None, None
        now = datetime.now(timezone.utc)
        delta = now - parsed
        latency_ms = delta.total_seconds() * 1000.0
        lag_seconds = delta.total_seconds()
        self._emit_metrics("latency", {"latency_ms": latency_ms, "lag_seconds": lag_seconds})
        return latency_ms, lag_seconds

    def _parse_timestamp(self, timestamp: Any) -> Optional[datetime]:
        if timestamp is None:
            return None
        if isinstance(timestamp, (int, float)):
            if timestamp > 1e12:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if isinstance(timestamp, str):
            for parser in (self._parse_iso, self._parse_numeric_string):
                parsed = parser(timestamp)
                if parsed:
                    return parsed
        return None

    def _parse_iso(self, value: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    def _parse_numeric_string(self, value: str) -> Optional[datetime]:
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

    def _emit_metrics(self, name: str, values: Dict[str, float]) -> None:
        if not self.metrics_callback:
            return
        try:
            self.metrics_callback(name, values)
        except Exception as exc:  # pragma: no cover - external callback safety
            self.logger.debug("Metric callback failed for %s: %s", name, exc)


__all__ = ["PolymarketClient", "BackoffConfig", "NormalizedMarketData"]
