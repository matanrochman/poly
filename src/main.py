"""Entry point for streaming Polymarket data and detecting arbitrage."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.data.polymarket_client import BackoffConfig, NormalizedMarketData, PolymarketClient
from src.infra.logging import configure_logging
from src.pricing.market_arbitrage import CompleteSetOpportunity, MarketArbitrageDetector

DEFAULT_CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config/settings.yaml"))
DEFAULT_MAX_LAG_SECONDS = 5.0


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load configuration from YAML, defaulting to an empty dict when missing."""

    if not path.exists():
        logging.getLogger(__name__).warning("Config file %s not found, using defaults", path)
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _backoff_from_config(config: Dict[str, Any]) -> BackoffConfig:
    return BackoffConfig(
        initial=float(config.get("initial", BackoffConfig.initial)),
        maximum=float(config.get("maximum", BackoffConfig.maximum)),
        factor=float(config.get("factor", BackoffConfig.factor)),
        jitter=float(config.get("jitter", BackoffConfig.jitter)),
    )


def build_polymarket_client(config: Dict[str, Any], logger: logging.Logger) -> PolymarketClient:
    """Instantiate the Polymarket client from configuration."""

    backoff = _backoff_from_config(config.get("backoff", {}))
    order_book_markets = config.get("order_book_markets") or []
    trade_markets = config.get("trade_markets") or order_book_markets
    return PolymarketClient(
        websocket_url=config.get("websocket_url", "wss://feed-external.polymarket.com/ws"),
        rest_base_url=config.get("rest_base_url", "https://clob.polymarket.com"),
        metadata_base_url=config.get("metadata_base_url", "https://gamma-api.polymarket.com"),
        order_book_markets=order_book_markets,
        trade_markets=trade_markets,
        subscribe_metadata=bool(config.get("subscribe_metadata", True)),
        backoff=backoff,
        logger=logger.getChild("client"),
    )


class MarketStreamApp:
    """Coordinates data streaming, recovery, and arbitrage detection."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.logger = logging.getLogger("polymarket.app")
        polymarket_config = config.get("polymarket", {})
        arbitrage_config = config.get("arbitrage", {})

        self.max_lag_seconds = float(polymarket_config.get("max_lag_seconds", DEFAULT_MAX_LAG_SECONDS))
        self.client = build_polymarket_client(polymarket_config, self.logger)
        self.detector = MarketArbitrageDetector(
            min_edge_bps=float(arbitrage_config.get("min_edge_bps", 10.0)),
        )

        self._sequence_tracker: Dict[str, int] = {}

    async def run(self) -> None:
        """Consume the Polymarket stream and surface arbitrage opportunities."""

        async for data in self.client.stream():
            processed = await self._prepare_data(data)
            if not processed:
                continue
            self._handle_opportunity(processed)

    async def _prepare_data(self, data: NormalizedMarketData) -> Optional[NormalizedMarketData]:
        """Drop or replace stale/gapped events with fresh REST snapshots."""

        if self._is_stale(data):
            self.logger.warning(
                "Dropping stale update for %s", data.market_id,
                extra={"event": "stale_data", "market_id": data.market_id, "lag_seconds": data.lag_seconds},
            )
            recovered = await self._recover_snapshot(data, reason="stale_data")
            return recovered

        if self._has_sequence_gap(data):
            recovered = await self._recover_snapshot(data, reason="sequence_gap")
            if not recovered:
                return None
            data = recovered

        return data

    def _is_stale(self, data: NormalizedMarketData) -> bool:
        return data.lag_seconds is not None and data.lag_seconds > self.max_lag_seconds

    def _has_sequence_gap(self, data: NormalizedMarketData) -> bool:
        if data.sequence is None:
            return False
        key = f"{data.type}:{data.market_id}:{data.outcome_id or '*'}"
        previous = self._sequence_tracker.get(key)
        self._sequence_tracker[key] = data.sequence
        if previous is None or data.sequence == previous + 1:
            return False

        gap = data.sequence - previous - 1
        self.logger.warning(
            "Sequence gap detected (%s -> %s) for %s", previous, data.sequence, key,
            extra={"event": "sequence_gap", "key": key, "previous": previous, "current": data.sequence, "gap": gap},
        )
        return True

    async def _recover_snapshot(self, data: NormalizedMarketData, reason: str) -> Optional[NormalizedMarketData]:
        """Fetch a REST snapshot to replace missing or stale data."""

        if data.type.startswith("order_book"):
            snapshot = await asyncio.to_thread(
                self.client.fetch_order_book_snapshot, data.market_id, data.outcome_id
            )
            channel = "orderbook"
        elif data.type.startswith("trade"):
            snapshot = await asyncio.to_thread(
                self.client.fetch_trades_snapshot, data.market_id, data.outcome_id
            )
            channel = "trades"
        else:
            return None

        if snapshot:
            self.logger.info(
                "Recovered snapshot for %s due to %s", data.market_id, reason,
                extra={
                    "event": "gap_recovery",
                    "market_id": data.market_id,
                    "outcome_id": data.outcome_id,
                    "channel": channel,
                    "reason": reason,
                },
            )
        else:
            self.logger.error(
                "Snapshot recovery failed for %s (%s)", data.market_id, reason,
                extra={"event": "gap_recovery_failed", "market_id": data.market_id, "channel": channel},
            )
        return snapshot

    def _handle_opportunity(self, data: NormalizedMarketData) -> None:
        if data.type not in {"order_book", "order_book_snapshot"}:
            return

        opportunity = self.detector.ingest(data)
        if not opportunity:
            return

        self.logger.info(
            "Arbitrage opportunity detected for %s", opportunity.market_id,
            extra=self._opportunity_payload(opportunity),
        )

    def _opportunity_payload(self, opportunity: CompleteSetOpportunity) -> Dict[str, Any]:
        return {
            "event": "arbitrage_opportunity",
            "market_id": opportunity.market_id,
            "direction": opportunity.direction,
            "edge": opportunity.edge,
            "notional": opportunity.notional,
            "max_size": opportunity.max_size,
            "details": opportunity.details,
        }


def main() -> None:
    configure_logging()
    config = load_config()
    app = MarketStreamApp(config)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
