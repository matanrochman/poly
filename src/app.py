"""Entry point to run a dry-run orchestrator and serve the dashboard."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict

from src.dashboard.app import InMemoryState, run_dashboard
from src.data.polymarket_client import PolymarketClient
from src.infra import logging as logging_setup
from src.infra.config import load_from_env
from src.pricing.market_arbitrage import MarketArbitrageDetector


async def consume_stream(client: PolymarketClient, detector: MarketArbitrageDetector, state: InMemoryState) -> None:
    async for message in client.stream():
        opportunity = detector.ingest(message)
        if opportunity:
            state.add_trade(
                {
                    "market_id": opportunity.market_id,
                    "direction": opportunity.direction,
                    "edge": opportunity.edge,
                    "notional": opportunity.notional,
                    "max_size": opportunity.max_size,
                    "details": opportunity.details,
                    "ts": asyncio.get_event_loop().time(),
                }
            )


async def main() -> None:
    logging_setup.configure_logging()
    logger = logging.getLogger("app")

    config = load_from_env()
    state = InMemoryState()

    client = PolymarketClient(order_book_markets=None, trade_markets=None, subscribe_metadata=False)
    detector = MarketArbitrageDetector(min_edge_bps=config.min_edge_bps)

    runner = consume_stream(client, detector, state)
    tasks = [asyncio.create_task(runner)]

    if config.dashboard.enable:
        tasks.append(asyncio.create_task(run_dashboard(config, state)))

    logger.info("Starting bot (dry_run=%s) with dashboard=%s", config.dry_run, config.dashboard.enable)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Shutdown requested")
    except Exception as exc:  # pragma: no cover - top-level guard
        logger.exception("Fatal error: %s", exc)
    finally:
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    asyncio.run(main())


__all__ = ["main"]
