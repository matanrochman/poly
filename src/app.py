"""Dry-run orchestrator wiring Polymarket stream to arbitrage detector and dashboard."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import uvicorn

from src.dashboard.app import DashboardState, create_dashboard_app
from src.data.polymarket_client import PolymarketClient
from src.infra.config import load_config
from src.infra.logging import configure_logging
from src.infra.storage import JsonlStore
from src.pricing.market_arbitrage import MarketArbitrageDetector


async def run_bot(config_path: str) -> None:
    configure_logging()
    cfg = load_config(config_path)
    logger = logging.getLogger(__name__)

    detector = MarketArbitrageDetector(min_edge_bps=cfg.min_edge_bps)
    state = DashboardState()
    audit = JsonlStore(cfg.persistence.audit_log_path)

    client = PolymarketClient(order_book_markets=[], trade_markets=[])

    async def consume() -> None:
        async for event in client.stream():
            opp = detector.ingest(event)
            if opp:
                state.record_opportunity(opp)
                audit.append({"type": "opportunity", "data": opp.__dict__})
                logger.info("Opportunity: %s", opp)

    async def serve_dashboard() -> None:
        if not cfg.dashboard.enable:
            return
        app = create_dashboard_app(state)
        config = uvicorn.Config(app, host=cfg.dashboard.host, port=cfg.dashboard.port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    await asyncio.gather(consume(), serve_dashboard())


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Polymarket Arb Bot")
    parser.add_argument("--config", default="config/settings.example.yaml")
    args = parser.parse_args()

    asyncio.run(run_bot(args.config))


if __name__ == "__main__":
    main()
