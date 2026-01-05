"""Dry-run orchestrator wiring Polymarket stream to arbitrage detector and dashboard."""

from __future__ import annotations

import asyncio
import logging
import signal
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

    client = PolymarketClient(
        websocket_url=cfg.polymarket.websocket_url,
        rest_base_url=cfg.polymarket.rest_base_url,
        metadata_base_url=cfg.polymarket.metadata_base_url,
        order_book_markets=cfg.polymarket.order_book_markets,
        trade_markets=cfg.polymarket.trade_markets,
        subscribe_metadata=cfg.polymarket.subscribe_metadata,
    )

    if not cfg.polymarket.order_book_markets:
        logger.warning("No order_book_markets configured; detector will not receive live updates.")

    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows/limited environments
            pass

    async def consume() -> None:
        async for event in client.stream():
            if stop_event.is_set():
                break
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

    tasks = [asyncio.create_task(consume()), asyncio.create_task(serve_dashboard())]
    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Polymarket Arb Bot")
    parser.add_argument("--config", default="config/settings.example.yaml")
    parser.add_argument("--live", action="store_true", help="Override config dry_run flag to run live")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run regardless of config")
    args = parser.parse_args()

    # Note: args.live/args.dry_run can be used later to override cfg.dry_run if execution logic is added.
    asyncio.run(run_bot(args.config))


if __name__ == "__main__":
    main()
