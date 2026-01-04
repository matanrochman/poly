"""FastAPI dashboard to view opportunities and recent actions."""

from __future__ import annotations

from dataclasses import asdict
from typing import List

from fastapi import FastAPI

from src.pricing.market_arbitrage import CompleteSetOpportunity


def create_dashboard_app(state: "DashboardState") -> FastAPI:
    app = FastAPI(title="Polymarket Arb Dashboard", version="0.1.0")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "opportunities": len(state.opportunities)}

    @app.get("/opportunities")
    async def opportunities() -> List[dict]:
        return [asdict(opp) for opp in state.opportunities[-50:]]

    @app.get("/actions")
    async def actions() -> List[dict]:
        return state.actions[-100:]
"""Minimal FastAPI dashboard for monitoring trades, allocations, and dry-runs."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.infra.config import AppConfig


class InMemoryState:
    """Holds recent trades, allocations, and health info for the dashboard."""

    def __init__(self) -> None:
        self.trades: List[Dict[str, Any]] = []
        self.allocations: Dict[str, float] = {}  # market_id -> allocation percent
        self.health: Dict[str, Any] = {"status": "initializing"}

    def add_trade(self, trade: Dict[str, Any]) -> None:
        self.trades.append(trade)
        if len(self.trades) > 200:
            self.trades = self.trades[-200:]

    def set_allocation(self, market_id: str, allocation: float) -> None:
        self.allocations[market_id] = allocation

    def snapshot(self) -> Dict[str, Any]:
        return {
            "trades": self.trades,
            "allocations": self.allocations,
            "health": self.health,
        }


def create_app(config: AppConfig, state: InMemoryState) -> FastAPI:
    app = FastAPI(title="Polymarket Bot Dashboard", version="0.1")

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {"status": "ok", "dry_run": config.dry_run}

    @app.get("/trades")
    async def trades() -> Dict[str, Any]:
        return {"trades": state.trades}

    @app.get("/allocations")
    async def allocations() -> Dict[str, Any]:
        return {"allocations": state.allocations}

    @app.post("/allocations/{market_id}")
    async def set_allocation(market_id: str, body: Dict[str, float]) -> JSONResponse:
        allocation = float(body.get("allocation", 0.0))
        state.set_allocation(market_id, allocation)
        return JSONResponse({"market_id": market_id, "allocation": allocation})

    @app.get("/state")
    async def full_state() -> Dict[str, Any]:
        return state.snapshot()

    return app


class DashboardState:
    def __init__(self) -> None:
        self.opportunities: List[CompleteSetOpportunity] = []
        self.actions: List[dict] = []

    def record_opportunity(self, opp: CompleteSetOpportunity) -> None:
        self.opportunities.append(opp)

    def record_action(self, action: dict) -> None:
        self.actions.append(action)


__all__ = ["create_dashboard_app", "DashboardState"]
async def run_dashboard(config: AppConfig, state: InMemoryState) -> None:
    """Launch the dashboard if enabled."""

    if not config.dashboard.enable:
        return

    import uvicorn

    app = create_app(config, state)
    config_kwargs = {"host": config.dashboard.host, "port": config.dashboard.port, "log_level": "info"}
    server = uvicorn.Server(uvicorn.Config(app, **config_kwargs))
    await server.serve()


__all__ = ["create_app", "run_dashboard", "InMemoryState"]
