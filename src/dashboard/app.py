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
