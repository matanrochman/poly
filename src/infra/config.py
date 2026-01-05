"""Config loading utilities for the trading bot and dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class TradingPairConfig:
    symbol: str
    min_order_size: float
    max_position_size: float
    taker_fee_bps: float
    maker_fee_bps: float
    latency_budget_ms: int


@dataclass
class VenueConfig:
    name: str
    rest_url: str
    websocket_url: str
    heartbeat_interval_ms: int
    trading_pairs: List[TradingPairConfig]


@dataclass
class RoutingConfig:
    default_venue: str
    failover_venue: Optional[str]
    latency_budget_ms: int


@dataclass
class RiskConfig:
    max_notional_usd: float
    max_position_sizes: dict
    daily_loss_limit_usd: float


@dataclass
class PersistenceConfig:
    database_url: str
    snapshot_interval_seconds: int
    audit_log_path: str = "var/audit.jsonl"


@dataclass
class DashboardConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    enable: bool = True


@dataclass
class PolymarketConfig:
    websocket_url: str
    rest_base_url: str
    metadata_base_url: str
    order_book_markets: list[str]
    trade_markets: list[str]
    subscribe_metadata: bool = True


@dataclass
class AppConfig:
    api_keys: dict
    polymarket: PolymarketConfig
    venues: List[VenueConfig]
    routing: RoutingConfig
    risk: RiskConfig
    persistence: PersistenceConfig
    dashboard: DashboardConfig
    min_edge_bps: float = 10.0
    dry_run: bool = True


def load_config(path: str | Path) -> AppConfig:
    resolved = Path(path).expanduser().resolve()
    with resolved.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    pm = raw.get("polymarket", {})
    venues = [
        VenueConfig(
            name=v["name"],
            rest_url=v["rest_url"],
            websocket_url=v["websocket_url"],
            heartbeat_interval_ms=v.get("heartbeat_interval_ms", 10_000),
            trading_pairs=[
                TradingPairConfig(
                    symbol=tp["symbol"],
                    min_order_size=tp["min_order_size"],
                    max_position_size=tp["max_position_size"],
                    taker_fee_bps=tp["taker_fee_bps"],
                    maker_fee_bps=tp["maker_fee_bps"],
                    latency_budget_ms=tp.get("latency_budget_ms", 75),
                )
                for tp in v.get("trading_pairs", [])
            ],
        )
        for v in raw.get("venues", [])
    ]

    routing = raw.get("routing", {})
    risk = raw.get("risk", {})
    persistence = raw.get("persistence", {})
    dashboard = raw.get("dashboard", {})

    return AppConfig(
        api_keys=raw.get("api_keys", {}),
        polymarket=PolymarketConfig(
            websocket_url=pm.get("websocket_url", "wss://feed-external.polymarket.com/ws"),
            rest_base_url=pm.get("rest_base_url", "https://clob.polymarket.com"),
            metadata_base_url=pm.get("metadata_base_url", "https://gamma-api.polymarket.com"),
            order_book_markets=pm.get("order_book_markets", []),
            trade_markets=pm.get("trade_markets", []),
            subscribe_metadata=pm.get("subscribe_metadata", True),
        ),
        venues=venues,
        routing=RoutingConfig(
            default_venue=routing.get("default_venue"),
            failover_venue=routing.get("failover_venue"),
            latency_budget_ms=routing.get("latency_budget_ms", 75),
        ),
        risk=RiskConfig(
            max_notional_usd=risk.get("max_notional_usd", 0),
            max_position_sizes=risk.get("max_position_sizes", {}),
            daily_loss_limit_usd=risk.get("daily_loss_limit_usd", 0),
        ),
        persistence=PersistenceConfig(
            database_url=persistence.get("database_url", "sqlite:///var/trading.db"),
            snapshot_interval_seconds=persistence.get("snapshot_interval_seconds", 30),
            audit_log_path=persistence.get("audit_log_path", "var/audit.jsonl"),
        ),
        dashboard=DashboardConfig(
            host=dashboard.get("host", "0.0.0.0"),
            port=dashboard.get("port", 8000),
            enable=dashboard.get("enable", True),
        ),
        min_edge_bps=raw.get("min_edge_bps", 10.0),
        dry_run=raw.get("dry_run", True),
    )


def env_or_default(key: str, default: str) -> str:
    return os.getenv(key, default)


__all__ = [
    "load_config",
    "AppConfig",
    "DashboardConfig",
    "VenueConfig",
    "TradingPairConfig",
    "RoutingConfig",
    "RiskConfig",
    "PersistenceConfig",
    "PolymarketConfig",
]
