"""Configuration loader for the bot and dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class TradingPairConfig:
    symbol: str
    min_order_size: float
    max_position_size: float
    taker_fee_bps: int
    maker_fee_bps: int
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
    max_position_sizes: Dict[str, float]
    daily_loss_limit_usd: float


@dataclass
class PersistenceConfig:
    database_url: str
    snapshot_interval_seconds: int


@dataclass
class DashboardConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    enable: bool = True


@dataclass
class AppConfig:
    api_keys: Dict[str, str]
    venues: List[VenueConfig]
    routing: RoutingConfig
    risk: RiskConfig
    persistence: PersistenceConfig
    dashboard: DashboardConfig
    dry_run: bool = True
    min_edge_bps: float = 10.0


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(path: str | Path) -> AppConfig:
    cfg = _load_yaml(Path(path))

    def tp(item: Dict[str, Any]) -> TradingPairConfig:
        return TradingPairConfig(
            symbol=item["symbol"],
            min_order_size=float(item.get("min_order_size", 0)),
            max_position_size=float(item.get("max_position_size", 0)),
            taker_fee_bps=int(item.get("taker_fee_bps", 0)),
            maker_fee_bps=int(item.get("maker_fee_bps", 0)),
            latency_budget_ms=int(item.get("latency_budget_ms", 0)),
        )

    venues = [
        VenueConfig(
            name=v["name"],
            rest_url=v["rest_url"],
            websocket_url=v["websocket_url"],
            heartbeat_interval_ms=int(v.get("heartbeat_interval_ms", 0)),
            trading_pairs=[tp(tp_item) for tp_item in v.get("trading_pairs", [])],
        )
        for v in cfg.get("venues", [])
    ]

    routing = cfg.get("routing", {})
    risk = cfg.get("risk", {})
    persistence = cfg.get("persistence", {})
    dashboard_cfg = cfg.get("dashboard", {})

    return AppConfig(
        api_keys=cfg.get("api_keys", {}),
        venues=venues,
        routing=RoutingConfig(
            default_venue=routing.get("default_venue"),
            failover_venue=routing.get("failover_venue"),
            latency_budget_ms=int(routing.get("latency_budget_ms", 0)),
        ),
        risk=RiskConfig(
            max_notional_usd=float(risk.get("max_notional_usd", 0)),
            max_position_sizes={k: float(v) for k, v in risk.get("max_position_sizes", {}).items()},
            daily_loss_limit_usd=float(risk.get("daily_loss_limit_usd", 0)),
        ),
        persistence=PersistenceConfig(
            database_url=persistence.get("database_url", "sqlite:///var/trading.db"),
            snapshot_interval_seconds=int(persistence.get("snapshot_interval_seconds", 30)),
        ),
        dashboard=DashboardConfig(
            host=dashboard_cfg.get("host", "0.0.0.0"),
            port=int(dashboard_cfg.get("port", 8000)),
            enable=bool(dashboard_cfg.get("enable", True)),
        ),
        dry_run=bool(cfg.get("dry_run", True)),
        min_edge_bps=float(cfg.get("min_edge_bps", 10.0)),
    )


def load_from_env() -> AppConfig:
    path = os.getenv("APP_CONFIG", "config/settings.example.yaml")
    return load_config(path)


__all__ = [
    "AppConfig",
    "TradingPairConfig",
    "VenueConfig",
    "RoutingConfig",
    "RiskConfig",
    "PersistenceConfig",
    "DashboardConfig",
    "load_config",
    "load_from_env",
]
