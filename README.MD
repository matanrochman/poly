## Polymarket Streaming Arbitrage Bot

This project streams Polymarket order book data, detects cross-outcome arbitrage, and optionally submits trades. Configuration is driven by YAML and environment variables so deployments can be tuned without code changes.

## Running the bot

Use the helper script to launch with sensible defaults:

```bash
scripts/run.sh --config config/settings.yaml --env-file .env
```

Key notes:

- The script sources an env file if present (default: `.env`) so you can provide secrets such as API keys.
- It passes `--dry-run` by default to avoid live order submission. Use `--live` to remove the dry-run flag once you are ready.
- Additional CLI overrides can be forwarded after `--`, for example:

```bash
scripts/run.sh -- --max-orders-per-minute 20 --min-edge-bps 25
```

## VPS sizing and latency guidance

- **Compute**: At least 2 vCPUs and 4 GB RAM to accommodate WebSocket parsing, recovery snapshots, and metrics emission without contention.
- **Disk**: A small SSD volume (10–20 GB) is sufficient for logs, snapshots, and virtualenvs.
- **Network**: Aim for sub-50 ms round-trip latency to the Polymarket endpoints (`feed-external.polymarket.com` and `clob.polymarket.com`). Prefer regions close to Chicago/NYC where typical US exchange connectivity is strong.
- **Stability**: Keep the instance on a dedicated IPv4 address with consistent bandwidth (100 Mbps+). Enable automatic restarts and basic process supervision.

## Operational checks before enabling live trading

Perform these checks every session before removing the dry-run guard:

- **Data freshness**: Confirm that stream lag stays below your configured `max_lag_seconds` (defaults to 5s). Investigate or restart if you see repeated `stale_data` or `sequence_gap` recoveries in the logs.
- **Balances and permissions**: Verify API keys are loaded (via your `.env`) and that the funded wallet/account is reachable. Query balances on the target venue and ensure trading permissions are active.
- **PnL snapshot**: Capture current inventory and notional exposure before going live. Record the snapshot in your runbook (wallet balances, outstanding orders, and any hedges) so PnL can be reconciled if orders are later placed.
- **Throttles and risk limits**: Set conservative values in `config/settings.yaml` (e.g., `max_orders_per_minute`, `min_edge_bps`, and `max_slippage_bps`). Start in dry-run to validate metrics and logs before lifting limits.
