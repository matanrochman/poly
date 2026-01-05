#!/usr/bin/env bash
# Run the Polymarket bot with configurable config/env inputs and dry-run defaults.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${ROOT_DIR}/config/settings.yaml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
DRY_RUN_DEFAULT="${DRY_RUN:-1}"

usage() {
  cat <<'EOF'
Usage: scripts/run.sh [--config PATH] [--env-file PATH] [--live] [-- EXTRA_ARGS...]

Options:
  --config PATH     Path to the YAML config file (default: CONFIG_PATH env or config/settings.yaml).
  --env-file PATH   Path to an env file to source before running (default: .env if present).
  --live            Disable the dry-run flag (live trading if your config allows it).
  --                Pass additional arguments directly to src.main (e.g., --max-orders-per-minute 10).
EOF
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --live)
      DRY_RUN_DEFAULT=0
      shift
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -f "$ENV_FILE" ]]; then
  echo "Loading environment from $ENV_FILE"
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
else
  echo "No env file found at $ENV_FILE (continuing without sourcing)."
fi

CMD=(python -m src.main --config "$CONFIG_PATH")
if [[ "$DRY_RUN_DEFAULT" != "0" ]]; then
  CMD+=(--dry-run)
fi
CMD+=("${EXTRA_ARGS[@]}")

echo "Running: ${CMD[*]}"
cd "$ROOT_DIR"
exec "${CMD[@]}"
