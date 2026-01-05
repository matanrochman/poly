#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run.sh [--config PATH] [--env-file PATH] [--live] [-- EXTRA_ARGS...]

Options:
  --config PATH     Path to the YAML config file (default: CONFIG_PATH env or config/settings.yaml).
  --env-file PATH   Path to an env file to source before running (default: .env if present).
  --live            Override config to disable dry-run (passes --live to the Python entrypoint).
  --                Pass additional arguments directly to src.app (e.g., --some-flag value).
USAGE
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG_PATH="${CONFIG_PATH:-config/settings.yaml}"
ENV_FILE=""
LIVE_FLAG=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --live)
      LIVE_FLAG="--live"
      shift 1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      # positional shorthand (scripts/run.sh config/settings.example.yaml)
      if [[ -z "${CONFIG_PATH_SET:-}" && "$1" != --* ]]; then
        CONFIG_PATH="$1"
        CONFIG_PATH_SET=1
        shift
      else
        echo "Unknown option: $1" >&2
        usage
        exit 1
      fi
      ;;
  esac
done

# default env file if present and not overridden
if [[ -z "$ENV_FILE" && -f ".env" ]]; then
  ENV_FILE=".env"
fi

if [[ -n "$ENV_FILE" && -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

exec python -m src.app --config "$CONFIG_PATH" $LIVE_FLAG "${EXTRA_ARGS[@]}"
