#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIG_FILE="${1:-config/settings.example.yaml}"

python -m src.app --config "$CONFIG_FILE"
