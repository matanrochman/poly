"""Lightweight JSONL storage backend for snapshots and audit trails."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .persistence import StorageBackend


class JsonLinesStorage(StorageLinesStorageBackend := StorageBackend):
    """Append-only JSONL storage suitable for dry-run audit logs."""

    def __init__(self, base_dir: str | Path = "var/logs") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, key: str, payload: bytes) -> None:
        path = self.base_dir / f"{key}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(payload + b"\n")


__all__ = ["JsonLinesStorage"]
