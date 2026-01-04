"""JSONL storage helper for audit and dry-run persistence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class JsonlStore:
    """Append-only JSONL file writer."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")


__all__ = ["JsonlStore"]
