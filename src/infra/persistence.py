"""Simple persistence interface for snapshots."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Protocol


class StorageBackend(Protocol):
    """Protocol for storage implementations."""

    def save(self, key: str, payload: bytes) -> None:
        """Persist a payload under the given key."""


@dataclass
class SnapshotStore:
    """Persists snapshots using the provided backend."""

    backend: StorageBackend

    def persist_snapshot(self, name: str, payload: bytes, timestamp: datetime | None = None) -> str:
        """Persist a snapshot and return its storage key."""

        suffix = (timestamp or datetime.utcnow()).isoformat()
        key = f"snapshots/{name}-{suffix}"
        self.backend.save(key, payload)
        return key


class FileSystemBackend:
    """Store snapshots on the local filesystem."""

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(self, key: str, payload: bytes) -> None:
        path = self.base_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


class SQLiteStorageBackend:
    """Persist snapshots to a SQLite database."""

    def __init__(self, path: Path):
        self.path = path
        self._initialize()

    def _initialize(self) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    key TEXT PRIMARY KEY,
                    payload BLOB NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def save(self, key: str, payload: bytes) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO snapshots (key, payload) VALUES (?, ?)",
                (key, payload),
            )
            conn.commit()
