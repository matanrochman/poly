"""Simple persistence interface for snapshots."""

from dataclasses import dataclass
from datetime import datetime
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
