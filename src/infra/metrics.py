"""Lightweight metrics sink for application instrumentation."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping


@dataclass
class MetricsSink:
    """Collects counters and gauges for reporting."""

    counters: Dict[str, int] = field(default_factory=dict)
    gauges: Dict[str, float] = field(default_factory=dict)
    metrics_file: Path = Path("var/metrics.prom")
    emit_textfile: bool = False
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("metrics"))
    _lock: threading.Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def incr(self, name: str, value: int = 1) -> None:
        """Increment a counter."""

        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + value
            self._persist_unlocked()

    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge value."""

        with self._lock:
            self.gauges[name] = float(value)
            self._persist_unlocked()

    def observe(self, name: str, values: Mapping[str, Any]) -> None:
        """Record an event, incrementing a counter and updating gauges."""

        with self._lock:
            counter_name = f"{name}_total"
            self.counters[counter_name] = self.counters.get(counter_name, 0) + 1
            for key, value in values.items():
                if isinstance(value, (int, float)):
                    self.gauges[f"{name}_{key}"] = float(value)
            self._persist_unlocked()
        self.log_event(name, dict(values))

    def export(self) -> Dict[str, float | int]:
        """Return a merged view of all current metrics."""

        with self._lock:
            snapshot = {**self.counters, **self.gauges}
        return snapshot

    def log_event(self, event: str, payload: Mapping[str, Any] | None = None) -> None:
        """Emit a structured metric event to stdout."""

        extras = {"event": event, **(payload or {})}
        self.logger.info(event, extra=extras)

    def _persist(self) -> None:
        with self._lock:
            self._persist_unlocked()

    def _persist_unlocked(self) -> None:
        if not self.emit_textfile:
            return
        try:
            self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
            payload = self._render_prom_text()
            temp_path = self.metrics_file.with_suffix(".tmp")
            temp_path.write_text(payload, encoding="utf-8")
            os.replace(temp_path, self.metrics_file)
        except Exception:
            return

    def _render_prom_text(self) -> str:
        lines = []
        for name, value in sorted(self.counters.items()):
            lines.append(f"{name} {int(value)}")
        for name, value in sorted(self.gauges.items()):
            lines.append(f"{name} {float(value)}")
        return "\n".join(lines) + "\n"
