"""Logging configuration helpers."""

import logging
import os


def configure_logging(default_level: str = "INFO") -> None:
    """Configure the root logger using environment overrides."""

    level_name = os.getenv("LOG_LEVEL", default_level)
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
