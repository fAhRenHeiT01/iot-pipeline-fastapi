"""Structured JSON logging configuration."""

from datetime import datetime, timezone
import json
import logging
import sys
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON lines with standard timestamps and metadata."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "props") and isinstance(record.props, dict):
            log_obj.update(record.props)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def get_logger(name: str = "fleet-iot", level: str = "INFO") -> logging.Logger:
    """Create or retrieve a structured JSON logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        logger.propagate = False
    return logger

