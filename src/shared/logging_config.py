"""
Centralized structured logging configuration for the Retail Data Platform.

Provides JSON-formatted log output with consistent fields across all
components (Spark jobs, Kafka producer, quality checks). This enables
log aggregation and search in production (ELK, Datadog, CloudWatch, etc.).

Usage:
    from src.shared.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Processing batch", extra={"records": 1500, "layer": "silver"})
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone


class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for machine parsing."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Include any extra fields passed via logger.info(..., extra={...})
        reserved_attrs = {
            "name",
            "msg",
            "args",
            "created",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "exc_info",
            "exc_text",
            "message",
        }
        for key, value in record.__dict__.items():
            if key not in reserved_attrs and not key.startswith("_"):
                log_entry[key] = value

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def get_logger(name, level=None):
    """
    Returns a configured logger with structured JSON output.

    Args:
        name: Logger name (typically __name__).
        level: Override log level. Defaults to LOG_LEVEL env var or INFO.

    Returns:
        logging.Logger with structured JSON formatting.
    """
    if level is None:
        level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers on repeated calls
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredJsonFormatter())
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger
