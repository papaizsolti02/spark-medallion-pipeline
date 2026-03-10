"""Structured logging configuration and utilities."""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Dict


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter for log aggregation systems.

    Produces JSON output suitable for consumption by log shipping systems
    like ELK Stack, Datadog, or CloudWatch.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string.

        Args:
            record: Log record to format.

        Returns:
            JSON-formatted log entry.
        """
        payload: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Include optional contextual fields attached via log extra parameter.
        for key in ("job", "layer", "run_id", "file_name"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(
    level: str = "INFO",
    log_format: str = "text",
) -> None:
    """Configure root logger once for the process.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_format: Output format ('text' for human-friendly, 'json' for structured).

    Notes:
        Idempotent - only configures root logger if not already configured.
    """
    root = logging.getLogger()
    if root.handlers:
        return

    level_value = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(level_value)

    handler = logging.StreamHandler(sys.stdout)
    if log_format.lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root.addHandler(handler)


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get a configured logger instance.

    Respects environment variables for configuration:
    - LOG_LEVEL: Logging level (default: INFO)
    - LOG_FORMAT: Output format 'text' or 'json' (default: text)

    Args:
        name: Logger name (typically __name__).
        level: Optional override for logging level.

    Returns:
        Configured logger instance.
    """
    env_level = level or os.getenv("LOG_LEVEL", "INFO")
    env_format = os.getenv("LOG_FORMAT", "text")
    configure_logging(level=env_level, log_format=env_format)
    return logging.getLogger(name)
