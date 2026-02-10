"""Structured logging utilities for the extraction pipeline."""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from time import perf_counter
from typing import Any


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "correlation_id"):
            log_data["correlation_id"] = record.correlation_id

        standard_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs", "pathname",
            "process", "processName", "relativeCreated", "stack_info",
            "exc_info", "exc_text", "thread", "threadName", "taskName",
            "message", "correlation_id",
        }

        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                if isinstance(value, datetime):
                    log_data[key] = value.isoformat()
                elif hasattr(value, "__dict__"):
                    log_data[key] = str(value)
                else:
                    try:
                        json.dumps(value)
                        log_data[key] = value
                    except (TypeError, ValueError):
                        log_data[key] = str(value)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log records."""

    _correlation_id: str | None = None

    @classmethod
    def set_correlation_id(cls, correlation_id: str) -> None:
        cls._correlation_id = correlation_id

    @classmethod
    def generate_correlation_id(cls) -> str:
        correlation_id = str(uuid.uuid4())
        cls._correlation_id = correlation_id
        return correlation_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = self._correlation_id
        return True


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure logging for the application."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.upper())

    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    handler.addFilter(CorrelationIdFilter())
    root_logger.addHandler(handler)

    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)


@contextmanager
def log_operation(logger: logging.Logger, operation: str, **context: Any):
    """Context manager for logging operation start/end with timing."""
    start_time = perf_counter()
    logger.info(f"Starting {operation}", extra=context)

    try:
        yield
    except Exception as e:
        duration_ms = int((perf_counter() - start_time) * 1000)
        logger.error(
            f"Failed {operation}",
            extra={**context, "duration_ms": duration_ms, "error": str(e)},
            exc_info=True,
        )
        raise
    else:
        duration_ms = int((perf_counter() - start_time) * 1000)
        logger.info(f"Completed {operation}", extra={**context, "duration_ms": duration_ms})
