"""Structured logging setup using structlog.

Provides dual output:
- Console: human-readable, colorized (stderr)
- File: JSON lines, daily rotation (logs/prio.log)

Usage:
    from src.core.logger import setup_logging, get_logger

    setup_logging(config)  # Call once at startup
    logger = get_logger("my_module")
    logger.info("something_happened", key="value")
"""

from __future__ import annotations

import logging
import re
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import structlog


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_initialized = False


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


class _CleanJsonFormatter(structlog.stdlib.ProcessorFormatter):
    """Strips ANSI color codes before JSON rendering."""

    def format(self, record: logging.LogRecord) -> str:
        result = super().format(record)
        return _strip_ansi(result)


def setup_logging(config: dict | None = None) -> None:
    """Initialize structured logging.

    Args:
        config: Logging config section with keys:
            - level: "DEBUG", "INFO", "WARNING", "ERROR" (default: "INFO")
            - file_rotation_mb: Max file size in MB before rotation (default: 50)
            - file_retention_days: Days to keep rotated files (default: 30)
    """
    global _initialized
    if _initialized:
        return

    config = config or {}
    level_str = config.get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    rotation_mb = config.get("file_rotation_mb", 50)
    retention_days = config.get("file_retention_days", 30)

    # Shared processors (run before formatting)
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.ExtraAdder(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    # Configure structlog
    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Root logger
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers
    root.handlers.clear()

    # Console handler (stderr, human-readable)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    use_colors = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=use_colors),
    )
    console_handler.setFormatter(console_formatter)
    root.addHandler(console_handler)

    # File handler (JSON lines, daily rotation)
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "prio.log"

    file_handler = TimedRotatingFileHandler(
        str(log_file),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        utc=True,
    )
    file_handler.setLevel(level)
    file_handler.maxBytes = rotation_mb * 1024 * 1024

    json_formatter = _CleanJsonFormatter(
        processor=structlog.processors.JSONRenderer(),
    )
    file_handler.setFormatter(json_formatter)
    root.addHandler(file_handler)

    _initialized = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger.

    If setup_logging() hasn't been called yet, initializes with defaults.
    """
    global _initialized
    if not _initialized:
        setup_logging()
    return structlog.get_logger(name)
