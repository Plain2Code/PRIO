"""
Structured JSON logging for the Prio Forex trading bot.

Provides dual-output logging:
  - Console: human-readable, colorized output via structlog
  - File:    machine-parseable JSON lines with rotating file handler

Usage:
    from src.utils.logging import setup_logging, get_logger

    setup_logging({
        "level": "INFO",
        "format": "json",
        "file_rotation_mb": 10,
        "file_retention_days": 30,
    })

    logger = get_logger("strategy.macd")
    logger.info("signal_detected", pair="EUR/USD", direction="long")
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOG_DIR = Path("logs")
_DEFAULT_CONFIG: dict[str, Any] = {
    "level": "INFO",
    "format": "json",
    "file_rotation_mb": 10,
    "file_retention_days": 30,
}
_CONFIGURED = False


# ---------------------------------------------------------------------------
# Processors (shared between console and file pipelines)
# ---------------------------------------------------------------------------

def _add_module_name(
    logger: logging.Logger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Ensure every log entry carries the originating module name."""
    if "module" not in event_dict:
        record: logging.LogRecord | None = event_dict.get("_record")
        if record is not None:
            event_dict["module"] = record.module
    return event_dict


def _drop_color_for_json(
    logger: logging.Logger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Strip ANSI escape codes that would pollute JSON output."""
    event_dict.pop("_colors", None)
    return event_dict


# ---------------------------------------------------------------------------
# Handler factories
# ---------------------------------------------------------------------------

def _build_console_handler(level: int) -> logging.StreamHandler:
    """Human-readable console handler writing to stderr."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty()),
        ],
    )
    handler.setFormatter(formatter)
    return handler


def _build_file_handler(
    level: int,
    rotation_mb: int,
    retention_days: int,
) -> logging.handlers.TimedRotatingFileHandler:
    """
    Rotating file handler that produces one JSON-lines log file per day.

    Files are rotated daily *and* when the file exceeds ``rotation_mb``.
    Old files are removed after ``retention_days`` days.
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _LOG_DIR / "prio.log"

    handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(log_path),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=True,
    )
    handler.setLevel(level)
    handler.maxBytes = rotation_mb * 1024 * 1024  # size-based rotation
    handler.namer = lambda name: name.replace(".log", "") + ".log"

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            _drop_color_for_json,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )
    handler.setFormatter(formatter)
    return handler


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def setup_logging(config: dict[str, Any] | None = None) -> None:
    """
    Initialise the logging subsystem.

    Parameters
    ----------
    config : dict, optional
        Configuration dictionary with the following optional keys:

        * ``level`` (str) -- root log level, e.g. ``"DEBUG"``, ``"INFO"``.
        * ``format`` (str) -- reserved for future use (always JSON to file).
        * ``file_rotation_mb`` (int) -- max file size in MiB before rotation.
        * ``file_retention_days`` (int) -- number of daily backups to keep.
    """
    global _CONFIGURED  # noqa: PLW0603

    cfg = {**_DEFAULT_CONFIG, **(config or {})}
    level = getattr(logging, cfg["level"].upper(), logging.INFO)
    rotation_mb: int = int(cfg["file_rotation_mb"])
    retention_days: int = int(cfg["file_retention_days"])

    # -- Shared processor chain used by structlog before handing off to
    #    stdlib.  Formatting-specific processors live in the handlers.
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_module_name,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    # -- Configure structlog itself
    structlog.configure(
        processors=shared_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # -- Configure the stdlib root logger with both handlers
    root = logging.getLogger()
    root.setLevel(level)

    # Remove any previously attached handlers (idempotent re-init)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    root.addHandler(_build_console_handler(level))
    root.addHandler(_build_file_handler(level, rotation_mb, retention_days))

    _CONFIGURED = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Return a named, structured logger.

    If ``setup_logging`` has not been called yet, it is invoked with the
    default configuration so that callers never encounter an unconfigured
    logging stack.

    Parameters
    ----------
    name : str
        Logical logger name (e.g. ``"strategy.macd"``, ``"risk.manager"``).

    Returns
    -------
    structlog.stdlib.BoundLogger
        A bound logger instance ready for use.

    Examples
    --------
    >>> logger = get_logger("execution.engine")
    >>> logger.info("order_submitted", order_id="abc-123", pair="GBP/USD")
    """
    if not _CONFIGURED:
        setup_logging()

    return structlog.get_logger(name)
