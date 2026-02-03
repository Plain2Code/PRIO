"""System status endpoints — health, logs, latency."""

from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import APIRouter, Request, HTTPException, Query
import structlog

logger = structlog.get_logger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGS_DIR = Path("logs")
_START_TIME = time.monotonic()

# ---------------------------------------------------------------------------
# Optional psutil import
# ---------------------------------------------------------------------------

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False
    logger.warning("psutil_not_available", detail="System metrics will be limited")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
async def system_status(request: Request):
    """
    Return system health information: uptime, memory usage, CPU usage,
    and broker connectivity.
    """
    state = request.app.state

    uptime_seconds = round(time.monotonic() - _START_TIME, 2)

    # System metrics via psutil (graceful fallback)
    memory_usage: dict | None = None
    cpu_percent: float | None = None

    if _HAS_PSUTIL:
        try:
            process = psutil.Process()
            mem_info = process.memory_info()
            memory_usage = {
                "rss_mb": round(mem_info.rss / (1024 * 1024), 2),
                "vms_mb": round(mem_info.vms / (1024 * 1024), 2),
                "percent": round(process.memory_percent(), 2),
            }
            cpu_percent = round(process.cpu_percent(interval=0.1), 2)
        except Exception as e:
            logger.warning("psutil_error", error=str(e))

    # Access bot from state
    bot = state.bot

    # Broker connectivity check
    broker_connected = False
    if bot.broker:
        try:
            await bot.broker.get_account()
            broker_connected = True
        except Exception:
            broker_connected = False

    # Execution engine status
    execution_engine_running = False
    execution_engine_connected = False
    if bot.execution:
        execution_engine_running = bot.execution.is_running
        execution_engine_connected = bot.execution.is_connected

    return {
        "status": "healthy",
        "uptime_seconds": uptime_seconds,
        "is_trading": bot.is_running,
        "trading_mode": bot.trading_mode,
        "memory_usage": memory_usage,
        "cpu_percent": cpu_percent,
        "broker_connected": broker_connected,
        "execution_engine": {
            "running": execution_engine_running,
            "connected": execution_engine_connected,
        },
    }


@router.get("/logs")
async def get_logs(
    request: Request,
    limit: int = Query(default=100, ge=1, le=1000),
    level: str = Query(default="INFO"),
):
    """
    Return recent log entries from the logs directory.
    Parses JSON-formatted log lines and filters by level.
    """
    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    level = level.upper()
    if level not in valid_levels:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid log level. Must be one of: {', '.join(sorted(valid_levels))}",
        )

    # Level hierarchy for filtering
    level_hierarchy = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
    min_level = level_hierarchy.get(level, 1)

    if not LOGS_DIR.exists():
        return {"logs": [], "message": "Logs directory not found"}

    # Find log files, sorted by modification time (newest first)
    log_files = sorted(
        LOGS_DIR.glob("*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not log_files:
        log_files = sorted(
            LOGS_DIR.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

    if not log_files:
        return {"logs": [], "message": "No log files found"}

    entries: list[dict] = []

    for log_file in log_files:
        if len(entries) >= limit:
            break

        try:
            with open(log_file) as f:
                lines = f.readlines()

            # Read lines in reverse (newest first)
            for line in reversed(lines):
                if len(entries) >= limit:
                    break

                line = line.strip()
                if not line:
                    continue

                # Try parsing as JSON
                try:
                    entry = json.loads(line)
                    entry_level = entry.get("level", entry.get("log_level", "INFO")).upper()
                    entry_level_num = level_hierarchy.get(entry_level, 1)

                    if entry_level_num >= min_level:
                        entries.append(entry)
                except json.JSONDecodeError:
                    # Plain text log line — include if it matches the level string
                    if level in line.upper():
                        entries.append({
                            "message": line,
                            "level": level,
                            "source": str(log_file.name),
                        })

        except Exception as e:
            logger.warning("log_read_error", file=str(log_file), error=str(e))

    return {
        "logs": entries,
        "count": len(entries),
        "level_filter": level,
        "limit": limit,
    }


@router.get("/latency")
async def get_latency(request: Request):
    """Return execution latency statistics from the execution engine."""
    state = request.app.state

    bot = state.bot

    if bot.execution is None:
        return {
            "latency": {
                "count": 0,
                "avg_ms": 0.0,
                "min_ms": 0.0,
                "max_ms": 0.0,
                "p95_ms": 0.0,
            },
            "message": "Execution engine not initialised",
        }

    try:
        stats = bot.execution.get_latency_stats()
        return {
            "latency": stats,
            "engine_running": bot.execution.is_running,
            "engine_connected": bot.execution.is_connected,
            "queue_depth": bot.execution.queue_depth,
        }
    except Exception as e:
        logger.error("latency_stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Error fetching latency stats: {e}")
