"""Trading control endpoints — thin layer over TradingBot.

All logic lives in orchestrator/trading_bot.py.
These routes just call bot methods and return results.
"""

from __future__ import annotations

import asyncio
import os

import structlog
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ModeRequest(BaseModel):
    mode: str  # "paper" | "live"


class TradingStatus(BaseModel):
    is_trading: bool
    mode: str
    active_pairs: list[str]
    uptime_seconds: float | None = None
    broker_connected: bool = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/start")
async def start_trading(request: Request):
    """Start the trading bot."""
    bot = request.app.state.bot

    if bot.is_running:
        raise HTTPException(status_code=400, detail="Trading is already running")

    # Verify credentials before starting
    api_key = os.getenv("CAPITALCOM_API_KEY")
    identifier = os.getenv("CAPITALCOM_IDENTIFIER")
    password = os.getenv("CAPITALCOM_PASSWORD")
    if not api_key or not identifier or not password:
        raise HTTPException(
            status_code=400,
            detail="Capital.com credentials not configured. Set CAPITALCOM_API_KEY, CAPITALCOM_IDENTIFIER, and CAPITALCOM_PASSWORD.",
        )

    try:
        # Launch bot in background task
        request.app.state.bot_task = asyncio.create_task(bot.start())
        logger.info("trading_started", mode=bot.trading_mode)

        return {
            "status": "started",
            "mode": bot.trading_mode,
            "pairs": bot.active_pairs,
        }
    except Exception as e:
        logger.error("trading_start_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to start trading: {e}")


@router.post("/stop")
async def stop_trading(request: Request):
    """Stop the trading bot gracefully."""
    bot = request.app.state.bot

    if not bot.is_running:
        raise HTTPException(status_code=400, detail="Trading is not running")

    await bot.stop()

    # Wait for the background task to finish
    bot_task = request.app.state.bot_task
    if bot_task and not bot_task.done():
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass

    logger.info("trading_stopped")
    return {"status": "stopped"}


@router.post("/kill-switch")
async def kill_switch(request: Request):
    """Emergency: close ALL positions and stop trading."""
    bot = request.app.state.bot

    result = await bot.kill_switch()

    # Cancel background task
    bot_task = request.app.state.bot_task
    if bot_task and not bot_task.done():
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass

    logger.warning("kill_switch_activated")
    return {
        "status": "killed",
        "positions_closed": result.get("positions_closed", []),
        "errors": result.get("errors") or None,
    }


@router.post("/mode")
async def set_mode(request: Request, body: ModeRequest):
    """Switch between paper and live trading mode."""
    bot = request.app.state.bot

    if body.mode not in ("paper", "live"):
        raise HTTPException(status_code=400, detail="Mode must be 'paper' or 'live'")

    if bot.is_running:
        raise HTTPException(
            status_code=400,
            detail="Cannot change mode while trading is active. Stop trading first.",
        )

    bot.trading_mode = body.mode
    os.environ["TRADING_MODE"] = body.mode
    logger.info("trading_mode_changed", mode=body.mode)

    return {"mode": body.mode}


@router.get("/status")
async def get_status(request: Request):
    """Return the current trading status."""
    bot = request.app.state.bot

    broker_connected = False
    if bot.broker and bot.is_running:
        try:
            await bot.broker.get_account()
            broker_connected = True
        except Exception:
            pass

    return TradingStatus(
        is_trading=bot.is_running,
        mode=bot.trading_mode,
        active_pairs=bot.active_pairs if bot.is_running else [],
        uptime_seconds=bot.uptime_seconds if bot.is_running else None,
        broker_connected=broker_connected,
    )
