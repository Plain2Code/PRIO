"""Prio Trading Bot — Entry Point.

Can run standalone (headless) or be imported by the API layer.

Standalone:
    python -m src.main

API mode:
    The FastAPI lifespan creates TradingBot + EventBus and wires
    Telegram + WebSocket subscribers.  See api/app.py.
"""

import asyncio
import os
import signal
from pathlib import Path

import structlog
import yaml
from dotenv import load_dotenv

from src.core.events import EventBus
from src.core.types import (
    EVT_BOT_STARTED,
    EVT_BOT_STOPPED,
    EVT_DAILY_SUMMARY,
    EVT_DRAWDOWN_WARNING,
    EVT_KILL_SWITCH,
    EVT_NEWS_BLOCKED,
    EVT_NEWS_UPCOMING,
    EVT_TRADE_CLOSED,
    EVT_TRADE_OPENED,
)
from src.notifications.telegram import TelegramAlerter
from src.orchestrator.trading_bot import TradingBot

load_dotenv()
logger = structlog.get_logger(__name__)


def load_config() -> dict:
    """Load configuration from YAML file."""
    config_path = Path("config/default.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


async def main() -> None:
    """Standalone entry point — headless bot with Telegram alerts."""
    config = load_config()

    # Create event bus + bot
    event_bus = EventBus()
    bot = TradingBot(config, event_bus)

    # Wire Telegram subscriber
    telegram = TelegramAlerter(config.get("telegram", {}))
    event_bus.subscribe(EVT_TRADE_OPENED, telegram.on_trade_opened)
    event_bus.subscribe(EVT_TRADE_CLOSED, telegram.on_trade_closed)
    event_bus.subscribe(EVT_DRAWDOWN_WARNING, telegram.on_drawdown)
    event_bus.subscribe(EVT_KILL_SWITCH, telegram.on_kill_switch)
    event_bus.subscribe(EVT_BOT_STARTED, telegram.on_bot_started)
    event_bus.subscribe(EVT_BOT_STOPPED, telegram.on_bot_stopped)
    event_bus.subscribe(EVT_NEWS_UPCOMING, telegram.on_news_upcoming)
    event_bus.subscribe(EVT_NEWS_BLOCKED, telegram.on_news_blocked)
    event_bus.subscribe(EVT_DAILY_SUMMARY, telegram.send_daily_summary)

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_event_loop()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        bot.is_running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
