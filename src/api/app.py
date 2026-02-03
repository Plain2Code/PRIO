"""FastAPI application factory.

Creates the TradingBot + EventBus, wires Telegram + WebSocket
subscribers, and exposes thin route handlers.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
import yaml
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import config as config_routes, dashboard, system, trading
from src.api.websocket import WebSocketHub, ws_router
from src.core.events import EventBus
from src.core.types import (
    EVT_BOT_STARTED,
    EVT_BOT_STOPPED,
    EVT_DAILY_SUMMARY,
    EVT_DRAWDOWN_WARNING,
    EVT_EQUITY_SNAPSHOT,
    EVT_KILL_SWITCH,
    EVT_NEWS_BLOCKED,
    EVT_NEWS_UPCOMING,
    EVT_RECOVERY_CHANGED,
    EVT_TRADE_CLOSED,
    EVT_TRADE_OPENED,
    EVT_TRAILING_STOP_MOVED,
)
from src.notifications.telegram import TelegramAlerter
from src.orchestrator.trading_bot import TradingBot

logger = structlog.get_logger(__name__)

load_dotenv()


def _load_config() -> dict:
    config_path = Path("config/default.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = _load_config()

    # Event bus
    event_bus = EventBus()

    # Orchestrator
    bot = TradingBot(cfg, event_bus)

    # Telegram subscriber
    telegram = TelegramAlerter(cfg.get("telegram", {}))
    event_bus.subscribe(EVT_TRADE_OPENED, telegram.on_trade_opened)
    event_bus.subscribe(EVT_TRADE_CLOSED, telegram.on_trade_closed)
    event_bus.subscribe(EVT_DRAWDOWN_WARNING, telegram.on_drawdown)
    event_bus.subscribe(EVT_KILL_SWITCH, telegram.on_kill_switch)
    event_bus.subscribe(EVT_BOT_STARTED, telegram.on_bot_started)
    event_bus.subscribe(EVT_BOT_STOPPED, telegram.on_bot_stopped)
    event_bus.subscribe(EVT_NEWS_UPCOMING, telegram.on_news_upcoming)
    event_bus.subscribe(EVT_NEWS_BLOCKED, telegram.on_news_blocked)
    event_bus.subscribe(EVT_DAILY_SUMMARY, telegram.send_daily_summary)

    # WebSocket subscriber
    ws_hub = WebSocketHub()
    event_bus.subscribe(EVT_TRADE_OPENED, ws_hub.on_trade)
    event_bus.subscribe(EVT_TRADE_CLOSED, ws_hub.on_trade)
    event_bus.subscribe(EVT_BOT_STARTED, ws_hub.on_status)
    event_bus.subscribe(EVT_BOT_STOPPED, ws_hub.on_status)
    event_bus.subscribe(EVT_DRAWDOWN_WARNING, ws_hub.on_alert)
    event_bus.subscribe(EVT_KILL_SWITCH, ws_hub.on_alert)
    event_bus.subscribe(EVT_EQUITY_SNAPSHOT, ws_hub.on_equity)
    event_bus.subscribe(EVT_RECOVERY_CHANGED, ws_hub.on_alert)
    event_bus.subscribe(EVT_TRAILING_STOP_MOVED, ws_hub.on_trade)
    event_bus.subscribe(EVT_NEWS_BLOCKED, ws_hub.on_alert)
    event_bus.subscribe(EVT_NEWS_UPCOMING, ws_hub.on_alert)

    # Expose on app.state so routes can access them
    app.state.config = cfg
    app.state.bot = bot
    app.state.event_bus = event_bus
    app.state.telegram = telegram
    app.state.ws_hub = ws_hub
    app.state.ws_clients = ws_hub.clients  # backward compat for ws_router
    app.state.bot_task = None

    logger.info("api_started")
    yield

    # Shutdown
    if bot.is_running:
        bot.is_running = False
    if app.state.bot_task and not app.state.bot_task.done():
        app.state.bot_task.cancel()
    logger.info("api_shutdown")


def create_app() -> FastAPI:
    app = FastAPI(title="Prio Trading Bot API", version="2.0.0", lifespan=lifespan)

    # CORS: In production (STATIC_DIR set), frontend is same-origin → no CORS needed.
    # For dev or external API access, allow configurable origins via ALLOWED_ORIGINS env var.
    allowed_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[o.strip() for o in allowed_origins.split(",")],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        app.add_middleware(
            CORSMiddleware,
            allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.include_router(trading.router, prefix="/api/trading", tags=["trading"])
    app.include_router(config_routes.router, prefix="/api/config", tags=["config"])
    app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])
    app.include_router(system.router, prefix="/api/system", tags=["system"])
    app.include_router(ws_router)

    # Serve React static build in Docker (STATIC_DIR env var set in Dockerfile)
    static_dir = os.getenv("STATIC_DIR")
    if static_dir and Path(static_dir).is_dir():
        from fastapi.responses import FileResponse
        from fastapi.staticfiles import StaticFiles

        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str):
            file_path = Path(static_dir) / full_path
            if file_path.is_file():
                return FileResponse(file_path)
            return FileResponse(Path(static_dir) / "index.html")

        app.mount(
            "/assets",
            StaticFiles(directory=str(Path(static_dir) / "assets")),
            name="static",
        )

    return app


app = create_app()
