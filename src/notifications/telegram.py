"""Telegram alerting module.

STANDALONE: Knows nothing about trading logic.  Receives plain data
and formats/sends Telegram messages.

Usage:
    from src.notifications.telegram import TelegramAlerter

    alerter = TelegramAlerter(config)
    await alerter.on_trade_opened(instrument="EUR_USD", direction="long", ...)
"""

from __future__ import annotations

import os
from typing import Any

import structlog
from telegram import Bot
from telegram.error import TelegramError

logger = structlog.get_logger(__name__)


class TelegramAlerter:
    """Sends structured alerts to a Telegram chat.

    Parameters
    ----------
    config : dict
        The ``telegram`` section of the YAML configuration.  Keys:
        ``enabled``, ``bot_token``, ``chat_id``, ``alerts`` (per-event flags).

    Environment variables ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID``
    take precedence over config values.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

        self.bot_token: str = os.environ.get(
            "TELEGRAM_BOT_TOKEN", config.get("bot_token", "")
        )
        self.chat_id: str = os.environ.get(
            "TELEGRAM_CHAT_ID", config.get("chat_id", "")
        )

        self.enabled: bool = config.get(
            "enabled",
            bool(os.environ.get("TELEGRAM_ENABLED", False)),
        )

        self.alert_settings: dict[str, bool] = config.get("alerts", {})

        self._bot: Bot | None = None
        if self.bot_token:
            self._bot = Bot(token=self.bot_token)

        logger.info(
            "telegram_alerter_init",
            enabled=self.enabled,
            has_token=bool(self.bot_token),
            has_chat_id=bool(self.chat_id),
        )

    # ------------------------------------------------------------------
    # Core send
    # ------------------------------------------------------------------

    async def send_message(self, text: str) -> bool:
        """Send a message.  Returns True on success."""
        if not self.enabled:
            return False

        if not self._bot or not self.chat_id:
            logger.warning("telegram_send_skipped", reason="missing credentials")
            return False

        try:
            await self._bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML",
            )
            return True
        except TelegramError as exc:
            logger.error("telegram_send_failed", error=str(exc))
            return False
        except Exception as exc:
            logger.error("telegram_send_unexpected_error", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Event handlers (called by EventBus subscriptions)
    # ------------------------------------------------------------------

    async def on_trade_opened(self, **data: Any) -> None:
        """Handle trade.opened event."""
        if not self.alert_settings.get("new_trade", True):
            return

        text = (
            "\U0001f4c8 <b>NEW TRADE</b>\n"
            f"Pair: {data.get('instrument', '?')}\n"
            f"Direction: {data.get('direction', '?')}\n"
            f"Units: {data.get('units', '?')}\n"
            f"Entry: {data.get('price', 0):.5f}\n"
            f"SL: {data.get('stop_loss', 0):.5f}\n"
            f"TP: {data.get('take_profit', 0):.5f}"
        )
        await self.send_message(text)

    async def on_trade_closed(self, **data: Any) -> None:
        """Handle trade.closed event."""
        if not self.alert_settings.get("trade_closed", True):
            return

        pnl = data.get("pnl", 0)
        pnl_pct = data.get("pnl_pct", 0)
        icon = "\u2705" if pnl >= 0 else "\u274c"
        pnl_sign = "+" if pnl >= 0 else ""

        direction = data.get("direction", "")
        direction_str = f"  ({direction.upper()})" if direction else ""
        entry = data.get("entry_price", 0)
        exit_ = data.get("exit_price", 0)
        units_val = data.get("units", 0)

        lines = [
            f"{icon} <b>TRADE CLOSED</b>{direction_str}",
            f"Pair: {data.get('instrument', '?')}",
        ]
        if units_val:
            lines.append(f"Units: {units_val}")
        if entry:
            lines.append(f"Entry: {entry:.5f}")
        if exit_:
            lines.append(f"Exit: {exit_:.5f}")
        lines.append(f"P&L: {pnl_sign}{pnl:.2f} ({pnl_sign}{pnl_pct:.2f}%)")
        lines.append(f"Reason: {data.get('reason', '?')}")

        await self.send_message("\n".join(lines))

    async def on_drawdown(self, **data: Any) -> None:
        """Handle drawdown.warning event."""
        if not self.alert_settings.get("drawdown", True):
            return

        text = (
            "\u26a0\ufe0f <b>DRAWDOWN ALERT</b>\n"
            f"Current: {data.get('pct', 0):.1f}%\n"
            f"Phase: {data.get('phase', '?')}"
        )
        await self.send_message(text)

    async def on_kill_switch(self, **data: Any) -> None:
        """Handle kill_switch.activated event."""
        if not self.alert_settings.get("kill_switch", True):
            return

        text = (
            "\U0001f6a8 <b>KILL SWITCH ACTIVATED</b>\n"
            f"Reason: {data.get('reason', 'manual')}"
        )
        await self.send_message(text)

    async def on_error(self, message: str = "", **data: Any) -> None:
        """Handle system errors."""
        if not self.alert_settings.get("system_error", True):
            return

        text = f"\U0001f6a8 <b>SYSTEM ERROR</b>\n{message}"
        await self.send_message(text)

    async def on_bot_started(self, **data: Any) -> None:
        """Handle bot started event."""
        mode = data.get("mode", "unknown")
        pairs = data.get("pairs", [])
        text = (
            f"\U0001f7e2 <b>Prio Bot gestartet</b>\n"
            f"Modus: {mode}\n"
            f"Paare: {len(pairs)}"
        )
        await self.send_message(text)

    async def on_bot_stopped(self, **data: Any) -> None:
        """Handle bot stopped event."""
        await self.send_message("\U0001f534 <b>Prio Bot gestoppt</b>")

    async def on_news_upcoming(self, **data: Any) -> None:
        """Handle news.upcoming — high-impact event approaching."""
        if not self.alert_settings.get("news_events", True):
            return

        text = (
            "\U0001f4f0 <b>NEWS ALERT</b>\n"
            f"Event: {data.get('title', '?')}\n"
            f"Currency: {data.get('country', '?')}\n"
            f"Impact: {data.get('impact', '?')}\n"
            f"Time (UTC): {data.get('datetime_utc', '?')}"
        )
        await self.send_message(text)

    async def on_news_blocked(self, **data: Any) -> None:
        """Handle news.blocked — trade blocked due to news blackout."""
        if not self.alert_settings.get("news_blocked", True):
            return

        events = data.get("events", [])
        events_str = ", ".join(events[:3])
        text = (
            "\u26d4 <b>NEWS BLACKOUT</b>\n"
            f"Pair: {data.get('instrument', '?')}\n"
            f"Events: {events_str}\n"
            f"Trading paused for this pair"
        )
        await self.send_message(text)

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    async def send_daily_summary(self, stats: dict[str, Any]) -> bool:
        """Format and send daily performance summary."""
        if not self.alert_settings.get("daily_summary", True):
            return False

        def _v(key: str, fmt: str = "") -> str:
            val = stats.get(key)
            if val is None:
                return "N/A"
            if fmt:
                return f"{val:{fmt}}"
            return str(val)

        pnl = stats.get("pnl", 0)
        pnl_icon = "\U0001f7e2" if pnl >= 0 else "\U0001f534"

        text = (
            f"\U0001f4ca <b>DAILY SUMMARY</b> \u2014 {_v('date')}\n"
            f"\n"
            f"Trades: {_v('total_trades')}\n"
            f"  Winners: {_v('winning_trades')}\n"
            f"  Losers: {_v('losing_trades')}\n"
            f"  Win Rate: {_v('win_rate', '.1f')}%\n"
            f"\n"
            f"{pnl_icon} P&L: {_v('pnl', '.2f')} ({_v('pnl_pct', '.2f')}%)\n"
            f"Balance: {_v('balance', '.2f')}\n"
            f"Equity: {_v('equity', '.2f')}\n"
            f"\n"
            f"Max Drawdown: {_v('max_drawdown', '.2f')}%\n"
            f"Sharpe Ratio: {_v('sharpe_ratio', '.2f')}\n"
            f"Profit Factor: {_v('profit_factor', '.2f')}"
        )
        return await self.send_message(text)

    # ------------------------------------------------------------------
    # Runtime management
    # ------------------------------------------------------------------

    def update_credentials(self, bot_token: str, chat_id: str) -> None:
        """Update bot credentials at runtime."""
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._bot = Bot(token=bot_token) if bot_token else None

    def enable(self) -> None:
        self.enabled = True

    def disable(self) -> None:
        self.enabled = False
