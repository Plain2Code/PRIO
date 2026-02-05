"""
Telegram alerting module for the Prio Forex trading bot.

Sends trade alerts, drawdown warnings, error notifications, and daily
performance summaries to a configured Telegram chat via the Bot API.

Usage:
    from src.utils.telegram import TelegramAlerter

    alerter = TelegramAlerter(config["telegram"])
    await alerter.alert_new_trade("EUR_USD", "LONG", 1000, 1.0850, 1.0820, 1.0910)
"""

from __future__ import annotations

import os
from typing import Any

import structlog
from telegram import Bot
from telegram.error import TelegramError

logger = structlog.get_logger("utils.telegram")


class TelegramAlerter:
    """Sends structured alerts to a Telegram chat.

    Parameters
    ----------
    config : dict
        The ``telegram`` section of the YAML configuration file.  Supported
        keys:

        * ``enabled`` (bool) -- master toggle (default ``False``).
        * ``bot_token`` (str) -- fallback token if env var is unset.
        * ``chat_id`` (str) -- fallback chat ID if env var is unset.
        * ``alerts`` (dict) -- per-event enable flags, e.g.
          ``{"new_trade": True, "trade_closed": True, ...}``.

    Environment variables ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID``
    take precedence over values in *config*.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

        # Credentials: env vars take precedence over config file values.
        self.bot_token: str = os.environ.get(
            "TELEGRAM_BOT_TOKEN", config.get("bot_token", "")
        )
        self.chat_id: str = os.environ.get(
            "TELEGRAM_CHAT_ID", config.get("chat_id", "")
        )

        # Master toggle.
        self.enabled: bool = config.get(
            "enabled",
            bool(os.environ.get("TELEGRAM_ENABLED", False)),
        )

        # Per-event alert settings.
        self.alert_settings: dict[str, bool] = config.get("alerts", {})

        # Initialise the bot instance (lazy -- won't connect until send).
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
        """Send a plain-text message to the configured Telegram chat.

        Returns ``True`` on success, ``False`` when alerting is disabled or
        when an error occurs (errors are logged, never raised).
        """
        if not self.enabled:
            return False

        if not self._bot or not self.chat_id:
            logger.warning(
                "telegram_send_skipped",
                reason="missing bot_token or chat_id",
            )
            return False

        try:
            await self._bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML",
            )
            logger.debug("telegram_message_sent", length=len(text))
            return True
        except TelegramError as exc:
            logger.error("telegram_send_failed", error=str(exc))
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("telegram_send_unexpected_error", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Trade alerts
    # ------------------------------------------------------------------

    async def alert_new_trade(
        self,
        instrument: str,
        direction: str,
        units: int,
        entry_price: float,
        sl: float,
        tp: float,
    ) -> bool:
        """Send a notification for a newly opened trade."""
        if not self.alert_settings.get("new_trade", True):
            return False

        text = (
            "\U0001f4c8 <b>NEW TRADE</b>\n"
            f"Pair: {instrument}\n"
            f"Direction: {direction.upper()}\n"
            f"Units: {units}\n"
            f"Entry: {entry_price:.5f}\n"
            f"SL: {sl:.5f}\n"
            f"TP: {tp:.5f}"
        )
        return await self.send_message(text)

    async def alert_trade_closed(
        self,
        instrument: str,
        direction: str,
        pnl: float,
        pnl_pct: float,
    ) -> bool:
        """Send a notification for a closed trade.

        Uses a green checkmark for profitable trades and a red cross for
        losing trades.
        """
        if not self.alert_settings.get("trade_closed", True):
            return False

        icon = "\u2705" if pnl >= 0 else "\u274c"
        pnl_sign = "+" if pnl >= 0 else ""

        text = (
            f"{icon} <b>TRADE CLOSED</b>\n"
            f"Pair: {instrument}\n"
            f"Direction: {direction.upper()}\n"
            f"P&L: {pnl_sign}{pnl:.2f} ({pnl_sign}{pnl_pct:.2f}%)"
        )
        return await self.send_message(text)

    # ------------------------------------------------------------------
    # Risk / system alerts
    # ------------------------------------------------------------------

    async def alert_drawdown(
        self, current_dd_pct: float, max_dd_pct: float
    ) -> bool:
        """Warn about elevated drawdown levels."""
        if not self.alert_settings.get("drawdown", True):
            return False

        text = (
            "\u26a0\ufe0f <b>DRAWDOWN ALERT</b>\n"
            f"Current: {current_dd_pct:.1f}%\n"
            f"Max Allowed: {max_dd_pct:.1f}%"
        )
        return await self.send_message(text)

    async def alert_connection_lost(self, error: str) -> bool:
        """Notify that the broker/data connection has been lost."""
        if not self.alert_settings.get("connection_lost", True):
            return False

        text = f"\U0001f534 <b>CONNECTION LOST</b>\n{error}"
        return await self.send_message(text)

    async def alert_system_error(self, error: str) -> bool:
        """Notify about a critical system error."""
        if not self.alert_settings.get("system_error", True):
            return False

        text = f"\U0001f6a8 <b>SYSTEM ERROR</b>\n{error}"
        return await self.send_message(text)

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    async def send_daily_summary(self, stats: dict[str, Any]) -> bool:
        """Format and send a daily performance summary.

        Parameters
        ----------
        stats : dict
            Expected keys (all optional -- missing keys are shown as "N/A"):

            * ``date`` -- reporting date.
            * ``total_trades`` -- number of trades taken today.
            * ``winning_trades`` -- number of profitable trades.
            * ``losing_trades`` -- number of losing trades.
            * ``win_rate`` -- win rate as a percentage.
            * ``pnl`` -- total P&L in account currency.
            * ``pnl_pct`` -- P&L as percentage of starting balance.
            * ``balance`` -- end-of-day balance.
            * ``equity`` -- end-of-day equity.
            * ``max_drawdown`` -- max drawdown during the day (%).
            * ``sharpe_ratio`` -- rolling Sharpe ratio.
            * ``profit_factor`` -- profit factor for the day.
        """
        if not self.alert_settings.get("daily_summary", True):
            return False

        def _v(key: str, fmt: str = "") -> str:
            """Format a stats value, returning 'N/A' if missing."""
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
        """Update bot credentials at runtime.

        Called from the API when the user changes Telegram settings.
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._bot = Bot(token=bot_token) if bot_token else None
        logger.info(
            "telegram_credentials_updated",
            has_token=bool(bot_token),
            has_chat_id=bool(chat_id),
        )

    def enable(self) -> None:
        """Enable Telegram alerting."""
        self.enabled = True
        logger.info("telegram_alerter_enabled")

    def disable(self) -> None:
        """Disable Telegram alerting."""
        self.enabled = False
        logger.info("telegram_alerter_disabled")
