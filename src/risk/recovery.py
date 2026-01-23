"""Drawdown recovery state machine.

STANDALONE: Receives equity/balance as numbers, returns phase.
Knows NOTHING about broker, strategy, or other modules.

Phases:
    0 = Normal trading
    1 = Cool-off period (no trading for N hours)
    2 = Reduced position sizing (scale factor applied)
    3 = Hard stop (manual intervention required)

Usage:
    from src.risk.recovery import DrawdownRecovery

    recovery = DrawdownRecovery(config)
    recovery.update(equity=10000, balance=10500)
    if recovery.can_trade():
        ...
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

logger = structlog.get_logger(__name__)


class DrawdownRecovery:
    """Manages drawdown detection and recovery phases.

    Parameters
    ----------
    config : dict
        The ``risk`` section of the YAML configuration.
    """

    def __init__(self, config: dict) -> None:
        self.max_drawdown_pct: float = config.get("max_drawdown_pct", 10.0)
        self.hard_stop_drawdown_pct: float = config.get("hard_stop_drawdown_pct", 15.0)
        self.cooloff_hours: int = config.get("cooloff_hours", 24)
        self.recovery_position_scale: float = config.get("recovery_position_scale", 0.5)
        self.recovery_trade_count: int = config.get("recovery_trade_count", 10)
        self.enabled: bool = config.get("drawdown_recovery_enabled", True)

        # Runtime state
        self._phase: int = 0
        self._peak_equity: float = 0.0
        self._pause_start_time: datetime | None = None
        self._recovery_profitable_trades: int = 0
        self._recovery_net_pnl: float = 0.0
        self._current_dd_pct: float = 0.0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def phase(self) -> int:
        """Current recovery phase (0-3)."""
        return self._phase

    @property
    def position_scale(self) -> float:
        """Position size multiplier for current phase."""
        if self._phase == 2:
            return self.recovery_position_scale
        return 1.0

    @property
    def current_drawdown_pct(self) -> float:
        """Current drawdown percentage from peak."""
        return self._current_dd_pct

    # ------------------------------------------------------------------
    # Core update
    # ------------------------------------------------------------------

    def update(self, equity: float, balance: float) -> None:
        """Update drawdown state from current equity/balance.

        Called periodically by the orchestrator's equity loop.
        May transition between phases.
        """
        # Track peak
        if equity > self._peak_equity:
            self._peak_equity = equity

        # Calculate drawdown
        if self._peak_equity > 0:
            self._current_dd_pct = ((self._peak_equity - equity) / self._peak_equity) * 100.0
        else:
            self._current_dd_pct = 0.0

        # Phase transitions
        if self._phase == 0:
            # Normal → check for drawdown triggers
            if self._current_dd_pct >= self.hard_stop_drawdown_pct:
                self._phase = 3
                logger.warning(
                    "recovery_hard_stop",
                    drawdown_pct=round(self._current_dd_pct, 2),
                    threshold=self.hard_stop_drawdown_pct,
                )
            elif self._current_dd_pct >= self.max_drawdown_pct and self.enabled:
                self._phase = 1
                self._pause_start_time = datetime.now(timezone.utc)
                logger.warning(
                    "recovery_cooloff_started",
                    drawdown_pct=round(self._current_dd_pct, 2),
                    cooloff_hours=self.cooloff_hours,
                )

        elif self._phase == 1:
            # Cool-off → check if time has elapsed
            if self._pause_start_time is not None:
                elapsed = (datetime.now(timezone.utc) - self._pause_start_time).total_seconds()
                if elapsed >= self.cooloff_hours * 3600:
                    self._phase = 2
                    self._recovery_profitable_trades = 0
                    self._recovery_net_pnl = 0.0
                    logger.info("recovery_phase2_started", detail="reduced sizing active")

            # Also check for hard stop during cooloff
            if self._current_dd_pct >= self.hard_stop_drawdown_pct:
                self._phase = 3
                logger.warning("recovery_hard_stop_during_cooloff", drawdown_pct=round(self._current_dd_pct, 2))

    def record_trade(self, pnl: float) -> None:
        """Record a closed trade's P&L during recovery phase 2.

        Transitions back to phase 0 only when BOTH conditions are met:
        1. Enough profitable trades (proves edge is back)
        2. Net PnL is positive (proves profitability, not just lucky wins)
        """
        if self._phase != 2:
            return

        self._recovery_net_pnl += pnl

        if pnl > 0:
            self._recovery_profitable_trades += 1
            logger.info(
                "recovery_profitable_trade",
                count=self._recovery_profitable_trades,
                target=self.recovery_trade_count,
                net_pnl=round(self._recovery_net_pnl, 2),
            )

        if (
            self._recovery_profitable_trades >= self.recovery_trade_count
            and self._recovery_net_pnl > 0
        ):
            self._phase = 0
            self._recovery_profitable_trades = 0
            self._recovery_net_pnl = 0.0
            logger.info("recovery_complete", detail="resuming normal trading")

    def can_trade(self) -> tuple[bool, str]:
        """Check if trading is allowed in current phase."""
        if self._phase == 0:
            return True, "ok"
        if self._phase == 1:
            if self._pause_start_time:
                elapsed = (datetime.now(timezone.utc) - self._pause_start_time).total_seconds()
                remaining_h = max(0, (self.cooloff_hours * 3600 - elapsed) / 3600)
                return False, f"Cool-off: {remaining_h:.1f}h remaining"
            return False, "Trading paused (drawdown recovery)"
        if self._phase == 2:
            return True, "ok (reduced sizing)"
        if self._phase == 3:
            return False, f"Hard stop: drawdown {self._current_dd_pct:.1f}%"
        return False, "Unknown recovery state"

    def reset(self) -> None:
        """Reset to normal (phase 0).  For manual override / kill switch reset."""
        self._phase = 0
        self._recovery_profitable_trades = 0
        self._recovery_net_pnl = 0.0
        self._pause_start_time = None
        logger.info("recovery_reset")
