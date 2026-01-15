"""Shared data types for the Prio trading system.

All dataclasses, enums, and type aliases live here.
Modules import types from here — never from each other.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


# ── Signal Direction ────────────────────────────────

class SignalDirection(Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


# ── Trade Signal ────────────────────────────────────

@dataclass
class TradeSignal:
    """Output of a strategy's generate_signal() method."""

    instrument: str
    direction: SignalDirection
    confidence: float  # 0.0 to 1.0
    entry_price: float
    stop_loss: float
    take_profit: float
    timeframe: str
    strategy_name: str
    metadata: dict = field(default_factory=dict)


# ── Order Request / Result ──────────────────────────

@dataclass
class OrderRequest:
    """Submitted to ExecutionEngine."""

    instrument: str
    units: int  # positive = buy, negative = sell
    order_type: str = "market"  # "market", "limit", "stop"
    price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    trailing_stop_distance: float | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class OrderResult:
    """Returned from ExecutionEngine."""

    success: bool
    order_id: str | None = None
    fill_price: float | None = None
    units_filled: int = 0
    timestamp: str | None = None
    latency_ms: float = 0.0
    error: str | None = None
    metadata: dict = field(default_factory=dict)


# ── Pair Diagnostic (for signal matrix) ─────────────

@dataclass
class PairDiagnostic:
    """Per-pair diagnostic state captured during the trading loop.

    The API signal matrix endpoint reads these directly —
    no re-computation needed.
    """

    instrument: str = ""
    regime: str | None = None
    regime_confidence: float = 0.0
    regime_pass: bool = True  # kept for backward compat — always True (soft gate now)
    regime_scale: float = 1.0  # 0.4–1.0, applied to position sizing
    adx: float | None = None
    adx_pass: bool = False
    hurst: float | None = None
    hurst_pass: bool = True  # True when unavailable (don't block without data)
    htf_direction: str | None = None
    htf_pass: bool = False
    signal_direction: str | None = None
    signal_strength: float | None = None
    signal_pass: bool = False
    confidence: float | None = None
    confidence_pass: bool = False
    rsi: float | None = None
    rsi_score: float | None = None
    rsi_pass: bool = False
    news_blackout: bool = False
    news_boost: bool = False
    news_events: list[str] | None = None
    verdict: str = ""  # READY, NO_DATA, NO_SIGNAL, RISK_BLOCK, NEWS_BLOCK, SIZE_ZERO, EXECUTED, EXEC_FAILED
    detail: str | None = None
    timestamp: str = ""


# ── Stop Update ─────────────────────────────────────

@dataclass
class StopUpdate:
    """Returned by StopManager.calculate_updates()."""

    position_id: str
    new_stop_loss: float
    update_type: str = "trailing"  # "trailing" or "break_even"


# ── Event Names ─────────────────────────────────────

EVT_BOT_STARTED = "bot.started"
EVT_BOT_STOPPED = "bot.stopped"

EVT_TRADE_OPENED = "trade.opened"
EVT_TRADE_CLOSED = "trade.closed"
EVT_ORDER_FAILED = "order.failed"

EVT_SIGNAL_GENERATED = "signal.generated"
EVT_RISK_BLOCKED = "risk.blocked"

EVT_DRAWDOWN_WARNING = "drawdown.warning"
EVT_RECOVERY_CHANGED = "recovery.phase_changed"
EVT_KILL_SWITCH = "kill_switch.activated"

EVT_EQUITY_SNAPSHOT = "equity.snapshot"
EVT_TRAILING_STOP_MOVED = "trailing_stop.moved"

EVT_NEWS_UPCOMING = "news.upcoming"
EVT_NEWS_BLOCKED = "news.blocked"

EVT_DAILY_SUMMARY = "daily.summary"
