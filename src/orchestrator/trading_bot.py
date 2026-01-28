"""Central Trading Bot Orchestrator.

This is the ONLY module that knows all other modules.  It wires them
together, steers data flow, and reads like a complete system description:

    1. Reconciliation — detect externally closed positions
    2. Per pair: fetch data → signal → ADX regime scaling → exit check →
       risk check → sizing → execute
    3. Equity loop — drawdown monitoring, recovery phase, correlation
    4. Position loop — trailing stops + break-even

Every standalone module receives plain data and returns plain data.
None of them knows the others exist.

Usage (standalone):
    bot = TradingBot(config, event_bus)
    await bot.start()

Usage (API):
    # api/app.py creates bot + event_bus, wires Telegram + WebSocket
    # then: asyncio.create_task(bot.start())
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from src.core.events import EventBus
from src.core.types import (
    EVT_BOT_STARTED,
    EVT_BOT_STOPPED,
    EVT_DAILY_SUMMARY,
    EVT_DRAWDOWN_WARNING,
    EVT_EQUITY_SNAPSHOT,
    EVT_KILL_SWITCH,
    EVT_ORDER_FAILED,
    EVT_RECOVERY_CHANGED,
    EVT_NEWS_BLOCKED,
    EVT_NEWS_UPCOMING,
    EVT_RISK_BLOCKED,
    EVT_SIGNAL_GENERATED,
    EVT_TRADE_CLOSED,
    EVT_TRADE_OPENED,
    EVT_TRAILING_STOP_MOVED,
    OrderRequest,
    PairDiagnostic,
)

logger = structlog.get_logger(__name__)


class TradingBot:
    """Central orchestrator.  Reads like the complete trading system.

    Every module is standalone.  This module is the ONLY place that
    knows all of them.  It calls them in order and passes data between
    them.

    Parameters
    ----------
    config : dict
        Full application configuration (all sections).
    event_bus : EventBus
        Shared event bus for publishing lifecycle/trade events.
    """

    # ── Construction ────────────────────────────────

    def __init__(self, config: dict, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus

        # State
        self.is_running: bool = False
        self.trading_mode: str = os.getenv("TRADING_MODE", "demo")
        self.start_time: float | None = None  # monotonic
        self.start_utc: datetime | None = None

        # Per-pair diagnostics — read by the API (no re-computation)
        self.diagnostics: dict[str, PairDiagnostic] = {}

        # Cached feature DataFrames: {instrument: {tf: DataFrame}}
        self._features_cache: dict[str, dict[str, Any]] = {}

        # Daily / weekly loss tracking
        self._daily_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._weekly_pnl: float = 0.0
        self._weekly_pnl_week: str = ""

        # Per-pair trade cooldown: {instrument: datetime of last entry}
        self._last_trade_time: dict[str, datetime] = {}
        self._trade_cooldown_hours: float = config.get("risk", {}).get("trade_cooldown_hours", 4.0)

        # Per-pair consecutive loss tracker — exponential cooldown backoff
        # 0 losses: base cooldown, 1: 2x, 2: 4x, 3+: 8x (capped)
        self._consecutive_losses: dict[str, int] = {}

        # Per-pair execution failure cooldown: {instrument: datetime}
        # Prevents API spam when orders fail repeatedly (e.g. spread issues)
        self._exec_fail_cooldown: dict[str, datetime] = {}
        self._exec_fail_cooldown_minutes: float = 15.0

        # One-time SL/TP repair tracking (don't retry same position)
        self._repaired_deals: set[str] = set()

        # Daily summary tracking (send once per day rollover)
        self._last_summary_date: str = ""

        # Modules (populated in initialize())
        self.broker: Any = None
        self.pipeline: Any = None
        self.strategy: Any = None
        self.risk_checks: Any = None
        self.sizer: Any = None
        self.stops: Any = None
        self.recovery: Any = None
        self.execution: Any = None
        self.store: Any = None
        self.calendar: Any = None

        # Config shortcuts
        trading_cfg = config.get("trading", {})
        pairs_cfg = trading_cfg.get("pairs", {})
        if isinstance(pairs_cfg, dict):
            self.all_pairs: dict[str, bool] = pairs_cfg
            self.active_pairs: list[str] = [p for p, on in pairs_cfg.items() if on]
        else:
            # Backward compat: list format → all enabled
            self.all_pairs = {p: True for p in pairs_cfg}
            self.active_pairs = list(pairs_cfg)
        tf_cfg = trading_cfg.get("timeframes", {})
        self.primary_tf: str = tf_cfg.get("primary", "H1")
        self.secondary_tfs: list[str] = tf_cfg.get("secondary", [])
        self.all_timeframes: list[str] = [self.primary_tf] + self.secondary_tfs

    # ── Lifecycle ───────────────────────────────────

    async def initialize(self) -> None:
        """Create all standalone modules.  None of them knows the others."""

        logger.info("bot_initializing", mode=self.trading_mode)

        # Broker
        from src.broker.capitalcom import CapitalComBroker

        self.broker = CapitalComBroker(
            api_key=os.getenv("CAPITALCOM_API_KEY", ""),
            identifier=os.getenv("CAPITALCOM_IDENTIFIER", ""),
            password=os.getenv("CAPITALCOM_PASSWORD", ""),
            environment=os.getenv("CAPITALCOM_ENVIRONMENT", "demo"),
        )

        # Verify connectivity
        account = await self.broker.get_account()
        logger.info(
            "broker_connected",
            balance=account.get("balance"),
            equity=account.get("equity"),
        )

        # Data pipeline
        from src.data.pipeline import DataPipeline

        self.pipeline = DataPipeline(broker=self.broker, config=self.config)

        # Strategy
        from src.strategy.trend_following import TrendFollowingStrategy

        self.strategy = TrendFollowingStrategy(
            config=self.config.get("strategy", {}),
        )

        # Risk — four standalone modules
        from src.risk.checks import RiskChecks
        from src.risk.recovery import DrawdownRecovery
        from src.risk.sizing import PositionSizer
        from src.risk.stops import StopManager

        risk_cfg = self.config.get("risk", {})
        self.risk_checks = RiskChecks(risk_cfg)
        self.sizer = PositionSizer(risk_cfg)
        self.stops = StopManager(risk_cfg)
        self.recovery = DrawdownRecovery(risk_cfg)

        # Execution engine
        from src.execution.engine import ExecutionEngine

        self.execution = ExecutionEngine(
            broker=self.broker,
            config=self.config.get("execution", {}),
        )

        # SQLite store
        from src.data.store import TradeStore

        self.store = TradeStore()

        # Economic calendar (news filter)
        from src.data.calendar import EconomicCalendar

        self.calendar = EconomicCalendar(self.config.get("calendar", {}))

        logger.info("bot_initialized")

    async def start(self) -> None:
        """Start all loops.  Blocks until stopped or cancelled."""
        await self.initialize()
        await self.store.init()
        await self.execution.start()

        self.is_running = True
        self.start_time = time.monotonic()
        self.start_utc = datetime.now(timezone.utc)

        # Compute correlation matrix BEFORE first trade to prevent cluster risk
        try:
            close_prices = await self.pipeline.get_close_prices_for_correlation(
                self.active_pairs,
            )
            if close_prices:
                self.risk_checks.update_correlation_matrix(close_prices)
                logger.info("startup_correlation_computed", pairs=len(close_prices))
        except Exception as e:
            logger.warning("startup_correlation_failed", error=str(e))

        # Reconstruct daily/weekly PnL from DB (survives restarts)
        try:
            now = datetime.now(timezone.utc)
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            # ISO week starts Monday
            week_start = (now - timedelta(days=now.weekday())).replace(
                hour=0, minute=0, second=0, microsecond=0
            ).isoformat()
            self._daily_pnl = await self.store.get_period_pnl(day_start)
            self._daily_pnl_date = now.strftime("%Y-%m-%d")
            self._weekly_pnl = await self.store.get_period_pnl(week_start)
            self._weekly_pnl_week = now.strftime("%G-W%V")
            if self._daily_pnl != 0 or self._weekly_pnl != 0:
                logger.info(
                    "pnl_reconstructed",
                    daily=round(self._daily_pnl, 2),
                    weekly=round(self._weekly_pnl, 2),
                )
        except Exception as e:
            logger.warning("pnl_reconstruction_failed", error=str(e))

        self._publish(EVT_BOT_STARTED, mode=self.trading_mode, pairs=self.active_pairs)
        logger.info("bot_started", mode=self.trading_mode, pairs=self.active_pairs)

        try:
            await asyncio.gather(
                self._trading_loop(),
                self._equity_loop(),
                self._position_loop(),
                self._calendar_loop(),
            )
        except asyncio.CancelledError:
            logger.info("bot_cancelled")
        except Exception as e:
            logger.error("bot_fatal_error", error=str(e))
            self._publish(EVT_KILL_SWITCH, reason=f"fatal: {e}")
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Gracefully stop the bot (called from API)."""
        self.is_running = False
        logger.info("bot_stop_requested")

    async def kill_switch(self) -> dict:
        """Emergency: close ALL positions and stop.

        Also closes all open DB trades so the database stays consistent
        and no stale 'open' records remain after restart.

        Returns
        -------
        dict
            ``{"positions_closed": [...], "errors": [...]}``
        """
        logger.warning("kill_switch_activated")
        results: list = []
        errors: list[str] = []

        if self.broker:
            try:
                results = await self.broker.close_all_positions()
                logger.info("kill_switch_positions_closed", count=len(results))
            except Exception as e:
                errors.append(str(e))
                logger.error("kill_switch_close_error", error=str(e))

        # Close all DB trades so no stale 'open' records remain
        if self.store:
            try:
                open_trades = await self.store.get_open_trades()
                for trade in open_trades:
                    inst = trade.get("instrument", "")
                    await self.store.close_trade(
                        instrument=inst,
                        close_reason="kill_switch",
                    )
                if open_trades:
                    logger.info("kill_switch_db_trades_closed", count=len(open_trades))
            except Exception as e:
                logger.warning("kill_switch_db_close_failed", error=str(e))

        self._publish(
            EVT_KILL_SWITCH,
            positions_closed=len(results),
            errors=errors,
        )

        self.is_running = False
        return {"positions_closed": results, "errors": errors}

    async def _shutdown(self) -> None:
        """Clean up resources."""
        if self.execution:
            await self.execution.stop()
        if self.broker:
            try:
                await self.broker.close()
            except Exception:
                pass

        self._publish(EVT_BOT_STOPPED)
        self.is_running = False
        logger.info("bot_stopped")

    # ── Trading Loop (THE recipe) ──────────────────

    async def _trading_loop(self) -> None:
        """Main trading loop — the complete system in one place.

        Each iteration:
        1. Reconcile externally closed positions
        2. For each pair:
           a. Fetch multi-timeframe features
           b. Regime filter
           c. Check exits on open positions
           d. Generate signal
           e. Risk check
           f. Position sizing
           g. Execute order
        """
        intervals = self._get_check_intervals(self.primary_tf)

        while self.is_running:
            try:
                # ── 1. Reconciliation ──────────────────────────
                broker_positions = await self.broker.get_positions()
                broker_instruments = {
                    p.get("instrument") for p in broker_positions if p.get("instrument")
                }
                try:
                    # Save open trades BEFORE reconcile (for entry data)
                    db_open_trades = await self.store.get_open_trades()
                    trade_by_inst: dict[str, dict] = {}
                    for t in db_open_trades:
                        trade_by_inst[t["instrument"]] = t

                    reconciled = await self.store.reconcile(broker_instruments)
                    if reconciled:
                        close_info = await self._resolve_reconciled_pnl(reconciled, trade_by_inst)
                        for inst in reconciled:
                            info = close_info.get(inst, {})
                            pnl = info.get("pnl", 0.0)
                            reason = info.get("reason", "external")
                            exit_price = info.get("exit_price", 0.0)
                            pnl_pct = info.get("pnl_pct", 0.0)

                            # Enrich DB record with actual close data
                            await self.store.enrich_reconciled_trade(
                                instrument=inst,
                                exit_price=exit_price,
                                pnl=pnl,
                                pnl_pct=pnl_pct,
                                close_reason=reason,
                            )

                            self.recovery.record_trade(pnl)
                            self._track_daily_pnl(pnl)

                            db_trade = trade_by_inst.get(inst, {})
                            self._publish(
                                EVT_TRADE_CLOSED,
                                instrument=inst,
                                direction=db_trade.get("direction", ""),
                                entry_price=db_trade.get("entry_price", 0),
                                exit_price=exit_price,
                                units=db_trade.get("units", 0),
                                pnl=pnl,
                                pnl_pct=pnl_pct,
                                reason=reason,
                            )
                            logger.info("trade_reconciled", instrument=inst, reason=reason, pnl=round(pnl, 2))

                            # Track consecutive losses based on ACTUAL PnL
                            if pnl > 0:
                                self._consecutive_losses[inst] = 0
                            else:
                                self._consecutive_losses[inst] = self._consecutive_losses.get(inst, 0) + 1

                            # Clear stale POSITION_OPEN diagnostic immediately
                            # so the signal matrix doesn't show phantom positions
                            # between reconciliation and next _process_pair() call.
                            if inst in self.diagnostics and self.diagnostics[inst].verdict == "POSITION_OPEN":
                                self._diag(inst, verdict="NO_SIGNAL", detail=f"closed ({reason})")
                except Exception as e:
                    logger.debug("reconciliation_failed", error=str(e))

                # ── 2. Per-pair processing ─────────────────────
                for pair in self.active_pairs:
                    if not self.is_running:
                        break
                    try:
                        await self._process_pair(pair, broker_positions)
                    except Exception as e:
                        logger.error("pair_error", pair=pair, error=str(e))
                        self._diag(pair, verdict="ERROR", detail=str(e)[:80])

                    await asyncio.sleep(2)  # Rate-limit between pairs

                # ── 3. Adaptive sleep ──────────────────────────
                interval = intervals["active"] if broker_positions else intervals["idle"]
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("trading_loop_error", error=str(e))
                await asyncio.sleep(10)

    async def _process_pair(
        self,
        pair: str,
        broker_positions: list[dict],
    ) -> None:
        """Process a single currency pair through the full pipeline.

        Steps:
        1. Fetch multi-timeframe features
        2. Generate signal (always — populates diagnostics)
        3. ADX-based regime scaling (derives regime from ADX)
        4. Exit check on open positions
        5. Risk check → sizing → execute
        """

        # ── 1. Fetch multi-timeframe features ────────
        features = await self.pipeline.get_features(
            pair, self.all_timeframes, count=500,
        )
        self._features_cache[pair] = features
        primary_df = features.get(self.primary_tf)

        if primary_df is None or primary_df.empty:
            self._diag(pair, verdict="NO_DATA")
            return

        # ── 2. Generate signal (ALWAYS — for diagnostics) ──
        signal = self.strategy.generate_signal(features)

        # ── 3. ADX-based regime ──────────────────────
        sd = getattr(self.strategy, "_last_diagnostics", {})
        adx_val = sd.get("adx")
        regime_label, regime_scale = self._get_regime_scale(adx_val)

        # Build full_diag from strategy._last_diagnostics
        full_diag: dict = {
            "regime": regime_label,
            "regime_confidence": 0.0,
            "regime_pass": True,
            "regime_scale": regime_scale,
            "adx": adx_val,
            "adx_pass": sd.get("adx_pass", False),
            "hurst": sd.get("hurst"),
            "hurst_pass": sd.get("hurst_pass", True),
            "htf_direction": sd.get("htf_direction"),
            "htf_pass": sd.get("htf_pass", False),
            "signal_direction": sd.get("signal_direction"),
            "signal_strength": sd.get("signal_strength"),
            "signal_pass": sd.get("signal_pass", False),
            "confidence": sd.get("confidence"),
            "confidence_pass": sd.get("confidence_pass", False),
            "rsi": sd.get("rsi"),
            "rsi_score": sd.get("rsi_score"),
            "rsi_pass": sd.get("rsi_pass", False),
        }
        # When signal succeeded, fill from signal metadata
        if signal is not None:
            meta = signal.metadata or {}
            full_diag["adx"] = meta.get("adx", full_diag["adx"])
            full_diag["adx_pass"] = True  # passed ADX gate
            full_diag["hurst_pass"] = True  # passed Hurst gate
            full_diag["htf_direction"] = meta.get("htf_trend", full_diag["htf_direction"])
            full_diag["htf_pass"] = True
            full_diag["signal_direction"] = signal.direction.value
            full_diag["signal_strength"] = meta.get("blended_signal", full_diag["signal_strength"])
            full_diag["signal_pass"] = True
            full_diag["confidence"] = signal.confidence
            full_diag["confidence_pass"] = True
            full_diag["rsi"] = meta.get("rsi", full_diag["rsi"])
            full_diag["rsi_score"] = meta.get("rsi_score", full_diag["rsi_score"])
            full_diag["rsi_pass"] = True

        # ── 4. News status (ALWAYS — for all pairs) ───
        _blocking_countries: list[str] = []
        if self.calendar and self.calendar.enabled:
            now_utc = datetime.now(timezone.utc)
            is_blocked, blocking_events = self.calendar.check_blackout(pair, now_utc)
            news_titles = [e.title for e in blocking_events]
            _blocking_countries = [e.country for e in blocking_events]
            full_diag["news_blackout"] = is_blocked
            full_diag["news_events"] = news_titles if news_titles else None

            if self.calendar.check_post_event_boost(pair, now_utc):
                full_diag["news_boost"] = True
                if signal is not None:
                    signal.metadata = signal.metadata or {}
                    signal.metadata["news_boost"] = True

            # Show upcoming events even when not in blackout
            if not news_titles:
                upcoming = self.calendar.get_events_for_instrument(pair, hours_ahead=4.0)
                if upcoming:
                    full_diag["news_events"] = [e.title for e in upcoming[:3]]

        # ── 5. Exit check on open positions ──────────
        pair_positions = [p for p in broker_positions if p.get("instrument") == pair]
        for pos in pair_positions:
            if self.strategy.should_exit(pos, features):
                logger.info("exit_signal", instrument=pair)
                try:
                    close_result = await self.broker.close_position(pair)
                except Exception as e:
                    logger.error("exit_close_failed", instrument=pair, error=str(e))
                    continue

                # Determine actual PnL from close confirmation:
                # 1. profit field (account currency, if broker provides it)
                # 2. Calculate from fill price and entry price
                # 3. Fallback to stale unrealized_pnl
                entry_price = pos.get("average_price", pos.get("entry_price", 0))
                units = abs(pos.get("units", 1))
                is_long = pos.get("units", 0) > 0

                profit_from_confirm = close_result.get("profit") if isinstance(close_result, dict) else None
                fill_price = float((close_result or {}).get("level", 0) or 0)

                if profit_from_confirm is not None:
                    pnl = float(profit_from_confirm)
                    exit_price = fill_price or entry_price
                elif fill_price and entry_price:
                    exit_price = fill_price
                    pnl = (exit_price - entry_price) * units if is_long else (entry_price - exit_price) * units
                else:
                    pnl = pos.get("unrealized_pnl", 0)
                    exit_price = pos.get("current_price", pos.get("close_price", 0))

                pnl_pct = (pnl / (entry_price * units)) * 100 if entry_price and units else 0

                await self.store.close_trade(
                    instrument=pair,
                    exit_price=exit_price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    close_reason="signal_reversal",
                )

                self.recovery.record_trade(pnl)
                self._track_daily_pnl(pnl)

                # Track consecutive losses for exponential cooldown
                if pnl > 0:
                    self._consecutive_losses[pair] = 0
                else:
                    self._consecutive_losses[pair] = self._consecutive_losses.get(pair, 0) + 1

                broker_positions.remove(pos)

                self._publish(
                    EVT_TRADE_CLOSED,
                    instrument=pair,
                    direction=pos.get("side", pos.get("direction", "")),
                    entry_price=entry_price,
                    exit_price=exit_price,
                    units=units,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    reason="signal",
                )

        # ── 5b. Already has an open position? ─────────
        still_open = any(p.get("instrument") == pair for p in broker_positions)
        if still_open:
            self._diag(pair, verdict="POSITION_OPEN", detail="already open", **full_diag)
            return

        # ── 6. No signal? ────────────────────────────
        if signal is None:
            reject = getattr(self.strategy, "last_reject_reason", None)
            verdict = "OUTSIDE_SESSION" if reject == "outside_session" else "NO_SIGNAL"
            self._diag(pair, verdict=verdict, detail=reject, **full_diag)
            return

        # ── 6b. Daily loss limit ─────────────────────
        account = await self.broker.get_account()
        balance = account.get("balance", 0.0)
        max_daily_loss = self.config.get("risk", {}).get("max_daily_loss_pct", 3.0)
        if self._daily_pnl < 0 and balance > 0:
            daily_loss_pct = abs(self._daily_pnl) / balance * 100
            if daily_loss_pct >= max_daily_loss:
                self._diag(pair, verdict="DAILY_LOSS", detail=f"{daily_loss_pct:.1f}% (limit {max_daily_loss}%)", **full_diag)
                return

        # ── 6b2. Weekly loss limit ────────────────────
        max_weekly_loss = self.config.get("risk", {}).get("max_weekly_loss_pct", 5.0)
        if self._weekly_pnl < 0 and balance > 0:
            weekly_loss_pct = abs(self._weekly_pnl) / balance * 100
            if weekly_loss_pct >= max_weekly_loss:
                self._diag(pair, verdict="WEEKLY_LOSS", detail=f"{weekly_loss_pct:.1f}% (limit {max_weekly_loss}%)", **full_diag)
                return

        # ── 6b3. Per-pair trade cooldown (exponential on consecutive losses) ──
        last_entry = self._last_trade_time.get(pair)
        if last_entry is not None:
            losses = self._consecutive_losses.get(pair, 0)
            effective_cooldown = self._trade_cooldown_hours * (2 ** min(losses, 3))
            hours_since = (datetime.now(timezone.utc) - last_entry).total_seconds() / 3600
            if hours_since < effective_cooldown:
                remaining = effective_cooldown - hours_since
                loss_info = f", {losses} consecutive losses" if losses > 0 else ""
                self._diag(pair, verdict="COOLDOWN", detail=f"{remaining:.1f}h remaining{loss_info}", **full_diag)
                return

        # ── 6b4. Execution failure cooldown (prevents API spam) ──
        exec_cd = self._exec_fail_cooldown.get(pair)
        if exec_cd is not None:
            minutes_since = (datetime.now(timezone.utc) - exec_cd).total_seconds() / 60
            if minutes_since < self._exec_fail_cooldown_minutes:
                remaining = self._exec_fail_cooldown_minutes - minutes_since
                self._diag(pair, verdict="EXEC_COOLDOWN", detail=f"{remaining:.0f}min remaining", **full_diag)
                return
            else:
                del self._exec_fail_cooldown[pair]

        # ── 6c. News blackout enforcement ────────────
        if full_diag.get("news_blackout"):
            blocking_events_desc = full_diag.get("news_events", [])
            event_desc = ", ".join(blocking_events_desc[:3]) if blocking_events_desc else "news blackout"
            self._diag(pair, verdict="NEWS_BLOCK", detail=event_desc, **full_diag)
            self._publish(
                EVT_NEWS_BLOCKED,
                instrument=pair,
                events=blocking_events_desc,
                countries=_blocking_countries,
            )
            return

        self._publish(
            EVT_SIGNAL_GENERATED,
            instrument=pair,
            direction=signal.direction.value,
            confidence=signal.confidence,
        )

        # ── 7. Risk check ────────────────────────────
        phase = self.recovery.phase

        # Use LIVE spread from broker (not stale candle-close bid/ask)
        spread = 0.0
        try:
            spread = await self.broker.get_spread(pair)
        except Exception:
            # Fallback: last candle's bid/ask (better than nothing)
            if "bid" in primary_df.columns and "ask" in primary_df.columns:
                bid = primary_df["bid"].iloc[-1]
                ask = primary_df["ask"].iloc[-1]
                if bid > 0 and ask > 0:
                    spread = ask - bid

        can_trade, reason = self.risk_checks.check(
            signal={
                "instrument": signal.instrument,
                "direction": signal.direction.value,
                "entry_price": signal.entry_price,
                "stop_loss": signal.stop_loss,
            },
            account=account,
            positions=broker_positions,
            recovery_phase=phase,
            spread=spread,
        )

        if not can_trade:
            self._diag(pair, verdict="RISK_BLOCK", detail=reason, **full_diag)
            self._publish(EVT_RISK_BLOCKED, instrument=pair, reason=reason)
            return

        # ── 8. Position sizing + soft scaling ────────
        trade_stats = await self.store.get_trade_stats()

        # Build prices dict from features cache for quote-to-EUR conversion
        pair_prices: dict[str, float] = {}
        for p, feats in self._features_cache.items():
            tf_df = feats.get(self.primary_tf)
            if tf_df is not None and not tf_df.empty and "close" in tf_df.columns:
                pair_prices[p] = float(tf_df["close"].iloc[-1])

        units = self.sizer.calculate(
            signal={
                "instrument": signal.instrument,
                "entry_price": signal.entry_price,
                "stop_loss": signal.stop_loss,
                "direction": signal.direction,
            },
            account=account,
            features_df=primary_df,
            recovery_phase=phase,
            trade_stats=trade_stats,
            prices=pair_prices,
        )

        if units == 0:
            self._diag(pair, verdict="SIZE_ZERO", **full_diag)
            return

        # Apply soft scaling: regime + confidence + news boost
        # Scaling can only REDUCE from the sizer output — never exceed it.
        # The sizer already enforces max_position_size_pct and margin caps.
        confidence_scale = 0.5 + 0.5 * signal.confidence  # 0.5 to 1.0
        cfg_boost = self.config.get("calendar", {}).get("news_boost_scale", 1.25)
        news_boost_scale = cfg_boost if (signal.metadata or {}).get("news_boost") else 1.0
        combined_scale = min(regime_scale * confidence_scale * news_boost_scale, 1.0)
        sign = 1 if units > 0 else -1
        scaled_units = int(abs(units) * combined_scale)
        # Enforce 1000-unit floor only if the sizer already approved ≥1000
        units = sign * max(scaled_units, 1000)

        # ── 9. Execute order ─────────────────────────
        order = OrderRequest(
            instrument=signal.instrument,
            units=units,
            order_type="market",
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            metadata={
                "strategy": signal.strategy_name,
                "confidence": signal.confidence,
                "regime": regime_label,
                "regime_scale": regime_scale,
                **(signal.metadata or {}),
            },
        )

        logger.info(
            "submitting_order",
            instrument=pair,
            units=units,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
        )

        result = await self.execution.submit_order(order)

        if result.success:
            logger.info(
                "trade_executed",
                instrument=pair,
                direction=signal.direction.value,
                units=units,
                price=result.fill_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                regime=regime_label,
                regime_scale=regime_scale,
                confidence=signal.confidence,
                news_boost=news_boost_scale > 1.0,
                latency_ms=result.latency_ms,
            )

            # Verify SL/TP were set on the broker side — Capital.com may
            # silently drop profitLevel/stopLevel on some instruments.
            await self._verify_sl_tp(
                pair, signal.stop_loss, signal.take_profit
            )

            await self.store.log_trade(
                instrument=pair,
                signal={
                    "direction": signal.direction.value,
                    "entry_price": signal.entry_price,
                    "stop_loss": signal.stop_loss,
                    "take_profit": signal.take_profit,
                    "strategy_name": signal.strategy_name,
                    "regime": regime_label,
                    "units": abs(units),
                },
                result={
                    "fill_price": result.fill_price,
                    "units_filled": result.units_filled,
                },
            )

            self._diag(
                pair, verdict="EXECUTED",
                detail=f"{signal.direction.value} {abs(units)}u @{result.fill_price} (scale {combined_scale:.2f})",
                **full_diag,
            )

            broker_positions.append({
                "instrument": pair,
                "units": units,
                "direction": signal.direction.value,
                "average_price": result.fill_price or signal.entry_price,
            })

            # Record entry time for cooldown
            self._last_trade_time[pair] = datetime.now(timezone.utc)

            self._publish(
                EVT_TRADE_OPENED,
                instrument=pair,
                direction=signal.direction.value,
                units=units,
                price=result.fill_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                strategy=signal.strategy_name,
                regime=regime_label,
            )
        else:
            error_str = str(result.error or "")
            is_spread_reject = "spread" in error_str.lower()
            verdict = "SPREAD_REJECT" if is_spread_reject else "EXEC_FAILED"

            logger.error("trade_failed", instrument=pair, error=error_str, verdict=verdict)
            self._diag(
                pair, verdict=verdict,
                detail=error_str[:80],
                **full_diag,
            )
            self._publish(
                EVT_ORDER_FAILED,
                instrument=pair,
                error=result.error,
            )
            # Only set exec-failure cooldown for actual execution failures,
            # not spread rejects (spread will clear on its own)
            if not is_spread_reject:
                self._exec_fail_cooldown[pair] = datetime.now(timezone.utc)

    # ── Equity Loop ────────────────────────────────

    async def _equity_loop(self) -> None:
        """Every 15s: equity check, drawdown/recovery, correlation update."""
        last_correlation_update: datetime | None = None

        while self.is_running:
            try:
                account = await self.broker.get_account()
                equity = account.get("equity", 0.0)
                balance = account.get("balance", 0.0)

                # Recovery module — phase transitions
                old_phase = self.recovery.phase
                self.recovery.update(equity, balance)

                if self.recovery.phase != old_phase:
                    logger.warning(
                        "recovery_phase_changed",
                        old=old_phase,
                        new=self.recovery.phase,
                    )
                    self._publish(
                        EVT_RECOVERY_CHANGED,
                        old_phase=old_phase,
                        new_phase=self.recovery.phase,
                    )

                # Equity snapshot to DB
                dd_pct = self.recovery.current_drawdown_pct
                await self.store.snapshot_equity(
                    account={
                        "balance": balance,
                        "equity": equity,
                        "unrealized_pnl": account.get("unrealized_pnl", 0.0),
                        "drawdown_pct": dd_pct,
                    },
                )

                self._publish(
                    EVT_EQUITY_SNAPSHOT,
                    balance=balance,
                    equity=equity,
                    drawdown_pct=dd_pct,
                    phase=self.recovery.phase,
                )

                # Drawdown warning
                if dd_pct > 5.0:
                    self._publish(
                        EVT_DRAWDOWN_WARNING,
                        pct=dd_pct,
                        phase=self.recovery.phase,
                    )

                # Daily summary — send once when the UTC date rolls over
                now = datetime.now(timezone.utc)
                today = now.strftime("%Y-%m-%d")
                if self._last_summary_date and today != self._last_summary_date:
                    await self._send_daily_summary(self._last_summary_date, balance, equity)
                self._last_summary_date = today

                # Daily correlation matrix update
                should_update = (
                    last_correlation_update is None
                    or (now - last_correlation_update).total_seconds() >= 86400
                )
                if should_update:
                    if self.pipeline:
                        try:
                            close_prices = await self.pipeline.get_close_prices_for_correlation(
                                self.active_pairs,
                            )
                            if close_prices:
                                self.risk_checks.update_correlation_matrix(close_prices)
                        except Exception as e:
                            logger.warning("correlation_update_failed", error=str(e))

                    try:
                        await self.store.prune_equity_snapshots()
                    except Exception as e:
                        logger.warning("equity_prune_failed", error=str(e))

                    last_correlation_update = now

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("equity_loop_error", error=str(e))

            await asyncio.sleep(15)

    # ── SL/TP Verification ─────────────────────────

    async def _verify_sl_tp(
        self,
        instrument: str,
        expected_sl: float | None,
        expected_tp: float | None,
    ) -> None:
        """Verify that SL/TP were actually set on the broker position.

        Capital.com sometimes silently ignores profitLevel/stopLevel.
        If missing, we retry via modify_order.
        """
        try:
            await asyncio.sleep(1.0)  # Brief delay for broker to settle
            positions = await self.broker.get_positions()

            for pos in positions:
                if pos.get("instrument") != instrument:
                    continue

                deal_id = pos.get("deal_id") or pos.get("position_id", "")
                actual_sl = pos.get("stop_loss")
                actual_tp = pos.get("take_profit")

                needs_fix = False
                fix_body: dict = {}

                if expected_sl and not actual_sl:
                    logger.warning(
                        "sl_missing_after_order",
                        instrument=instrument,
                        expected_sl=expected_sl,
                    )
                    fix_body["stop_loss"] = expected_sl
                    needs_fix = True

                if expected_tp and not actual_tp:
                    logger.warning(
                        "tp_missing_after_order",
                        instrument=instrument,
                        expected_tp=expected_tp,
                    )
                    fix_body["take_profit"] = expected_tp
                    needs_fix = True

                if needs_fix and deal_id:
                    try:
                        await self.broker.modify_order(deal_id, **fix_body)
                        logger.info(
                            "sl_tp_fixed",
                            instrument=instrument,
                            deal_id=deal_id,
                            **fix_body,
                        )
                    except Exception as e:
                        logger.error(
                            "sl_tp_fix_failed",
                            instrument=instrument,
                            error=str(e),
                        )
                elif not needs_fix:
                    logger.debug(
                        "sl_tp_verified",
                        instrument=instrument,
                        sl=actual_sl,
                        tp=actual_tp,
                    )
                break  # Found the position

        except Exception as e:
            logger.warning("sl_tp_verify_error", instrument=instrument, error=str(e))

    # ── One-time SL/TP repair for existing positions ──

    async def _repair_missing_sl_tp(
        self,
        positions: list[dict],
        flat_features: dict[str, Any],
    ) -> None:
        """Check existing positions for missing SL or TP and fix once.

        Priority: use stored SL/TP from DB (original entry values).
        Fallback: ATR-based defaults from strategy config.
        Each deal_id is only attempted once (tracked in ``_repaired_deals``).
        """
        strat_cfg = self.config.get("strategy", {}).get("trend_following", {})
        sl_mult = strat_cfg.get("atr_stop_multiplier", 1.5)
        tp_mult = strat_cfg.get("atr_tp_multiplier", 10.0)

        # Load stored trade data from DB for original SL/TP values
        db_trades_by_inst: dict[str, dict] = {}
        try:
            open_trades = await self.store.get_open_trades()
            for t in open_trades:
                db_trades_by_inst[t["instrument"]] = t
        except Exception:
            pass  # DB unavailable — fall back to ATR

        for pos in positions:
            deal_id = pos.get("deal_id") or pos.get("position_id", "")
            if not deal_id or deal_id in self._repaired_deals:
                continue

            instrument = pos.get("instrument", "")
            existing_sl = pos.get("stop_loss")
            existing_tp = pos.get("take_profit")

            # Both set → nothing to repair
            if existing_sl and existing_tp:
                self._repaired_deals.add(deal_id)
                continue

            # Priority 1: Use stored SL/TP from DB (original entry values)
            db_trade = db_trades_by_inst.get(instrument, {})
            fix_sl = existing_sl
            fix_tp = existing_tp

            if not fix_sl and db_trade.get("stop_loss"):
                fix_sl = db_trade["stop_loss"]
            if not fix_tp and db_trade.get("take_profit"):
                fix_tp = db_trade["take_profit"]

            # Priority 2: Fall back to current ATR if no stored values
            if not fix_sl or not fix_tp:
                df = flat_features.get(instrument)
                if df is None or df.empty or "atr_14" not in df.columns:
                    self._repaired_deals.add(deal_id)
                    continue

                import pandas as pd

                atr_val = float(df["atr_14"].iloc[-1])
                if pd.isna(atr_val) or atr_val <= 0:
                    self._repaired_deals.add(deal_id)
                    continue

                entry_price = pos.get("average_price", 0.0)
                if entry_price == 0:
                    self._repaired_deals.add(deal_id)
                    continue

                units = pos.get("units", 0)
                is_long = units > 0

                if not fix_sl:
                    if is_long:
                        fix_sl = round(entry_price - atr_val * sl_mult, 5)
                    else:
                        fix_sl = round(entry_price + atr_val * sl_mult, 5)

                if not fix_tp:
                    if is_long:
                        fix_tp = round(entry_price + atr_val * tp_mult, 5)
                    else:
                        fix_tp = round(entry_price - atr_val * tp_mult, 5)

            try:
                await self.broker.modify_order(
                    deal_id,
                    stop_loss=fix_sl,
                    take_profit=fix_tp,
                )
                logger.info(
                    "position_sl_tp_repaired",
                    deal_id=deal_id,
                    instrument=instrument,
                    sl=fix_sl,
                    tp=fix_tp,
                    was_missing_sl=not existing_sl,
                    was_missing_tp=not existing_tp,
                )
            except Exception as e:
                logger.error(
                    "position_repair_failed",
                    deal_id=deal_id,
                    instrument=instrument,
                    error=str(e),
                )

            # Mark as attempted regardless of success (don't retry forever)
            self._repaired_deals.add(deal_id)

    # ── Reconciliation PnL Resolution ─────────────

    async def _resolve_reconciled_pnl(
        self,
        reconciled: list[str],
        trade_by_inst: dict[str, dict],
    ) -> dict[str, dict]:
        """Determine actual PnL and close reason for externally closed positions.

        Queries broker activity (last hour) to find the close event.
        Falls back to SL/TP heuristic when activity is unavailable.

        Returns {instrument: {"pnl": float, "pnl_pct": float,
                              "reason": str, "exit_price": float}}
        """
        result: dict[str, dict] = {}

        # Try broker activity API for close reason + exit price
        activity_by_inst: dict[str, dict] = {}
        try:
            activities = await self.broker.get_activity(last_period=3600)
            for act in reversed(activities):  # newest first
                inst = act.get("instrument", "")
                if inst in reconciled and inst not in activity_by_inst:
                    activity_by_inst[inst] = act
        except Exception as e:
            logger.debug("reconciliation_activity_fetch_failed", error=str(e))

        _REASON_MAP = {
            "SL": "sl_hit", "STOP_LOSS": "sl_hit",
            "TP": "tp_hit", "TAKE_PROFIT": "tp_hit",
            "USER": "manual_close", "DEALER": "manual_close",
            "CLOSE_OUT": "margin_close", "SYSTEM": "system_close",
        }

        for inst in reconciled:
            trade = trade_by_inst.get(inst, {})
            entry_price = trade.get("entry_price", 0.0)
            direction = trade.get("direction", "")
            units = abs(trade.get("units", 0))
            stored_sl = trade.get("stop_loss")
            stored_tp = trade.get("take_profit")

            act = activity_by_inst.get(inst)
            exit_price = 0.0
            reason = "external"

            if act:
                source = (act.get("source") or "").upper()
                reason = _REASON_MAP.get(source, "external")
                details = act.get("details") or {}
                exit_price = float(details.get("level", 0) or 0)

            # Calculate PnL — priority chain:
            # 1. From activity exit price + entry price
            # 2. From stored TP/SL level + entry price (if reason known)
            # 3. Zero (unknown)
            pnl = 0.0
            pnl_pct = 0.0

            if exit_price and entry_price and units:
                if direction in ("long", "BUY"):
                    pnl = (exit_price - entry_price) * units
                else:
                    pnl = (entry_price - exit_price) * units
            elif reason == "tp_hit" and stored_tp and entry_price and units:
                exit_price = float(stored_tp)
                if direction in ("long", "BUY"):
                    pnl = (stored_tp - entry_price) * units
                else:
                    pnl = (entry_price - stored_tp) * units
            elif reason == "sl_hit" and stored_sl and entry_price and units:
                exit_price = float(stored_sl)
                if direction in ("long", "BUY"):
                    pnl = (stored_sl - entry_price) * units
                else:
                    pnl = (entry_price - stored_sl) * units

            if entry_price and units:
                pnl_pct = (pnl / (entry_price * units)) * 100

            result[inst] = {
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "reason": reason,
                "exit_price": exit_price,
            }
            logger.debug(
                "reconciled_pnl_resolved",
                instrument=inst,
                reason=reason,
                pnl=round(pnl, 2),
                exit_price=round(exit_price, 5),
                source="activity" if act else "heuristic",
            )

        return result

    # ── Position Loop ──────────────────────────────

    async def _position_loop(self) -> None:
        """Every 5s (active) / 15s (idle): trailing stops and break-even."""
        positions: list[dict] = []

        while self.is_running:
            try:
                positions = await self.broker.get_positions()

                if positions and self._features_cache:
                    # Build {instrument: primary_df} from cache
                    flat_features: dict[str, Any] = {}
                    for inst, tf_dict in self._features_cache.items():
                        if isinstance(tf_dict, dict):
                            flat_features[inst] = tf_dict.get(self.primary_tf)
                        else:
                            flat_features[inst] = tf_dict

                    # One-time repair for positions missing SL or TP
                    await self._repair_missing_sl_tp(positions, flat_features)

                    # Build deal_id → position lookup for passing existing TP
                    pos_by_deal: dict[str, dict] = {}
                    for pos in positions:
                        did = pos.get("deal_id") or pos.get("position_id", "")
                        if did:
                            pos_by_deal[did] = pos

                    updates = self.stops.calculate_updates(positions, flat_features)
                    for update in updates:
                        try:
                            # Pass existing TP so modify_order doesn't clear it
                            existing_pos = pos_by_deal.get(update.position_id, {})
                            existing_tp = existing_pos.get("take_profit")

                            await self.broker.modify_order(
                                update.position_id,
                                stop_loss=update.new_stop_loss,
                                take_profit=existing_tp,
                            )
                            self._publish(
                                EVT_TRAILING_STOP_MOVED,
                                position_id=update.position_id,
                                new_stop_loss=update.new_stop_loss,
                                update_type=update.update_type,
                            )
                        except Exception as e:
                            logger.warning(
                                "stop_update_failed",
                                deal=update.position_id,
                                error=str(e),
                            )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("position_loop_error", error=str(e))

            await asyncio.sleep(5 if positions else 15)

    # ── Calendar Loop ─────────────────────────────

    async def _calendar_loop(self) -> None:
        """Periodically refresh economic calendar data and alert on upcoming events."""
        if not self.calendar or not self.calendar.enabled:
            return

        from src.data.calendar import MIN_FETCH_INTERVAL_SECONDS

        interval = max(
            self.calendar.refresh_interval_minutes * 60,
            MIN_FETCH_INTERVAL_SECONDS,
        )

        # Track already-alerted events to avoid spamming
        alerted: set[str] = set()

        while self.is_running:
            try:
                await self.calendar.refresh()

                # Alert on high-impact events within the next 2 hours
                upcoming = self.calendar.get_upcoming_events(
                    hours_ahead=2.0, impact_filter="High",
                )
                for event in upcoming:
                    key = f"{event.country}:{event.title}:{event.datetime_utc.date()}"
                    if key not in alerted:
                        alerted.add(key)
                        self._publish(
                            EVT_NEWS_UPCOMING,
                            title=event.title,
                            country=event.country,
                            impact=event.impact,
                            datetime_utc=event.datetime_utc.isoformat(),
                        )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("calendar_loop_error", error=str(e))

            await asyncio.sleep(interval)

    # ── ADX-Based Regime Scaling ─────────────────

    @staticmethod
    def _get_regime_scale(adx: float | None) -> tuple[str, float]:
        """Derive market regime label and position sizing scale from ADX.

        ADX directly measures trend strength — no ML model needed.

        Returns ``(regime_label, scale)``:

        * ADX >= 25  →  ``"trending"``, scale 1.0
        * ADX 15–25  →  ``"building"``, scale 0.75
        * ADX < 15   →  ``"weak"``,     scale 0.5
        * ADX is None → ``"unknown"``,  scale 0.7
        """
        if adx is None:
            return ("unknown", 0.7)
        if adx >= 25:
            return ("trending", 1.0)
        elif adx >= 15:
            return ("building", 0.75)
        else:
            return ("weak", 0.5)

    # ── Daily / Weekly Loss Tracking ─────────────

    def _track_daily_pnl(self, pnl: float) -> None:
        """Accumulate closed-trade P&L for the current day and week.

        Resets automatically when the UTC date/week changes.
        """
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        if today != self._daily_pnl_date:
            self._daily_pnl = 0.0
            self._daily_pnl_date = today
        self._daily_pnl += pnl

        # ISO week: resets on Monday
        current_week = now.strftime("%G-W%V")
        if current_week != self._weekly_pnl_week:
            self._weekly_pnl = 0.0
            self._weekly_pnl_week = current_week
        self._weekly_pnl += pnl

    async def _send_daily_summary(self, date_str: str, balance: float, equity: float) -> None:
        """Compute and publish daily summary for the previous trading day."""
        try:
            stats = await self.store.get_daily_summary_stats(date_str)
            stats["balance"] = balance
            stats["equity"] = equity
            stats["max_drawdown"] = self.recovery.current_drawdown_pct
            self._publish(EVT_DAILY_SUMMARY, stats=stats)
            logger.info("daily_summary_sent", date=date_str, pnl=stats.get("pnl", 0))
        except Exception as e:
            logger.warning("daily_summary_failed", error=str(e))

    # ── Diagnostics ────────────────────────────────

    def _diag(self, pair: str, **kwargs: Any) -> None:
        """Capture per-pair diagnostic state for the signal matrix.

        The API reads ``self.diagnostics`` directly — no re-computation.
        """
        self.diagnostics[pair] = PairDiagnostic(
            instrument=pair,
            regime=kwargs.get("regime"),
            regime_confidence=kwargs.get("regime_confidence", 0.0),
            regime_pass=kwargs.get("regime_pass", True),
            regime_scale=kwargs.get("regime_scale", 1.0),
            adx=kwargs.get("adx"),
            adx_pass=kwargs.get("adx_pass", False),
            hurst=kwargs.get("hurst"),
            hurst_pass=kwargs.get("hurst_pass", True),
            htf_direction=kwargs.get("htf_direction"),
            htf_pass=kwargs.get("htf_pass", False),
            signal_direction=kwargs.get("signal_direction"),
            signal_strength=kwargs.get("signal_strength"),
            signal_pass=kwargs.get("signal_pass", False),
            confidence=kwargs.get("confidence"),
            confidence_pass=kwargs.get("confidence_pass", False),
            rsi=kwargs.get("rsi"),
            rsi_score=kwargs.get("rsi_score"),
            rsi_pass=kwargs.get("rsi_pass", False),
            news_blackout=kwargs.get("news_blackout", False),
            news_boost=kwargs.get("news_boost", False),
            news_events=kwargs.get("news_events"),
            verdict=kwargs.get("verdict", ""),
            detail=kwargs.get("detail"),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    # ── Event Publishing ───────────────────────────

    def _publish(self, event: str, **data: Any) -> None:
        """Fire an event on the bus.  Subscribers (Telegram, WS, DB) react."""
        asyncio.ensure_future(self.event_bus.publish(event, **data))

    # ── Helpers ─────────────────────────────────────

    @staticmethod
    def _get_check_intervals(timeframe: str) -> dict[str, int]:
        """Adaptive check intervals based on primary timeframe."""
        base_map = {
            "M1": 30, "M5": 60, "M15": 120, "M30": 300,
            "H1": 300, "H4": 600, "D1": 1800,
        }
        base = base_map.get(timeframe, 300)
        return {"active": max(base // 2, 15), "idle": base}

    @property
    def uptime_seconds(self) -> float:
        """Seconds since start (0.0 if not running)."""
        if self.start_time is None:
            return 0.0
        return time.monotonic() - self.start_time
