import math

import pandas as pd

from src.core.types import SignalDirection, TradeSignal
from src.strategy.base import BaseStrategy


class TrendFollowingStrategy(BaseStrategy):
    """Multi-Timeframe Multi-Speed Trend Following + ADX Filter + ATR Stops.

    Higher timeframe confirms the trend direction (with minimum ATR-normalised
    EMA separation to avoid whipsaws).  Entry timeframe uses a three-speed
    blended signal (fast/medium/slow EMAs normalised by ATR via tanh).
    ADX filters out range-bound markets, Hurst exponent filters mean-reverting
    regimes, and ATR sizes stops dynamically.
    """

    # ------------------------------------------------------------------
    # Per-pair session windows (start_utc, end_utc).
    # If start > end, the window wraps midnight.
    # Trade only when EITHER currency in the pair is in its home session.
    # ------------------------------------------------------------------
    PAIR_SESSIONS: dict[str, tuple[int, int]] = {
        "EUR_USD": (7, 21),   # London open → NY close
        "GBP_JPY": (0, 16),   # Tokyo open → London close
        "AUD_NZD": (21, 10),  # Sydney open → Tokyo close (wraps)
        "USD_CAD": (8, 21),   # London morning → NY close
        "EUR_GBP": (7, 16),   # London session (both currencies)
        "NZD_JPY": (21, 10),  # Sydney → Tokyo (wraps)
        "AUD_CAD": (12, 6),   # NY open → Sydney close (wraps, avoids 06-12 dead zone)
    }

    # ------------------------------------------------------------------
    # Default configuration values
    # ------------------------------------------------------------------
    DEFAULTS = {
        "ema_fast": 12,
        "ema_slow": 26,
        "adx_threshold": 20,
        "adx_exit_threshold": 0,
        "adx_period": 14,
        "atr_period": 14,
        "atr_stop_multiplier": 1.5,
        "atr_tp_multiplier": 10.0,
        "min_confidence": 0.0,
        "confirmation_tf": "H4",
        "entry_tf": "H1",
        "session_filter": False,
        "session_start_utc": 7,
        "session_end_utc": 20,
        "candle_confirmation": False,
    }

    def __init__(self, config: dict):
        super().__init__(config)

        strat_cfg = config.get("trend_following", {})

        self.ema_fast: int = strat_cfg.get("ema_fast", self.DEFAULTS["ema_fast"])
        self.ema_slow: int = strat_cfg.get("ema_slow", self.DEFAULTS["ema_slow"])
        self.adx_threshold: float = strat_cfg.get("adx_threshold", self.DEFAULTS["adx_threshold"])
        self.adx_exit_threshold: float = strat_cfg.get("adx_exit_threshold", self.DEFAULTS["adx_exit_threshold"])
        self.adx_period: int = strat_cfg.get("adx_period", self.DEFAULTS["adx_period"])
        self.atr_period: int = strat_cfg.get("atr_period", self.DEFAULTS["atr_period"])
        self.atr_stop_multiplier: float = strat_cfg.get("atr_stop_multiplier", self.DEFAULTS["atr_stop_multiplier"])
        self.atr_tp_multiplier: float = strat_cfg.get("atr_tp_multiplier", self.DEFAULTS["atr_tp_multiplier"])
        self.min_confidence: float = strat_cfg.get("min_confidence", self.DEFAULTS["min_confidence"])
        self.confirmation_tf: str = strat_cfg.get("confirmation_tf", self.DEFAULTS["confirmation_tf"])
        self.entry_tf: str = strat_cfg.get("entry_tf", self.DEFAULTS["entry_tf"])

        # Session filter (disabled by default -- spread check handles liquidity)
        self.session_filter: bool = strat_cfg.get("session_filter", self.DEFAULTS["session_filter"])
        self.session_start_utc: int = strat_cfg.get("session_start_utc", self.DEFAULTS["session_start_utc"])
        self.session_end_utc: int = strat_cfg.get("session_end_utc", self.DEFAULTS["session_end_utc"])

        # Candle confirmation (disabled by default -- reduces trade frequency without adding edge)
        self.candle_confirmation: bool = strat_cfg.get("candle_confirmation", self.DEFAULTS["candle_confirmation"])

        # Multi-speed signal blending (Man AHL technique)
        ms_cfg = strat_cfg.get("multi_speed", {})
        self.fast_ema_pair: list[int] = ms_cfg.get("fast_ema", [5, 13])
        self.medium_ema_pair: list[int] = ms_cfg.get("medium_ema", [12, 26])
        self.slow_ema_pair: list[int] = ms_cfg.get("slow_ema", [50, 100])
        self.speed_weights: list[float] = ms_cfg.get("weights", [0.25, 0.50, 0.25])
        self.min_signal_strength: float = ms_cfg.get("min_signal_strength", 0.3)
        self.use_kama_slow: bool = ms_cfg.get("use_kama_slow", False)

        # Hurst exponent filter (mean-reversion detection)
        self.hurst_threshold: float = strat_cfg.get("hurst_threshold", 0.0)

        # Multi-timeframe config override
        mt_cfg = strat_cfg.get("multi_timeframe", {})
        if mt_cfg.get("confirmation_tf"):
            self.confirmation_tf = mt_cfg["confirmation_tf"]
        if mt_cfg.get("entry_tf"):
            self.entry_tf = mt_cfg["entry_tf"]

        # Derived column names produced by the data pipeline
        self._ema_fast_col = f"ema_{self.ema_fast}"
        self._ema_slow_col = f"ema_{self.ema_slow}"
        self._adx_col = f"adx_{self.adx_period}"
        self._atr_col = f"atr_{self.atr_period}"
        self._rsi_col = "rsi_14"
        self._plus_di_col = "plus_di"
        self._minus_di_col = "minus_di"

        # Per-call diagnostics: populated on every generate_signal() call
        self._last_diagnostics: dict = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate_signal(self, features: dict[str, pd.DataFrame]) -> TradeSignal | None:
        """Generate a trade signal from multi-timeframe feature DataFrames.

        Hard filters (5 — all must pass):
        1. ADX on entry TF must exceed threshold (trending market).
        2. Hurst exponent above threshold (not mean-reverting).
        3. Higher-TF trend direction via EMA fast vs slow (with minimum
           ATR-normalised separation to avoid whipsaws).
        4. Multi-speed blended signal exceeds min strength + agrees with HTF.
        5. ATR must be valid (> 0).

        Soft influences (metadata for orchestrator sizing):
        - Confidence composite (signal strength + Hurst + RSI directional)
          → scales position size via orchestrator.

        Side effect: ``_last_diagnostics`` is populated with every
        computed value + pass/fail so the orchestrator can build a
        complete signal matrix row even when the signal is rejected.
        """
        self._last_diagnostics = {}
        self._last_reject_reason = None

        if not self.enabled:
            self._last_reject_reason = "disabled"
            return None

        entry_df = features.get(self.entry_tf)
        confirm_df = features.get(self.confirmation_tf)

        if entry_df is None or confirm_df is None:
            self._last_reject_reason = "missing_timeframe"
            return None

        if len(entry_df) < 2 or len(confirm_df) < 2:
            self._last_reject_reason = "insufficient_data"
            return None

        # --- 0. Per-pair session filter ---
        instrument = entry_df.attrs.get("instrument", "")
        if self.session_filter and not self._in_trading_session(entry_df, instrument):
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "outside_session"
            return None

        # --- 1. ADX filter on entry timeframe ---
        current_adx = entry_df[self._adx_col].iloc[-1]
        adx_f = float(current_adx) if pd.notna(current_adx) else None
        self._last_diagnostics["adx"] = adx_f
        self._last_diagnostics["adx_pass"] = adx_f is not None and adx_f >= self.adx_threshold

        if pd.isna(current_adx) or current_adx < self.adx_threshold:
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "adx_below_threshold"
            return None

        # --- 1b. Hurst filter (mean-reverting market → no entry) ---
        if self.hurst_threshold > 0 and "hurst_exponent" in entry_df.columns:
            hurst_val = entry_df["hurst_exponent"].iloc[-1]
            hurst_f = float(hurst_val) if pd.notna(hurst_val) else None
            self._last_diagnostics["hurst"] = hurst_f
            hurst_ok = hurst_f is None or hurst_f >= self.hurst_threshold
            self._last_diagnostics["hurst_pass"] = hurst_ok
            if not hurst_ok:
                self._compute_remaining_diagnostics(entry_df, confirm_df)
                self._last_reject_reason = "hurst_mean_reverting"
                return None

        # --- 2. Higher-TF trend direction ---
        htf_direction = self._htf_trend_direction(confirm_df)
        self._last_diagnostics["htf_direction"] = htf_direction.value
        self._last_diagnostics["htf_pass"] = htf_direction is not SignalDirection.FLAT

        if htf_direction is SignalDirection.FLAT:
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "htf_flat"
            return None

        # --- 3. Multi-speed blended signal ---
        blended_signal, speed_breakdown = self._compute_multi_speed_signal(entry_df)
        self._last_diagnostics["signal_strength"] = round(blended_signal, 4)
        self._last_diagnostics["signal_pass"] = abs(blended_signal) >= self.min_signal_strength

        if abs(blended_signal) < self.min_signal_strength:
            if blended_signal != 0:
                weak_dir = SignalDirection.LONG if blended_signal > 0 else SignalDirection.SHORT
                self._last_diagnostics["signal_direction"] = weak_dir.value
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "signal_too_weak"
            return None
        signal_direction = SignalDirection.LONG if blended_signal > 0 else SignalDirection.SHORT

        self._last_diagnostics["signal_direction"] = signal_direction.value

        if signal_direction is SignalDirection.FLAT:
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "signal_flat"
            return None

        direction = signal_direction

        # --- 3a. HTF-Signal direction agreement ---
        if signal_direction != htf_direction:
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "htf_signal_disagree"
            return None

        # --- 3b. Candle confirmation (disabled by default) ---
        if self.candle_confirmation and not self._candle_confirms(entry_df, direction):
            self._compute_remaining_diagnostics(entry_df, confirm_df)
            self._last_reject_reason = "candle_not_confirmed"
            return None

        # --- 4. Confidence + RSI (soft — no gate, used for sizing) ---
        confidence = self._calculate_confidence(entry_df, blended_signal, direction)
        self._last_diagnostics["confidence"] = round(confidence, 4)
        self._last_diagnostics["confidence_pass"] = True  # no longer a gate

        rsi_val = entry_df[self._rsi_col].iloc[-1]
        rsi_score = self._score_rsi_directional(rsi_val, direction)
        self._last_diagnostics["rsi"] = float(rsi_val) if pd.notna(rsi_val) else None
        self._last_diagnostics["rsi_score"] = round(rsi_score, 2)
        self._last_diagnostics["rsi_pass"] = True  # no longer a gate

        # --- 5. Build the signal ---
        close = float(entry_df["close"].iloc[-1])
        atr = float(entry_df[self._atr_col].iloc[-1])

        if pd.isna(atr) or atr <= 0:
            self._last_reject_reason = "atr_invalid"
            return None

        if direction is SignalDirection.LONG:
            stop_loss = close - atr * self.atr_stop_multiplier
            take_profit = close + atr * self.atr_tp_multiplier
        else:  # SHORT
            stop_loss = close + atr * self.atr_stop_multiplier
            take_profit = close - atr * self.atr_tp_multiplier

        instrument = entry_df.attrs.get("instrument", "UNKNOWN")

        return TradeSignal(
            instrument=instrument,
            direction=direction,
            confidence=round(confidence, 4),
            entry_price=close,
            stop_loss=round(stop_loss, 5),
            take_profit=round(take_profit, 5),
            timeframe=self.entry_tf,
            strategy_name="TrendFollowing",
            metadata={
                "adx": round(float(current_adx), 2),
                "atr": round(atr, 5),
                "ema_fast": float(entry_df[self._ema_fast_col].iloc[-1]),
                "ema_slow": float(entry_df[self._ema_slow_col].iloc[-1]),
                "rsi": float(rsi_val) if pd.notna(rsi_val) else 0.0,
                "rsi_score": round(rsi_score, 2),
                "htf_trend": htf_direction.value,
                "blended_signal": round(blended_signal, 4),
                "speed_breakdown": speed_breakdown,
            },
        )

    def should_exit(self, position: dict, features: dict[str, pd.DataFrame]) -> bool:
        """Decide whether an open position should be closed.

        Exit condition: Signal reversal only.
        The multi-speed blended signal must clearly reverse against the
        position direction.  Trailing stops, break-even, SL and TP are
        handled by StopManager / broker — not here.

        ADX-based exit was removed: ADX measures trend *strength*, not
        direction.  During healthy pullbacks ADX dips temporarily, causing
        premature exits that destroy the trend-following edge.
        """
        entry_df = features.get(self.entry_tf)
        if entry_df is None or len(entry_df) < 2:
            return False

        blended_signal, _ = self._compute_multi_speed_signal(entry_df)
        pos_dir = position.get("side", position.get("direction", ""))
        if isinstance(pos_dir, SignalDirection):
            pos_dir = pos_dir.value
        if pos_dir == SignalDirection.LONG.value and blended_signal < -self.min_signal_strength:
            return True
        if pos_dir == SignalDirection.SHORT.value and blended_signal > self.min_signal_strength:
            return True

        return False

    # ------------------------------------------------------------------
    # Diagnostic helpers
    # ------------------------------------------------------------------

    def _compute_remaining_diagnostics(
        self,
        entry_df: pd.DataFrame,
        confirm_df: pd.DataFrame,
    ) -> None:
        """Fill any missing diagnostic values after an early rejection.

        Only computes values that are NOT already in ``_last_diagnostics``.
        All helper methods are pure / side-effect-free so this is safe.
        """
        d = self._last_diagnostics

        # ADX
        if "adx" not in d:
            adx_val = entry_df[self._adx_col].iloc[-1]
            adx_f = float(adx_val) if pd.notna(adx_val) else None
            d["adx"] = adx_f
            d["adx_pass"] = adx_f is not None and adx_f >= self.adx_threshold

        # Hurst exponent
        if "hurst" not in d and self.hurst_threshold > 0 and "hurst_exponent" in entry_df.columns:
            h = entry_df["hurst_exponent"].iloc[-1]
            hurst_f = float(h) if pd.notna(h) else None
            d["hurst"] = hurst_f
            d["hurst_pass"] = hurst_f is None or hurst_f >= self.hurst_threshold

        # HTF trend direction
        if "htf_direction" not in d:
            htf = self._htf_trend_direction(confirm_df)
            d["htf_direction"] = htf.value
            d["htf_pass"] = htf is not SignalDirection.FLAT

        # Signal strength (multi-speed)
        if "signal_strength" not in d:
            blended, _ = self._compute_multi_speed_signal(entry_df)
            d["signal_strength"] = round(blended, 4)
            d["signal_pass"] = abs(blended) >= self.min_signal_strength
            if blended != 0:
                sig_dir = SignalDirection.LONG if blended > 0 else SignalDirection.SHORT
                d.setdefault("signal_direction", sig_dir.value)

        # Derive a direction for downstream diagnostics
        direction = None
        sig_dir_str = d.get("signal_direction")
        if sig_dir_str == "long":
            direction = SignalDirection.LONG
        elif sig_dir_str == "short":
            direction = SignalDirection.SHORT

        # Confidence (soft — always passes, used for sizing)
        if "confidence" not in d:
            blended = d.get("signal_strength", 0.0) or 0.0
            conf = self._calculate_confidence(entry_df, blended, direction)
            d["confidence"] = round(conf, 4)
            d["confidence_pass"] = True

        # RSI (soft — always passes, included as metadata)
        if "rsi" not in d:
            rsi_val = entry_df[self._rsi_col].iloc[-1]
            d["rsi"] = float(rsi_val) if pd.notna(rsi_val) else None
            if direction is not None:
                score = self._score_rsi_directional(rsi_val, direction)
            else:
                score = 0.5
            d["rsi_score"] = round(score, 2)
            d["rsi_pass"] = True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _htf_trend_direction(self, df: pd.DataFrame) -> SignalDirection:
        """Return the higher-timeframe trend direction based on EMA positioning.

        Requires a minimum ATR-normalised separation between EMAs to avoid
        whipsaws when the higher timeframe is range-bound.
        """
        ema_fast = df[self._ema_fast_col].iloc[-1]
        ema_slow = df[self._ema_slow_col].iloc[-1]

        if pd.isna(ema_fast) or pd.isna(ema_slow):
            return SignalDirection.FLAT

        # Minimum separation gate: avoid flipping on noise when EMAs are close
        if self._atr_col in df.columns:
            atr_val = df[self._atr_col].iloc[-1]
            if pd.notna(atr_val) and atr_val > 0:
                separation = abs(ema_fast - ema_slow) / atr_val
                if separation < 0.1:
                    return SignalDirection.FLAT

        if ema_fast > ema_slow:
            return SignalDirection.LONG
        elif ema_fast < ema_slow:
            return SignalDirection.SHORT
        return SignalDirection.FLAT

    def _in_trading_session(self, df: pd.DataFrame, instrument: str = "") -> bool:
        """Check if the latest candle falls within the pair's trading session.

        Uses per-pair session windows from PAIR_SESSIONS.  Falls back to
        the global session_start_utc / session_end_utc for unknown pairs.
        Handles midnight-wrapping windows (e.g. 21→10 for AUD/NZD).
        """
        last_ts = df.index[-1]
        if hasattr(last_ts, "hour"):
            hour = last_ts.hour
        else:
            hour = pd.Timestamp(last_ts).hour

        start, end = self.PAIR_SESSIONS.get(
            instrument, (self.session_start_utc, self.session_end_utc),
        )

        if start < end:
            return start <= hour < end
        # Wraps midnight (e.g. 21 → 10)
        return hour >= start or hour < end

    def _candle_confirms(self, df: pd.DataFrame, direction: SignalDirection) -> bool:
        """Check that the most recent candle closed in the signal direction."""
        last = df.iloc[-1]
        if direction is SignalDirection.LONG:
            return float(last["close"]) > float(last["open"])
        elif direction is SignalDirection.SHORT:
            return float(last["close"]) < float(last["open"])
        return False

    def _compute_multi_speed_signal(self, df: pd.DataFrame) -> tuple[float, dict]:
        """Compute a blended signal from fast, medium, and slow EMA speeds.

        Each speed calculates a continuous signal strength:
            signal = (ema_fast - ema_slow) / ATR

        This normalises the separation by volatility, making signals
        comparable across instruments and market conditions.
        """
        atr_col = self._atr_col
        if atr_col not in df.columns:
            return 0.0, {}

        atr_val = float(df[atr_col].iloc[-1])
        if pd.isna(atr_val) or atr_val <= 0:
            return 0.0, {}

        speeds = [
            ("fast", self.fast_ema_pair),
            ("medium", self.medium_ema_pair),
            ("slow", self.slow_ema_pair),
        ]
        weights = self.speed_weights

        signals: list[float] = []
        breakdown: dict[str, float] = {}

        for (name, pair), weight in zip(speeds, weights):
            fast_col = f"ema_{pair[0]}"
            slow_col = f"ema_{pair[1]}"

            # KAMA option for slow speed
            if name == "slow" and self.use_kama_slow and "kama_10" in df.columns:
                close_val = float(df["close"].iloc[-1])
                kama_val = float(df["kama_10"].iloc[-1])
                if pd.isna(kama_val):
                    signals.append(0.0)
                    breakdown[name] = 0.0
                    continue
                sig = math.tanh((close_val - kama_val) / atr_val)
                signals.append(sig)
                breakdown[name] = round(sig, 4)
                continue

            if fast_col not in df.columns or slow_col not in df.columns:
                signals.append(0.0)
                breakdown[name] = 0.0
                continue

            fast_val = float(df[fast_col].iloc[-1])
            slow_val = float(df[slow_col].iloc[-1])

            if pd.isna(fast_val) or pd.isna(slow_val):
                signals.append(0.0)
                breakdown[name] = 0.0
                continue

            # tanh normalizes each speed band to [-1, 1] so weights
            # reflect actual contribution (slow EMA separation no
            # longer dominates despite lower weight)
            sig = math.tanh((fast_val - slow_val) / atr_val)
            signals.append(sig)
            breakdown[name] = round(sig, 4)

        # Weighted blend
        blended = sum(s * w for s, w in zip(signals, weights))
        breakdown["blended"] = round(blended, 4)

        return blended, breakdown

    def _calculate_confidence(
        self,
        df: pd.DataFrame,
        blended_signal: float = 0.0,
        direction: SignalDirection | None = None,
    ) -> float:
        """Compute a composite confidence score in [0.0, 1.0].

        3 components (ADX removed — already influences sizing via hard gate
        at 20 + regime scale in the orchestrator, triple-stacking was
        over-suppressing moderate trends in the 20-25 ADX zone):

        1. Signal strength (35%) -- abs(blended), capped at 1.0.
        2. Hurst exponent (30%) -- H > 0.5 = trending persistence.
        3. RSI directional (35%) -- momentum alignment with trade direction.
        """
        # 1. Signal strength component (0-1)
        signal_score = min(abs(blended_signal), 1.0)

        # 2. Hurst exponent component (0-1)
        hurst_score = 0.5  # default when unavailable
        if "hurst_exponent" in df.columns:
            h = df["hurst_exponent"].iloc[-1]
            if pd.notna(h):
                hurst_score = max(0.0, min((float(h) - 0.4) / 0.3, 1.0))

        # 3. RSI directional score (0-1)
        rsi_score = 0.5  # neutral when direction unknown
        if direction is not None:
            rsi_val = df[self._rsi_col].iloc[-1]
            rsi_score = self._score_rsi_directional(rsi_val, direction)

        confidence = (
            0.35 * signal_score
            + 0.30 * hurst_score
            + 0.35 * rsi_score
        )
        return max(0.0, min(confidence, 1.0))

    def _score_rsi_directional(self, rsi_val: float, direction: SignalDirection) -> float:
        """Direction-aware RSI scoring for trend following.

        For LONG: strong upward momentum (RSI 55-75) scores highest.
        For SHORT: strong downward momentum (RSI 25-45) scores highest.
        Extremes (RSI > 75 for longs) are still acceptable in trends,
        unlike the old scoring that penalised RSI > 80.
        """
        if pd.isna(rsi_val):
            return 0.5  # neutral when unavailable

        rsi_f = float(rsi_val)

        if direction == SignalDirection.LONG:
            if 55 <= rsi_f <= 75:
                return 0.9   # Strong momentum
            elif rsi_f > 75:
                return 0.7   # Extended but trending
            elif 40 <= rsi_f < 55:
                return 0.4   # Weak momentum
            else:  # < 40
                return 0.1   # Wrong direction for a long
        elif direction == SignalDirection.SHORT:
            if 25 <= rsi_f <= 45:
                return 0.9   # Strong momentum
            elif rsi_f < 25:
                return 0.7   # Extended but trending
            elif 45 < rsi_f <= 60:
                return 0.4   # Weak momentum
            else:  # > 60
                return 0.1   # Wrong direction for a short

        return 0.5  # FLAT fallback
