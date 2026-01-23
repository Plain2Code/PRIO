"""Trailing stop and break-even management.

STANDALONE: Receives positions and features as plain data.
Returns a list of stop-loss updates.  Knows NOTHING about broker,
strategy, or other risk modules.

Usage:
    from src.risk.stops import StopManager

    stops = StopManager(config)
    updates = stops.calculate_updates(positions, features)
"""

from __future__ import annotations

import structlog

from src.core.types import StopUpdate

logger = structlog.get_logger(__name__)


class StopManager:
    """Manages trailing stops and break-even for open positions.

    Both activation thresholds use ATR multiples (not percentage of
    entry price) so they scale correctly across instruments and
    volatility regimes.  The old percentage-based activation was
    effectively dead code for forex — 2% of EUR/USD ≈ 220 pips,
    far beyond any typical TP target.

    Parameters
    ----------
    config : dict
        The ``risk`` section of the YAML configuration.
    """

    def __init__(self, config: dict) -> None:
        # Trailing stop
        ts_cfg: dict = config.get("trailing_stop", {})
        self.trailing_enabled: bool = ts_cfg.get("enabled", True)
        self.activation_atr: float = ts_cfg.get("activation_atr", 1.5)
        self.trail_distance_atr: float = ts_cfg.get("trail_distance_atr", 2.0)

        # Break-even
        be_cfg: dict = config.get("break_even", {})
        self.break_even_enabled: bool = be_cfg.get("enabled", True)
        self.break_even_activation_atr: float = be_cfg.get("activation_atr", 1.0)

    def calculate_updates(
        self,
        positions: list[dict],
        features: dict[str, "pd.DataFrame"],
    ) -> list[StopUpdate]:
        """Evaluate trailing stops and break-even for all positions.

        Progression:
        1. Entry: SL at -1.5 ATR from entry.
        2. +1.0 ATR profit: SL moves to entry (break-even, risk-free).
        3. +1.5 ATR profit: Trailing stop activates (SL = price - 2.0 ATR).
        4. SL never moves backwards — only ratchets in profit direction.

        Parameters
        ----------
        positions : list[dict]
            Open positions with keys: ``instrument``, ``units``,
            ``average_price``, ``stop_loss``, ``deal_id``.
        features : dict[str, pd.DataFrame]
            Feature DataFrames keyed by instrument.

        Returns
        -------
        list[StopUpdate]
            Stop-loss updates to apply.
        """
        import pandas as pd

        updates: list[StopUpdate] = []

        for pos in positions:
            instrument: str = pos.get("instrument", "")
            df = features.get(instrument)
            if df is None or df.empty:
                continue

            current_price = float(df["close"].iloc[-1])
            entry_price = pos.get("average_price", 0.0)
            existing_sl = pos.get("stop_loss")
            units = pos.get("units", 0)
            is_long = units > 0
            deal_id = pos.get("deal_id") or pos.get("position_id") or instrument

            if entry_price == 0:
                continue

            # ATR for activation thresholds
            if "atr_14" not in df.columns:
                continue

            atr_val = float(df["atr_14"].iloc[-1])
            if pd.isna(atr_val) or atr_val <= 0:
                continue

            # Profit in ATR multiples
            if is_long:
                profit_atr = (current_price - entry_price) / atr_val
            else:
                profit_atr = (entry_price - current_price) / atr_val

            # -- Trailing stop takes priority when profit is high enough --
            # This avoids a 1-tick delay when price jumps past both
            # break-even and trailing activation on the same candle.
            applied = False
            if self.trailing_enabled and profit_atr >= self.activation_atr:
                ts_update = self._check_trailing(
                    deal_id, instrument, current_price, existing_sl, is_long, df
                )
                if ts_update:
                    updates.append(ts_update)
                    applied = True

            # -- Break-even check (only if trailing didn't produce an update) --
            if not applied and self.break_even_enabled and profit_atr >= self.break_even_activation_atr:
                be_update = self._check_break_even(
                    deal_id, entry_price, existing_sl, is_long
                )
                if be_update:
                    updates.append(be_update)

        return updates

    def _check_break_even(
        self,
        deal_id: str,
        entry_price: float,
        existing_sl: float | None,
        is_long: bool,
    ) -> StopUpdate | None:
        """Move SL to break-even if not already there."""
        new_sl = round(entry_price, 5)

        # Compare rounded values to avoid re-sending identical SL
        if is_long:
            if existing_sl is not None and existing_sl >= new_sl:
                return None
        else:
            if existing_sl is not None and existing_sl <= new_sl:
                return None

        logger.info("break_even_triggered", deal_id=deal_id, new_sl=new_sl)
        return StopUpdate(position_id=deal_id, new_stop_loss=new_sl, update_type="break_even")

    def _check_trailing(
        self,
        deal_id: str,
        instrument: str,
        current_price: float,
        existing_sl: float | None,
        is_long: bool,
        df: "pd.DataFrame",
    ) -> StopUpdate | None:
        """Calculate ATR-based trailing stop."""
        if "atr_14" not in df.columns:
            return None

        atr_val = float(df["atr_14"].iloc[-1])
        trail_distance = atr_val * self.trail_distance_atr

        # Round first, then compare — avoids spamming broker with
        # identical values caused by sub-pip price movements.
        # Require at least 1 pip improvement to send an update.
        min_pip = 0.01 if "JPY" in instrument.upper() else 0.0001

        if is_long:
            new_sl = round(current_price - trail_distance, 5)
            if existing_sl is not None and new_sl < existing_sl + min_pip:
                return None
        else:
            new_sl = round(current_price + trail_distance, 5)
            if existing_sl is not None and new_sl > existing_sl - min_pip:
                return None

        logger.info(
            "trailing_stop_update",
            instrument=instrument,
            deal_id=deal_id,
            new_sl=new_sl,
            existing_sl=existing_sl,
            atr=atr_val,
        )
        return StopUpdate(position_id=deal_id, new_stop_loss=new_sl, update_type="trailing")
