"""Position sizing calculator.

STANDALONE: Receives plain data (signal dict, account dict, features DataFrame,
recovery phase int, trade stats dict).  Returns number of units (int).
Knows NOTHING about broker, strategy, or other risk modules.

Usage:
    from src.risk.sizing import PositionSizer

    sizer = PositionSizer(config)
    units = sizer.calculate(signal, account, features_df, recovery_phase)
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from src.core.types import SignalDirection

logger = structlog.get_logger(__name__)


class PositionSizer:
    """Calculates position size based on risk parameters.

    Parameters
    ----------
    config : dict
        The ``risk`` section of the YAML configuration.
    """

    def __init__(self, config: dict) -> None:
        self.position_sizing_method: str = config.get("position_sizing_method", "fixed_pct")
        self.fixed_position_pct: float = config.get("fixed_position_pct", 1.0)
        self.max_position_size_pct: float = config.get("max_position_size_pct", 5.0)
        self.default_leverage: float = config.get("default_leverage", 30.0)
        self.max_margin_per_position_pct: float = config.get("max_margin_per_position_pct", 15.0)

        # Recovery scaling
        self.recovery_position_scale: float = config.get("recovery_position_scale", 0.5)

        # Volatility targeting
        vol_cfg: dict = config.get("volatility_target", {})
        self.vol_target_enabled: bool = vol_cfg.get("enabled", True)
        self.vol_target_value: float = vol_cfg.get("target_vol", 0.0)
        self.vol_target_min_scale: float = vol_cfg.get("min_scale", 0.3)
        self.vol_target_max_scale: float = vol_cfg.get("max_scale", 2.0)

        # Adaptive Kelly settings
        self.adaptive_min_trades: int = config.get("adaptive_min_trades", 50)

    def calculate(
        self,
        signal: dict,
        account: dict,
        features_df: pd.DataFrame | None = None,
        recovery_phase: int = 0,
        trade_stats: dict | None = None,
        prices: dict[str, float] | None = None,
    ) -> int:
        """Calculate position size in units.

        Parameters
        ----------
        signal : dict
            Must have ``instrument``, ``entry_price``, ``stop_loss``, ``direction``.
        account : dict
            Must have ``balance``.
        features_df : pd.DataFrame | None
            For volatility targeting (needs ``realized_vol_20``).
        recovery_phase : int
            0=normal, 2=reduced sizing.
        trade_stats : dict | None
            For adaptive Kelly (needs ``count``, ``win_rate``, ``avg_win_loss_ratio``).
        prices : dict[str, float] | None
            Latest close prices for all traded pairs (e.g. {"EUR_USD": 1.08, ...}).
            Used to convert pip values to account currency (EUR).

        Returns
        -------
        int
            Number of units. Positive for buy, negative for sell.
            Returns 0 if position cannot be sized.
        """
        balance: float = account.get("balance", 0.0)
        entry_price = signal.get("entry_price", 0.0)
        stop_loss = signal.get("stop_loss", 0.0)
        stop_distance = abs(entry_price - stop_loss)
        instrument = signal.get("instrument", "")

        if stop_distance == 0 or balance == 0:
            return 0

        # Convert stop_distance to account currency (EUR) per unit
        quote_to_eur = self._get_quote_to_eur(instrument, prices or {})
        stop_distance_eur = stop_distance * quote_to_eur

        if stop_distance_eur <= 0:
            return 0

        # Base sizing method
        if self.position_sizing_method == "adaptive_kelly" and trade_stats:
            units = self._adaptive_kelly(balance, stop_distance_eur, trade_stats)
        else:
            risk_amount = balance * self.fixed_position_pct / 100.0
            units = int(risk_amount / stop_distance_eur)

        # Cap: max_position_size_pct
        max_pos_risk = balance * self.max_position_size_pct / 100.0
        max_pos_units = int(max_pos_risk / stop_distance_eur)
        if max_pos_units > 0:
            units = min(abs(units), max_pos_units)

        # Volatility targeting
        if self.vol_target_enabled and features_df is not None and "realized_vol_20" in features_df.columns:
            realized_vol = features_df["realized_vol_20"].iloc[-1]
            if pd.notna(realized_vol) and realized_vol > 0:
                target = self.vol_target_value if self.vol_target_value > 0 else 0.005
                vol_scale = target / realized_vol
                vol_scale = max(self.vol_target_min_scale, min(vol_scale, self.vol_target_max_scale))
                units = int(abs(units) * vol_scale)

        # Margin cap
        if balance > 0 and self.default_leverage > 0:
            max_margin = balance * self.max_margin_per_position_pct / 100.0
            price = entry_price if entry_price > 0 else 1.0
            # Convert notional to EUR: units * price (in quote) * quote_to_eur
            max_units_by_margin = int(max_margin * self.default_leverage / (price * quote_to_eur))
            if max_units_by_margin > 0 and abs(units) > max_units_by_margin:
                units = max_units_by_margin

        # Recovery phase 2: reduce position size
        if recovery_phase == 2:
            units = int(abs(units) * self.recovery_position_scale)

        # Capital.com minimum: 1000 units for forex
        # After applying the floor, verify that the forced minimum doesn't
        # exceed max_position_size_pct — reject the trade if it does.
        # Also reject if recovery scaling reduced below floor (don't override recovery).
        if abs(units) < 1000:
            if recovery_phase == 2:
                logger.info(
                    "recovery_below_floor",
                    instrument=instrument,
                    recovery_units=abs(units),
                    floor=1000,
                )
                return 0
            floor_risk = 1000 * stop_distance_eur
            max_allowed_risk = balance * self.max_position_size_pct / 100.0
            if floor_risk > max_allowed_risk:
                logger.warning(
                    "floor_exceeds_max_risk",
                    instrument=instrument,
                    floor_risk_eur=round(floor_risk, 2),
                    max_allowed_eur=round(max_allowed_risk, 2),
                )
                return 0
            units = 1000

        # Direction
        direction = signal.get("direction", "")
        if isinstance(direction, SignalDirection):
            direction = direction.value
        if direction == "short":
            units = -units

        logger.debug(
            "position_sized",
            instrument=instrument,
            units=units,
            stop_distance=round(stop_distance, 5),
            quote_to_eur=round(quote_to_eur, 6),
            stop_distance_eur=round(stop_distance_eur, 6),
        )

        return units

    # ------------------------------------------------------------------
    # Quote-to-EUR conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _build_eur_rates(prices: dict[str, float]) -> dict[str, float]:
        """Build conversion table: 1 unit of currency X = how many EUR.

        Uses available pair prices to triangulate cross rates.
        E.g. EUR_USD=1.08 → USD_to_EUR=0.926, EUR_GBP=0.84 → GBP_to_EUR=1.19
        Then GBP_JPY=208 + GBP_to_EUR → JPY_to_EUR via triangulation.
        """
        rates: dict[str, float] = {"EUR": 1.0}

        # Pass 1: direct EUR pairs
        for pair, price in prices.items():
            parts = pair.split("_")
            if len(parts) != 2 or price <= 0:
                continue
            base, quote = parts
            if base == "EUR" and quote not in rates:
                # EUR/XXX: 1 EUR = price XXX → 1 XXX = 1/price EUR
                rates[quote] = 1.0 / price
            elif quote == "EUR" and base not in rates:
                # XXX/EUR: 1 XXX = price EUR
                rates[base] = price

        # Pass 2+: triangulate through known rates
        for _ in range(5):
            changed = False
            for pair, price in prices.items():
                parts = pair.split("_")
                if len(parts) != 2 or price <= 0:
                    continue
                base, quote = parts
                if base in rates and quote not in rates:
                    # 1 BASE = price QUOTE → 1 QUOTE = (1/price) BASE
                    # QUOTE_to_EUR = BASE_to_EUR / price
                    rates[quote] = rates[base] / price
                    changed = True
                elif quote in rates and base not in rates:
                    # 1 BASE = price QUOTE → BASE_to_EUR = price * QUOTE_to_EUR
                    rates[base] = price * rates[quote]
                    changed = True
            if not changed:
                break

        return rates

    def _get_quote_to_eur(self, instrument: str, prices: dict[str, float]) -> float:
        """Get conversion rate for 1 unit of quote currency → EUR."""
        parts = instrument.replace("/", "_").split("_")
        if len(parts) != 2:
            return 1.0

        quote = parts[1]
        if quote == "EUR":
            return 1.0

        if not prices:
            return 1.0

        eur_rates = self._build_eur_rates(prices)
        rate = eur_rates.get(quote)
        if rate is not None and rate > 0:
            return rate

        logger.warning("quote_conversion_fallback", instrument=instrument, quote=quote)
        return 1.0

    # ------------------------------------------------------------------
    # Sizing methods
    # ------------------------------------------------------------------

    def _adaptive_kelly(
        self, balance: float, stop_distance_eur: float, stats: dict
    ) -> int:
        if stats.get("count", 0) < self.adaptive_min_trades:
            risk_amount = balance * self.fixed_position_pct / 100.0
            return int(risk_amount / stop_distance_eur)

        win_rate = stats.get("win_rate", 0.0)
        avg_ratio = stats.get("avg_win_loss_ratio", 0.0)
        if avg_ratio <= 0:
            risk_amount = balance * self.fixed_position_pct / 100.0
            return int(risk_amount / stop_distance_eur)

        kelly_fraction = win_rate - (1.0 - win_rate) / avg_ratio
        kelly_fraction = max(kelly_fraction, 0.0) * 0.5  # Half-Kelly
        kelly_fraction = min(kelly_fraction, self.max_position_size_pct / 100.0)

        return int((kelly_fraction * balance) / stop_distance_eur)
