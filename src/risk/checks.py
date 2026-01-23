"""Pre-trade risk checks.

STANDALONE: Receives plain data (signal dict, account dict, positions list,
recovery phase int).  Returns (bool, reason).  Knows NOTHING about broker,
strategy, execution, or other risk modules.

Usage:
    from src.risk.checks import RiskChecks

    checks = RiskChecks(config)
    can_trade, reason = checks.check(signal, account, positions, recovery_phase)
"""

from __future__ import annotations

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

_DEFAULT_UNKNOWN_CORRELATION: float = 0.50


def _get_pip_value(instrument: str) -> float:
    """Return the value of one pip.  JPY pairs use 0.01; others 0.0001."""
    if "JPY" in instrument.upper():
        return 0.01
    return 0.0001


class RiskChecks:
    """Evaluates whether a proposed trade satisfies all risk rules.

    Parameters
    ----------
    config : dict
        The ``risk`` section of the YAML configuration.
    """

    def __init__(self, config: dict) -> None:
        self.max_open_positions: int = config.get("max_open_positions", 5)
        self.max_correlated_positions: int = config.get("max_correlated_positions", 2)
        self.correlation_threshold: float = config.get("correlation_threshold", 0.70)
        self.max_margin_usage_pct: float = config.get("max_margin_usage_pct", 50.0)
        self.max_spread_pips: float = config.get("max_spread_pips", 3.0)

        # Dynamic correlation matrix (set externally)
        self.correlation_matrix: pd.DataFrame | None = None

    def check(
        self,
        signal: dict,
        account: dict,
        positions: list[dict],
        recovery_phase: int = 0,
        spread: float = 0.0,
    ) -> tuple[bool, str]:
        """Run all pre-trade risk checks.

        Parameters
        ----------
        signal : dict
            Must have ``instrument`` key.
        account : dict
            Must have ``balance``, ``equity``, ``margin_used`` keys.
        positions : list[dict]
            Currently open positions.
        recovery_phase : int
            0=normal, 1=cooloff, 2=reduced, 3=hard_stop.
        spread : float
            Current spread in price units for the instrument.

        Returns
        -------
        tuple[bool, str]
            ``(True, "ok")`` if all checks pass, else ``(False, reason)``.
        """
        instrument = signal.get("instrument", "")

        # 0. Recovery phase checks (handled by DrawdownRecovery module)
        if recovery_phase == 3:
            return False, "Hard stop active"
        if recovery_phase == 1:
            return False, "Drawdown cool-off active"

        balance = account.get("balance", 0.0)

        # 1. Max open positions
        if len(positions) >= self.max_open_positions:
            return False, f"Max open positions reached: {len(positions)}/{self.max_open_positions}"

        # 2. Correlation check
        open_instruments = [p.get("instrument", "") for p in positions]
        correlated_count = 0
        for open_inst in open_instruments:
            corr = self._get_correlation(instrument, open_inst)
            if abs(corr) >= self.correlation_threshold:
                correlated_count += 1
        if correlated_count >= self.max_correlated_positions:
            return False, (
                f"Too many correlated positions for {instrument}: "
                f"{correlated_count} (limit {self.max_correlated_positions})"
            )

        # 3. Margin usage check
        margin_used = account.get("margin_used", 0.0)
        if balance > 0:
            total_margin_pct = (margin_used / balance) * 100.0
            if total_margin_pct >= self.max_margin_usage_pct:
                return False, f"Margin usage too high: {total_margin_pct:.1f}% (limit {self.max_margin_usage_pct:.1f}%)"

        # 4. Spread check
        if spread > 0:
            pip_value = _get_pip_value(instrument)
            spread_pips = spread / pip_value
            if spread_pips > self.max_spread_pips:
                return False, f"Spread too wide: {spread_pips:.1f} pips (limit {self.max_spread_pips:.1f})"

        return True, "ok"

    # ------------------------------------------------------------------
    # Correlation
    # ------------------------------------------------------------------

    def update_correlation_matrix(self, close_prices: dict[str, pd.Series]) -> None:
        """Recompute rolling Pearson correlation from close price series."""
        if not close_prices:
            return
        df = pd.DataFrame(close_prices)
        returns = df.pct_change().dropna()
        window = 60
        if len(returns) < window:
            return
        self.correlation_matrix = returns.tail(window).corr()

    def _get_correlation(self, pair_a: str, pair_b: str) -> float:
        if self.correlation_matrix is None:
            return _DEFAULT_UNKNOWN_CORRELATION
        if pair_a not in self.correlation_matrix.columns or pair_b not in self.correlation_matrix.columns:
            return _DEFAULT_UNKNOWN_CORRELATION
        corr = self.correlation_matrix.loc[pair_a, pair_b]
        return float(corr) if pd.notna(corr) else _DEFAULT_UNKNOWN_CORRELATION

