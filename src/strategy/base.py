"""Abstract base class for trading strategies.

STANDALONE: Knows only about core/types for data types.
Strategies receive feature DataFrames and return signals.
They know NOTHING about broker, risk, execution, or ML.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.core.types import SignalDirection, TradeSignal  # noqa: F401 — re-export for convenience


class BaseStrategy(ABC):
    """Abstract interface for all trading strategies."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.enabled = True
        self._last_reject_reason: str | None = None

    @abstractmethod
    def generate_signal(self, features: dict[str, pd.DataFrame]) -> TradeSignal | None:
        """Generate a trade signal from multi-timeframe features.

        Parameters
        ----------
        features : dict[str, pd.DataFrame]
            Keys are timeframe strings (e.g. ``"H1"``, ``"H4"``).

        Returns
        -------
        TradeSignal | None
            Signal if conditions are met, else None.
        """
        ...

    @abstractmethod
    def should_exit(self, position: dict, features: dict[str, pd.DataFrame]) -> bool:
        """Check if an existing position should be closed."""
        ...

    @property
    def last_reject_reason(self) -> str | None:
        """Why the most recent generate_signal() returned None."""
        return self._last_reject_reason

    def enable(self) -> None:
        self.enabled = True

    def disable(self) -> None:
        self.enabled = False
