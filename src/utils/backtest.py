"""
Backtesting engine for the Prio Forex trading bot.

Simulates trading on historical data by reusing live strategy logic
(``BaseStrategy.generate_signal``).  Trades are filled at the next bar's
open and SL/TP exits are checked intra-bar using High/Low.

Usage:
    from src.utils.backtest import BacktestEngine

    engine = BacktestEngine(config["backtest"])
    result = engine.run(strategy, df, features, risk_config)
    print(result.metrics)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from src.strategy.base import BaseStrategy, TradeSignal, SignalDirection
from src.utils.metrics import calculate_all_metrics


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
    """Container for backtesting output."""

    trades: pd.DataFrame
    """Trade log with columns: entry_time, exit_time, instrument, direction,
    units, entry_price, exit_price, pnl, pnl_pct, fees."""

    equity_curve: pd.Series
    """Cumulative equity indexed by bar timestamp."""

    metrics: dict[str, Any]
    """Full set of performance metrics (from :func:`calculate_all_metrics`)."""

    initial_balance: float
    final_balance: float
    total_trades: int
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float
    profit_factor: float


# ---------------------------------------------------------------------------
# Internal position representation
# ---------------------------------------------------------------------------

@dataclass
class _Position:
    """Tracks an open virtual position during a backtest."""

    instrument: str
    direction: SignalDirection
    units: int
    entry_price: float
    entry_time: Any  # pd.Timestamp
    stop_loss: float
    take_profit: float
    fees: float = 0.0
    financing_cost: float = 0.0
    last_financing_date: Any = None  # pd.Timestamp or None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class BacktestEngine:
    """Event-driven backtesting engine.

    Parameters
    ----------
    config : dict
        Backtesting configuration.  Supported keys:

        * ``initial_balance`` (float) -- starting account equity
          (default ``10_000``).
        * ``commission_pct`` (float) -- per-trade commission as a
          percentage of notional (default ``0.0`` because the broker is
          spread-based and spread cost is embedded in the fill price).
    """

    def __init__(self, config: dict) -> None:
        self.initial_balance: float = config.get("initial_balance", 10_000.0)
        self.commission_pct: float = config.get("commission_pct", 0.0)
        self.overnight_rate_long: float = config.get("overnight_financing_rate_long", 0.0)
        self.overnight_rate_short: float = config.get("overnight_financing_rate_short", 0.0)

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def run(
        self,
        strategy: BaseStrategy,
        df: pd.DataFrame,
        features: dict[str, pd.DataFrame],
        risk_config: dict,
        initial_balance: float = 10_000.0,
    ) -> BacktestResult:
        """Execute a full backtest.

        Parameters
        ----------
        strategy : BaseStrategy
            An instantiated strategy whose ``generate_signal`` method will
            be called at every bar.
        df : pd.DataFrame
            Primary-timeframe OHLCV DataFrame (must have ``open``, ``high``,
            ``low``, ``close`` columns and a datetime index).
        features : dict[str, pd.DataFrame]
            Multi-timeframe feature DataFrames keyed by timeframe string
            (e.g. ``"H1"``, ``"H4"``).  Sliced up to the current bar
            before being handed to the strategy to prevent lookahead bias.
        risk_config : dict
            Risk parameters.  Supported keys:

            * ``max_open_positions`` (int) -- concurrent position limit.
            * ``fixed_position_pct`` (float) -- percentage of balance
              risked per trade for position sizing.
        initial_balance : float
            Starting account balance (overrides the constructor default).

        Returns
        -------
        BacktestResult
        """
        balance: float = initial_balance
        equity_values: list[float] = []
        equity_times: list = []
        closed_trades: list[dict] = []
        open_positions: list[_Position] = []

        max_open = risk_config.get("max_open_positions", 5)
        fixed_pct = risk_config.get("fixed_position_pct", 1.0)

        bars = df.reset_index()
        if "time" not in bars.columns and "datetime" not in bars.columns:
            # Use the index name or fall back to the first column.
            time_col = df.index.name or bars.columns[0]
            bars = bars.rename(columns={time_col: "time"})

        time_col = "time" if "time" in bars.columns else "datetime"

        for i in range(1, len(bars)):
            current_bar = bars.iloc[i]
            prev_bar = bars.iloc[i - 1]
            current_time = current_bar[time_col]

            # ---- 0. Apply overnight financing --------------------------------
            for pos in open_positions:
                financing = self._apply_overnight_financing(pos, current_time, float(current_bar["close"]))
                balance -= financing

            # ---- 1. Check SL / TP on open positions ----------------------
            positions_to_remove: list[int] = []
            for pos_idx, pos in enumerate(open_positions):
                is_closed, exit_price = self._simulate_trade_exit(pos, current_bar)
                if is_closed:
                    pnl = self._calculate_pnl(pos, exit_price)
                    pnl_pct = (pnl / (abs(pos.units) * pos.entry_price)) * 100.0 if pos.entry_price != 0 else 0.0
                    balance += pnl

                    closed_trades.append({
                        "entry_time": pos.entry_time,
                        "exit_time": current_time,
                        "instrument": pos.instrument,
                        "direction": pos.direction.value,
                        "units": pos.units,
                        "entry_price": pos.entry_price,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "fees": pos.fees,
                        "financing_cost": pos.financing_cost,
                    })
                    positions_to_remove.append(pos_idx)

            # Remove closed positions (iterate in reverse to keep indices valid).
            for idx in reversed(positions_to_remove):
                open_positions.pop(idx)

            # ---- 2. Generate signal using previous bar's features ---------
            # Slice features up to the *previous* bar to avoid lookahead.
            sliced_features: dict[str, pd.DataFrame] = {}
            for tf_key, tf_df in features.items():
                # Include only rows whose index <= prev_bar time.
                prev_time = prev_bar[time_col]
                sliced_features[tf_key] = tf_df.loc[:prev_time]

            signal: TradeSignal | None = strategy.generate_signal(sliced_features)

            # ---- 3. Act on signal -----------------------------------------
            if signal is not None and signal.direction != SignalDirection.FLAT:
                # Simplified risk check: max open positions.
                if len(open_positions) < max_open:
                    # Position sizing: risk fixed_pct of balance.
                    stop_distance = abs(signal.entry_price - signal.stop_loss)
                    if stop_distance > 0:
                        risk_amount = balance * fixed_pct / 100.0
                        units = int(risk_amount / stop_distance)
                    else:
                        units = 0

                    if units > 0:
                        # Fill at current bar's open (the bar *after* the signal bar).
                        fill_price = float(current_bar["open"])
                        fees = abs(units) * fill_price * self.commission_pct / 100.0
                        balance -= fees

                        # Adjust SL/TP relative to fill (maintain distance).
                        sl_distance = abs(signal.entry_price - signal.stop_loss)
                        tp_distance = abs(signal.take_profit - signal.entry_price)

                        if signal.direction == SignalDirection.LONG:
                            adj_sl = fill_price - sl_distance
                            adj_tp = fill_price + tp_distance
                        else:
                            adj_sl = fill_price + sl_distance
                            adj_tp = fill_price - tp_distance

                        signed_units = units if signal.direction == SignalDirection.LONG else -units

                        open_positions.append(
                            _Position(
                                instrument=signal.instrument,
                                direction=signal.direction,
                                units=signed_units,
                                entry_price=fill_price,
                                entry_time=current_time,
                                stop_loss=adj_sl,
                                take_profit=adj_tp,
                                fees=fees,
                            )
                        )

            # ---- 4. Record equity -----------------------------------------
            # Mark-to-market open positions at current bar's close.
            unrealised = 0.0
            close_price = float(current_bar["close"])
            for pos in open_positions:
                unrealised += self._calculate_pnl(pos, close_price)

            equity_values.append(balance + unrealised)
            equity_times.append(current_time)

        # ---- 5. Force-close remaining positions at last bar's close -------
        if open_positions:
            last_bar = bars.iloc[-1]
            last_time = last_bar[time_col]
            last_close = float(last_bar["close"])

            for pos in open_positions:
                pnl = self._calculate_pnl(pos, last_close)
                pnl_pct = (pnl / (abs(pos.units) * pos.entry_price)) * 100.0 if pos.entry_price != 0 else 0.0
                balance += pnl

                closed_trades.append({
                    "entry_time": pos.entry_time,
                    "exit_time": last_time,
                    "instrument": pos.instrument,
                    "direction": pos.direction.value,
                    "units": pos.units,
                    "entry_price": pos.entry_price,
                    "exit_price": last_close,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "fees": pos.fees,
                    "financing_cost": pos.financing_cost,
                })

            open_positions.clear()

            # Update last equity point to reflect final balance.
            if equity_values:
                equity_values[-1] = balance

        # ---- 6. Build result objects --------------------------------------
        trades_df = pd.DataFrame(closed_trades) if closed_trades else pd.DataFrame(
            columns=[
                "entry_time", "exit_time", "instrument", "direction",
                "units", "entry_price", "exit_price", "pnl", "pnl_pct",
                "fees", "financing_cost",
            ]
        )

        equity_curve = pd.Series(
            data=equity_values,
            index=pd.DatetimeIndex(equity_times),
            name="equity",
            dtype=float,
        ) if equity_values else pd.Series(dtype=float, name="equity")

        metrics = calculate_all_metrics(trades_df, equity_curve)

        return BacktestResult(
            trades=trades_df,
            equity_curve=equity_curve,
            metrics=metrics,
            initial_balance=initial_balance,
            final_balance=balance,
            total_trades=len(trades_df),
            win_rate=metrics.get("win_rate", 0.0),
            max_drawdown=metrics.get("max_drawdown", 0.0),
            sharpe_ratio=metrics.get("sharpe_ratio", 0.0),
            profit_factor=metrics.get("profit_factor", 0.0),
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _simulate_trade_exit(
        position: _Position, bar: pd.Series
    ) -> tuple[bool, float]:
        """Check whether a position's SL or TP is hit within *bar*.

        Uses the bar's ``high`` and ``low`` to determine if price crossed
        the stop-loss or take-profit level.  When both are hit within the
        same bar, the stop-loss is assumed to have been hit first (worst-
        case assumption).

        Parameters
        ----------
        position : _Position
            The open virtual position.
        bar : pd.Series
            Current OHLCV bar (must have ``high`` and ``low``).

        Returns
        -------
        tuple[bool, float]
            ``(True, exit_price)`` if the position was closed, otherwise
            ``(False, 0.0)``.
        """
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])

        is_long = position.units > 0

        sl_hit = False
        tp_hit = False

        if is_long:
            # Long position: SL hit if low <= SL; TP hit if high >= TP.
            sl_hit = bar_low <= position.stop_loss
            tp_hit = bar_high >= position.take_profit
        else:
            # Short position: SL hit if high >= SL; TP hit if low <= TP.
            sl_hit = bar_high >= position.stop_loss
            tp_hit = bar_low <= position.take_profit

        if sl_hit and tp_hit:
            # Worst-case: assume SL is hit first.
            return True, position.stop_loss

        if sl_hit:
            return True, position.stop_loss

        if tp_hit:
            return True, position.take_profit

        return False, 0.0

    @staticmethod
    def _calculate_pnl(position: _Position, exit_price: float) -> float:
        """Compute realised P&L for a position.

        Parameters
        ----------
        position : _Position
            The open position.
        exit_price : float
            Price at which the position is closed.

        Returns
        -------
        float
            Signed P&L in account currency.
        """
        if position.units > 0:
            # Long: profit when exit > entry.
            return (exit_price - position.entry_price) * abs(position.units)
        else:
            # Short: profit when exit < entry.
            return (position.entry_price - exit_price) * abs(position.units)

    def _apply_overnight_financing(
        self, position: _Position, current_time: Any, current_price: float
    ) -> float:
        """Apply overnight financing (swap) cost for holding a position.

        Charges are applied once per calendar day.  Returns the financing
        cost deducted from the account balance for this bar.
        """
        if self.overnight_rate_long == 0 and self.overnight_rate_short == 0:
            return 0.0

        current_date = pd.Timestamp(current_time)
        if position.last_financing_date is not None:
            last_date = pd.Timestamp(position.last_financing_date)
            if current_date.date() <= last_date.date():
                return 0.0  # Already charged today
            days = (current_date.date() - last_date.date()).days
        else:
            entry_date = pd.Timestamp(position.entry_time)
            if current_date.date() <= entry_date.date():
                return 0.0  # Same day as entry
            days = (current_date.date() - entry_date.date()).days

        if days <= 0:
            return 0.0

        # Determine rate based on direction
        is_long = position.units > 0
        annual_rate = self.overnight_rate_long if is_long else self.overnight_rate_short
        daily_rate = annual_rate / 365.0

        notional = abs(position.units) * current_price
        cost = notional * daily_rate * days

        position.financing_cost += cost
        position.last_financing_date = current_time

        return cost


# ---------------------------------------------------------------------------
# Walk-Forward Engine
# ---------------------------------------------------------------------------

@dataclass
class WalkForwardResult:
    """Aggregated results from a walk-forward backtest."""

    window_results: list[dict[str, Any]]
    """Per-window results: sharpe, max_drawdown, trades, pnl, balance."""

    aggregate_sharpe: float
    aggregate_max_drawdown: float
    aggregate_total_trades: int
    aggregate_total_pnl: float
    final_balance: float
    worst_window_sharpe: float
    worst_window_drawdown: float


class WalkForwardEngine:
    """Sliding-window walk-forward backtesting engine.

    Splits historical data into overlapping train/test windows and runs
    the backtest only on each out-of-sample (OOS) test window.  The
    account balance carries over between windows for realistic
    compounding.

    Parameters
    ----------
    config : dict
        Walk-forward configuration.  Keys:

        * ``train_months`` (int) -- months of data used for training
          (default ``12``).
        * ``test_months`` (int) -- months of out-of-sample data
          (default ``3``).
        * ``step_months`` (int) -- window advance step (default ``1``).
    backtest_config : dict
        Passed through to :class:`BacktestEngine` for each window.
    """

    def __init__(self, config: dict, backtest_config: dict) -> None:
        self.train_months: int = config.get("train_months", 12)
        self.test_months: int = config.get("test_months", 3)
        self.step_months: int = config.get("step_months", 1)
        self.backtest_config = backtest_config

    def run(
        self,
        strategy: BaseStrategy,
        df: pd.DataFrame,
        features: dict[str, pd.DataFrame],
        risk_config: dict,
        initial_balance: float = 10_000.0,
    ) -> WalkForwardResult:
        """Execute a walk-forward backtest.

        Parameters
        ----------
        strategy : BaseStrategy
            Strategy instance.
        df : pd.DataFrame
            Full primary-timeframe OHLCV DataFrame.
        features : dict[str, pd.DataFrame]
            Full multi-timeframe feature DataFrames.
        risk_config : dict
            Risk parameters forwarded to each window's backtest.
        initial_balance : float
            Starting balance for the first window.

        Returns
        -------
        WalkForwardResult
        """
        window_results: list[dict[str, Any]] = []
        balance = initial_balance

        # Build date windows
        start_date = df.index.min()
        end_date = df.index.max()
        train_offset = pd.DateOffset(months=self.train_months)
        test_offset = pd.DateOffset(months=self.test_months)
        step_offset = pd.DateOffset(months=self.step_months)

        window_start = start_date

        while True:
            train_end = window_start + train_offset
            test_end = train_end + test_offset

            if train_end >= end_date:
                break

            # Slice OOS test window
            test_df = df.loc[train_end:test_end]
            if test_df.empty or len(test_df) < 10:
                window_start += step_offset
                continue

            # Slice features up to test window end
            test_features: dict[str, pd.DataFrame] = {}
            for tf_key, tf_df in features.items():
                test_features[tf_key] = tf_df.loc[:test_end]

            # Run backtest on OOS window
            engine = BacktestEngine(self.backtest_config)
            result = engine.run(
                strategy=strategy,
                df=test_df,
                features=test_features,
                risk_config=risk_config,
                initial_balance=balance,
            )

            window_info = {
                "window_start": str(train_end),
                "window_end": str(min(test_end, end_date)),
                "sharpe": result.sharpe_ratio,
                "max_drawdown": result.max_drawdown,
                "total_trades": result.total_trades,
                "pnl": result.final_balance - balance,
                "start_balance": balance,
                "end_balance": result.final_balance,
                "win_rate": result.win_rate,
            }
            window_results.append(window_info)

            # Carry balance forward
            balance = result.final_balance

            window_start += step_offset
            if test_end >= end_date:
                break

        if not window_results:
            return WalkForwardResult(
                window_results=[],
                aggregate_sharpe=0.0,
                aggregate_max_drawdown=0.0,
                aggregate_total_trades=0,
                aggregate_total_pnl=0.0,
                final_balance=initial_balance,
                worst_window_sharpe=0.0,
                worst_window_drawdown=0.0,
            )

        sharpes = [w["sharpe"] for w in window_results]
        drawdowns = [w["max_drawdown"] for w in window_results]
        total_trades = sum(w["total_trades"] for w in window_results)
        total_pnl = balance - initial_balance

        return WalkForwardResult(
            window_results=window_results,
            aggregate_sharpe=float(np.mean(sharpes)) if sharpes else 0.0,
            aggregate_max_drawdown=max(drawdowns) if drawdowns else 0.0,
            aggregate_total_trades=total_trades,
            aggregate_total_pnl=total_pnl,
            final_balance=balance,
            worst_window_sharpe=min(sharpes) if sharpes else 0.0,
            worst_window_drawdown=max(drawdowns) if drawdowns else 0.0,
        )
