"""
Performance metrics calculation for the Prio Forex trading bot.

Provides standard quantitative-finance metrics for evaluating trading
strategy performance: risk-adjusted returns, drawdown analysis, and
trade-level statistics.

All functions accept pandas Series or DataFrames and return scalar floats
(or a summary dict).

Usage:
    from src.utils.metrics import sharpe_ratio, max_drawdown, calculate_all_metrics

    sr = sharpe_ratio(daily_returns)
    dd = max_drawdown(equity_curve)
    report = calculate_all_metrics(trades_df, equity_curve)
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Risk-adjusted return metrics
# ---------------------------------------------------------------------------

def sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """Annualised Sharpe ratio.

    Parameters
    ----------
    returns : pd.Series
        Period returns (e.g. daily percentage returns as decimals).
    risk_free_rate : float
        Per-period risk-free rate (default ``0.0``).
    periods_per_year : int
        Number of trading periods in a year (252 for daily).

    Returns
    -------
    float
        Annualised Sharpe ratio.  Returns ``0.0`` when standard deviation
        is zero or the series is empty.
    """
    if returns.empty:
        return 0.0

    excess = returns - risk_free_rate
    std = excess.std(ddof=1)

    if std == 0 or np.isnan(std):
        return 0.0

    return float((excess.mean() / std) * np.sqrt(periods_per_year))


def sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """Annualised Sortino ratio (penalises only downside volatility).

    Parameters
    ----------
    returns : pd.Series
        Period returns.
    risk_free_rate : float
        Per-period risk-free rate.
    periods_per_year : int
        Number of trading periods in a year.

    Returns
    -------
    float
        Annualised Sortino ratio.  Returns ``0.0`` when downside deviation
        is zero or the series is empty.
    """
    if returns.empty:
        return 0.0

    excess = returns - risk_free_rate
    downside = excess[excess < 0]

    if downside.empty:
        # No negative returns -- infinite Sortino; cap at 0.0 to avoid NaN.
        return 0.0

    downside_std = downside.std(ddof=1)

    if downside_std == 0 or np.isnan(downside_std):
        return 0.0

    return float((excess.mean() / downside_std) * np.sqrt(periods_per_year))


def calmar_ratio(
    returns: pd.Series,
    equity_curve: pd.Series,
    periods_per_year: int = 252,
) -> float:
    """Calmar ratio: annualised return divided by maximum drawdown.

    Parameters
    ----------
    returns : pd.Series
        Period returns.
    equity_curve : pd.Series
        Cumulative equity curve (absolute values, not returns).
    periods_per_year : int
        Number of trading periods in a year.

    Returns
    -------
    float
        Calmar ratio.  Returns ``0.0`` when max drawdown is zero.
    """
    if returns.empty or equity_curve.empty:
        return 0.0

    annualised_return = returns.mean() * periods_per_year
    mdd = max_drawdown(equity_curve)

    if mdd == 0:
        return 0.0

    return float(annualised_return / abs(mdd))


# ---------------------------------------------------------------------------
# Drawdown metrics
# ---------------------------------------------------------------------------

def max_drawdown(equity_curve: pd.Series) -> float:
    """Maximum peak-to-trough decline as a decimal fraction.

    Parameters
    ----------
    equity_curve : pd.Series
        Cumulative equity values (absolute, not returns).

    Returns
    -------
    float
        Maximum drawdown expressed as a positive decimal (e.g. ``0.15``
        for a 15 % decline).  Returns ``0.0`` for empty or flat curves.
    """
    if equity_curve.empty:
        return 0.0

    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak

    if drawdown.empty:
        return 0.0

    return float(abs(drawdown.min()))


def max_drawdown_duration(equity_curve: pd.Series) -> int:
    """Number of periods in the longest drawdown.

    A drawdown begins when the equity drops below its running peak and
    ends when a new peak is reached.

    Parameters
    ----------
    equity_curve : pd.Series
        Cumulative equity values.

    Returns
    -------
    int
        Length (in periods) of the longest drawdown episode.  Returns
        ``0`` for empty or monotonically increasing curves.
    """
    if equity_curve.empty:
        return 0

    peak = equity_curve.cummax()
    in_drawdown = equity_curve < peak

    longest = 0
    current = 0

    for is_dd in in_drawdown:
        if is_dd:
            current += 1
            longest = max(longest, current)
        else:
            current = 0

    return longest


# ---------------------------------------------------------------------------
# Trade-level metrics
# ---------------------------------------------------------------------------

def win_rate(trades: pd.DataFrame) -> float:
    """Percentage of profitable trades.

    Parameters
    ----------
    trades : pd.DataFrame
        Must contain a ``pnl`` column.

    Returns
    -------
    float
        Win rate as a decimal (e.g. ``0.60`` for 60 %).  Returns ``0.0``
        if *trades* is empty.
    """
    if trades.empty or "pnl" not in trades.columns:
        return 0.0

    total = len(trades)
    winners = (trades["pnl"] > 0).sum()

    return float(winners / total)


def profit_factor(trades: pd.DataFrame) -> float:
    """Ratio of gross profit to gross loss.

    Parameters
    ----------
    trades : pd.DataFrame
        Must contain a ``pnl`` column.

    Returns
    -------
    float
        Profit factor.  Returns ``0.0`` if there are no trades, and
        ``float('inf')`` if there are only winning trades.
    """
    if trades.empty or "pnl" not in trades.columns:
        return 0.0

    gross_profit = trades.loc[trades["pnl"] > 0, "pnl"].sum()
    gross_loss = abs(trades.loc[trades["pnl"] < 0, "pnl"].sum())

    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0

    return float(gross_profit / gross_loss)


def expectancy(trades: pd.DataFrame) -> float:
    """Average P&L per trade (mathematical expectancy).

    Parameters
    ----------
    trades : pd.DataFrame
        Must contain a ``pnl`` column.

    Returns
    -------
    float
        Mean trade P&L.  Returns ``0.0`` for empty DataFrames.
    """
    if trades.empty or "pnl" not in trades.columns:
        return 0.0

    return float(trades["pnl"].mean())


def avg_win_loss_ratio(trades: pd.DataFrame) -> float:
    """Average winning trade divided by average losing trade (absolute).

    Parameters
    ----------
    trades : pd.DataFrame
        Must contain a ``pnl`` column.

    Returns
    -------
    float
        Win/loss size ratio.  Returns ``0.0`` when there are no winners
        or no losers.
    """
    if trades.empty or "pnl" not in trades.columns:
        return 0.0

    winners = trades.loc[trades["pnl"] > 0, "pnl"]
    losers = trades.loc[trades["pnl"] < 0, "pnl"]

    if winners.empty or losers.empty:
        return 0.0

    avg_win = winners.mean()
    avg_loss = abs(losers.mean())

    if avg_loss == 0:
        return 0.0

    return float(avg_win / avg_loss)


# ---------------------------------------------------------------------------
# Aggregate report
# ---------------------------------------------------------------------------

def calculate_all_metrics(
    trades: pd.DataFrame,
    equity_curve: pd.Series,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> dict:
    """Compute all available performance metrics in a single call.

    Parameters
    ----------
    trades : pd.DataFrame
        Trade log with at least a ``pnl`` column.  Optional columns
        ``pnl_pct`` and ``entry_time`` / ``exit_time`` enrich the report.
    equity_curve : pd.Series
        Cumulative equity curve indexed by time.
    risk_free_rate : float
        Per-period risk-free rate for Sharpe / Sortino.
    periods_per_year : int
        Number of trading periods in one year.

    Returns
    -------
    dict
        Dictionary with the following keys:

        * ``sharpe_ratio``
        * ``sortino_ratio``
        * ``calmar_ratio``
        * ``max_drawdown``
        * ``max_drawdown_duration``
        * ``win_rate``
        * ``profit_factor``
        * ``expectancy``
        * ``avg_win_loss_ratio``
        * ``total_trades``
        * ``winning_trades``
        * ``losing_trades``
        * ``gross_profit``
        * ``gross_loss``
        * ``net_profit``
    """
    # Derive period returns from the equity curve.
    if equity_curve.empty:
        returns = pd.Series(dtype=float)
    else:
        returns = equity_curve.pct_change().dropna()

    # Trade counts.
    total = len(trades) if not trades.empty else 0
    winners = int((trades["pnl"] > 0).sum()) if total > 0 and "pnl" in trades.columns else 0
    losers = int((trades["pnl"] < 0).sum()) if total > 0 and "pnl" in trades.columns else 0

    gross_profit = float(trades.loc[trades["pnl"] > 0, "pnl"].sum()) if total > 0 and "pnl" in trades.columns else 0.0
    gross_loss = float(trades.loc[trades["pnl"] < 0, "pnl"].sum()) if total > 0 and "pnl" in trades.columns else 0.0

    return {
        "sharpe_ratio": sharpe_ratio(returns, risk_free_rate, periods_per_year),
        "sortino_ratio": sortino_ratio(returns, risk_free_rate, periods_per_year),
        "calmar_ratio": calmar_ratio(returns, equity_curve, periods_per_year),
        "max_drawdown": max_drawdown(equity_curve),
        "max_drawdown_duration": max_drawdown_duration(equity_curve),
        "win_rate": win_rate(trades),
        "profit_factor": profit_factor(trades),
        "expectancy": expectancy(trades),
        "avg_win_loss_ratio": avg_win_loss_ratio(trades),
        "total_trades": total,
        "winning_trades": winners,
        "losing_trades": losers,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "net_profit": gross_profit + gross_loss,
    }
