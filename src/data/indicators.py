"""
Technical indicators library for the Prio Forex trading bot.

All functions are pure: they accept pandas DataFrames/Series and return
new Series/DataFrames.  No side effects, no global state, no TA-Lib.

Implementations use numpy/pandas vectorised operations for performance.

Usage:
    from src.data.indicators import ema, rsi, macd, bollinger_bands, atr, adx

    df["ema_12"] = ema(df["close"], 12)
    df["rsi_14"] = rsi(df["close"], 14)
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Moving Averages
# ---------------------------------------------------------------------------

def ema(series: pd.Series, period: int) -> pd.Series:
    """
    Exponential Moving Average.

    Parameters
    ----------
    series : pd.Series
        Price series (typically close prices).
    period : int
        Look-back window.

    Returns
    -------
    pd.Series
        EMA values.  The first ``period - 1`` values will be NaN while the
        EMA warms up (pandas default behaviour for ``ewm``).
    """
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    """
    Simple Moving Average.

    Parameters
    ----------
    series : pd.Series
        Price series.
    period : int
        Look-back window.

    Returns
    -------
    pd.Series
        SMA values.
    """
    return series.rolling(window=period, min_periods=period).mean()


# ---------------------------------------------------------------------------
# Momentum / Oscillators
# ---------------------------------------------------------------------------

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Relative Strength Index (Wilder's smoothing).

    Parameters
    ----------
    series : pd.Series
        Price series (typically close prices).
    period : int
        Look-back window (default 14).

    Returns
    -------
    pd.Series
        RSI values in the range [0, 100].
    """
    delta = series.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    # Wilder's smoothing is equivalent to EWM with alpha = 1/period
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    result = 100.0 - (100.0 / (1.0 + rs))

    # Where avg_loss is zero, RSI = 100 (all gains)
    result = result.where(avg_loss != 0, 100.0)
    return result


def macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    Moving Average Convergence/Divergence.

    Parameters
    ----------
    series : pd.Series
        Price series.
    fast : int
        Fast EMA period (default 12).
    slow : int
        Slow EMA period (default 26).
    signal : int
        Signal EMA period (default 9).

    Returns
    -------
    pd.DataFrame
        Columns: ``macd``, ``signal``, ``histogram``.
    """
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line

    return pd.DataFrame(
        {"macd": macd_line, "signal": signal_line, "histogram": histogram},
        index=series.index,
    )


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------

def bollinger_bands(
    series: pd.Series,
    period: int = 20,
    std: float = 2.0,
) -> pd.DataFrame:
    """
    Bollinger Bands.

    Parameters
    ----------
    series : pd.Series
        Price series (typically close prices).
    period : int
        SMA look-back window (default 20).
    std : float
        Number of standard deviations (default 2.0).

    Returns
    -------
    pd.DataFrame
        Columns: ``upper``, ``middle``, ``lower``, ``bandwidth``, ``pct_b``.

        * ``bandwidth`` = (upper - lower) / middle
        * ``pct_b`` = (price - lower) / (upper - lower)
    """
    middle = sma(series, period)
    rolling_std = series.rolling(window=period, min_periods=period).std(ddof=0)
    upper = middle + std * rolling_std
    lower = middle - std * rolling_std

    bandwidth = (upper - lower) / middle
    band_width_raw = upper - lower
    pct_b = (series - lower) / band_width_raw

    return pd.DataFrame(
        {
            "upper": upper,
            "middle": middle,
            "lower": lower,
            "bandwidth": bandwidth,
            "pct_b": pct_b,
        },
        index=series.index,
    )


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average True Range.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns ``high``, ``low``, ``close``.
    period : int
        Look-back window (default 14).

    Returns
    -------
    pd.Series
        ATR values (Wilder's smoothing).
    """
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder's smoothing (alpha = 1 / period)
    return true_range.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()


# ---------------------------------------------------------------------------
# Trend
# ---------------------------------------------------------------------------

def adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Average Directional Index with +DI and -DI.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns ``high``, ``low``, ``close``.
    period : int
        Look-back window (default 14).

    Returns
    -------
    pd.DataFrame
        Columns: ``adx``, ``plus_di``, ``minus_di``.
    """
    high = df["high"]
    low = df["low"]

    # Directional movement
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)

    # Smoothed ATR, +DM, -DM using Wilder's smoothing
    atr_vals = atr(df, period)
    smooth_plus_dm = plus_dm.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    smooth_minus_dm = minus_dm.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    # Directional indicators
    plus_di = 100.0 * smooth_plus_dm / atr_vals
    minus_di = 100.0 * smooth_minus_dm / atr_vals

    # Handle division by zero (ATR = 0 means no movement)
    plus_di = plus_di.where(atr_vals != 0, 0.0)
    minus_di = minus_di.where(atr_vals != 0, 0.0)

    # Directional index
    di_sum = plus_di + minus_di
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum).where(di_sum != 0, 0.0)

    # ADX is the smoothed DX
    adx_values = dx.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    return pd.DataFrame(
        {"adx": adx_values, "plus_di": plus_di, "minus_di": minus_di},
        index=df.index,
    )


# ---------------------------------------------------------------------------
# Regime / Statistical
# ---------------------------------------------------------------------------

def hurst_exponent(series: pd.Series, max_lag: int = 100) -> float:
    """
    Estimate the Hurst exponent via the rescaled-range (R/S) method.

    * H < 0.5 : mean-reverting
    * H ~ 0.5 : random walk
    * H > 0.5 : trending

    Parameters
    ----------
    series : pd.Series
        Price series (typically log returns or close prices).
    max_lag : int
        Maximum lag for the R/S calculation (default 100).

    Returns
    -------
    float
        Estimated Hurst exponent.  Returns ``np.nan`` if the series is too
        short or computation fails.
    """
    ts = series.dropna().values.astype(np.float64)
    n = len(ts)

    if n < 20:
        return np.nan

    max_lag = min(max_lag, n // 2)
    lags = range(2, max_lag + 1)

    rs_values: list[float] = []
    lag_values: list[int] = []

    for lag in lags:
        # Number of non-overlapping sub-series of length ``lag``
        n_sub = n // lag
        if n_sub < 1:
            continue

        rs_for_lag: list[float] = []
        for i in range(n_sub):
            sub = ts[i * lag : (i + 1) * lag]
            mean_sub = np.mean(sub)
            deviate = np.cumsum(sub - mean_sub)
            r = np.max(deviate) - np.min(deviate)
            s = np.std(sub, ddof=1)
            if s > 0:
                rs_for_lag.append(r / s)

        if rs_for_lag:
            rs_values.append(np.mean(rs_for_lag))
            lag_values.append(lag)

    if len(rs_values) < 2:
        return np.nan

    log_lags = np.log(lag_values)
    log_rs = np.log(rs_values)

    # OLS: log(R/S) = H * log(lag) + c
    coeffs = np.polyfit(log_lags, log_rs, 1)
    return float(coeffs[0])


# ---------------------------------------------------------------------------
# Spread / Forex-specific
# ---------------------------------------------------------------------------

def realized_volatility(close: pd.Series, period: int = 20) -> pd.Series:
    """
    Realized volatility — rolling standard deviation of log-returns.

    Used by institutional trend-followers (AQR, Man AHL) for volatility
    targeting: position size is scaled inversely with realized vol so that
    each trade carries roughly equal risk.

    Parameters
    ----------
    close : pd.Series
        Close prices.
    period : int
        Look-back window (default 20).

    Returns
    -------
    pd.Series
        Rolling realized volatility.
    """
    log_returns = np.log(close / close.shift(1))
    return log_returns.rolling(window=period, min_periods=period).std()


def kama(series: pd.Series, period: int = 10, fast: int = 2, slow: int = 30) -> pd.Series:
    """
    Kaufman Adaptive Moving Average (KAMA).

    Adjusts its speed based on the Efficiency Ratio (ER):
    - In strong trends: ER → 1, KAMA tracks closely (fast EMA speed)
    - In choppy markets: ER → 0, KAMA barely moves (slow EMA speed)

    Parameters
    ----------
    series : pd.Series
        Price series (typically close prices).
    period : int
        ER look-back period (default 10).
    fast : int
        Fast EMA period for the smoothing constant (default 2).
    slow : int
        Slow EMA period for the smoothing constant (default 30).

    Returns
    -------
    pd.Series
        KAMA values.
    """
    values = series.values.astype(np.float64)
    n = len(values)
    kama_out = np.full(n, np.nan)

    fast_sc = 2.0 / (fast + 1.0)
    slow_sc = 2.0 / (slow + 1.0)

    # Seed KAMA with the first valid value after warm-up
    if n <= period:
        return pd.Series(kama_out, index=series.index)

    kama_out[period - 1] = values[period - 1]

    for i in range(period, n):
        # Direction: net price change over period
        direction = abs(values[i] - values[i - period])

        # Volatility: sum of absolute bar-to-bar changes over period
        volatility = 0.0
        for j in range(1, period + 1):
            volatility += abs(values[i - period + j] - values[i - period + j - 1])

        # Efficiency Ratio
        er = direction / volatility if volatility != 0 else 0.0

        # Smoothing Constant (squared)
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2

        # KAMA update
        kama_out[i] = kama_out[i - 1] + sc * (values[i] - kama_out[i - 1])

    return pd.Series(kama_out, index=series.index)


# ---------------------------------------------------------------------------
# Spread / Forex-specific
# ---------------------------------------------------------------------------

def spread_in_pips(bid: float, ask: float, instrument: str) -> float:
    """
    Calculate the spread in pips for a forex pair.

    JPY pairs (instrument containing ``JPY``) use a pip multiplier of 100,
    all other pairs use 10 000.

    Parameters
    ----------
    bid : float
        Current bid price.
    ask : float
        Current ask price.
    instrument : str
        Instrument identifier, e.g. ``"EUR_USD"`` or ``"USD_JPY"``.

    Returns
    -------
    float
        Spread measured in pips.
    """
    raw_spread = ask - bid
    multiplier = 100.0 if "JPY" in instrument.upper() else 10_000.0
    return raw_spread * multiplier
