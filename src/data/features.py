"""Unified feature computation for the Prio trading system.

STANDALONE: This module knows only about data/indicators.py.
It receives a raw OHLCV DataFrame and returns it enriched with all
technical indicator columns needed by strategy, ML, and risk modules.

Usage:
    from src.data.features import compute_features

    df = compute_features(df, instrument="EUR_USD", config={})
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.data.indicators import (
    adx,
    atr,
    bollinger_bands,
    ema,
    hurst_exponent,
    kama,
    macd,
    realized_volatility,
    rsi,
    spread_in_pips,
)


def compute_features(
    df: pd.DataFrame,
    instrument: str = "",
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Compute all technical indicator features on an OHLCV DataFrame.

    This is the SINGLE source of truth for feature computation.  Both the
    DataPipeline (live trading) and ModelTrainer (regime training) call
    this function — no duplication.

    Parameters
    ----------
    df : pd.DataFrame
        Must have at least ``close``, ``high``, ``low`` columns.
        Accepts broker-format columns (``mid_close``, etc.) and maps
        them automatically.
    instrument : str
        Instrument identifier (e.g. ``"EUR_USD"``).  Needed for
        spread pip calculation.
    config : dict, optional
        Indicator configuration overrides.  Recognised keys:
        ``ema_periods``, ``rsi_period``, ``macd_fast``, ``macd_slow``,
        ``macd_signal``, ``bollinger_period``, ``bollinger_std``,
        ``atr_period``, ``adx_period``.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with additional feature columns appended.
    """
    cfg = config or {}
    df = df.copy()

    # -- Column mapping (broker format → standard OHLCV) ------------------
    col_map = {
        "mid_close": "close",
        "mid_open": "open",
        "mid_high": "high",
        "mid_low": "low",
    }
    for src_col, dst_col in col_map.items():
        if src_col in df.columns and dst_col not in df.columns:
            df[dst_col] = df[src_col]

    for required in ("close", "high", "low"):
        if required not in df.columns:
            raise ValueError(f"DataFrame missing required column: '{required}'")

    close = df["close"]

    # -- Exponential Moving Averages --------------------------------------
    ema_periods = cfg.get("ema_periods", [5, 9, 12, 13, 21, 26, 50, 100, 200])
    for period in ema_periods:
        df[f"ema_{period}"] = ema(close, period)

    # -- RSI ---------------------------------------------------------------
    rsi_period = int(cfg.get("rsi_period", 14))
    df[f"rsi_{rsi_period}"] = rsi(close, rsi_period)
    df["rsi"] = df[f"rsi_{rsi_period}"]

    # -- MACD --------------------------------------------------------------
    macd_fast = int(cfg.get("macd_fast", 12))
    macd_slow = int(cfg.get("macd_slow", 26))
    macd_signal_period = int(cfg.get("macd_signal", 9))
    macd_df = macd(close, fast=macd_fast, slow=macd_slow, signal=macd_signal_period)
    df["macd"] = macd_df["macd"]
    df["macd_signal"] = macd_df["signal"]
    df["macd_histogram"] = macd_df["histogram"]

    # -- Bollinger Bands ---------------------------------------------------
    bb_period = int(cfg.get("bollinger_period", 20))
    bb_std = float(cfg.get("bollinger_std", 2.0))
    bb = bollinger_bands(close, period=bb_period, std=bb_std)
    df["bb_upper"] = bb["upper"]
    df["bb_lower"] = bb["lower"]
    df["bb_middle"] = bb["middle"]
    df["bb_bandwidth"] = bb["bandwidth"]
    df["bb_pct_b"] = bb["pct_b"]

    # -- ATR ---------------------------------------------------------------
    atr_period = int(cfg.get("atr_period", 14))
    df[f"atr_{atr_period}"] = atr(df, period=atr_period)

    # -- ADX ---------------------------------------------------------------
    adx_period = int(cfg.get("adx_period", 14))
    adx_df = adx(df, period=adx_period)
    df[f"adx_{adx_period}"] = adx_df["adx"]
    df["adx"] = adx_df["adx"]
    df["plus_di"] = adx_df["plus_di"]
    df["minus_di"] = adx_df["minus_di"]

    # -- Hurst Exponent (rolling with stride for performance) --------------
    # R/S method requires returns, not price levels — raw prices inflate H > 1.0
    log_returns = np.log(close / close.shift(1))
    hurst_window = 100
    hurst_values = pd.Series(np.nan, index=df.index)
    if len(df) >= hurst_window:
        stride = 5
        for i in range(hurst_window, len(df), stride):
            segment = log_returns.iloc[i - hurst_window : i]
            hurst_values.iloc[i] = hurst_exponent(segment)
        hurst_values = hurst_values.ffill()
    df["hurst_exponent"] = hurst_values

    # -- Derived features --------------------------------------------------

    # EMA slopes (rate of change)
    if "ema_12" in df.columns:
        df["ema_slope_fast"] = df["ema_12"].pct_change()
    if "ema_50" in df.columns:
        df["ema_slope_slow"] = df["ema_50"].pct_change()

    # Volatility ratio: ATR normalised by close price
    atr_col = f"atr_{atr_period}"
    df["volatility_ratio"] = df[atr_col] / close
    df["atr_normalized"] = df["volatility_ratio"]

    # -- Realized Volatility -----------------------------------------------
    df["realized_vol_20"] = realized_volatility(close, period=20)

    # -- KAMA (Kaufman Adaptive Moving Average) ----------------------------
    df["kama_10"] = kama(close, period=10, fast=2, slow=30)

    # -- Spread (only if bid/ask columns are present) ----------------------
    if instrument and "bid" in df.columns and "ask" in df.columns:
        df["spread"] = df.apply(
            lambda row: spread_in_pips(row["bid"], row["ask"], instrument),
            axis=1,
        )

    return df
