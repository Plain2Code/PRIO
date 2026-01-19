"""Data pipeline for the Prio Forex trading bot.

STANDALONE: Knows about broker (for fetching) and data/features (for computing).
Does NOT know about strategy, risk, ML, or execution.

Fetches OHLCV candles from the broker and delegates feature computation
to data/features.py.

Usage:
    from src.data.pipeline import DataPipeline

    pipeline = DataPipeline(broker, config)
    features = await pipeline.get_features("EUR_USD", ["H1", "H4"])
"""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
import structlog

from src.broker.base import BaseBroker
from src.data.features import compute_features

logger = structlog.get_logger(__name__)


class DataPipeline:
    """
    Fetches candles from the broker, enriches them with technical indicator
    features, and provides multi-timeframe capabilities.

    Parameters
    ----------
    broker : BaseBroker
        Broker instance that implements ``get_candles(instrument,
        granularity, count)`` returning a list of candle dicts.
    config : dict
        Full configuration dictionary (the ``indicators`` sub-key is read
        for indicator parameters).
    """

    def __init__(self, broker: BaseBroker, config: dict[str, Any]) -> None:
        self._broker = broker
        self._config = config
        self._indicator_cfg: dict[str, Any] = config.get("indicators", {})

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    async def fetch_candles(
        self,
        instrument: str,
        granularity: str,
        count: int = 500,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV candles from the broker and return as a DataFrame.

        Parameters
        ----------
        instrument : str
            Instrument identifier (e.g. ``"EUR_USD"``).
        granularity : str
            Candle granularity (e.g. ``"M5"``, ``"H1"``, ``"D1"``).
        count : int
            Number of candles to fetch (default 500).

        Returns
        -------
        pd.DataFrame
            Columns: ``open``, ``high``, ``low``, ``close``, ``volume``,
            ``time``.  Index is a ``DatetimeIndex`` (UTC).
        """
        logger.info(
            "fetching_candles",
            instrument=instrument,
            granularity=granularity,
            count=count,
        )

        raw = await self._broker.get_candles(
            instrument=instrument,
            granularity=granularity,
            count=count,
        )

        # get_candles() returns a DataFrame (Capital.com adapter).
        # Normalise column names so the rest of the pipeline sees
        # open / high / low / close / volume with a DatetimeIndex.
        if isinstance(raw, pd.DataFrame):
            df = self._normalise_broker_df(raw)
        else:
            df = self._candles_to_dataframe(raw)

        logger.info(
            "candles_fetched",
            instrument=instrument,
            granularity=granularity,
            rows=len(df),
        )
        return df

    # ------------------------------------------------------------------
    # Feature computation
    # ------------------------------------------------------------------

    def compute_features(self, df: pd.DataFrame, instrument: str) -> pd.DataFrame:
        """Delegate feature computation to the unified data/features.py."""
        return compute_features(df, instrument=instrument, config=self._indicator_cfg)

    # ------------------------------------------------------------------
    # Multi-timeframe
    # ------------------------------------------------------------------

    async def get_multi_timeframe_features(
        self,
        instrument: str,
        timeframes: list[str],
        count: int = 500,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch candles and compute features for multiple timeframes in
        parallel.

        Parameters
        ----------
        instrument : str
            Instrument identifier.
        timeframes : list[str]
            Granularities to fetch, e.g. ``["M15", "H1", "H4", "D1"]``.
        count : int
            Number of candles per timeframe (default 500).

        Returns
        -------
        dict[str, pd.DataFrame]
            Mapping from timeframe string to the feature-enriched
            DataFrame.
        """
        logger.info(
            "multi_timeframe_fetch",
            instrument=instrument,
            timeframes=timeframes,
            count=count,
        )

        async def _fetch_and_compute(tf: str, delay: float) -> tuple[str, pd.DataFrame]:
            if delay > 0:
                await asyncio.sleep(delay)
            df = await self.fetch_candles(instrument, tf, count)
            df = self.compute_features(df, instrument)
            df.attrs["instrument"] = instrument

            # Warn if the latest row has too many NaN features (insufficient warmup)
            if not df.empty:
                last_row = df.iloc[-1]
                nan_count = int(last_row.isna().sum())
                total = len(last_row)
                if nan_count > total * 0.3:
                    logger.warning(
                        "feature_warmup_incomplete",
                        instrument=instrument,
                        timeframe=tf,
                        nan_features=nan_count,
                        total_features=total,
                        rows=len(df),
                    )

            return tf, df

        # Stagger requests to avoid Capital.com 429 rate limits
        results = await asyncio.gather(
            *[_fetch_and_compute(tf, i * 0.3) for i, tf in enumerate(timeframes)],
        )

        return dict(results)

    async def get_features(
        self,
        instrument: str,
        timeframes: list[str],
        count: int = 500,
    ) -> dict[str, pd.DataFrame]:
        """Convenience alias for get_multi_timeframe_features.

        This is the primary entry point used by the Orchestrator.
        """
        return await self.get_multi_timeframe_features(instrument, timeframes, count)

    # ------------------------------------------------------------------
    # Correlation data
    # ------------------------------------------------------------------

    async def get_close_prices_for_correlation(
        self,
        instruments: list[str],
        granularity: str = "D1",
        count: int = 120,
    ) -> dict[str, pd.Series]:
        """Fetch daily close prices for a list of instruments.

        Used by :class:`RiskManager` to build a dynamic rolling
        correlation matrix.

        Parameters
        ----------
        instruments : list[str]
            Instrument identifiers.
        granularity : str
            Candle granularity (default ``"D1"``).
        count : int
            Number of candles to fetch per instrument (default 120).

        Returns
        -------
        dict[str, pd.Series]
            Mapping of instrument -> close price Series indexed by date.
        """
        result: dict[str, pd.Series] = {}

        async def _fetch_one(inst: str, delay: float) -> None:
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                df = await self.fetch_candles(inst, granularity, count)
                if not df.empty and "close" in df.columns:
                    result[inst] = df["close"]
            except Exception:
                logger.warning("correlation_fetch_failed", instrument=inst)

        await asyncio.gather(
            *[_fetch_one(inst, i * 0.3) for i, inst in enumerate(instruments)]
        )

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_broker_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalise a DataFrame returned by the broker into the standard
        pipeline format (``open``, ``high``, ``low``, ``close``, ``volume``
        columns with a UTC DatetimeIndex).
        """
        if df.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        out = pd.DataFrame()

        # Map mid_* columns → open/high/low/close
        col_map = {
            "mid_open": "open", "mid_high": "high",
            "mid_low": "low", "mid_close": "close",
        }
        for src, dst in col_map.items():
            if src in df.columns:
                out[dst] = df[src].astype(float)
            elif dst in df.columns:
                out[dst] = df[dst].astype(float)

        if "volume" in df.columns:
            out["volume"] = df["volume"].astype(int)
        else:
            out["volume"] = 0

        # Carry bid/ask close for spread calculation
        if "bid_close" in df.columns:
            out["bid"] = df["bid_close"].astype(float)
        if "ask_close" in df.columns:
            out["ask"] = df["ask_close"].astype(float)

        # Time index
        if "time" in df.columns:
            out.index = pd.to_datetime(df["time"], utc=True)
            out.index.name = "time"
        elif df.index.name == "time":
            out.index = df.index

        out.sort_index(inplace=True)
        return out

    @staticmethod
    def _candles_to_dataframe(candles: list[dict[str, Any]]) -> pd.DataFrame:
        """
        Convert a list of candle dicts (as returned by the broker) to a
        pandas DataFrame with a UTC DatetimeIndex.

        Expected dict keys:
        ``time``, ``mid`` or (``open``, ``high``, ``low``, ``close``),
        ``volume``.

        The method handles both flat dicts and nested ``mid``
        sub-dict format.
        """
        if not candles:
            return pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"],
            )

        rows: list[dict[str, Any]] = []
        for c in candles:
            # Some brokers nest prices under ``mid`` / ``bid`` / ``ask``.
            mid = c.get("mid", {})
            row: dict[str, Any] = {
                "time": c.get("time"),
                "open": float(mid.get("o", c.get("open", 0))),
                "high": float(mid.get("h", c.get("high", 0))),
                "low": float(mid.get("l", c.get("low", 0))),
                "close": float(mid.get("c", c.get("close", 0))),
                "volume": int(c.get("volume", 0)),
            }

            # Optionally carry bid/ask if the broker provides them.
            if "bid" in c:
                row["bid"] = float(
                    c["bid"] if isinstance(c["bid"], (int, float))
                    else c["bid"].get("c", c["bid"].get("close", 0))
                )
            if "ask" in c:
                row["ask"] = float(
                    c["ask"] if isinstance(c["ask"], (int, float))
                    else c["ask"].get("c", c["ask"].get("close", 0))
                )

            rows.append(row)

        df = pd.DataFrame(rows)

        # Parse and set the time index.
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        return df
