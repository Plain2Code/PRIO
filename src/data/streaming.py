"""
Price streaming module for the Prio Forex trading bot.

Connects to the broker's streaming endpoint, emits tick events,
aggregates ticks into candles at multiple granularities, and fires
candle-close callbacks when a candle completes.

Usage:
    from src.data.streaming import PriceStreamer
    from src.broker.capitalcom import CapitalComBroker

    broker = CapitalComBroker(api_key, identifier, password)
    streamer = PriceStreamer(broker, ["EUR_USD", "GBP_USD"])

    streamer.on_tick(my_tick_handler)
    streamer.on_candle_close(my_candle_handler)
    await streamer.start()
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from src.broker.base import BaseBroker
from src.utils.logging import get_logger

logger = get_logger("data.streaming")

# Granularity durations in seconds, used for candle aggregation.
_GRANULARITY_SECONDS: dict[str, int] = {
    "M1": 60,
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}

# The granularities for which we aggregate ticks into candles.
_DEFAULT_CANDLE_GRANULARITIES: list[str] = ["M5", "M15", "H1", "H4", "D1"]

# Callback type aliases for clarity.
TickCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None] | None]
CandleCallback = Callable[
    [str, str, dict[str, Any]],  # instrument, granularity, candle_dict
    Coroutine[Any, Any, None] | None,
]


class _CandleBuilder:
    """
    Aggregates ticks into a single candle for one (instrument, granularity)
    pair.

    A candle is considered *complete* once the first tick whose timestamp
    falls into the **next** candle period arrives.  At that point the
    finished candle dict is returned and the builder resets for the new
    period.
    """

    __slots__ = (
        "_instrument",
        "_granularity",
        "_period_seconds",
        "_open",
        "_high",
        "_low",
        "_close",
        "_volume",
        "_period_start",
    )

    def __init__(self, instrument: str, granularity: str) -> None:
        self._instrument = instrument
        self._granularity = granularity
        self._period_seconds = _GRANULARITY_SECONDS[granularity]
        self._reset()

    # ------------------------------------------------------------------ #
    # Public
    # ------------------------------------------------------------------ #

    def on_tick(self, tick: dict[str, Any]) -> dict[str, Any] | None:
        """
        Feed a tick into the builder.

        Parameters
        ----------
        tick : dict
            Must contain ``time`` (ISO str or epoch), ``bid``, ``ask``.

        Returns
        -------
        dict | None
            A completed candle dict if this tick crosses the period
            boundary, otherwise ``None``.
        """
        tick_time = self._parse_time(tick["time"])
        mid_price = (float(tick["bid"]) + float(tick["ask"])) / 2.0

        period_start = self._floor_to_period(tick_time)

        # First ever tick -- just initialise.
        if self._period_start is None:
            self._start_candle(period_start, mid_price)
            return None

        # Same period -- update running candle.
        if period_start == self._period_start:
            self._update(mid_price)
            return None

        # New period -- finalise the previous candle, then start fresh.
        completed = self._to_dict()
        self._start_candle(period_start, mid_price)
        return completed

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _start_candle(self, period_start: datetime, price: float) -> None:
        self._period_start = period_start
        self._open = price
        self._high = price
        self._low = price
        self._close = price
        self._volume = 1

    def _update(self, price: float) -> None:
        if price > self._high:
            self._high = price
        if price < self._low:
            self._low = price
        self._close = price
        self._volume += 1

    def _reset(self) -> None:
        self._period_start: datetime | None = None
        self._open: float = 0.0
        self._high: float = 0.0
        self._low: float = 0.0
        self._close: float = 0.0
        self._volume: int = 0

    def _to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self._instrument,
            "granularity": self._granularity,
            "time": self._period_start.isoformat() if self._period_start else "",
            "open": self._open,
            "high": self._high,
            "low": self._low,
            "close": self._close,
            "volume": self._volume,
        }

    def _floor_to_period(self, dt: datetime) -> datetime:
        """Round *down* to the start of the current candle period."""
        epoch = int(dt.timestamp())
        floored_epoch = (epoch // self._period_seconds) * self._period_seconds
        return datetime.fromtimestamp(floored_epoch, tz=timezone.utc)

    @staticmethod
    def _parse_time(raw: str | float | int | datetime) -> datetime:
        if isinstance(raw, datetime):
            if raw.tzinfo is None:
                return raw.replace(tzinfo=timezone.utc)
            return raw
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        # ISO string -- strip trailing "Z" for fromisoformat compatibility
        s = str(raw).rstrip("Z")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


class PriceStreamer:
    """
    Streams prices from the broker, dispatches tick and candle-close
    events, and automatically reconnects on disconnect.

    Parameters
    ----------
    broker : BaseBroker
        Broker instance that implements an async ``stream_prices``
        method yielding tick dicts.
    instruments : list[str]
        Instruments to stream (e.g. ``["EUR_USD", "GBP_USD"]``).
    candle_granularities : list[str] | None
        Granularities to aggregate into candles.  Defaults to
        ``["M5", "M15", "H1", "H4", "D1"]``.
    reconnect_delay : float
        Initial delay (seconds) before reconnecting after a disconnect
        (default 5.0).  Uses exponential back-off capped at 60 s.
    max_reconnect_delay : float
        Maximum reconnect delay in seconds (default 60.0).
    """

    def __init__(
        self,
        broker: BaseBroker,
        instruments: list[str],
        candle_granularities: list[str] | None = None,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
    ) -> None:
        self._broker = broker
        self._instruments = instruments
        self._granularities = candle_granularities or list(_DEFAULT_CANDLE_GRANULARITIES)
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._tick_callbacks: list[TickCallback] = []
        self._candle_callbacks: list[CandleCallback] = []

        # One candle builder per (instrument, granularity).
        self._builders: dict[tuple[str, str], _CandleBuilder] = {}
        for inst in instruments:
            for gran in self._granularities:
                if gran not in _GRANULARITY_SECONDS:
                    logger.warning("unsupported_granularity", granularity=gran)
                    continue
                self._builders[(inst, gran)] = _CandleBuilder(inst, gran)

        self._running = False
        self._stream_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Callback registration
    # ------------------------------------------------------------------

    def on_tick(self, callback: TickCallback) -> None:
        """
        Register a callback invoked on every tick.

        Parameters
        ----------
        callback : Callable[[dict], None | Coroutine]
            Receives the raw tick dict from the broker.  May be sync or
            async.
        """
        self._tick_callbacks.append(callback)

    def on_candle_close(self, callback: CandleCallback) -> None:
        """
        Register a callback invoked when a candle closes.

        Parameters
        ----------
        callback : Callable[[str, str, dict], None | Coroutine]
            Receives ``(instrument, granularity, candle_dict)``.  May be
            sync or async.
        """
        self._candle_callbacks.append(callback)

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start streaming prices.  This coroutine runs until ``stop`` is
        called or the event loop is shut down.

        Reconnection is handled automatically with exponential back-off.
        """
        if self._running:
            logger.warning("streamer_already_running")
            return

        self._running = True
        logger.info(
            "streamer_starting",
            instruments=self._instruments,
            granularities=self._granularities,
        )
        self._stream_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the price streamer gracefully."""
        if not self._running:
            return

        self._running = False
        logger.info("streamer_stopping")

        if self._stream_task is not None:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        logger.info("streamer_stopped")

    # ------------------------------------------------------------------
    # Core streaming loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        """
        Main loop: connect, consume ticks, dispatch events, reconnect
        on failure.
        """
        delay = self._reconnect_delay

        while self._running:
            try:
                logger.info("stream_connecting", instruments=self._instruments)
                async for tick in self._broker.stream_prices(self._instruments):
                    if not self._running:
                        break

                    # Reset back-off on successful data receipt.
                    delay = self._reconnect_delay

                    await self._handle_tick(tick)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "stream_disconnected",
                    error=str(exc),
                    reconnect_in=delay,
                )

                if not self._running:
                    break

                await asyncio.sleep(delay)
                # Exponential back-off
                delay = min(delay * 2, self._max_reconnect_delay)

    # ------------------------------------------------------------------
    # Event dispatching
    # ------------------------------------------------------------------

    async def _handle_tick(self, tick: dict[str, Any]) -> None:
        """Process a single tick: dispatch to callbacks and candle builders."""
        instrument = tick.get("instrument", "")

        # --- Tick callbacks ------------------------------------------------
        for cb in self._tick_callbacks:
            try:
                result = cb(tick)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("tick_callback_error", instrument=instrument)

        # --- Candle aggregation --------------------------------------------
        for gran in self._granularities:
            key = (instrument, gran)
            builder = self._builders.get(key)
            if builder is None:
                continue

            try:
                completed = builder.on_tick(tick)
            except Exception:
                logger.exception(
                    "candle_builder_error",
                    instrument=instrument,
                    granularity=gran,
                )
                continue

            if completed is not None:
                logger.debug(
                    "candle_closed",
                    instrument=instrument,
                    granularity=gran,
                    candle_time=completed.get("time"),
                )
                await self._dispatch_candle(instrument, gran, completed)

    async def _dispatch_candle(
        self,
        instrument: str,
        granularity: str,
        candle: dict[str, Any],
    ) -> None:
        """Invoke all registered candle-close callbacks."""
        for cb in self._candle_callbacks:
            try:
                result = cb(instrument, granularity, candle)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception(
                    "candle_callback_error",
                    instrument=instrument,
                    granularity=granularity,
                )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        """Return ``True`` if the streamer is actively running."""
        return self._running

    @property
    def instruments(self) -> list[str]:
        """Return the list of instruments being streamed."""
        return list(self._instruments)
