"""
Abstract base class for broker adapters and shared data types.

Defines the interface that all broker implementations must satisfy,
along with standardised dataclasses used across the trading system.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncGenerator

import pandas as pd


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class AccountInfo:
    """Snapshot of the trading account."""

    balance: float
    equity: float
    margin_used: float
    margin_available: float
    unrealized_pnl: float
    currency: str = "USD"
    open_trade_count: int = 0
    open_position_count: int = 0


@dataclass(frozen=True, slots=True)
class Position:
    """An open position held by the account."""

    instrument: str
    units: int  # positive = long, negative = short
    average_price: float
    unrealized_pnl: float
    side: str  # "long" | "short"
    margin_used: float = 0.0
    financing: float = 0.0
    deal_id: str = ""
    stop_loss: float | None = None
    take_profit: float | None = None


@dataclass(frozen=True, slots=True)
class Order:
    """A pending (non-filled) order."""

    order_id: str
    instrument: str
    order_type: str  # "MARKET" | "LIMIT" | "STOP" | "MARKET_IF_TOUCHED" | "TRAILING_STOP_LOSS"
    units: int
    price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    trailing_stop_distance: float | None = None
    time_in_force: str = "GTC"
    state: str = "PENDING"
    create_time: str = ""


@dataclass(frozen=True, slots=True)
class Candle:
    """A single candlestick bar."""

    time: datetime
    volume: int
    complete: bool
    # Mid prices
    mid_open: float = 0.0
    mid_high: float = 0.0
    mid_low: float = 0.0
    mid_close: float = 0.0
    # Bid prices
    bid_open: float = 0.0
    bid_high: float = 0.0
    bid_low: float = 0.0
    bid_close: float = 0.0
    # Ask prices
    ask_open: float = 0.0
    ask_high: float = 0.0
    ask_low: float = 0.0
    ask_close: float = 0.0


@dataclass(frozen=True, slots=True)
class PriceTick:
    """A single streaming price tick."""

    instrument: str
    time: str
    bid: float
    ask: float
    spread: float = 0.0
    tradeable: bool = True
    status: str = "tradeable"


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class BrokerError(Exception):
    """Generic broker-level error."""


class OrderError(BrokerError):
    """Error related to order creation, modification, or cancellation."""


class BrokerConnectionError(BrokerError):
    """Error establishing or maintaining a connection to the broker."""


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class BaseBroker(abc.ABC):
    """
    Abstract interface that every broker adapter must implement.

    All methods are **async** so that the execution engine can use them
    inside an ``asyncio`` event loop without blocking.
    """

    # -- Account & position queries -----------------------------------------

    @abc.abstractmethod
    async def get_account(self) -> dict:
        """
        Return account information.

        Expected keys: balance, equity, margin_used, margin_available,
        unrealized_pnl, currency, open_trade_count, open_position_count.
        """
        ...

    @abc.abstractmethod
    async def get_positions(self) -> list[dict]:
        """
        Return a list of all currently open positions.

        Each dict should contain at minimum: instrument, units,
        average_price, unrealized_pnl, side.
        """
        ...

    @abc.abstractmethod
    async def get_open_orders(self) -> list[dict]:
        """
        Return a list of all pending (non-filled) orders.

        Each dict should contain at minimum: order_id, instrument,
        order_type, units, price, stop_loss, take_profit, state.
        """
        ...

    # -- Order management ---------------------------------------------------

    @abc.abstractmethod
    async def create_market_order(
        self,
        instrument: str,
        units: int,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """
        Place a market order.

        Parameters
        ----------
        instrument : str
            The currency pair, e.g. ``"EUR_USD"``.
        units : int
            Positive for buy, negative for sell.
        stop_loss : float | None
            Optional stop-loss price.
        take_profit : float | None
            Optional take-profit price.

        Returns
        -------
        dict
            Order fill / confirmation details from the broker.
        """
        ...

    @abc.abstractmethod
    async def create_limit_order(
        self,
        instrument: str,
        units: int,
        price: float,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """
        Place a limit order.

        Parameters
        ----------
        instrument : str
            The currency pair.
        units : int
            Positive for buy, negative for sell.
        price : float
            Limit price at which the order should fill.
        stop_loss : float | None
            Optional stop-loss price.
        take_profit : float | None
            Optional take-profit price.

        Returns
        -------
        dict
            Order confirmation details from the broker.
        """
        ...

    @abc.abstractmethod
    async def create_stop_order(
        self,
        instrument: str,
        units: int,
        price: float,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """
        Place a stop order.

        Parameters
        ----------
        instrument : str
            The currency pair.
        units : int
            Positive for buy, negative for sell.
        price : float
            Stop price at which the order becomes a market order.
        stop_loss : float | None
            Optional stop-loss price.
        take_profit : float | None
            Optional take-profit price.

        Returns
        -------
        dict
            Order confirmation details from the broker.
        """
        ...

    @abc.abstractmethod
    async def close_position(self, instrument: str) -> dict:
        """
        Close the entire position for *instrument*.

        Returns
        -------
        dict
            Details of the closed position / fill.
        """
        ...

    @abc.abstractmethod
    async def close_all_positions(self) -> list[dict]:
        """
        Close **all** open positions.

        Returns
        -------
        list[dict]
            A list of closure confirmations, one per position.
        """
        ...

    @abc.abstractmethod
    async def modify_order(
        self,
        order_id: str,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        trailing_stop_distance: float | None = None,
    ) -> dict:
        """
        Modify an existing pending order.

        Parameters
        ----------
        order_id : str
            Broker-assigned order identifier.
        stop_loss : float | None
            New stop-loss price (``None`` = leave unchanged).
        take_profit : float | None
            New take-profit price (``None`` = leave unchanged).
        trailing_stop_distance : float | None
            New trailing-stop distance (``None`` = leave unchanged).

        Returns
        -------
        dict
            Updated order details.
        """
        ...

    # -- Market data --------------------------------------------------------

    @abc.abstractmethod
    async def get_candles(
        self,
        instrument: str,
        granularity: str,
        count: int,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch historical candlestick data.

        Parameters
        ----------
        instrument : str
            The currency pair, e.g. ``"EUR_USD"``.
        granularity : str
            Candle period, e.g. ``"M1"``, ``"H1"``, ``"D"``.
        count : int
            Number of candles to retrieve.
        from_time : str | None
            RFC-3339 start time (inclusive).
        to_time : str | None
            RFC-3339 end time (inclusive).

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: time, volume, mid_open, mid_high,
            mid_low, mid_close, bid_open, bid_high, bid_low, bid_close,
            ask_open, ask_high, ask_low, ask_close.
        """
        ...

    @abc.abstractmethod
    async def stream_prices(
        self,
        instruments: list[str],
    ) -> AsyncGenerator[PriceTick, None]:
        """
        Open a streaming connection and yield ``PriceTick`` objects.

        The generator runs indefinitely until the caller breaks out or
        the connection is lost.

        Parameters
        ----------
        instruments : list[str]
            Currency pairs to subscribe to.

        Yields
        ------
        PriceTick
            Real-time bid/ask price update.
        """
        ...
        # Need `yield` to make this an async generator in the type system.
        yield  # type: ignore[misc]

    @abc.abstractmethod
    async def get_spread(self, instrument: str) -> float:
        """
        Return the current bid-ask spread for *instrument* in price units.
        """
        ...
