"""
Capital.com REST API broker adapter.

Implements :class:`BaseBroker` using the Capital.com REST API
via ``httpx.AsyncClient``.

Authentication is session-based: a POST to /api/v1/session returns
CST and X-SECURITY-TOKEN headers that must be sent on every subsequent
request.  Sessions expire after 10 minutes of inactivity and are
automatically refreshed.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from typing import Any, AsyncGenerator

import httpx
import pandas as pd
import structlog

from src.broker.base import (
    AccountInfo,
    BaseBroker,
    BrokerConnectionError,
    BrokerError,
    Candle,
    Order,
    OrderError,
    Position,
    PriceTick,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# URL configuration per environment
# ---------------------------------------------------------------------------

_API_URLS: dict[str, str] = {
    "demo": "https://demo-api-capital.backend-capital.com",
    "live": "https://api-capital.backend-capital.com",
}

_API_PREFIX = "/api/v1"

# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

# Internal format (EUR_USD) -> Capital.com epic format (EURUSD)
def _to_epic(instrument: str) -> str:
    """Convert internal instrument format to Capital.com epic."""
    return instrument.replace("_", "")


def _from_epic(epic: str) -> str:
    """Convert Capital.com epic to internal instrument format."""
    # Forex pairs are 6 chars (EURUSD -> EUR_USD)
    if len(epic) == 6 and epic.isalpha():
        return f"{epic[:3]}_{epic[3:]}"
    return epic


# Granularity mapping: internal -> Capital.com resolution
_GRANULARITY_MAP: dict[str, str] = {
    "M1": "MINUTE",
    "M5": "MINUTE_5",
    "M15": "MINUTE_15",
    "M30": "MINUTE_30",
    "H1": "HOUR",
    "H4": "HOUR_4",
    "D1": "DAY",
    "W1": "WEEK",
}


def _parse_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# CapitalComBroker
# ---------------------------------------------------------------------------

class CapitalComBroker(BaseBroker):
    """
    Concrete broker adapter that talks to the Capital.com REST API.

    Parameters
    ----------
    api_key : str
        Capital.com API key.
    identifier : str
        Login email / identifier for Capital.com account.
    password : str
        Account password.
    environment : str
        ``"demo"`` or ``"live"``.
    timeout : float
        Default HTTP request timeout in seconds.
    """

    def __init__(
        self,
        api_key: str,
        identifier: str,
        password: str,
        environment: str = "demo",
        timeout: float = 15.0,
    ) -> None:
        if environment not in ("demo", "live"):
            raise ValueError(
                f"environment must be 'demo' or 'live', got '{environment}'"
            )

        self._api_key = api_key
        self._identifier = identifier
        self._password = password
        self._environment = environment

        self._api_base = _API_URLS[environment]

        # Session tokens (set after login)
        self._cst: str | None = None
        self._security_token: str | None = None
        self._session_time: float = 0.0  # monotonic time of last session create/refresh
        self._session_ttl: float = 540.0  # refresh after 9 minutes (session expires at 10)

        # Rate limiting: 1 request per 300ms (~3/s, conservative for Capital.com)
        self._last_request_time: float = 0.0
        self._min_request_interval: float = 0.3

        self._client = httpx.AsyncClient(
            base_url=self._api_base,
            timeout=timeout,
        )

    # -- Lifecycle ----------------------------------------------------------

    async def close(self) -> None:
        """Close the session and HTTP client gracefully."""
        if self._cst:
            try:
                await self._client.delete(
                    f"{_API_PREFIX}/session",
                    headers=self._auth_headers(),
                )
            except Exception:
                pass
            self._cst = None
            self._security_token = None
        await self._client.aclose()

    async def __aenter__(self) -> "CapitalComBroker":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # -- Session management -------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        """Return headers with session tokens."""
        headers: dict[str, str] = {}
        if self._cst:
            headers["CST"] = self._cst
        if self._security_token:
            headers["X-SECURITY-TOKEN"] = self._security_token
        return headers

    async def _ensure_session(self) -> None:
        """Create or refresh the session if expired."""
        now = time.monotonic()
        if self._cst and (now - self._session_time) < self._session_ttl:
            return  # Session still valid

        try:
            response = await self._client.post(
                f"{_API_PREFIX}/session",
                headers={"X-CAP-API-KEY": self._api_key},
                json={
                    "identifier": self._identifier,
                    "password": self._password,
                    "encryptedPassword": False,
                },
            )
        except httpx.HTTPError as exc:
            raise BrokerConnectionError(
                f"Session creation failed: {exc}"
            ) from exc

        if response.status_code >= 400:
            raise BrokerConnectionError(
                f"Session creation failed ({response.status_code}): {response.text}"
            )

        self._cst = response.headers.get("CST", "")
        self._security_token = response.headers.get("X-SECURITY-TOKEN", "")
        self._session_time = time.monotonic()

        if not self._cst:
            raise BrokerConnectionError(
                "Session creation succeeded but no CST token received"
            )

        logger.info("session_created", environment=self._environment)

    # -- Rate limiting ------------------------------------------------------

    async def _rate_limit(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.monotonic()

    # -- Internal request helpers -------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> dict:
        """
        Execute an authenticated HTTP request against the Capital.com API.

        Automatically handles session refresh and rate limiting.
        """
        await self._ensure_session()
        await self._rate_limit()

        full_path = f"{_API_PREFIX}{path}"

        try:
            response = await self._client.request(
                method,
                full_path,
                json=json_body,
                params=params,
                headers=self._auth_headers(),
            )
        except httpx.TimeoutException as exc:
            raise BrokerConnectionError(
                f"Request timed out: {method} {path}"
            ) from exc
        except httpx.HTTPError as exc:
            raise BrokerConnectionError(
                f"HTTP transport error: {method} {path}: {exc}"
            ) from exc

        if response.status_code >= 400:
            error_body = response.text
            msg = (
                f"Capital.com API error {response.status_code} "
                f"for {method} {path}: {error_body}"
            )
            if "/positions" in path or "/workingorders" in path:
                if method in ("POST", "PUT", "DELETE"):
                    raise OrderError(msg)
            raise BrokerError(msg)

        # Some endpoints return empty body (e.g. DELETE)
        if response.status_code == 200 and response.text:
            return response.json()
        return {}

    async def _confirm_deal(self, deal_reference: str) -> dict:
        """Get deal confirmation to verify execution."""
        return await self._request("GET", f"/confirms/{deal_reference}")

    @staticmethod
    def _normalize_order_response(raw: dict, requested_size: int) -> dict:
        """Map Capital.com confirmation keys to the standard broker interface.

        Capital.com returns ``dealId``, ``level``, ``size`` etc.
        The execution engine expects ``order_id``, ``fill_price``, ``units_filled``.
        """
        raw["order_id"] = raw.get("dealId") or raw.get("dealReference") or ""
        raw["fill_price"] = raw.get("level")
        raw["units_filled"] = raw.get("size", requested_size)
        return raw

    # -- Account & position queries -----------------------------------------

    async def get_account(self) -> dict:
        """Fetch account information from Capital.com."""
        data = await self._request("GET", "/accounts")
        accounts = data.get("accounts", [])

        if not accounts:
            raise BrokerError("No accounts found")

        # Use the first account (or the active one)
        acct = accounts[0]
        acc_info = acct.get("balance", {})

        info = AccountInfo(
            balance=_parse_float(acc_info.get("balance")),
            equity=_parse_float(acc_info.get("balance")) + _parse_float(acc_info.get("profitLoss")),
            margin_used=_parse_float(acc_info.get("deposit")) - _parse_float(acc_info.get("available")),
            margin_available=_parse_float(acc_info.get("available")),
            unrealized_pnl=_parse_float(acc_info.get("profitLoss")),
            currency=acct.get("currency", "USD"),
        )
        return dataclasses.asdict(info)

    async def get_positions(self) -> list[dict]:
        """Fetch all open positions."""
        data = await self._request("GET", "/positions")
        positions: list[dict] = []

        for pos in data.get("positions", []):
            position_data = pos.get("position", {})
            market_data = pos.get("market", {})

            direction = position_data.get("direction", "BUY")
            size = _parse_float(position_data.get("size", 0))
            units = int(size) if direction == "BUY" else -int(size)
            epic = market_data.get("epic", "")

            # Parse stop/take-profit levels from Capital.com
            stop_level = position_data.get("stopLevel")
            profit_level = position_data.get("profitLevel")

            position = Position(
                instrument=_from_epic(epic),
                units=units,
                average_price=_parse_float(position_data.get("level")),
                unrealized_pnl=_parse_float(position_data.get("upl") or position_data.get("profit") or 0),
                side="long" if direction == "BUY" else "short",
                margin_used=_parse_float(position_data.get("margin", 0)),
                deal_id=position_data.get("dealId", ""),
                stop_loss=_parse_float(stop_level) if stop_level is not None else None,
                take_profit=_parse_float(profit_level) if profit_level is not None else None,
            )
            positions.append(dataclasses.asdict(position))

        return positions

    async def get_open_orders(self) -> list[dict]:
        """Fetch all pending working orders."""
        data = await self._request("GET", "/workingorders")
        orders: list[dict] = []

        for raw in data.get("workingOrders", []):
            order_data = raw.get("workingOrderData", {})
            market_data = raw.get("marketData", {})
            epic = market_data.get("epic", "")

            direction = order_data.get("direction", "BUY")
            size = _parse_float(order_data.get("orderSize", 0))
            units = int(size) if direction == "BUY" else -int(size)

            order_type_raw = order_data.get("orderType", "LIMIT")
            order_type = order_type_raw.upper()

            order = Order(
                order_id=order_data.get("dealId", ""),
                instrument=_from_epic(epic),
                order_type=order_type,
                units=units,
                price=_parse_float(order_data.get("orderLevel"))
                if order_data.get("orderLevel")
                else None,
                stop_loss=_parse_float(order_data.get("stopDistance"))
                if order_data.get("stopDistance")
                else None,
                take_profit=_parse_float(order_data.get("profitDistance"))
                if order_data.get("profitDistance")
                else None,
                time_in_force=order_data.get("timeInForce", "GOOD_TILL_CANCELLED"),
                state="PENDING",
                create_time=order_data.get("createdDateUTC", ""),
            )
            orders.append(dataclasses.asdict(order))

        return orders

    # -- History queries ----------------------------------------------------

    async def get_activity(
        self,
        last_period: int = 3600,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        """Fetch account activity from Capital.com.

        Returns deal events including SL/TP triggers, manual closes, etc.

        Use either ``last_period`` (seconds, max 86400) **or** a date range
        via ``date_from`` / ``date_to`` (format ``YYYY-MM-DDTHH:MM:SS``).
        When ``date_from`` is provided, ``last_period`` is ignored and
        the API fetches day-by-day to cover the full range.

        Each item contains:
          - epic, dealId, source (SL|TP|USER|SYSTEM|CLOSE_OUT|DEALER),
            type, status, date/dateUTC
        """
        all_activities: list[dict] = []

        if date_from:
            # Date-range mode: walk day-by-day (API max 1 day per request)
            from datetime import datetime as _dt, timedelta as _td, timezone as _tz
            fmt = "%Y-%m-%dT%H:%M:%S"
            start = _dt.strptime(date_from, fmt)
            end = _dt.strptime(date_to, fmt) if date_to else _dt.now(_tz.utc).replace(tzinfo=None)
            cursor = start
            while cursor < end:
                chunk_end = min(cursor + _td(days=1), end)
                try:
                    data = await self._request(
                        "GET",
                        "/history/activity",
                        params={
                            "from": cursor.strftime(fmt),
                            "to": chunk_end.strftime(fmt),
                        },
                    )
                    all_activities.extend(data.get("activities", []))
                except Exception:
                    pass  # Skip failed chunks, continue
                cursor = chunk_end
        else:
            data = await self._request(
                "GET",
                "/history/activity",
                params={"lastPeriod": min(last_period, 86400)},
            )
            all_activities = data.get("activities", [])

        # Normalise epic → internal instrument format
        for act in all_activities:
            raw_epic = act.get("epic", "")
            if raw_epic:
                act["instrument"] = _from_epic(raw_epic)

        logger.info("get_activity", count=len(all_activities))
        return all_activities

    async def get_transactions(
        self,
        last_period: int = 3600,
        tx_type: str = "TRADE",
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        """Fetch financial transactions from Capital.com.

        ``tx_type`` can be ALL, TRADE, SWAP, DEPOSIT, WITHDRAWAL, etc.

        Use either ``last_period`` (seconds, max 86400) **or** a date range
        via ``date_from`` / ``date_to`` (format ``YYYY-MM-DDTHH:MM:SS``).
        """
        all_txns: list[dict] = []

        params_base: dict[str, Any] = {}
        if tx_type and tx_type != "ALL":
            params_base["type"] = tx_type

        if date_from:
            from datetime import datetime as _dt, timedelta as _td, timezone as _tz
            fmt = "%Y-%m-%dT%H:%M:%S"
            start = _dt.strptime(date_from, fmt)
            end = _dt.strptime(date_to, fmt) if date_to else _dt.now(_tz.utc).replace(tzinfo=None)
            cursor = start
            while cursor < end:
                chunk_end = min(cursor + _td(days=1), end)
                try:
                    data = await self._request(
                        "GET",
                        "/history/transactions",
                        params={
                            "from": cursor.strftime(fmt),
                            "to": chunk_end.strftime(fmt),
                            **params_base,
                        },
                    )
                    chunk_txns = data.get("transactions", [])
                    all_txns.extend(chunk_txns)
                except Exception as e:
                    logger.warning(
                        "get_transactions_chunk_error",
                        error=str(e),
                        date_from=cursor.strftime(fmt),
                        date_to=chunk_end.strftime(fmt),
                    )
                cursor = chunk_end
        else:
            data = await self._request(
                "GET",
                "/history/transactions",
                params={"lastPeriod": min(last_period, 86400), **params_base},
            )
            all_txns = data.get("transactions", [])

        logger.info("get_transactions", count=len(all_txns), tx_type=tx_type)
        return all_txns

    # -- Order management ---------------------------------------------------

    async def create_market_order(
        self,
        instrument: str,
        units: int,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """Place a market order (open position) on Capital.com."""
        epic = _to_epic(instrument)
        direction = "BUY" if units > 0 else "SELL"
        size = abs(units)

        body: dict[str, Any] = {
            "epic": epic,
            "direction": direction,
            "size": size,
        }

        if stop_loss is not None:
            body["stopLevel"] = stop_loss
        if take_profit is not None:
            body["profitLevel"] = take_profit

        logger.info(
            "creating_market_order",
            epic=epic,
            direction=direction,
            size=size,
            stop_level=body.get("stopLevel"),
            profit_level=body.get("profitLevel"),
        )
        data = await self._request("POST", "/positions", json_body=body)

        # Confirm the deal and normalize response
        deal_ref = data.get("dealReference", "")
        if deal_ref:
            try:
                confirmation = await self._confirm_deal(deal_ref)
                return self._normalize_order_response(confirmation, size)
            except Exception as e:
                logger.warning("deal_confirmation_failed", error=str(e))

        return self._normalize_order_response(data, size)

    async def create_limit_order(
        self,
        instrument: str,
        units: int,
        price: float,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """Place a limit order (working order) on Capital.com."""
        epic = _to_epic(instrument)
        direction = "BUY" if units > 0 else "SELL"
        size = abs(units)

        body: dict[str, Any] = {
            "epic": epic,
            "direction": direction,
            "size": size,
            "level": price,
            "type": "LIMIT",
        }

        # Use absolute price levels (not distance) — stop_loss/take_profit
        # are absolute prices from the strategy, matching stopLevel/profitLevel.
        if stop_loss is not None:
            body["stopLevel"] = stop_loss
        if take_profit is not None:
            body["profitLevel"] = take_profit

        logger.info("creating_limit_order", epic=epic, direction=direction, size=size, price=price)
        data = await self._request("POST", "/workingorders", json_body=body)

        deal_ref = data.get("dealReference", "")
        if deal_ref:
            try:
                confirmation = await self._confirm_deal(deal_ref)
                return self._normalize_order_response(confirmation, size)
            except Exception as e:
                logger.warning("deal_confirmation_failed", error=str(e))

        return self._normalize_order_response(data, size)

    async def create_stop_order(
        self,
        instrument: str,
        units: int,
        price: float,
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """Place a stop order (working order) on Capital.com."""
        epic = _to_epic(instrument)
        direction = "BUY" if units > 0 else "SELL"
        size = abs(units)

        body: dict[str, Any] = {
            "epic": epic,
            "direction": direction,
            "size": size,
            "level": price,
            "type": "STOP",
        }

        if stop_loss is not None:
            body["stopLevel"] = stop_loss
        if take_profit is not None:
            body["profitLevel"] = take_profit

        logger.info("creating_stop_order", epic=epic, direction=direction, size=size, price=price)
        data = await self._request("POST", "/workingorders", json_body=body)

        deal_ref = data.get("dealReference", "")
        if deal_ref:
            try:
                confirmation = await self._confirm_deal(deal_ref)
                return self._normalize_order_response(confirmation, size)
            except Exception as e:
                logger.warning("deal_confirmation_failed", error=str(e))

        return self._normalize_order_response(data, size)

    async def close_position(self, instrument: str) -> dict:
        """Close the full position for the given instrument."""
        # Find the position's dealId by instrument
        data = await self._request("GET", "/positions")
        results: list[dict] = []

        for pos in data.get("positions", []):
            market_data = pos.get("market", {})
            epic = market_data.get("epic", "")
            if _from_epic(epic) == instrument:
                deal_id = pos.get("position", {}).get("dealId", "")
                if deal_id:
                    logger.info("closing_position", deal_id=deal_id, instrument=instrument)
                    result = await self._request(
                        "DELETE", f"/positions/{deal_id}"
                    )
                    # Confirm the close
                    deal_ref = result.get("dealReference", "")
                    if deal_ref:
                        try:
                            confirmation = await self._confirm_deal(deal_ref)
                            results.append(confirmation)
                        except Exception:
                            results.append(result)
                    else:
                        results.append(result)

        if not results:
            raise BrokerError(f"No open position found for {instrument}")

        return results[0] if len(results) == 1 else {"closed": results}

    async def close_all_positions(self) -> list[dict]:
        """Close every open position with deal confirmation."""
        data = await self._request("GET", "/positions")
        results: list[dict] = []

        for pos in data.get("positions", []):
            deal_id = pos.get("position", {}).get("dealId", "")
            epic = pos.get("market", {}).get("epic", "")
            if deal_id:
                try:
                    logger.info("closing_position", deal_id=deal_id, epic=epic)
                    result = await self._request(
                        "DELETE", f"/positions/{deal_id}"
                    )
                    # Confirm the close (same as close_position)
                    deal_ref = result.get("dealReference", "")
                    if deal_ref:
                        try:
                            confirmation = await self._confirm_deal(deal_ref)
                            results.append(confirmation)
                        except Exception:
                            results.append(result)
                    else:
                        results.append(result)
                except BrokerError as exc:
                    logger.error("close_position_failed", epic=epic, error=str(exc))
                    results.append({"epic": epic, "error": str(exc)})

        return results

    async def modify_order(
        self,
        order_id: str,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        trailing_stop_distance: float | None = None,
    ) -> dict:
        """Modify an existing position's stop loss / take profit.

        IMPORTANT: Capital.com PUT /positions/{id} replaces ALL stop/profit
        fields.  If we only send stopLevel, the existing profitLevel gets
        cleared (and vice versa).  We therefore always fetch the current
        position first and merge the existing values with the requested
        changes so nothing is lost.
        """
        if stop_loss is None and take_profit is None and trailing_stop_distance is None:
            return {"status": "no_changes"}

        # Capital.com PUT replaces ALL fields — if we only send stopLevel,
        # the existing profitLevel gets deleted.  When the caller already
        # provides both values we can skip the extra GET.
        final_sl = stop_loss
        final_tp = take_profit

        if final_sl is None or final_tp is None:
            # Need to fetch current position to fill the missing value
            try:
                positions_data = await self._request("GET", "/positions")
                for pos in positions_data.get("positions", []):
                    pos_data = pos.get("position", {})
                    if pos_data.get("dealId") == order_id:
                        if final_sl is None:
                            sl_raw = pos_data.get("stopLevel")
                            final_sl = _parse_float(sl_raw) if sl_raw is not None else None
                        if final_tp is None:
                            tp_raw = pos_data.get("profitLevel")
                            final_tp = _parse_float(tp_raw) if tp_raw is not None else None
                        break
            except Exception as e:
                logger.warning("modify_order_fetch_failed", order_id=order_id, error=str(e))

        body: dict[str, Any] = {}
        if final_sl is not None:
            body["stopLevel"] = final_sl
        if final_tp is not None:
            body["profitLevel"] = final_tp
        if trailing_stop_distance is not None:
            body["trailingStop"] = True
            body["trailingStopDistance"] = trailing_stop_distance

        if not body:
            return {"status": "no_changes"}

        logger.info("modifying_position", order_id=order_id, changes=body)

        # Try as position update first
        try:
            data = await self._request(
                "PUT", f"/positions/{order_id}", json_body=body
            )
            return data
        except BrokerError:
            pass

        # Fall back to working order update (use absolute levels, not distances)
        order_body: dict[str, Any] = {}
        if stop_loss is not None:
            order_body["stopLevel"] = stop_loss
        if take_profit is not None:
            order_body["profitLevel"] = take_profit

        data = await self._request(
            "PUT", f"/workingorders/{order_id}", json_body=order_body
        )
        return data

    # -- Market data --------------------------------------------------------

    def _parse_candles(self, prices: list[dict]) -> list[dict[str, Any]]:
        """Parse raw Capital.com candle dicts into flat row dicts."""
        rows: list[dict[str, Any]] = []
        for candle in prices:
            snapshot_time = candle.get("snapshotTime", "")
            open_price = candle.get("openPrice", {})
            high_price = candle.get("highPrice", {})
            low_price = candle.get("lowPrice", {})
            close_price = candle.get("closePrice", {})

            mid_o = (_parse_float(open_price.get("bid")) + _parse_float(open_price.get("ask"))) / 2
            mid_h = (_parse_float(high_price.get("bid")) + _parse_float(high_price.get("ask"))) / 2
            mid_l = (_parse_float(low_price.get("bid")) + _parse_float(low_price.get("ask"))) / 2
            mid_c = (_parse_float(close_price.get("bid")) + _parse_float(close_price.get("ask"))) / 2

            rows.append(
                {
                    "time": snapshot_time,
                    "volume": int(candle.get("lastTradedVolume", 0)),
                    "mid_open": mid_o,
                    "mid_high": mid_h,
                    "mid_low": mid_l,
                    "mid_close": mid_c,
                    "bid_open": _parse_float(open_price.get("bid")),
                    "bid_high": _parse_float(high_price.get("bid")),
                    "bid_low": _parse_float(low_price.get("bid")),
                    "bid_close": _parse_float(close_price.get("bid")),
                    "ask_open": _parse_float(open_price.get("ask")),
                    "ask_high": _parse_float(high_price.get("ask")),
                    "ask_low": _parse_float(low_price.get("ask")),
                    "ask_close": _parse_float(close_price.get("ask")),
                }
            )
        return rows

    async def get_candles(
        self,
        instrument: str,
        granularity: str,
        count: int,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch historical candlestick data and return a pandas DataFrame.

        If *count* exceeds the Capital.com per-request limit (1000), multiple
        paginated requests are made automatically, walking backwards in time.

        Columns: time, volume, mid_open, mid_high, mid_low, mid_close,
        bid_open, bid_high, bid_low, bid_close, ask_open, ask_high,
        ask_low, ask_close.
        """
        epic = _to_epic(instrument)
        resolution = _GRANULARITY_MAP.get(granularity, granularity)
        page_size = 1000
        all_rows: list[dict[str, Any]] = []
        remaining = count
        current_to = to_time  # None = "now"

        while remaining > 0:
            fetch = min(remaining, page_size)
            params: dict[str, Any] = {
                "resolution": resolution,
                "max": fetch,
            }
            if from_time is not None and current_to is None:
                params["from"] = from_time
            if current_to is not None:
                params["to"] = current_to

            data = await self._request("GET", f"/prices/{epic}", params=params)
            prices = data.get("prices", [])

            if not prices:
                break

            batch = self._parse_candles(prices)
            all_rows = batch + all_rows  # prepend older data
            remaining -= len(prices)

            if len(prices) < fetch:
                break  # no more data available

            # Next page: use the earliest timestamp as the new "to"
            earliest = prices[0].get("snapshotTime", "")
            if not earliest or earliest == current_to:
                break
            current_to = earliest

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            df = df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
        return df

    async def stream_prices(
        self,
        instruments: list[str],
    ) -> AsyncGenerator[PriceTick, None]:
        """
        Stream prices by polling the Capital.com prices endpoint.

        Capital.com supports WebSocket streaming via Lightstreamer,
        but polling is used here for simplicity and reliability.
        Polls every 1 second for the latest price of each instrument.
        """
        while True:
            for instrument in instruments:
                try:
                    epic = _to_epic(instrument)
                    data = await self._request(
                        "GET",
                        f"/prices/{epic}",
                        params={"resolution": "MINUTE", "max": 1},
                    )

                    prices = data.get("prices", [])
                    if prices:
                        latest = prices[-1]
                        close_price = latest.get("closePrice", {})
                        bid = _parse_float(close_price.get("bid"))
                        ask = _parse_float(close_price.get("ask"))

                        yield PriceTick(
                            instrument=instrument,
                            time=latest.get("snapshotTime", ""),
                            bid=bid,
                            ask=ask,
                            spread=round(ask - bid, 6) if bid and ask else 0.0,
                            tradeable=True,
                            status="tradeable",
                        )
                except Exception as e:
                    logger.warning("price_polling_error", instrument=instrument, error=str(e))

            await asyncio.sleep(1.0)

    async def get_spread(self, instrument: str) -> float:
        """
        Fetch the current bid-ask spread for *instrument*.

        Uses the prices endpoint to get the latest candle's close.
        """
        epic = _to_epic(instrument)
        data = await self._request(
            "GET",
            f"/prices/{epic}",
            params={"resolution": "MINUTE", "max": 1},
        )

        prices = data.get("prices", [])
        if not prices:
            raise BrokerError(f"No pricing data returned for {instrument}")

        close_price = prices[-1].get("closePrice", {})
        bid = _parse_float(close_price.get("bid"))
        ask = _parse_float(close_price.get("ask"))

        if not bid or not ask:
            raise BrokerError(f"Incomplete pricing data for {instrument}")

        return round(ask - bid, 6)
