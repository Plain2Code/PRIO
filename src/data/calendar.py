"""Economic calendar module — Forex Factory news filter.

STANDALONE: Fetches and caches economic calendar data from Forex Factory.
Receives plain data (instrument, datetime), returns plain data (events, decisions).
Knows NOTHING about strategy, broker, risk, or orchestrator.

Usage:
    from src.data.calendar import EconomicCalendar

    calendar = EconomicCalendar(config)
    await calendar.refresh()
    blocked, events = calendar.check_blackout("EUR_USD", now_utc)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import structlog

logger = structlog.get_logger(__name__)

# Forex Factory free calendar endpoint (JSON)
FF_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Rate limit: max 2 requests per 5 minutes
MIN_FETCH_INTERVAL_SECONDS = 150

# Eastern Time zone (handles EST/EDT automatically)
ET = ZoneInfo("America/New_York")

# Impact rank for threshold comparison
_IMPACT_RANK = {"Low": 1, "Medium": 2, "High": 3}

# Pair -> constituent currencies
PAIR_CURRENCIES: dict[str, tuple[str, str]] = {
    "EUR_USD": ("EUR", "USD"),
    "GBP_JPY": ("GBP", "JPY"),
    "AUD_NZD": ("AUD", "NZD"),
    "USD_CAD": ("USD", "CAD"),
    "EUR_GBP": ("EUR", "GBP"),
    "NZD_JPY": ("NZD", "JPY"),
    "AUD_CAD": ("AUD", "CAD"),
    # Common extras in case user re-enables
    "GBP_USD": ("GBP", "USD"),
    "USD_JPY": ("USD", "JPY"),
    "AUD_USD": ("AUD", "USD"),
    "NZD_USD": ("NZD", "USD"),
    "USD_CHF": ("USD", "CHF"),
    "EUR_JPY": ("EUR", "JPY"),
    "GBP_AUD": ("GBP", "AUD"),
    "EUR_AUD": ("EUR", "AUD"),
    "EUR_NZD": ("EUR", "NZD"),
    "EUR_CAD": ("EUR", "CAD"),
    "GBP_CHF": ("GBP", "CHF"),
    "GBP_NZD": ("GBP", "NZD"),
    "GBP_CAD": ("GBP", "CAD"),
    "AUD_JPY": ("AUD", "JPY"),
    "AUD_CHF": ("AUD", "CHF"),
    "NZD_CAD": ("NZD", "CAD"),
    "NZD_CHF": ("NZD", "CHF"),
    "CAD_CHF": ("CAD", "CHF"),
    "CHF_JPY": ("CHF", "JPY"),
    "CAD_JPY": ("CAD", "JPY"),
}


@dataclass
class CalendarEvent:
    """A single economic calendar event."""

    title: str
    country: str  # currency code: "USD", "EUR", etc.
    datetime_utc: datetime
    impact: str  # "High", "Medium", "Low"
    forecast: str = ""
    previous: str = ""


class EconomicCalendar:
    """Fetches and caches Forex Factory economic calendar data.

    Parameters
    ----------
    config : dict
        The ``calendar`` section of the YAML configuration.
    """

    TRACKED_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}

    def __init__(self, config: dict) -> None:
        self.enabled: bool = config.get("enabled", True)
        self.blackout_hours: float = config.get("blackout_hours", 1.0)
        self.post_event_boost_hours: float = config.get("post_event_boost_hours", 0.5)
        self.impact_threshold: str = config.get("impact_threshold", "High")
        self.refresh_interval_minutes: int = config.get("refresh_interval_minutes", 5)
        self.currency_overrides: dict[str, dict] = config.get(
            "currency_overrides", {}
        )

        # Internal state
        self._events: list[CalendarEvent] = []
        self._last_fetch: datetime | None = None
        self._fetch_error: str | None = None
        self._raw_count: int = 0

    # ------------------------------------------------------------------
    # Fetch & parse
    # ------------------------------------------------------------------

    async def refresh(self) -> bool:
        """Fetch calendar from Forex Factory. Returns True on success.

        Respects rate limiting (150s minimum between fetches).
        On failure, logs the error and returns False (events remain stale).
        """
        if not self.enabled:
            return False

        now = datetime.now(timezone.utc)
        if (
            self._last_fetch
            and (now - self._last_fetch).total_seconds() < MIN_FETCH_INTERVAL_SECONDS
        ):
            return False  # Rate limit guard

        try:
            import httpx

            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(FF_CALENDAR_URL)
                resp.raise_for_status()
                raw = resp.json()

            self._events = self._parse_events(raw)
            self._raw_count = len(raw)
            self._last_fetch = now
            self._fetch_error = None

            logger.info(
                "calendar_refreshed",
                events=len(self._events),
                raw=self._raw_count,
            )
            return True

        except Exception as e:
            self._fetch_error = str(e)
            logger.warning("calendar_refresh_failed", error=str(e))
            return False

    def _parse_events(self, raw_events: list[dict]) -> list[CalendarEvent]:
        """Parse raw FF JSON events into CalendarEvent objects."""
        events: list[CalendarEvent] = []
        for item in raw_events:
            country = (item.get("country") or "").strip().upper()
            if country not in self.TRACKED_CURRENCIES:
                continue

            impact = (item.get("impact") or "").strip()
            if impact not in _IMPACT_RANK:
                continue

            dt = self._parse_ff_datetime(
                item.get("date", ""),
                item.get("time", ""),
            )
            if dt is None:
                continue

            events.append(
                CalendarEvent(
                    title=(item.get("title") or "").strip(),
                    country=country,
                    datetime_utc=dt,
                    impact=impact,
                    forecast=(item.get("forecast") or "").strip(),
                    previous=(item.get("previous") or "").strip(),
                )
            )

        events.sort(key=lambda e: e.datetime_utc)
        return events

    @staticmethod
    def _parse_ff_datetime(date_str: str, time_str: str) -> datetime | None:
        """Parse Forex Factory date + time into UTC datetime.

        Supports two formats:
        1. ISO 8601: date_str = "2026-02-08T01:00:00-05:00" (time_str ignored)
        2. Legacy:   date_str = "2024-01-15", time_str = "8:30am"
        """
        date_str = (date_str or "").strip()
        if not date_str:
            return None

        # Format 1: ISO 8601 with timezone (e.g. "2026-02-08T01:00:00-05:00")
        if "T" in date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                return dt.astimezone(timezone.utc)
            except ValueError:
                pass

        # Format 2: Legacy separate date + time fields
        time_str = (time_str or "").strip()
        if not time_str or time_str.lower() in ("tentative", ""):
            return None

        if time_str.lower() == "all day":
            try:
                dt = datetime.strptime(date_str, "%m-%d-%Y")
            except ValueError:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    return None
            return dt.replace(tzinfo=timezone.utc)

        try:
            dt_str = f"{date_str} {time_str}"
            try:
                dt_et = datetime.strptime(dt_str, "%m-%d-%Y %I:%M%p")
            except ValueError:
                dt_et = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p")
            dt_et = dt_et.replace(tzinfo=ET)
            return dt_et.astimezone(timezone.utc)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def check_blackout(
        self,
        instrument: str,
        now_utc: datetime,
    ) -> tuple[bool, list[CalendarEvent]]:
        """Check if an instrument is in a news blackout window.

        Returns (is_blocked, list_of_blocking_events).
        """
        if not self.enabled or not self._events:
            return False, []

        currencies = self._get_currencies(instrument)
        if not currencies:
            return False, []

        blocking: list[CalendarEvent] = []
        for event in self._events:
            if event.country not in currencies:
                continue
            if not self._meets_impact_threshold(event, event.country):
                continue

            # Get blackout window for this currency
            hours = self._blackout_hours_for(event.country)
            window_start = event.datetime_utc - timedelta(hours=hours)
            window_end = event.datetime_utc

            if window_start <= now_utc <= window_end:
                blocking.append(event)

        return (len(blocking) > 0, blocking)

    def check_post_event_boost(
        self,
        instrument: str,
        now_utc: datetime,
    ) -> bool:
        """Check if instrument is in post-event boost window.

        Returns True if a high-impact event recently occurred for this pair.
        """
        if not self.enabled or not self._events or self.post_event_boost_hours <= 0:
            return False

        currencies = self._get_currencies(instrument)
        if not currencies:
            return False

        for event in self._events:
            if event.country not in currencies:
                continue
            if not self._meets_impact_threshold(event, event.country):
                continue

            boost_start = event.datetime_utc
            boost_end = event.datetime_utc + timedelta(hours=self.post_event_boost_hours)

            if boost_start <= now_utc <= boost_end:
                return True

        return False

    def get_upcoming_events(
        self,
        hours_ahead: float = 24.0,
        impact_filter: str | None = None,
    ) -> list[CalendarEvent]:
        """Get all upcoming events in the next N hours."""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours_ahead)

        results: list[CalendarEvent] = []
        for event in self._events:
            if event.datetime_utc < now:
                continue
            if event.datetime_utc > cutoff:
                break  # events are sorted by time

            if impact_filter:
                min_rank = _IMPACT_RANK.get(impact_filter, 3)
                event_rank = _IMPACT_RANK.get(event.impact, 0)
                if event_rank < min_rank:
                    continue

            results.append(event)

        return results

    def get_events_for_instrument(
        self,
        instrument: str,
        hours_ahead: float = 24.0,
    ) -> list[CalendarEvent]:
        """Get upcoming events relevant to a specific pair's currencies."""
        currencies = self._get_currencies(instrument)
        if not currencies:
            return []

        upcoming = self.get_upcoming_events(hours_ahead)
        return [e for e in upcoming if e.country in currencies]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_currencies(self, instrument: str) -> tuple[str, ...]:
        """Resolve instrument to its constituent currencies."""
        pair = PAIR_CURRENCIES.get(instrument)
        if pair:
            return pair

        # Fallback: split on underscore
        parts = instrument.split("_")
        if len(parts) == 2:
            return (parts[0].upper(), parts[1].upper())

        return ()

    def _blackout_hours_for(self, currency: str) -> float:
        """Get blackout hours for a currency, respecting overrides."""
        override = self.currency_overrides.get(currency, {})
        return override.get("blackout_hours", self.blackout_hours)

    def _meets_impact_threshold(
        self, event: CalendarEvent, currency: str | None = None
    ) -> bool:
        """Check if event meets the configured impact threshold."""
        # Per-currency threshold override
        threshold = self.impact_threshold
        if currency:
            override = self.currency_overrides.get(currency, {})
            threshold = override.get("impact_threshold", threshold)

        min_rank = _IMPACT_RANK.get(threshold, 3)
        event_rank = _IMPACT_RANK.get(event.impact, 0)
        return event_rank >= min_rank

    # ------------------------------------------------------------------
    # Status / diagnostics
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return calendar module status for the API."""
        return {
            "enabled": self.enabled,
            "event_count": len(self._events),
            "last_fetch": self._last_fetch.isoformat() if self._last_fetch else None,
            "fetch_error": self._fetch_error,
            "blackout_hours": self.blackout_hours,
            "impact_threshold": self.impact_threshold,
        }
