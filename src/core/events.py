"""Lightweight async event bus for decoupling modules.

Usage:
    bus = EventBus()
    bus.subscribe("trade.opened", my_callback)
    await bus.publish("trade.opened", instrument="EUR_USD", price=1.085)

Callbacks run concurrently via asyncio.gather().
A failing callback never blocks other callbacks.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


class EventBus:
    """Process-local async publish/subscribe event bus."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)

    def subscribe(self, event: str, callback: Callable) -> None:
        """Register a callback for an event name."""
        self._subscribers[event].append(callback)

    def unsubscribe(self, event: str, callback: Callable) -> None:
        """Remove a specific callback."""
        try:
            self._subscribers[event].remove(callback)
        except ValueError:
            pass

    async def publish(self, event: str, **kwargs: Any) -> None:
        """Fire an event. All registered callbacks run concurrently.

        Exceptions in individual callbacks are logged but don't block others.
        """
        callbacks = self._subscribers.get(event, [])
        if not callbacks:
            return

        tasks = []
        for cb in callbacks:
            if inspect.iscoroutinefunction(cb):
                tasks.append(cb(**kwargs))
            else:
                # Wrap sync callbacks
                tasks.append(asyncio.to_thread(cb, **kwargs))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "event_callback_error",
                    event=event,
                    callback=callbacks[i].__qualname__,
                    error=str(result),
                )

    def clear(self) -> None:
        """Remove all subscriptions. Used in tests."""
        self._subscribers.clear()
