"""
Execution engine for order management with retry logic, heartbeat
monitoring, spread checking, and latency tracking.

Reads orders from an internal async queue, executes them against
a :class:`BaseBroker` adapter with exponential-backoff retries, and
exposes latency statistics for operational monitoring.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timezone

import structlog

from src.broker.base import BaseBroker, BrokerError
from src.core.types import OrderRequest, OrderResult

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------

# Dynamic spread threshold: max_spread_pips * pip_value.
# JPY pairs use pip = 0.01; all others use pip = 0.0001.
# Config can override per-instrument via ``spread_thresholds``.
_DEFAULT_MAX_SPREAD_PIPS: float = 3.0


def _spread_threshold_for(instrument: str, max_pips: float) -> float:
    """Calculate the spread threshold in price units for any instrument."""
    pip = 0.01 if "JPY" in instrument.upper() else 0.0001
    return max_pips * pip


class ExecutionEngine:
    """
    Asynchronous execution engine that manages order submission, retries,
    heartbeat monitoring, and latency tracking.

    Parameters
    ----------
    broker : BaseBroker
        The broker adapter used to place and manage orders.
    config : dict
        The ``execution`` section of the application YAML config.  Recognised
        keys (all optional, shown with their defaults):

        - ``max_retries`` (int): 3
        - ``retry_delay_seconds`` (float): 1.0
        - ``max_retry_delay_seconds`` (float): 30.0
        - ``heartbeat_interval_seconds`` (int): 30
        - ``reconnect_delay_seconds`` (float): 5.0
        - ``max_order_latency_ms`` (int): 1000
        - ``spread_check_enabled`` (bool): True
        - ``spread_thresholds`` (dict[str, float]): per-instrument overrides
    """

    def __init__(self, broker: BaseBroker, config: dict) -> None:
        self._broker = broker
        self._config = config

        # Configuration with defaults ----------------------------------------
        self._max_retries: int = config.get("max_retries", 3)
        self._retry_delay: float = config.get("retry_delay_seconds", 1.0)
        self._max_retry_delay: float = config.get("max_retry_delay_seconds", 30.0)
        self._heartbeat_interval: int = config.get("heartbeat_interval_seconds", 30)
        self._reconnect_delay: float = config.get("reconnect_delay_seconds", 5.0)
        self._max_order_latency_ms: int = config.get("max_order_latency_ms", 1000)
        self._spread_check_enabled: bool = config.get("spread_check_enabled", True)
        self._max_spread_pips: float = config.get("max_spread_pips", _DEFAULT_MAX_SPREAD_PIPS)
        self._spread_overrides: dict[str, float] = config.get("spread_thresholds", {})

        # Internal state -----------------------------------------------------
        self._order_queue: asyncio.Queue[
            tuple[OrderRequest, asyncio.Future[OrderResult]]
        ] = asyncio.Queue()
        self.is_running: bool = False
        self.is_connected: bool = True

        self._heartbeat_task: asyncio.Task | None = None
        self._processor_task: asyncio.Task | None = None

        # Circuit breaker: pause order submission after consecutive failures
        self._consecutive_failures: int = 0
        self._circuit_breaker_until: float = 0.0  # monotonic timestamp
        self._circuit_breaker_threshold: int = 3
        self._circuit_breaker_pause_s: float = 60.0

        # Metrics / diagnostics ----------------------------------------------
        self._latency_history: deque[float] = deque(maxlen=100)
        self._last_heartbeat: float = 0.0

    # -- Lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Start the order-processing loop and heartbeat monitor."""
        if self.is_running:
            logger.warning("execution_engine.already_running")
            return

        self.is_running = True
        self.is_connected = True
        self._last_heartbeat = time.monotonic()

        self._processor_task = asyncio.create_task(
            self._process_orders(), name="execution-order-processor"
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="execution-heartbeat"
        )

        logger.info(
            "execution_engine.started",
            max_retries=self._max_retries,
            heartbeat_interval=self._heartbeat_interval,
            spread_check=self._spread_check_enabled,
        )

    async def stop(self) -> None:
        """Gracefully shut down the engine.

        Remaining orders in the queue are drained and processed before
        the background tasks are cancelled.
        """
        if not self.is_running:
            return

        logger.info(
            "execution_engine.stopping",
            remaining_orders=self._order_queue.qsize(),
        )

        self.is_running = False

        # Drain any remaining orders so callers receive a result.
        while not self._order_queue.empty():
            try:
                order_request, future = self._order_queue.get_nowait()
                if not future.done():
                    result = await self._execute_with_retry(order_request)
                    future.set_result(result)
                self._order_queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as exc:
                logger.error("execution_engine.drain_error", error=str(exc))
                if not future.done():
                    future.set_result(
                        OrderResult(success=False, error=f"Shutdown drain error: {exc}")
                    )

        # Cancel background tasks.
        for task in (self._processor_task, self._heartbeat_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._processor_task = None
        self._heartbeat_task = None

        logger.info("execution_engine.stopped")

    # -- Public interface ----------------------------------------------------

    async def submit_order(self, order_request: OrderRequest) -> OrderResult:
        """Submit an order for asynchronous execution.

        The order is placed on the internal queue and processed by the
        background loop.  The call blocks until the order is executed (or
        fails), then returns the :class:`OrderResult`.
        """
        if not self.is_running:
            return OrderResult(
                success=False,
                error="Execution engine is not running",
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[OrderResult] = loop.create_future()
        await self._order_queue.put((order_request, future))

        logger.info(
            "execution_engine.order_submitted",
            instrument=order_request.instrument,
            units=order_request.units,
            order_type=order_request.order_type,
            queue_depth=self._order_queue.qsize(),
        )

        return await future

    # -- Internal: order processing ------------------------------------------

    async def _process_orders(self) -> None:
        """Main loop that reads orders from the queue and executes them."""
        logger.info("execution_engine.processor_started")

        while self.is_running:
            try:
                order_request, future = await asyncio.wait_for(
                    self._order_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                # No orders pending; loop back to check ``is_running``.
                continue
            except asyncio.CancelledError:
                return

            try:
                # 0. Circuit breaker check
                if self._consecutive_failures >= self._circuit_breaker_threshold:
                    now_mono = time.monotonic()
                    if now_mono < self._circuit_breaker_until:
                        remaining = self._circuit_breaker_until - now_mono
                        result = OrderResult(
                            success=False,
                            error=f"Circuit breaker active ({remaining:.0f}s remaining)",
                        )
                        if not future.done():
                            future.set_result(result)
                        self._order_queue.task_done()
                        continue
                    # Cooldown expired — reset and proceed
                    self._consecutive_failures = 0

                # 1. Pre-flight spread check
                if self._spread_check_enabled:
                    spread_ok, spread_value = await self._check_spread(
                        order_request.instrument
                    )
                    if not spread_ok:
                        result = OrderResult(
                            success=False,
                            error=(
                                f"Spread too wide for {order_request.instrument}: "
                                f"{spread_value:.5f}"
                            ),
                        )
                        if not future.done():
                            future.set_result(result)
                        self._order_queue.task_done()
                        continue

                # 2. Execute with latency tracking
                start_ns = time.perf_counter_ns()
                result = await self._execute_with_retry(order_request)
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000

                # 3. Record latency + circuit breaker tracking
                result.latency_ms = elapsed_ms
                self._latency_history.append(elapsed_ms)

                if result.success:
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= self._circuit_breaker_threshold:
                        self._circuit_breaker_until = (
                            time.monotonic() + self._circuit_breaker_pause_s
                        )
                        logger.warning(
                            "execution_engine.circuit_breaker_activated",
                            consecutive_failures=self._consecutive_failures,
                            pause_s=self._circuit_breaker_pause_s,
                        )

                if elapsed_ms > self._max_order_latency_ms:
                    logger.warning(
                        "execution_engine.high_latency",
                        instrument=order_request.instrument,
                        latency_ms=round(elapsed_ms, 2),
                        threshold_ms=self._max_order_latency_ms,
                    )

                # 4. Deliver result to the caller
                if not future.done():
                    future.set_result(result)

            except Exception as exc:
                logger.error(
                    "execution_engine.process_error",
                    instrument=order_request.instrument,
                    error=str(exc),
                )
                if not future.done():
                    future.set_result(
                        OrderResult(success=False, error=f"Processing error: {exc}")
                    )

            finally:
                self._order_queue.task_done()

        logger.info("execution_engine.processor_stopped")

    # -- Internal: retry logic -----------------------------------------------

    async def _execute_with_retry(self, order_request: OrderRequest) -> OrderResult:
        """Execute an order against the broker with exponential-backoff retries.

        Returns an :class:`OrderResult` regardless of outcome (success or
        final failure).
        """
        last_error: str = ""

        for attempt in range(self._max_retries):
            try:
                broker_result = await self._dispatch_order(order_request)

                # Build a successful OrderResult from the broker response dict.
                return OrderResult(
                    success=True,
                    order_id=broker_result.get("order_id"),
                    fill_price=broker_result.get("fill_price"),
                    units_filled=broker_result.get("units_filled", abs(order_request.units)),
                    timestamp=broker_result.get(
                        "timestamp",
                        datetime.now(timezone.utc).isoformat(),
                    ),
                    metadata=broker_result.get("metadata", {}),
                )

            except BrokerError as exc:
                last_error = str(exc)
                delay = min(
                    self._retry_delay * (2 ** attempt),
                    self._max_retry_delay,
                )
                logger.warning(
                    "execution_engine.retry",
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    delay_s=round(delay, 2),
                    error=last_error,
                    instrument=order_request.instrument,
                )
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(delay)

            except Exception as exc:
                # Non-broker (fatal) error — do not retry.
                logger.error(
                    "execution_engine.fatal_error",
                    instrument=order_request.instrument,
                    error=str(exc),
                )
                return OrderResult(
                    success=False,
                    error=f"Fatal execution error: {exc}",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )

        # All retries exhausted.
        logger.error(
            "execution_engine.retries_exhausted",
            instrument=order_request.instrument,
            max_retries=self._max_retries,
            last_error=last_error,
        )
        return OrderResult(
            success=False,
            error=f"Max retries ({self._max_retries}) exhausted. Last error: {last_error}",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    async def _dispatch_order(self, order_request: OrderRequest) -> dict:
        """Route the order request to the appropriate broker method."""
        otype = order_request.order_type.lower()

        if otype == "market":
            return await self._broker.create_market_order(
                instrument=order_request.instrument,
                units=order_request.units,
                stop_loss=order_request.stop_loss,
                take_profit=order_request.take_profit,
            )
        elif otype == "limit":
            if order_request.price is None:
                raise ValueError("Limit order requires a price")
            return await self._broker.create_limit_order(
                instrument=order_request.instrument,
                units=order_request.units,
                price=order_request.price,
                stop_loss=order_request.stop_loss,
                take_profit=order_request.take_profit,
            )
        elif otype == "stop":
            if order_request.price is None:
                raise ValueError("Stop order requires a price")
            return await self._broker.create_stop_order(
                instrument=order_request.instrument,
                units=order_request.units,
                price=order_request.price,
                stop_loss=order_request.stop_loss,
                take_profit=order_request.take_profit,
            )
        else:
            raise ValueError(f"Unsupported order type: {order_request.order_type}")

    # -- Internal: heartbeat -------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Periodically poll the broker to verify connectivity."""
        logger.info("execution_engine.heartbeat_started")

        while self.is_running:
            try:
                await asyncio.sleep(self._heartbeat_interval)

                if not self.is_running:
                    break

                await self._broker.get_account()

                self.is_connected = True
                self._last_heartbeat = time.monotonic()

                logger.debug("execution_engine.heartbeat_ok")

            except asyncio.CancelledError:
                return

            except Exception as exc:
                self.is_connected = False
                logger.error(
                    "execution_engine.heartbeat_failed",
                    error=str(exc),
                )
                await self._attempt_reconnect()

    async def _attempt_reconnect(self) -> None:
        """Try to restore broker connectivity after a heartbeat failure."""
        logger.info(
            "execution_engine.reconnecting",
            delay_s=self._reconnect_delay,
        )
        await asyncio.sleep(self._reconnect_delay)

        try:
            await self._broker.get_account()
            self.is_connected = True
            self._last_heartbeat = time.monotonic()
            logger.info("execution_engine.reconnected")
        except Exception as exc:
            logger.error(
                "execution_engine.reconnect_failed",
                error=str(exc),
            )

    # -- Internal: spread check ----------------------------------------------

    async def _check_spread(self, instrument: str) -> tuple[bool, float]:
        """Verify the current spread is within acceptable limits.

        Returns
        -------
        tuple[bool, float]
            ``(is_acceptable, spread_value)``
        """
        try:
            spread = await self._broker.get_spread(instrument)
            # Per-instrument override (price units) → dynamic pip-based threshold
            if instrument in self._spread_overrides:
                threshold = self._spread_overrides[instrument]
            else:
                threshold = _spread_threshold_for(instrument, self._max_spread_pips)
            is_ok = spread <= threshold

            if not is_ok:
                logger.warning(
                    "execution_engine.spread_too_wide",
                    instrument=instrument,
                    spread=spread,
                    threshold=threshold,
                )

            return is_ok, spread

        except Exception as exc:
            # Cannot determine spread — reject order to avoid trading
            # into unknown spread conditions (e.g. during API issues).
            logger.warning(
                "execution_engine.spread_check_error",
                instrument=instrument,
                error=str(exc),
            )
            return False, 0.0

    # -- Metrics / diagnostics -----------------------------------------------

    def get_latency_stats(self) -> dict:
        """Return summary statistics from the recent latency history.

        Returns
        -------
        dict
            Keys: ``count``, ``avg_ms``, ``min_ms``, ``max_ms``, ``p95_ms``.
            All values are ``0.0`` when no orders have been executed yet.
        """
        if not self._latency_history:
            return {
                "count": 0,
                "avg_ms": 0.0,
                "min_ms": 0.0,
                "max_ms": 0.0,
                "p95_ms": 0.0,
            }

        sorted_latencies = sorted(self._latency_history)
        count = len(sorted_latencies)
        p95_index = min(int(count * 0.95), count - 1)

        return {
            "count": count,
            "avg_ms": round(sum(sorted_latencies) / count, 2),
            "min_ms": round(sorted_latencies[0], 2),
            "max_ms": round(sorted_latencies[-1], 2),
            "p95_ms": round(sorted_latencies[p95_index], 2),
        }

    @property
    def last_heartbeat(self) -> float:
        """Monotonic timestamp of the last successful heartbeat."""
        return self._last_heartbeat

    @property
    def queue_depth(self) -> int:
        """Number of orders currently waiting in the queue."""
        return self._order_queue.qsize()
