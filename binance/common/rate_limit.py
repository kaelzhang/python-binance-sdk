import asyncio
import time
from typing import List, Optional, Tuple

from binance.common.constants import HEADER_RETRY_AFTER


def parse_retry_after(response) -> Optional[int]:
    """Read the integer `Retry-After` (seconds) from a response, or None."""
    value = response.headers.get(HEADER_RETRY_AFTER)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def depth_weight(limit: int) -> int:
    """Verified weight tiers for GET /api/v3/depth."""
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1000:
        return 50
    return 250


# Conservative static defaults; runtime truth comes from X-MBX-USED-WEIGHT
# headers and exchangeInfo.rateLimits. `depth` is computed via depth_weight().
REST_ENDPOINT_WEIGHTS = {
    'exchangeInfo': 20,
    'account': 20,
    'myTrades': 20,
    'allOrders': 20,
    'openOrders': 6,
    'order': 4,
    'ticker/24hr': 80,
    'ticker/price': 4,
    'ticker/bookTicker': 4,
}


class SlidingWindowRateLimiter:
    """Count-based sliding-window limiter (e.g. WS messages, connections)."""

    def __init__(self, max_count: int, window: float) -> None:
        self._max = max(1, int(max_count))
        self._window = window
        self._events: List[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self._window
                self._events = [t for t in self._events if t > cutoff]
                if len(self._events) < self._max:
                    self._events.append(time.monotonic())
                    return
                wait = self._events[0] + self._window - now
                if wait > 0:
                    await asyncio.sleep(wait)


class WeightRateLimiter:
    """Weighted sliding-window limiter for REST REQUEST_WEIGHT."""

    def __init__(
        self,
        limit: int,
        window: float,
        safety_ratio: float = 1.0
    ) -> None:
        self._limit = max(1, int(limit * safety_ratio))
        self._window = window
        self._events: List[Tuple[float, int]] = []
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = 1) -> None:
        weight = max(1, int(weight))
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self._window
                self._events = [(t, w) for (t, w) in self._events if t > cutoff]
                used = sum(w for _, w in self._events)
                if used + weight <= self._limit:
                    self._events.append((time.monotonic(), weight))
                    return
                wait = self._events[0][0] + self._window - now
                if wait > 0:
                    await asyncio.sleep(wait)
