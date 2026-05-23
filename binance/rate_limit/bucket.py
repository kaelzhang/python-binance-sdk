import asyncio
import time
from typing import List, Optional, Tuple

from binance.common.exceptions import (
    RateLimitReachedException,
    TooManyStreamsException
)
from binance.rate_limit.types import RateLimitRule, RateLimitKind, EnforceMode


class RateLimitBucket:
    """Runtime state for one rate-limit pool.

    WEIGHT/COUNT use a monotonic sliding window of (timestamp, cost); CAP is a
    plain current-count ceiling. Usage is ALWAYS accounted -- EnforceMode only
    decides the action when a request would exceed the effective limit.
    """

    def __init__(self, rule: RateLimitRule) -> None:
        self._rule = rule
        self._limit = rule.limit
        self._events: List[Tuple[float, int]] = []
        self._count = 0                              # CAP current count
        self._authoritative: Optional[int] = None    # from sync()
        self._pending = 0
        self._lock = asyncio.Lock()

    @property
    def rule(self) -> RateLimitRule:
        return self._rule

    @property
    def effective_limit(self) -> int:
        return max(1, int(self._limit * self._rule.safety_ratio))

    @property
    def pending(self) -> int:
        return self._pending

    @property
    def has_authoritative(self) -> bool:
        return self._authoritative is not None

    def _prune(self, now: float) -> None:
        cutoff = now - self._rule.interval_seconds
        self._events = [(t, c) for (t, c) in self._events if t > cutoff]

    def _windowed_used(self, now: float) -> int:
        self._prune(now)
        estimate = sum(c for _, c in self._events)
        if self._authoritative is not None:
            return max(estimate, self._authoritative)
        return estimate

    @property
    def used(self) -> int:
        if self._rule.kind == RateLimitKind.CAP:
            return self._count
        return self._windowed_used(time.monotonic())

    def record(self, cost: int = 1) -> None:
        self._events.append((time.monotonic(), max(1, int(cost))))

    def sync(self, authoritative_used: int) -> None:
        self._authoritative = max(0, int(authoritative_used))

    def set_limit(self, limit: int) -> None:
        self._limit = max(1, int(limit))

    def _retry_after_exact(self, now: float) -> float:
        if not self._events:
            return 0.0
        return self._events[0][0] + self._rule.interval_seconds - now

    def _retry_after(self, now: float) -> int:
        return max(0, int(self._retry_after_exact(now)) + 1)

    async def acquire(self, cost: int = 1) -> None:
        cost = max(1, int(cost))
        self._pending += 1
        try:
            async with self._lock:
                while True:
                    now = time.monotonic()
                    if self._windowed_used(now) + cost <= self.effective_limit:
                        self._events.append((time.monotonic(), cost))
                        return
                    if self._rule.enforce == EnforceMode.TRACK:
                        self._events.append((time.monotonic(), cost))
                        return
                    if self._rule.enforce == EnforceMode.RAISE:
                        raise RateLimitReachedException(
                            self._rule.scope.value,
                            self._rule.type.value,
                            self._rule.interval,
                            self._retry_after(now))
                    wait = self._retry_after_exact(now)
                    if wait > 0:
                        await asyncio.sleep(wait)
        finally:
            self._pending -= 1

    # CAP-only -----------------------------------------------------------
    def reserve(self, projected_total: int) -> None:
        if projected_total > self.effective_limit:
            raise TooManyStreamsException(projected_total, self.effective_limit)
        self._count = projected_total

    def release(self, count: int) -> None:
        self._count = max(0, self._count - int(count))
