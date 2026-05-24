"""Per-pool runtime state and enforcement.

:class:`RateLimitBucket` is the live counter for a single
:class:`~binance.rate_limit.types.RateLimitRule`: a sliding window for
``WEIGHT``/``COUNT`` pools or a current-count ceiling for ``CAP`` pools. The
:class:`~binance.rate_limit.core.RateLimiter` owns one bucket per pool and drives
them; application code normally goes through the limiter, not buckets directly.
"""

import asyncio
import time
from typing import List, Optional, Tuple

from binance.common.exceptions import (
    RateLimitReachedException,
    TooManyStreamsException
)
from binance.rate_limit.types import RateLimitRule, RateLimitKind, EnforceMode


# Smallest sleep used when blocked, so the loop always yields and re-checks
# instead of spinning the event loop.
_MIN_WAIT = 0.01


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
        self._count = 0                               # CAP current count
        # authoritative header reading: (monotonic_ts, value). Valid only for
        # one interval -- after that the server window has rolled, so it decays.
        self._authoritative: Optional[Tuple[float, int]] = None
        self._pending = 0
        self._lock = asyncio.Lock()

    @property
    def rule(self) -> RateLimitRule:
        """The rule this bucket was built from."""
        return self._rule

    @property
    def effective_limit(self) -> int:
        """The enforced cap: limit x safety_ratio, floored at 1."""
        return max(1, int(self._limit * self._rule.safety_ratio))

    @property
    def pending(self) -> int:
        """Number of callers currently inside acquire() waiting on this bucket."""
        return self._pending

    def _authoritative_used(self, now: float) -> Optional[int]:
        if self._authoritative is None:
            return None
        ts, value = self._authoritative
        if now - ts >= self._rule.interval_seconds:
            return None                               # stale: window rolled
        return value

    @property
    def has_authoritative(self) -> bool:
        """Whether a still-fresh synced reading backs this bucket
        (snapshot ``source='header'``); a synced value is trusted for one interval."""
        return self._authoritative_used(time.monotonic()) is not None

    def _prune(self, now: float) -> None:
        cutoff = now - self._rule.interval_seconds
        self._events = [(t, c) for (t, c) in self._events if t > cutoff]

    def _windowed_used(self, now: float) -> int:
        self._prune(now)
        estimate = sum(c for _, c in self._events)
        auth = self._authoritative_used(now)
        if auth is not None:
            return max(estimate, auth)
        return estimate

    @property
    def used(self) -> int:
        """Current usage: live count for CAP, else the in-window
        ``max(local estimate, fresh authoritative reading)``."""
        if self._rule.kind == RateLimitKind.CAP:
            return self._count
        return self._windowed_used(time.monotonic())

    def record(self, cost: int = 1) -> None:
        """Account ``cost`` against the window with no limit check
        (track-only / disabled-guard mode); ``cost`` clamped to >= 1."""
        self._events.append((time.monotonic(), max(1, int(cost))))

    def sync(self, authoritative_used: int) -> None:
        """Reconcile against an authoritative header reading; it overrides the
        local estimate (via ``max``) for one interval, then decays as stale."""
        self._authoritative = (time.monotonic(), max(0, int(authoritative_used)))

    def set_limit(self, limit: int) -> None:
        """Override the documented limit (e.g. from exchangeInfo); safety_ratio still applies."""
        self._limit = max(1, int(limit))

    def _blocked_wait(self, now: float) -> float:
        # Soonest moment some headroom frees: the oldest event expiring and/or
        # the authoritative reading going stale. Floored at _MIN_WAIT so a
        # blocked acquire never spins.
        candidates = []
        if self._events:
            candidates.append(
                self._events[0][0] + self._rule.interval_seconds - now)
        if self._authoritative is not None:
            ts, _ = self._authoritative
            candidates.append(ts + self._rule.interval_seconds - now)
        positive = [w for w in candidates if w > 0]
        if positive:
            return max(min(positive), _MIN_WAIT)
        return _MIN_WAIT

    def _retry_after(self, now: float) -> int:
        return max(1, int(self._blocked_wait(now)) + 1)

    async def acquire(self, cost: int = 1) -> None:
        """Account ``cost``, enforcing the rule on overflow: SLEEP waits (never
        busy-spins), RAISE raises ``RateLimitReachedException``, TRACK records
        anyway. A ``cost`` larger than the whole effective limit fails fast.
        """
        cost = max(1, int(cost))
        # A single request larger than the whole budget can never be admitted;
        # fail fast instead of blocking forever.
        if cost > self.effective_limit:
            raise RateLimitReachedException(
                self._rule.scope.value,
                self._rule.type.value,
                self._rule.interval,
                int(self._rule.interval_seconds))
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
                    await asyncio.sleep(self._blocked_wait(now))
        finally:
            self._pending -= 1

    # CAP-only -----------------------------------------------------------
    def reserve(self, projected_total: int) -> None:
        """CAP only: set the current count to ``projected_total`` (absolute, so
        idempotent under resubscribe); raises ``TooManyStreamsException`` over the cap."""
        projected_total = max(0, int(projected_total))
        if projected_total > self.effective_limit:
            raise TooManyStreamsException(projected_total, self.effective_limit)
        self._count = projected_total

    def release(self, count: int) -> None:
        """CAP only: decrement the current count by ``count``, floored at 0."""
        self._count = max(0, self._count - max(0, int(count)))
