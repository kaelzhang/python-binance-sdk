"""Read-only structures returned for rate-limit monitoring.

A :class:`RateLimitSnapshot` (with its per-pool :class:`RateLimitWindow`
entries) is what :meth:`binance.client.Client.rate_limit_snapshot` and
:meth:`binance.rate_limit.core.RateLimiter.snapshot` hand back. Both are frozen
dataclasses: they perform no I/O and are safe to store, compare, or poll
frequently from a monitoring loop or a pre-trade risk gate.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from binance.rate_limit.types import RateLimitScope, RateLimitType, RateLimitSource


@dataclass(frozen=True)
class RateLimitWindow:
    """A point-in-time view of a single rate-limit pool.

    Attributes:
        scope: :class:`~binance.rate_limit.types.RateLimitScope` member —
            compare with the member, e.g. ``w.scope == RateLimitScope.IP``.
            ``str(w.scope)`` yields the wire string (``'ip'``, ``'account'``,
            or ``'connection'``).
        type: :class:`~binance.rate_limit.types.RateLimitType` member — compare
            with the member, e.g. ``w.type == RateLimitType.REQUEST_WEIGHT``.
            ``str(w.type)`` yields the wire string (e.g. ``'request_weight'``,
            ``'orders'``, ``'ws_streams'``).
        interval: Window label such as ``'1m'`` or ``'10s'``; ``''`` for the
            window-less per-connection stream cap.
        used: Current usage within the window -- summed weight, event count, or
            current stream count, depending on the pool's kind.
        limit: The *effective* cap actually enforced: the documented limit
            already multiplied by the rule's safety ratio (so it may be lower
            than Binance's raw number).
        remaining: ``max(limit - used, 0)``. Floored at ``0`` even when ``used``
            overshoots ``limit`` (a header reconciliation can report usage above
            the safety-adjusted cap).
        utilization: ``used / limit``. ``0.0`` when idle; can exceed ``1.0``
            when an authoritative header reports usage above the effective cap.
        pending: Number of callers currently blocked or queued waiting on this
            pool. Only nonzero for ``SLEEP``-mode pools under contention.
        source: :class:`~binance.rate_limit.types.RateLimitSource` member —
            compare with the member, e.g. ``w.source == RateLimitSource.HEADER``.
            ``str(w.source)`` yields ``'header'`` when ``used`` reflects an
            authoritative Binance response header (reconciled and trustworthy),
            or ``'client'`` when it is only the local proactive estimate.
    """

    scope: RateLimitScope
    type: RateLimitType
    interval: str
    used: int
    limit: int
    remaining: int
    utilization: float
    pending: int
    source: RateLimitSource


@dataclass(frozen=True)
class RateLimitSnapshot:
    """An immutable, point-in-time view of every rate-limit pool.

    Returned by :meth:`binance.client.Client.rate_limit_snapshot` (and
    :meth:`binance.rate_limit.core.RateLimiter.snapshot`). It captures no
    network state of its own -- everything is local -- so it is cheap to take
    and safe to poll.

    Attributes:
        windows: One :class:`RateLimitWindow` per pool: the shared IP/account
            pools, plus a messages+streams pair for each registered WebSocket
            connection.
        pending: Total number of callers blocked or queued across all pools
            right now.
        retry_after: Seconds remaining on an active ``429``/``418`` server
            back-off, or ``None`` when no server-imposed wait is in effect.
        throttled: ``True`` when anything is queued (``pending > 0``) or a
            ``retry_after`` is active -- a quick "are we being limited right
            now?" flag.
        at: Wall-clock capture time as ``time.time()`` epoch seconds.
    """

    windows: Tuple[RateLimitWindow, ...]
    pending: int
    retry_after: Optional[int]
    throttled: bool
    at: float

    @property
    def max_utilization(self) -> float:
        """The highest :attr:`RateLimitWindow.utilization` across all pools.

        A single number for "how close to a limit are we?" -- convenient for
        alerting thresholds. Can exceed ``1.0`` (see
        :attr:`RateLimitWindow.utilization`). Returns ``0.0`` when there are no
        windows.
        """
        return max((w.utilization for w in self.windows), default=0.0)
