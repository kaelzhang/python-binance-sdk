"""The unified rate-limit core.

:class:`RateLimiter` is the single source of truth for every Binance rate-limit
pool. It owns one :class:`~binance.rate_limit.bucket.RateLimitBucket` per shared
IP/account pool plus per-connection message/stream buckets, gates the REST and
WebSocket transports proactively (``acquire_*``), reconciles against response
headers and ``exchangeInfo``, and exposes a read-only
:class:`~binance.rate_limit.snapshot.RateLimitSnapshot` for monitoring. A
``Client`` keeps one private instance, surfaced only through
``client.rate_limit_snapshot()``.
"""

import time
from typing import Dict, Iterable, List, Optional

from binance.rate_limit.types import RateLimitType, RateLimitRule
from binance.rate_limit.bucket import RateLimitBucket
from binance.rate_limit.snapshot import RateLimitWindow, RateLimitSnapshot
from binance.rate_limit.defaults import (
    DEFAULT_RULES, WS_MESSAGE_RULE, WS_STREAMS_RULE
)


_EXCHANGE_INFO_TYPE = {
    'REQUEST_WEIGHT': RateLimitType.REQUEST_WEIGHT,
    'ORDERS': RateLimitType.ORDERS,
    'RAW_REQUESTS': RateLimitType.RAW_REQUESTS,
}

_INTERVAL_SECONDS = {'SECOND': 1, 'MINUTE': 60, 'HOUR': 3600, 'DAY': 86400}


class RateLimiter:
    """Single source of truth for every Binance rate-limit pool.

    Shared IP/account pools (weight, raw, orders, ws-connections) live here; the
    per-connection pools (ws-messages, ws-streams) are created per connection.
    When `enabled` is False the proactive `acquire_*` calls only record usage
    (so monitoring still works) and never block or raise; the ws-streams CAP is
    a hard Binance limit and is always enforced.
    """

    def __init__(
        self,
        *,
        rules: Iterable[RateLimitRule] = DEFAULT_RULES,
        enabled: bool = True
    ) -> None:
        self._enabled = enabled
        self._shared: List[RateLimitBucket] = [RateLimitBucket(r) for r in rules]
        self._connections: Dict[str, Dict[RateLimitType, RateLimitBucket]] = {}
        self._retry_after_until: Optional[float] = None

    def _buckets_of(self, limit_type: RateLimitType) -> List[RateLimitBucket]:
        return [b for b in self._shared if b.rule.type == limit_type]

    # ---- configuration / reconciliation ------------------------------
    def configure_from_exchange_info(self, rate_limits) -> None:
        """Update pool limits from an ``exchangeInfo`` ``rateLimits`` array.

        Lets the limits track your account's real caps (e.g. a higher VIP tier)
        instead of the conservative built-in defaults. Each entry is matched to
        a shared bucket by ``rateLimitType`` and by interval
        (``interval``x``intervalNum`` -> seconds); unknown types/intervals are
        ignored. The client calls this automatically for any REST response that
        carries a ``rateLimits`` array.

        Args:
            rate_limits: The ``rateLimits`` list from an ``exchangeInfo``
                response (``None`` or empty is tolerated as a no-op).
        """
        for entry in rate_limits or []:
            limit_type = _EXCHANGE_INFO_TYPE.get(entry.get('rateLimitType'))
            if limit_type is None:
                continue
            unit = _INTERVAL_SECONDS.get(entry.get('interval'))
            if unit is None:
                continue
            seconds = float(unit * int(entry.get('intervalNum', 1)))
            for bucket in self._shared:
                if (bucket.rule.type == limit_type
                        and bucket.rule.interval_seconds == seconds):
                    bucket.set_limit(int(entry['limit']))

    def sync_from_headers(self, used_weight, order_count) -> None:
        """Reconcile the weight and order pools against authoritative headers.

        Each mapping is keyed by interval label (e.g. ``'1m'``, ``'10s'``) and
        applied to the matching bucket via :meth:`RateLimitBucket.sync`, so a
        server reading can only raise a pool's ``used``, never hide local usage.
        Called after every REST response.

        Args:
            used_weight: ``{interval: used}`` from ``X-MBX-USED-WEIGHT-*``.
            order_count: ``{interval: used}`` from ``X-MBX-ORDER-COUNT-*``.
        """
        self._sync(RateLimitType.REQUEST_WEIGHT, used_weight)
        self._sync(RateLimitType.ORDERS, order_count)

    def _sync(self, limit_type, by_interval) -> None:
        for label, value in (by_interval or {}).items():
            for bucket in self._shared:
                if (bucket.rule.type == limit_type
                        and bucket.rule.interval == label):
                    bucket.sync(int(value))

    def note_retry_after(self, seconds, status) -> None:
        """Record a server-imposed back-off deadline from a 429/418 response.

        A snapshot then reports the remaining seconds as
        :attr:`RateLimitSnapshot.retry_after` and sets ``throttled`` until it
        elapses. A falsy or non-positive ``seconds`` is ignored.

        Args:
            seconds: The ``Retry-After`` value (seconds) parsed from the
                response, or ``None``.
            status: The HTTP status that triggered it (e.g. ``429``/``418``);
                accepted for context and forward compatibility.
        """
        if seconds and int(seconds) > 0:
            self._retry_after_until = time.time() + int(seconds)

    def _retry_after(self) -> Optional[int]:
        if self._retry_after_until is None:
            return None
        remaining = int(round(self._retry_after_until - time.time()))
        if remaining <= 0:
            self._retry_after_until = None
            return None
        return remaining

    # ---- proactive enforcement: REST ---------------------------------
    async def _consume(self, limit_type: RateLimitType, cost: int) -> None:
        for bucket in self._buckets_of(limit_type):
            if self._enabled:
                await bucket.acquire(cost)
            else:
                bucket.record(cost)

    async def acquire_rest(self, *, weight: int, is_order: bool) -> None:
        """Account (and, if enabled, gate) a REST request before it is sent.

        Consumes the request's ``weight`` from the IP request-weight pool, one
        unit from the IP raw-requests pool, and -- when ``is_order`` -- one unit
        from each account orders pool. When the guard is enabled this may
        ``await`` (weight/raw are ``SLEEP``) or raise
        :class:`~binance.common.exceptions.RateLimitReachedException` (orders are
        ``RAISE``); when disabled it only records usage.

        Args:
            weight: The endpoint's request weight (keyword-only).
            is_order: ``True`` for order-placing endpoints, so the account
                orders pools are also consumed (keyword-only).

        Raises:
            RateLimitReachedException: If an order would exceed an orders pool
                (guard enabled).
        """
        await self._consume(RateLimitType.REQUEST_WEIGHT, weight)
        await self._consume(RateLimitType.RAW_REQUESTS, 1)
        if is_order:
            await self._consume(RateLimitType.ORDERS, 1)

    # ---- proactive enforcement: WebSocket ----------------------------
    async def acquire_connection(self) -> None:
        """Account one WebSocket connection attempt against the IP pool.

        Called by a :class:`~binance.subscribe.stream.Stream` before each
        connect, so a reconnect storm stays under Binance's 300/5min cap. May
        ``await`` when the guard is enabled (the pool is ``SLEEP``-enforced).
        """
        await self._consume(RateLimitType.WS_CONNECTIONS, 1)

    def register_connection(self, connection_id: str) -> None:
        """Create the per-connection message and stream buckets for ``connection_id``.

        Idempotent: an already-registered id is left untouched (its live counts
        are preserved). The per-acquire helpers auto-register on first use, so
        calling this explicitly is optional.

        Args:
            connection_id: Stable id for one WebSocket connection (e.g.
                ``'data'`` or ``'user'``).
        """
        if connection_id not in self._connections:
            self._connections[connection_id] = {
                RateLimitType.WS_MESSAGES: RateLimitBucket(WS_MESSAGE_RULE),
                RateLimitType.WS_STREAMS: RateLimitBucket(WS_STREAMS_RULE),
            }

    def unregister_connection(self, connection_id: str) -> None:
        """Drop a connection's message and stream buckets (e.g. on close).

        Their usage no longer appears in a snapshot. Unknown ids are ignored.
        """
        self._connections.pop(connection_id, None)

    async def acquire_message(self, connection_id: str) -> None:
        """Account one outgoing message on ``connection_id`` (5/s per connection).

        Auto-registers the connection if needed. May ``await`` when the guard is
        enabled (the per-connection message pool is ``SLEEP``-enforced); only
        records usage when disabled.

        Args:
            connection_id: The sending connection's id.
        """
        self.register_connection(connection_id)
        bucket = self._connections[connection_id][RateLimitType.WS_MESSAGES]
        if self._enabled:
            await bucket.acquire(1)
        else:
            bucket.record(1)

    def reserve_streams(self, connection_id: str, projected_total: int) -> None:
        """Enforce the 1024-streams-per-connection cap (absolute set).

        Auto-registers the connection, then sets its stream count to
        ``projected_total`` (a set, not an increment -- callers pass the intended
        new total, which keeps resubscribe idempotent).

        Args:
            connection_id: The connection whose streams are being set.
            projected_total: The intended total stream count after the change.

        Raises:
            TooManyStreamsException: If ``projected_total`` exceeds the cap.
        """
        self.register_connection(connection_id)
        bucket = self._connections[connection_id][RateLimitType.WS_STREAMS]
        bucket.reserve(projected_total)

    def release_streams(self, connection_id: str, count: int) -> None:
        """Decrease a connection's reserved stream count by ``count``.

        The inverse of :meth:`reserve_streams` when streams are unsubscribed.
        No-op for an unknown connection id.
        """
        conn = self._connections.get(connection_id)
        if conn is not None:
            conn[RateLimitType.WS_STREAMS].release(count)

    # ---- monitoring ---------------------------------------------------
    def snapshot(self) -> RateLimitSnapshot:
        """Capture a read-only :class:`RateLimitSnapshot` of every pool.

        Emits one window per shared pool plus the message+stream pair of each
        registered connection, and aggregates total pending, the remaining
        server ``retry_after``, and the ``throttled`` flag. Local and
        allocation-cheap -- no network, safe to poll frequently.
        """
        windows: List[RateLimitWindow] = []
        total_pending = 0
        for bucket in self._shared:
            windows.append(self._window(bucket))
            total_pending += bucket.pending
        for conn in self._connections.values():
            for bucket in conn.values():
                windows.append(self._window(bucket))
                total_pending += bucket.pending
        retry_after = self._retry_after()
        return RateLimitSnapshot(
            windows=tuple(windows),
            pending=total_pending,
            retry_after=retry_after,
            throttled=total_pending > 0 or retry_after is not None,
            at=time.time(),
        )

    def _window(self, bucket: RateLimitBucket) -> RateLimitWindow:
        used = bucket.used
        limit = bucket.effective_limit
        return RateLimitWindow(
            scope=bucket.rule.scope.value,
            type=bucket.rule.type.value,
            interval=bucket.rule.interval,
            used=used,
            limit=limit,
            remaining=max(0, limit - used),
            utilization=used / limit,
            pending=bucket.pending,
            source='header' if bucket.has_authoritative else 'client',
        )
