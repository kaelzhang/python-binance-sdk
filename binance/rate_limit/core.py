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
        self._sync(RateLimitType.REQUEST_WEIGHT, used_weight)
        self._sync(RateLimitType.ORDERS, order_count)

    def _sync(self, limit_type, by_interval) -> None:
        for label, value in (by_interval or {}).items():
            for bucket in self._shared:
                if (bucket.rule.type == limit_type
                        and bucket.rule.interval == label):
                    bucket.sync(int(value))

    def note_retry_after(self, seconds, status) -> None:
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
        await self._consume(RateLimitType.REQUEST_WEIGHT, weight)
        await self._consume(RateLimitType.RAW_REQUESTS, 1)
        if is_order:
            await self._consume(RateLimitType.ORDERS, 1)

    # ---- proactive enforcement: WebSocket ----------------------------
    async def acquire_connection(self) -> None:
        await self._consume(RateLimitType.WS_CONNECTIONS, 1)

    def register_connection(self, connection_id: str) -> None:
        if connection_id not in self._connections:
            self._connections[connection_id] = {
                RateLimitType.WS_MESSAGES: RateLimitBucket(WS_MESSAGE_RULE),
                RateLimitType.WS_STREAMS: RateLimitBucket(WS_STREAMS_RULE),
            }

    def unregister_connection(self, connection_id: str) -> None:
        self._connections.pop(connection_id, None)

    async def acquire_message(self, connection_id: str) -> None:
        self.register_connection(connection_id)
        bucket = self._connections[connection_id][RateLimitType.WS_MESSAGES]
        if self._enabled:
            await bucket.acquire(1)
        else:
            bucket.record(1)

    def reserve_streams(self, connection_id: str, projected_total: int) -> None:
        self.register_connection(connection_id)
        bucket = self._connections[connection_id][RateLimitType.WS_STREAMS]
        bucket.reserve(projected_total)

    def release_streams(self, connection_id: str, count: int) -> None:
        conn = self._connections.get(connection_id)
        if conn is not None:
            conn[RateLimitType.WS_STREAMS].release(count)

    # ---- monitoring ---------------------------------------------------
    def snapshot(self) -> RateLimitSnapshot:
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
