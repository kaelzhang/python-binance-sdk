"""Built-in defaults for the rate-limit core.

Holds the response-header / weight helpers (:func:`parse_retry_after`,
:func:`depth_weight`) and the default
:class:`~binance.rate_limit.types.RateLimitRule` set for every documented
Binance pool: :data:`DEFAULT_RULES` (the shared IP/account pools, used by a new
:class:`~binance.rate_limit.core.RateLimiter`) plus :data:`WS_MESSAGE_RULE` and
:data:`WS_STREAMS_RULE` (instantiated per WebSocket connection). The numeric
limits live in :mod:`binance.common.constants` and are reconciled at runtime
against response headers and ``exchangeInfo``.
"""

from typing import Optional

from binance.core.common.constants import (
    HEADER_RETRY_AFTER,
    DEFAULT_REQUEST_WEIGHT_LIMIT, DEFAULT_REQUEST_WEIGHT_INTERVAL,
    DEFAULT_WEIGHT_SAFETY_RATIO,
    DEFAULT_RAW_REQUESTS_LIMIT, DEFAULT_RAW_REQUESTS_INTERVAL,
    DEFAULT_ORDERS_10S_LIMIT, DEFAULT_ORDERS_10S_INTERVAL,
    DEFAULT_ORDERS_1D_LIMIT, DEFAULT_ORDERS_1D_INTERVAL,
    WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW,
    WS_MAX_MESSAGES_PER_SEC, WS_MESSAGE_WINDOW,
    WS_MAX_STREAMS_PER_CONNECTION,
)
from binance.core.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)


def parse_retry_after(response) -> Optional[int]:
    """Read the integer `Retry-After` (seconds) from a response, or None.

    Only the integer-seconds form is parsed; Binance does not send the
    RFC 7231 HTTP-date form of `Retry-After`.
    """
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


DEFAULT_RULES = (
    RateLimitRule(RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT,
                  DEFAULT_REQUEST_WEIGHT_INTERVAL, DEFAULT_REQUEST_WEIGHT_LIMIT,
                  RateLimitKind.WEIGHT, EnforceMode.SLEEP,
                  DEFAULT_WEIGHT_SAFETY_RATIO),
    RateLimitRule(RateLimitScope.IP, RateLimitType.RAW_REQUESTS,
                  DEFAULT_RAW_REQUESTS_INTERVAL, DEFAULT_RAW_REQUESTS_LIMIT,
                  RateLimitKind.COUNT, EnforceMode.SLEEP),
    RateLimitRule(RateLimitScope.ACCOUNT, RateLimitType.ORDERS,
                  DEFAULT_ORDERS_10S_INTERVAL, DEFAULT_ORDERS_10S_LIMIT,
                  RateLimitKind.COUNT, EnforceMode.RAISE),
    RateLimitRule(RateLimitScope.ACCOUNT, RateLimitType.ORDERS,
                  DEFAULT_ORDERS_1D_INTERVAL, DEFAULT_ORDERS_1D_LIMIT,
                  RateLimitKind.COUNT, EnforceMode.RAISE),
    RateLimitRule(RateLimitScope.IP, RateLimitType.WS_CONNECTIONS,
                  WS_CONNECTION_WINDOW, WS_CONNECTION_SAFETY,
                  RateLimitKind.COUNT, EnforceMode.SLEEP),
)

WS_MESSAGE_RULE = RateLimitRule(
    RateLimitScope.CONNECTION, RateLimitType.WS_MESSAGES,
    WS_MESSAGE_WINDOW, WS_MAX_MESSAGES_PER_SEC,
    RateLimitKind.COUNT, EnforceMode.SLEEP)

WS_STREAMS_RULE = RateLimitRule(
    RateLimitScope.CONNECTION, RateLimitType.WS_STREAMS,
    0.0, WS_MAX_STREAMS_PER_CONNECTION, RateLimitKind.CAP, EnforceMode.RAISE)
