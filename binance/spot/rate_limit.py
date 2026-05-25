"""Spot market default rate-limit rules and helpers.

The rate-limit *engine* (RateLimiter / buckets / rule types) is market-agnostic
and lives in :mod:`binance.core.rate_limit`. This module pins the Spot market's
default rule set and Spot-specific weight helpers.

Verified 2026-05-23 against Binance Spot API docs (rest-api.md LIMITS,
faqs/rate_limits.md).  Numeric limits are reconciled at runtime against
response headers and ``exchangeInfo``.
"""

from binance.core.common.constants import (
    WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW,
)
from binance.core.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)
from binance.spot.constants import (
    DEFAULT_REQUEST_WEIGHT_LIMIT, DEFAULT_REQUEST_WEIGHT_INTERVAL,
    DEFAULT_WEIGHT_SAFETY_RATIO,
    DEFAULT_RAW_REQUESTS_LIMIT, DEFAULT_RAW_REQUESTS_INTERVAL,
    DEFAULT_ORDERS_10S_LIMIT, DEFAULT_ORDERS_10S_INTERVAL,
    DEFAULT_ORDERS_1D_LIMIT, DEFAULT_ORDERS_1D_INTERVAL,
)


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

__all__ = ['depth_weight', 'DEFAULT_RULES']
