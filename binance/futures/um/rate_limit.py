"""USDⓈ-M Futures default rate-limit rules.

Verified 2026-05-25 against ``GET /fapi/v1/exchangeInfo`` ``rateLimits`` array
(live response):
- REQUEST_WEIGHT: 2400 / 1 min / IP
- ORDERS (account): 1200 / 1 min
- ORDERS (account): 300 / 10 s

The RAW_REQUESTS pool is not reported by the USDⓈ-M exchangeInfo (unlike Spot
which has a 300000/5min pool); it is therefore omitted here.  The WS_CONNECTIONS
pool reuses the common Spot constant (300/5min per IP) -- Binance's general WS
policy -- since fstream.binance.com is governed by the same rule.

The rate-limit *engine* (RateLimiter / buckets / rule types) is market-agnostic
and lives in :mod:`binance.core.rate_limit`. This module pins the USDⓈ-M
market's default rule set.
"""

from binance.core.common.constants import (
    WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW,
)
from binance.core.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)

# Confirmed from GET /fapi/v1/exchangeInfo rateLimits (2026-05-25):
#   {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 2400}
UM_REQUEST_WEIGHT_LIMIT = 2400
UM_REQUEST_WEIGHT_INTERVAL = 60.0   # 1 minute in seconds
UM_WEIGHT_SAFETY_RATIO = 0.9        # stay at 90% client-side

# Confirmed from GET /fapi/v1/exchangeInfo rateLimits (2026-05-25):
#   {'rateLimitType': 'ORDERS', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 1200}
#   {'rateLimitType': 'ORDERS', 'interval': 'SECOND', 'intervalNum': 10, 'limit': 300}
UM_ORDERS_1M_LIMIT = 1200
UM_ORDERS_1M_INTERVAL = 60.0
UM_ORDERS_10S_LIMIT = 300
UM_ORDERS_10S_INTERVAL = 10.0

DEFAULT_RULES = (
    RateLimitRule(
        RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT,
        UM_REQUEST_WEIGHT_INTERVAL, UM_REQUEST_WEIGHT_LIMIT,
        RateLimitKind.WEIGHT, EnforceMode.SLEEP,
        UM_WEIGHT_SAFETY_RATIO,
    ),
    # ORDERS pools are included for completeness / symmetry with Spot;
    # the current phase only implements read-only market-data endpoints
    # which do not consume the ORDERS pool.
    RateLimitRule(
        RateLimitScope.ACCOUNT, RateLimitType.ORDERS,
        UM_ORDERS_10S_INTERVAL, UM_ORDERS_10S_LIMIT,
        RateLimitKind.COUNT, EnforceMode.RAISE,
    ),
    RateLimitRule(
        RateLimitScope.ACCOUNT, RateLimitType.ORDERS,
        UM_ORDERS_1M_INTERVAL, UM_ORDERS_1M_LIMIT,
        RateLimitKind.COUNT, EnforceMode.RAISE,
    ),
    RateLimitRule(
        RateLimitScope.IP, RateLimitType.WS_CONNECTIONS,
        WS_CONNECTION_WINDOW, WS_CONNECTION_SAFETY,
        RateLimitKind.COUNT, EnforceMode.SLEEP,
    ),
)

__all__ = ['DEFAULT_RULES']
