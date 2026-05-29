"""COIN-M Futures default rate-limit rules.

Verified 2026-05-25 against ``GET /dapi/v1/exchangeInfo`` ``rateLimits`` array
(live response):
- REQUEST_WEIGHT: 2400 / 1 min / IP
- ORDERS (account): 1200 / 1 min

Note: Unlike USDⓈ-M, the COIN-M ``exchangeInfo`` does NOT report a 10-second
ORDERS pool.  The 1-minute ORDERS pool is included for completeness / future
symmetry; the current phase implements only read-only market-data endpoints
which do not consume the ORDERS pool.

The RAW_REQUESTS pool is not reported by the COIN-M ``exchangeInfo``; it is
therefore omitted here.  The WS_CONNECTIONS pool reuses the common constant
(300/5min per IP) — Binance's general WS policy — since ``dstream.binance.com``
is governed by the same rule.

The rate-limit *engine* (RateLimiter / buckets / rule types) is market-agnostic
and lives in :mod:`binance.core.rate_limit`. This module pins the COIN-M
market's default rule set.
"""

from binance.core.common.constants import (
    WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW,
)
from binance.core.rate_limit.defaults import build_ws_message_rule
from binance.core.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)
from binance.futures.cm.constants import WS_MAX_MESSAGES_PER_SEC

# Confirmed from GET /dapi/v1/exchangeInfo rateLimits (2026-05-25):
#   {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 2400}
CM_REQUEST_WEIGHT_LIMIT = 2400
CM_REQUEST_WEIGHT_INTERVAL = 60.0   # 1 minute in seconds
CM_WEIGHT_SAFETY_RATIO = 0.9        # stay at 90% client-side

# Confirmed from GET /dapi/v1/exchangeInfo rateLimits (2026-05-25):
#   {'rateLimitType': 'ORDERS', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 1200}
CM_ORDERS_1M_LIMIT = 1200
CM_ORDERS_1M_INTERVAL = 60.0

DEFAULT_RULES = (
    RateLimitRule(
        RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT,
        CM_REQUEST_WEIGHT_INTERVAL, CM_REQUEST_WEIGHT_LIMIT,
        RateLimitKind.WEIGHT, EnforceMode.SLEEP,
        CM_WEIGHT_SAFETY_RATIO,
    ),
    # ORDERS pool included for completeness; market-data endpoints do not consume it.
    RateLimitRule(
        RateLimitScope.ACCOUNT, RateLimitType.ORDERS,
        CM_ORDERS_1M_INTERVAL, CM_ORDERS_1M_LIMIT,
        RateLimitKind.COUNT, EnforceMode.RAISE,
    ),
    RateLimitRule(
        RateLimitScope.IP, RateLimitType.WS_CONNECTIONS,
        WS_CONNECTION_WINDOW, WS_CONNECTION_SAFETY,
        RateLimitKind.COUNT, EnforceMode.SLEEP,
    ),
)

# COIN-M WS streams: 10 incoming messages per second per connection.
WS_MESSAGE_RULE = build_ws_message_rule(WS_MAX_MESSAGES_PER_SEC)

__all__ = ['DEFAULT_RULES', 'WS_MESSAGE_RULE']
