"""Spot-market-specific constants (hosts and rate-limit defaults).

Genuinely market-agnostic constants and enums stay in
:mod:`binance.core.common.constants`; Spot-specific connection hosts and
default rate-limit values live here.
"""

REST_API_VERSION = 'v3'
REST_API_HOST = 'https://api.binance.com'
STREAM_HOST = 'wss://stream.binance.com'
WS_API_HOST = 'wss://ws-api.binance.com/ws-api/v3'

# Spot rate-limit defaults — verified 2026-05-23 against Binance Spot API docs
# (rest-api.md LIMITS, faqs/rate_limits.md)
DEFAULT_REQUEST_WEIGHT_LIMIT = 6000      # weight / interval / IP (since 2023-08-25)
DEFAULT_REQUEST_WEIGHT_INTERVAL = 60.0   # seconds
DEFAULT_WEIGHT_SAFETY_RATIO = 0.9        # only use 90% of the budget client-side

DEFAULT_RAW_REQUESTS_LIMIT = 300000
DEFAULT_RAW_REQUESTS_INTERVAL = 300.0    # 5 minutes
DEFAULT_ORDERS_10S_LIMIT = 100
DEFAULT_ORDERS_10S_INTERVAL = 10.0
DEFAULT_ORDERS_1D_LIMIT = 200000
DEFAULT_ORDERS_1D_INTERVAL = 86400.0
