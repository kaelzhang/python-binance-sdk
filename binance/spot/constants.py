"""Spot-market-specific constants (hosts and rate-limit defaults).

Genuinely market-agnostic constants and enums stay in
:mod:`binance.core.common.constants`; Spot-specific connection hosts and
default rate-limit values live here.
"""

REST_API_VERSION = 'v3'
REST_API_HOST = 'https://api.binance.com'
STREAM_HOST = 'wss://stream.binance.com'
WS_API_HOST = 'wss://ws-api.binance.com/ws-api/v3'

# Spot rate-limit defaults — verified 2026-05-30 against Binance Spot API docs.
# REST limits: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits
# WS-API ORDERS pool: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/rate-limits
DEFAULT_REQUEST_WEIGHT_LIMIT = 6000      # weight / interval / IP (since 2023-08-25)
DEFAULT_REQUEST_WEIGHT_INTERVAL = 60.0   # seconds
DEFAULT_WEIGHT_SAFETY_RATIO = 0.9        # only use 90% of the budget client-side

DEFAULT_RAW_REQUESTS_LIMIT = 300000
DEFAULT_RAW_REQUESTS_INTERVAL = 300.0    # 5 minutes
DEFAULT_ORDERS_10S_LIMIT = 50            # 50 orders / 10s (docs WS-API rate-limits)
DEFAULT_ORDERS_10S_INTERVAL = 10.0
DEFAULT_ORDERS_1D_LIMIT = 160000         # 160000 orders / day (docs WS-API rate-limits)
DEFAULT_ORDERS_1D_INTERVAL = 86400.0

# Spot WS streams incoming-message rate-limit (docs:
# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
# — "WebSocket connections have a limit of 5 incoming messages per second.").
WS_MAX_MESSAGES_PER_SEC = 5
