"""Spot-market-specific constants (hosts).

Genuinely market-agnostic constants and enums stay in
:mod:`binance.core.common.constants`; only the Spot connection hosts live here.
"""

REST_API_VERSION = 'v3'
REST_API_HOST = 'https://api.binance.com'
STREAM_HOST = 'wss://stream.binance.com'
WS_API_HOST = 'wss://ws-api.binance.com/ws-api/v3'
