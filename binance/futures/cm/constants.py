"""COIN-M Futures market constants (hosts).

Genuinely market-agnostic constants and enums stay in
:mod:`binance.core.common.constants`; only the COIN-M connection hosts live
here.

Host references:
- REST / dapi: https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info
- Streams / dstream: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
- WS-API / dapi ws: reserved for future order / account endpoints.
"""

# REST API base host for COIN-M Futures.
CM_REST_HOST = 'https://dapi.binance.com'

# WebSocket market-data stream base host.
CM_STREAM_HOST = 'wss://dstream.binance.com'

# WebSocket API base host (order / account endpoints; reserved for future use).
CM_WS_API_HOST = 'wss://ws-dapi.binance.com/ws-dapi/v1'

# COIN-M WS streams incoming-message rate-limit (docs:
# https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
# — "WebSocket connections have a limit of 10 incoming messages per second.").
WS_MAX_MESSAGES_PER_SEC = 10
