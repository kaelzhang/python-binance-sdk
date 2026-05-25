"""USDⓈ-M Futures market constants (hosts).

Genuinely market-agnostic constants and enums stay in
:mod:`binance.core.common.constants`; only the USDⓈ-M connection hosts live
here.

Host references:
- REST / fapi: https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
- Streams / fstream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams
- WS-API / fapi ws: reserved for future order / account endpoints.
"""

# REST API base host for USDⓈ-M Futures.
UM_REST_HOST = 'https://fapi.binance.com'

# WebSocket market-data stream base host.
UM_STREAM_HOST = 'wss://fstream.binance.com'

# WebSocket API base host (order / account endpoints; reserved for future use).
UM_WS_API_HOST = 'wss://ws-fapi.binance.com/ws-fapi/v1'
