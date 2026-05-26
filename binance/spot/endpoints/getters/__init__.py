"""Spot WS-API getter mixins.

Pre-declared stub methods for every WS-API endpoint, organized into four
mixins by responsibility. The combined :class:`WsApiGetters` is the surface
that ``define_ws_getter`` patches at import time (see ``registry.py``) to
replace each stub with a real coroutine that issues the request and returns
the response ``result``.
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.spot.endpoints.getters.account import WsApiAccountGetters
from binance.spot.endpoints.getters.general import WsApiGeneralGetters
from binance.spot.endpoints.getters.market_data import WsApiMarketDataGetters
from binance.spot.endpoints.getters.trading import WsApiTradingGetters


class WsApiGetters(
    WsApiGeneralGetters,
    WsApiMarketDataGetters,
    WsApiAccountGetters,
    WsApiTradingGetters,
):
    """Internal mixin providing dynamically-generated async methods for every Binance WebSocket-API endpoint.

    The entire request/response surface -- general (``ping``/``time``/
    ``exchangeInfo``), market data (``depth``/``klines``/``trades.*``/
    ``ticker.*``/...), account (``account.*``/``myTrades``/...) and trading
    (``order.*``/``orderList.*``/``sor.*``/``openOrders.*``) -- is served over
    the WebSocket API rather than REST. Every method here is an ``await``-able
    coroutine that issues a single id-correlated request over the shared WS-API
    connection via :meth:`_ws_api_request` and returns the response ``result``.
    Public method names and signatures are identical to the former REST methods;
    only the transport changed.
    """

    _ws_api_request: Callable[..., Awaitable]
