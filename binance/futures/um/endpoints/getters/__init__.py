"""USDⓈ-M Futures getter mixins.

Pre-declared stub methods for every USDⓈ-M Futures endpoint, organized into
three mixins by responsibility (UM has no `general` endpoints — no ping /
time / exchangeInfo). The combined :class:`UMFuturesGetters` is the surface
that ``define_getter`` patches at import time (see ``registry.py``) to
replace each stub with a real coroutine that issues the request via WS-API
or REST and returns the response.
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.futures.um.endpoints.getters.account import UMAccountGetters
from binance.futures.um.endpoints.getters.market_data import UMMarketDataGetters
from binance.futures.um.endpoints.getters.trading import UMTradingGetters


class UMFuturesGetters(
    UMMarketDataGetters,
    UMAccountGetters,
    UMTradingGetters,
):
    """Internal mixin providing async methods for every USDⓈ-M Futures endpoint.

    Covers two transports:

    - **WS-API** (trading / account): coroutines that issue a single
      id-correlated request over the shared WS-API connection via
      :meth:`_ws_api_request` — ``create_order``, ``modify_order``,
      ``cancel_order``, ``get_order``, ``get_account`` (v2),
      ``get_balance`` (v2), ``get_position``, ``get_position_mode``,
      ``create_algo_order``, ``cancel_algo_order``.
    - **REST** (market-data + trading/account/position): coroutines that issue
      an HTTP request via :meth:`_request` (RestTransport) and return the
      decoded JSON response.
    """

    _request: Callable[..., Awaitable]
    _ws_api_request: Callable[..., Awaitable]
