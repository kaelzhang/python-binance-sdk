"""COIN-M Futures getter mixins.

Pre-declared stub methods for every COIN-M Futures endpoint, organized into
three mixins by responsibility (CM has no `general` endpoints — no ping /
time / exchangeInfo). The combined :class:`CMFuturesGetters` is the surface
that ``define_getter`` patches at import time (see ``registry.py``) to
replace each stub with a real coroutine that issues the request via WS-API
or REST and returns the response.
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.futures.cm.endpoints.getters.account import CMAccountGetters
from binance.futures.cm.endpoints.getters.market_data import CMMarketDataGetters
from binance.futures.cm.endpoints.getters.trading import CMTradingGetters


class CMFuturesGetters(
    CMMarketDataGetters,
    CMAccountGetters,
    CMTradingGetters,
):
    """Internal mixin providing async methods for every COIN-M Futures endpoint.

    Covers two transports:

    - **WS-API** (trading / account): coroutines that issue a single
      id-correlated request over the shared WS-API connection via
      :meth:`_ws_api_request` — ``create_order``, ``modify_order``,
      ``cancel_order``, ``get_order``, ``get_account``, ``get_balance``,
      ``get_position``.
    - **REST** (market-data + trading/account/position): coroutines that issue
      an HTTP request via :meth:`_request` (RestTransport) and return the
      decoded JSON response.
    """

    _request: Callable[..., Awaitable]
    _ws_api_request: Callable[..., Awaitable]
