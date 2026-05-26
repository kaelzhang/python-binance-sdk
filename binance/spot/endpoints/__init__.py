"""Spot WS-API endpoints package.

Public surface preserves the pre-split flat-module imports:
``WS_APIS``, ``WsApiGetters``, ``define_ws_getter`` and the nine
``_*_weight`` helpers used by tests. Importing this package triggers
``registry.py`` which patches the stub methods on :class:`WsApiGetters`
with real coroutines via ``define_ws_getter``.
"""

# Re-export weight helpers (used by tests).
from binance.spot.endpoints.weights import (
    _depth_weight,
    _execution_rules_weight,
    _my_trades_weight,
    _open_orders_status_weight,
    _order_test_weight,
    _per_symbol_ticker_weight,
    _ticker_24hr_weight,
    _ticker_book_weight,
    _ticker_price_weight,
)

# Re-export the combined getter class (consumed by ``binance.spot.client``).
from binance.spot.endpoints.getters import WsApiGetters

# Importing ``registry`` triggers the ``define_ws_getter`` injection loop that
# patches each stub on :class:`WsApiGetters` with a real coroutine.
from binance.spot.endpoints.registry import (
    WS_APIS,
    define_ws_getter,
)


__all__ = [
    'WS_APIS',
    'WsApiGetters',
    'define_ws_getter',
    '_depth_weight',
    '_execution_rules_weight',
    '_my_trades_weight',
    '_open_orders_status_weight',
    '_order_test_weight',
    '_per_symbol_ticker_weight',
    '_ticker_24hr_weight',
    '_ticker_book_weight',
    '_ticker_price_weight',
]
