"""COIN-M Futures endpoint registry and getter mixins.

Public surface preserves the pre-split flat-module imports:
``WS_API_ENDPOINTS``, ``REST_ENDPOINTS``, ``CMFuturesGetters`` and the
``_*_weight`` helpers used by tests. Importing this package triggers
``registry.py`` which patches the stub methods on :class:`CMFuturesGetters`
with real coroutines via ``define_getter``.
"""

# Weight helpers (used by registry and tests).
from binance.futures.cm.endpoints.weights import (
    _cm_all_orders_weight,
    _cm_open_orders_weight,
    _cm_user_trades_weight,
    _depth_weight,
)

# Combined getter class (consumed by ``binance.futures.cm.client``).
from binance.futures.cm.endpoints.getters import CMFuturesGetters

# Importing ``registry`` triggers the ``define_getter`` injection loop that
# patches each stub on :class:`CMFuturesGetters` with a real coroutine.
from binance.futures.cm.endpoints.registry import (
    REST_ENDPOINTS,
    WS_API_ENDPOINTS,
)


__all__ = [
    # Endpoint registries
    'REST_ENDPOINTS',
    'WS_API_ENDPOINTS',
    # Combined getter mixin
    'CMFuturesGetters',
    # Weight helpers
    '_cm_all_orders_weight',
    '_cm_open_orders_weight',
    '_cm_user_trades_weight',
    '_depth_weight',
]
