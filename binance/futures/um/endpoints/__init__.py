"""USDⓈ-M Futures endpoint registry and getter mixins.

Public surface preserves the pre-split flat-module imports:
``WS_API_ENDPOINTS``, ``REST_ENDPOINTS``, ``UMFuturesGetters`` and the two
``_*_weight`` helpers used by tests. Importing this package triggers
``registry.py`` which patches the stub methods on :class:`UMFuturesGetters`
with real coroutines via ``define_getter``.
"""

# Weight helpers (used by registry and tests).
from binance.futures.um.endpoints.weights import (
    _depth_weight,
    _premium_index_weight,
    _um_open_orders_weight,
)

# Combined getter class (consumed by ``binance.futures.um.client``).
from binance.futures.um.endpoints.getters import UMFuturesGetters

# Importing ``registry`` triggers the ``define_getter`` injection loop that
# patches each stub on :class:`UMFuturesGetters` with a real coroutine.
from binance.futures.um.endpoints.registry import (
    REST_ENDPOINTS,
    WS_API_ENDPOINTS,
)


__all__ = [
    # Endpoint registries
    'REST_ENDPOINTS',
    'WS_API_ENDPOINTS',
    # Combined getter mixin
    'UMFuturesGetters',
    # Weight helpers
    '_depth_weight',
    '_premium_index_weight',
    '_um_open_orders_weight',
]
