"""Dynamic-weight helpers for USDⓈ-M Futures endpoints.

Most endpoints have a static integer weight; the helpers here compute the
weight from request kwargs for endpoints whose cost depends on whether a
``symbol`` is supplied.
"""


def _premium_index_weight(kwargs) -> int:
    """`premiumIndex` weight: 1 when ``symbol`` is given, 10 otherwise."""
    return 1 if 'symbol' in kwargs else 10


def _um_open_orders_weight(kwargs) -> int:
    """`openOrders` weight: 1 when scoped to a ``symbol``, else 40."""
    return 1 if 'symbol' in kwargs else 40


def _um_open_algo_orders_weight(kwargs) -> int:
    """`openAlgoOrders` weight: 1 with ``symbol``, 40 without.

    Docs:
      https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Current-All-Algo-Open-Orders
    """
    return 1 if 'symbol' in kwargs else 40


def _um_force_orders_weight(kwargs) -> int:
    """`forceOrders` weight: 20 with ``symbol``, 50 without.

    Docs:
      https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Users-Force-Orders
    """
    return 20 if 'symbol' in kwargs else 50


def _depth_weight(kwargs) -> int:
    """`GET /fapi/v1/depth` weight depends on the ``limit`` parameter.

    Per Binance UM docs:
      limit 5/10/20/50 -> weight 2
      limit 100        -> weight 5
      limit 500        -> weight 10
      limit 1000       -> weight 20

    Defaults to limit=500 (Binance docs default).
    """
    limit = kwargs.get('limit', 500)
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20
