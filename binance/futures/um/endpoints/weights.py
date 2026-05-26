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
