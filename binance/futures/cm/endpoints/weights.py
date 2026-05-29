"""Dynamic-weight helpers for COIN-M Futures endpoints.

Most endpoints have a static integer weight; the helpers here compute the
weight from request kwargs for endpoints whose cost depends on whether a
``symbol`` (or ``pair``) is supplied.
"""


def _cm_open_orders_weight(kwargs) -> int:
    """`openOrders` weight: 1 when scoped to a ``symbol``, else 40."""
    return 1 if 'symbol' in kwargs else 40


def _cm_all_orders_weight(kwargs) -> int:
    """`allOrders` weight: 20 with ``symbol``; 40 with ``pair``.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/All-Orders
    """
    return 40 if 'pair' in kwargs else 20


def _cm_user_trades_weight(kwargs) -> int:
    """`userTrades` weight: 20 with ``symbol``; 40 with ``pair``.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Account-Trade-List
    """
    return 40 if 'pair' in kwargs else 20


def _depth_weight(kwargs) -> int:
    """`GET /dapi/v1/depth` weight depends on the ``limit`` parameter.

    Per Binance COIN-M docs (same table as USDⓈ-M):
      limit 5/10/20/50 -> weight 2
      limit 100        -> weight 5
      limit 500        -> weight 10
      limit 1000       -> weight 20

    Defaults to limit=500 (Binance docs default).

    Defined locally rather than imported from UM to respect the layering
    rule (no ``cm -> um`` imports).
    """
    limit = kwargs.get('limit', 500)
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20
