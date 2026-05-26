"""Dynamic-weight helpers for WS-API endpoints.

Most endpoints have a static integer weight; the helpers in this module compute
the weight from the request kwargs for endpoints whose cost depends on the
``symbol``/``symbols``/``limit``/``orderId``/``computeCommissionRates`` params.
"""

from binance.spot.rate_limit import depth_weight


def _order_test_weight(kwargs) -> int:
    """`order.test` weight: 20 when `computeCommissionRates` is truthy, else 1."""
    return 20 if kwargs.get('computeCommissionRates') else 1


def _open_orders_status_weight(kwargs) -> int:
    """`openOrders.status` weight: 6 when scoped to a `symbol`, else 80."""
    return 6 if 'symbol' in kwargs else 80


def _depth_weight(kwargs) -> int:
    """`depth` weight from the requested ``limit`` (Binance tiers)."""
    return depth_weight(int(kwargs.get('limit', 100)))


def _ticker_24hr_weight(kwargs) -> int:
    """`ticker.24hr` weight.

    A single ``symbol`` costs 2. A ``symbols`` list is tiered by count
    (<=20 -> 2, <=100 -> 40, else 80). Querying every symbol (neither
    ``symbol`` nor ``symbols``) is the most expensive at 80.
    """
    if 'symbol' in kwargs:
        return 2
    symbols = kwargs.get('symbols')
    if symbols is None:
        return 80
    count = len(symbols)
    if count <= 20:
        return 2
    if count <= 100:
        return 40
    return 80


def _ticker_price_weight(kwargs) -> int:
    """`ticker.price` weight: 2 for a single ``symbol``, else 4."""
    return 2 if 'symbol' in kwargs else 4


def _ticker_book_weight(kwargs) -> int:
    """`ticker.book` weight: 2 for a single ``symbol``, else 4."""
    return 2 if 'symbol' in kwargs else 4


def _my_trades_weight(kwargs) -> int:
    """`myTrades` weight: 5 when scoped by ``orderId``, else 20."""
    return 5 if 'orderId' in kwargs else 20


def _per_symbol_ticker_weight(kwargs) -> int:
    """`ticker` / `ticker.tradingDay` weight: 4 per symbol; capped at 200 once
    more than 50 symbols are requested. A single ``symbol`` costs 4."""
    symbols = kwargs.get('symbols')
    if symbols is not None:
        n = len(symbols)
        return 200 if n > 50 else 4 * n
    return 4


def _execution_rules_weight(kwargs) -> int:
    """`executionRules` weight: a single ``symbol`` costs 2; a ``symbols`` list
    costs 2 each capped at 40; ``symbolStatus`` or an unscoped query costs 40."""
    if 'symbol' in kwargs:
        return 2
    symbols = kwargs.get('symbols')
    if symbols is not None:
        return min(2 * len(symbols), 40)
    return 40
