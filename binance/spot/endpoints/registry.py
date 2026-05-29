"""WS-API endpoint registry and stub-to-getter injection.

Maps every public python method name on :class:`WsApiGetters` to its Binance
WebSocket-API method name, security level, request weight, and whether it
consumes the account ORDERS pool. Importing this module triggers the
``define_ws_getter`` loop at the bottom, which patches each stub on the
combined :class:`WsApiGetters` class with a real coroutine.

Each entry's ``weight`` may be an ``int`` OR a callable ``(kwargs) -> int``
for endpoints whose weight depends on the request params (see ``weights.py``).

WS-API documentation:
https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/general-api-information
"""

from binance.core.common.constants import SecurityType
from binance.core.getters import define_getter as define_ws_getter
from binance.spot.endpoints.getters import WsApiGetters
from binance.spot.endpoints.weights import (
    _depth_weight,
    _execution_rules_weight,
    _my_prevented_matches_weight,
    _my_trades_weight,
    _open_orders_status_weight,
    _order_test_weight,
    _per_symbol_ticker_weight,
    _ticker_24hr_weight,
    _ticker_book_weight,
    _ticker_price_weight,
)


WS_APIS = [
    # ----- general ---------------------------------------------------------
    # These three historically took NO params on the REST path
    # (``params=False``); preserve that so no stray kwargs are forwarded.
    dict(
        name='ping',
        ws_method='ping',
        params=False,
        security_type=SecurityType.NONE,
        weight=1
    ),
    dict(
        name='get_server_time',
        ws_method='time',
        params=False,
        security_type=SecurityType.NONE,
        weight=1
    ),
    dict(
        name='get_exchange_info',
        ws_method='exchangeInfo',
        params=False,
        security_type=SecurityType.NONE,
        weight=20
    ),

    # ----- market data (NONE) ----------------------------------------------
    dict(
        name='get_orderbook',
        ws_method='depth',
        security_type=SecurityType.NONE,
        weight=_depth_weight
    ),
    dict(
        name='get_recent_trades',
        ws_method='trades.recent',
        security_type=SecurityType.NONE,
        weight=25
    ),
    dict(
        name='get_historical_trades',
        ws_method='trades.historical',
        # F-03: Binance reclassified historical trades from MARKET_DATA to
        # NONE (2023-07-11); over the WS-API it is a public (NONE) request.
        security_type=SecurityType.NONE,
        weight=25
    ),
    dict(
        name='get_aggregate_trades',
        ws_method='trades.aggregate',
        security_type=SecurityType.NONE,
        weight=4
    ),
    dict(
        name='get_klines',
        ws_method='klines',
        security_type=SecurityType.NONE,
        weight=2
    ),
    dict(
        name='get_average_price',
        ws_method='avgPrice',
        security_type=SecurityType.NONE,
        weight=2
    ),
    dict(
        name='get_ticker',
        ws_method='ticker.24hr',
        security_type=SecurityType.NONE,

        # 2 for one symbol; tiered by `symbols` count; 80 for all symbols.
        weight=_ticker_24hr_weight
    ),
    dict(
        name='get_ticker_price',
        ws_method='ticker.price',
        security_type=SecurityType.NONE,

        # 2 for a single symbol, else 4.
        weight=_ticker_price_weight
    ),
    dict(
        name='get_orderbook_ticker',
        ws_method='ticker.book',
        security_type=SecurityType.NONE,

        # 2 for a single symbol, else 4.
        weight=_ticker_book_weight
    ),
    dict(name='get_ui_klines', ws_method='uiKlines',
         security_type=SecurityType.NONE, weight=2),
    dict(name='get_rolling_window_ticker', ws_method='ticker',
         security_type=SecurityType.NONE, weight=_per_symbol_ticker_weight),
    dict(name='get_trading_day_ticker', ws_method='ticker.tradingDay',
         security_type=SecurityType.NONE, weight=_per_symbol_ticker_weight),
    dict(name='get_historical_block_trades', ws_method='blockTrades.historical',
         security_type=SecurityType.NONE, weight=25),
    dict(name='get_execution_rules', ws_method='executionRules',
         security_type=SecurityType.NONE, weight=_execution_rules_weight),
    dict(name='get_reference_price', ws_method='referencePrice',
         security_type=SecurityType.NONE, weight=2),
    dict(name='get_reference_price_calculation', ws_method='referencePrice.calculation',
         security_type=SecurityType.NONE, weight=2),

    # ----- account (USER_DATA) ---------------------------------------------
    dict(
        name='get_account',
        ws_method='account.status',
        security_type=SecurityType.USER_DATA,
        weight=20
    ),
    dict(
        name='get_trades',
        ws_method='myTrades',
        security_type=SecurityType.USER_DATA,

        # 5 when scoped by `orderId`, else 20.
        weight=_my_trades_weight
    ),
    dict(
        name='get_commission',
        ws_method='account.commission',
        security_type=SecurityType.USER_DATA,
        weight=20
    ),
    dict(
        name='get_order_rate_limit',
        ws_method='account.rateLimits.orders',
        security_type=SecurityType.USER_DATA,
        weight=40
    ),
    dict(
        name='get_prevented_matches',
        ws_method='myPreventedMatches',
        security_type=SecurityType.USER_DATA,

        # 2 when scoped by `preventedMatchId`, 20 when scoped by `orderId`.
        # Docs: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/account-requests
        weight=_my_prevented_matches_weight
    ),
    dict(
        name='get_allocations',
        ws_method='myAllocations',
        security_type=SecurityType.USER_DATA,
        weight=20
    ),
    dict(name='get_order_amendments', ws_method='order.amendments',
         security_type=SecurityType.USER_DATA, weight=4),
    dict(name='get_my_filters', ws_method='myFilters',
         security_type=SecurityType.USER_DATA, weight=40),

    # ----- session management (NONE) ---------------------------------------
    dict(name='get_session_status', ws_method='session.status',
         params=False, security_type=SecurityType.NONE, weight=2),
    dict(name='get_session_subscriptions', ws_method='session.subscriptions',
         params=False, security_type=SecurityType.NONE, weight=2),

    # ----- order.* ---------------------------------------------------------
    dict(
        name='create_order',
        ws_method='order.place',
        security_type=SecurityType.TRADE,
        weight=1,

        # Placing a new order counts against the account ORDERS pool.
        is_order=True
    ),
    dict(
        name='create_test_order',
        ws_method='order.test',
        security_type=SecurityType.TRADE,

        # 1, or 20 when `computeCommissionRates` is requested.
        weight=_order_test_weight
    ),
    dict(
        name='get_order',
        ws_method='order.status',
        security_type=SecurityType.USER_DATA,
        weight=4
    ),
    dict(
        name='cancel_order',
        ws_method='order.cancel',
        security_type=SecurityType.TRADE,
        weight=1
    ),
    dict(
        name='cancel_replace_order',
        ws_method='order.cancelReplace',
        security_type=SecurityType.TRADE,
        weight=1,

        # cancelReplace places a NEW order, so it consumes the ORDERS pool.
        is_order=True
    ),
    dict(
        name='amend_order',
        ws_method='order.amend.keepPriority',
        security_type=SecurityType.TRADE,
        weight=4,

        # Order Amend Keep Priority MODIFIES an order rather than placing one.
        # The Binance WS-API spec documents its "Unfilled Order Count" as 0,
        # so it does NOT consume the account ORDERS pool.
        is_order=False
    ),

    # ----- openOrders.* ----------------------------------------------------
    dict(
        name='get_open_orders',
        ws_method='openOrders.status',
        security_type=SecurityType.USER_DATA,

        # 6 with a `symbol`, 80 across all symbols.
        weight=_open_orders_status_weight
    ),
    dict(
        name='cancel_all_orders',
        ws_method='openOrders.cancelAll',
        security_type=SecurityType.TRADE,
        weight=1
    ),

    # ----- allOrders -------------------------------------------------------
    dict(
        name='get_all_orders',
        ws_method='allOrders',
        security_type=SecurityType.USER_DATA,
        weight=20
    ),

    # ----- sor.* -----------------------------------------------------------
    dict(
        name='create_sor_order',
        ws_method='sor.order.place',
        security_type=SecurityType.TRADE,
        weight=1,

        # Placing a new SOR order counts against the account ORDERS pool.
        is_order=True
    ),
    dict(name='create_test_sor_order', ws_method='sor.order.test',
         security_type=SecurityType.TRADE, weight=_order_test_weight, is_order=False),

    # ----- orderList.* -----------------------------------------------------
    dict(
        name='create_oco',
        ws_method='orderList.place.oco',
        security_type=SecurityType.TRADE,
        weight=1,

        # Placing a new OCO order list counts against the account ORDERS pool.
        is_order=True
    ),
    dict(
        name='create_oto',
        ws_method='orderList.place.oto',
        security_type=SecurityType.TRADE,
        weight=1,

        # Placing a new OTO order list counts against the account ORDERS pool.
        is_order=True
    ),
    dict(
        name='create_otoco',
        ws_method='orderList.place.otoco',
        security_type=SecurityType.TRADE,
        weight=1,

        # Placing a new OTOCO order list counts against the account ORDERS pool.
        is_order=True
    ),
    dict(name='create_opo', ws_method='orderList.place.opo',
         security_type=SecurityType.TRADE, weight=1, is_order=True),
    dict(name='create_opoco', ws_method='orderList.place.opoco',
         security_type=SecurityType.TRADE, weight=1, is_order=True),
    dict(
        name='cancel_oco',
        ws_method='orderList.cancel',
        security_type=SecurityType.TRADE,
        weight=1
    ),
    dict(
        name='get_oco',
        ws_method='orderList.status',
        security_type=SecurityType.USER_DATA,
        weight=4
    ),
    dict(
        name='get_all_oco',
        ws_method='allOrderLists',
        security_type=SecurityType.USER_DATA,
        weight=20
    ),
    dict(
        name='get_open_oco',
        ws_method='openOrderLists.status',
        security_type=SecurityType.USER_DATA,
        weight=6
    )
]


# Neither VSCode Python language server nor Jedi server could handle
#   class methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So we need to declare those methods (as stubs) with their docstrings first,
#   then override them.

for getter_setting in WS_APIS:
    define_ws_getter(WsApiGetters, **getter_setting)  # type: ignore[arg-type]  # WS_APIS entries are always valid; mypy cannot narrow dict[str, object]
