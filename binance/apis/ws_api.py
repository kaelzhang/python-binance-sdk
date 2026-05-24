from typing import (
    Awaitable,
    Callable,
    Union
)

from binance.common.constants import SecurityType

# WS-API trading endpoints ref:
# https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-api.md
#
# Each entry maps a public python method name to its Binance WebSocket-API
# method, security level, request weight, and whether it consumes the account
# ORDERS pool (`is_order`). `weight` may be an int OR a callable
# `(kwargs) -> int` for endpoints whose weight depends on the request params.


def _order_test_weight(kwargs) -> int:
    """`order.test` weight: 20 when `computeCommissionRates` is truthy, else 1."""
    return 20 if kwargs.get('computeCommissionRates') else 1


def _open_orders_status_weight(kwargs) -> int:
    """`openOrders.status` weight: 6 when scoped to a `symbol`, else 80."""
    return 6 if 'symbol' in kwargs else 80


WS_APIS = [
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


def define_ws_getter(
    Target,
    name,
    ws_method,
    params=True,
    *,
    security_type,
    weight: Union[int, Callable[[dict], int]],
    is_order=False
):
    """Internal factory: build a concrete async WS-API closure and install it on ``Target``.

    Mirrors ``binance.apis.rest.define_getter`` for the WebSocket-API path. The
    generated coroutine forwards its keyword arguments straight to
    :meth:`_ws_api_request` with the registered ``security``/``weight``/
    ``is_order``. ``weight`` may be a static ``int`` or a callable
    ``(kwargs) -> int`` resolved per call for params-dependent endpoints
    (e.g. ``order.test``, ``openOrders.status``).
    """
    weight_is_dynamic = callable(weight)

    def getter(self, **kwargs):
        resolved_weight = weight(kwargs) if weight_is_dynamic else weight

        return self._ws_api_request(
            ws_method,
            kwargs if params else None,
            security=security_type,
            weight=resolved_weight,
            is_order=is_order
        )

    origin = getattr(Target, name)

    # Migrate the docstring to the new getter.
    getter.__doc__ = origin.__doc__

    setattr(Target, name, getter)


# Neither VSCode Python language server nor Jedi server could handle
#   class methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So, however, we need to just create those methods and docstrings first,
#   then override them.

# pylint: disable=no-member


class WsApiGetters:
    """Internal mixin providing dynamically-generated async methods for Binance WebSocket-API trading endpoints.

    The trading surface (``order.*``, ``orderList.*``, ``sor.*``,
    ``openOrders.*``) is served over the WebSocket API rather than REST. Every
    method here is an ``await``-able coroutine that issues a single id-correlated
    request over the shared WS-API connection via :meth:`_ws_api_request` and
    returns the response ``result``. Public method names and signatures are
    identical to the former REST methods; only the transport changed.
    """

    _ws_api_request: Callable[..., Awaitable]

    def create_order(self, **kwargs) -> Awaitable:
        """Sends in a new order.

        Weight: 1

        Args:
            symbol (str): The symbol name.
            side (OrderSide):
            type (OrderType):
            timeInForce (:obj:`TimeInForce`, optional):
            quantity (:obj:`float`, optional):
            quoteOrderQty (:obj:`float`, optional):
            price (:obj:`float`, optional):
            newClientOrderId (:obj:`str`, optional): A unique id for the order. Automatically generated if not sent.
            stopPrice (:obj:`float`, optional): Used with `STOP_LOSS`, `STOP_LOSS_LIMIT`, `TAKE_PROFIT`, and `TAKE_PROFIT_LIMIT` orders.
            icebergQty (:obj:`float`, optional): Used with `LIMIT`, `STOP_LOSS_LIMIT`, and `TAKE_PROFIT_LIMIT` to create an iceberg order.
            newOrderRespType (:obj:`OrderRespType`, optional): Set the response JSON. `ACK`, `RESULT`, or `FULL`; `MARKET` and `LIMIT` order types default to `FULL`, all other orders default to `ACK`.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Additional mandatory parameters based on ``type`` (OrderType):
            LIMIT: ``timeInForce``, ``quantity``, ``price``
            MARKET:	``quantity`` or ``quoteOrderQty``
            STOP_LOSS: ``quantity``, ``stopPrice``
            STOP_LOSS_LIMIT: ``timeInForce``, ``quantity``, ``price``, ``stopPrice``
            TAKE_PROFIT: ``quantity``, ``stopPrice``
            TAKE_PROFIT_LIMIT: ``timeInForce``, ``quantity``, ``price``, ``stopPrice``
            LIMIT_MAKER: ``quantity``, ``price``

        Returns:
            Response `ACK`::

                {
                    'symbol': 'BTCUSDT',
                    'orderId': 28,
                    'orderListId': -1, # Unless OCO, value will be - 1
                    'clientOrderId': '6gCrw2kRUAF9CvJDGP16IP',
                    'transactTime': 1507725176595
                }

            Response `RESULT`::

                {
                    'symbol': 'BTCUSDT',
                    'orderId': 28,
                    'orderListId': -1, # Unless OCO, value will be - 1
                    'clientOrderId': '6gCrw2kRUAF9CvJDGP16IP',
                    'transactTime': 1507725176595,
                    'price': '0.00000000',
                    'origQty': '10.00000000',
                    'executedQty': '10.00000000',
                    'cummulativeQuoteQty': '10.00000000',
                    'status': 'FILLED',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'SELL'
                }

            Response `FULL`::

                {
                    'symbol': 'BTCUSDT',
                    'orderId': 28,
                    'orderListId': -1, # Unless OCO, value will be - 1
                    'clientOrderId': '6gCrw2kRUAF9CvJDGP16IP',
                    'transactTime': 1507725176595,
                    'price': '0.00000000',
                    'origQty': '10.00000000',
                    'executedQty': '10.00000000',
                    'cummulativeQuoteQty': '10.00000000',
                    'status': 'FILLED',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'SELL',
                    'fills': [
                        {
                            'price': '4000.00000000',
                            'qty': '1.00000000',
                            'commission': '4.00000000',
                            'commissionAsset': 'USDT'
                        },
                        {
                            'price': '3999.00000000',
                            'qty': '5.00000000',
                            'commission': '19.99500000',
                            'commissionAsset': 'USDT'
                        }

                        # ,...
                    ]
                }
        """
        ...  # pragma: no cover

    def create_test_order(self, **kwargs) -> Awaitable:
        """Tests new order creation and signature/recvWindow long. Creates and validates a new order but does not send it into the matching engine.

        Which has the same parameters as `client.create_order()`, plus:

        Args:
            computeCommissionRates (:obj:`bool`, optional): When `True`, also
                computes and returns the commission rates for the order. This
                raises the request weight from 1 to 20.
        """
        ...  # pragma: no cover

    def get_order(self, **kwargs) -> Awaitable:
        """Checks an order's status.

        Args:
            symbol (str):
            orderId (:obj:`long`, optional):
            origClientOrderId (:obj:`str`, optional): The value cannot be greater than 60000
            recvWindow (:obj:`long`, optional):
            timestamp (long):

            Either ``orderId`` or ``origClientOrderId`` must be sent.
            For some historical orders `cummulativeQuoteQty` will be < 0, meaning the data is not available at this time.

        Returns:
            dict: A dict of order info. For example::

                {
                    'symbol': 'LTCBTC',
                    'orderId': 1,
                    'orderListId': -1 # Unless part of an OCO, the value will always be - 1.
                    'clientOrderId': 'myOrder1',
                    'price': '0.1',
                    'origQty': '1.0',
                    'executedQty': '0.0',
                    'cummulativeQuoteQty': '0.0',
                    'status': 'NEW',
                    'timeInForce': 'GTC',
                    'type': 'LIMIT',
                    'side': 'BUY',
                    'stopPrice': '0.0',
                    'icebergQty': '0.0',
                    'time': 1499827319559,
                    'updateTime': 1499827319559,
                    'isWorking': True,
                    'origQuoteOrderQty': '0.000000'
                }
        """
        ...  # pragma: no cover

    def cancel_order(self, **kwargs) -> Awaitable:
        """Cancel an active order.

        Args:
            symbol (str):
            orderId (:obj:`long`, optional):
            origClientOrderId (:obj:`str`, optional):
            newClientOrderId (:obj:`str`, optional): Used to uniquely identify this cancel. Automatically generated by default.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000
            timestamp (long):

            Either ``orderId`` or ``origClientOrderId`` must be sent.

        Returns:
            dict: A dict of order status. For example::

                {
                    'symbol': 'LTCBTC',
                    'origClientOrderId': 'myOrder1',
                    'orderId': 4,
                    'orderListId': -1, # Unless part of an OCO, the value will always be - 1.
                    'clientOrderId': 'cancelMyOrder1',
                    'price': '2.00000000',
                    'origQty': '1.00000000',
                    'executedQty': '0.00000000',
                    'cummulativeQuoteQty': '0.00000000',
                    'status': 'CANCELED',
                    'timeInForce': 'GTC',
                    'type': 'LIMIT',
                    'side': 'BUY'
                }

        """
        ...  # pragma: no cover

    def cancel_replace_order(self, **kwargs) -> Awaitable:
        """Cancel an existing order and place a new order on the same symbol atomically.

        Filters and order count are evaluated before the processing of the
        cancellation and order placement.

        Weight: 1

        Args:
            symbol (str):
            cancelReplaceMode (str): ``STOP_ON_FAILURE`` (if the cancel request
                fails, the new order is not placed) or ``ALLOW_FAILURE`` (new
                order placement is attempted even if the cancel request fails).
            side (OrderSide):
            type (OrderType):
            cancelOrderId (:obj:`long`, optional): Cancel an order by ``orderId``.
            cancelOrigClientOrderId (:obj:`str`, optional): Cancel an order by client order id.
            cancelNewClientOrderId (:obj:`str`, optional): Used to uniquely identify this cancel. Automatically generated by default.
            timeInForce (:obj:`TimeInForce`, optional):
            quantity (:obj:`float`, optional):
            quoteOrderQty (:obj:`float`, optional):
            price (:obj:`float`, optional):
            newClientOrderId (:obj:`str`, optional): Used to identify the new order.
            stopPrice (:obj:`float`, optional):
            icebergQty (:obj:`float`, optional):
            newOrderRespType (:obj:`OrderRespType`, optional):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

            Either ``cancelOrderId`` or ``cancelOrigClientOrderId`` must be sent.

        Returns:
            dict: A dict carrying both the cancel result and the new order
            result, for example::

                {
                    'cancelResult': 'SUCCESS',
                    'newOrderResult': 'SUCCESS',
                    'cancelResponse': {
                        'symbol': 'BTCUSDT',
                        'origClientOrderId': 'DnLo3vTAQcjha43lAZhZ0y',
                        'orderId': 9,
                        'orderListId': -1,
                        'clientOrderId': 'osxN3JXAtJvKvCqGeMWMVR',
                        'price': '0.01000000',
                        'origQty': '0.000100',
                        'executedQty': '0.00000000',
                        'cummulativeQuoteQty': '0.00000000',
                        'status': 'CANCELED',
                        'timeInForce': 'GTC',
                        'type': 'LIMIT',
                        'side': 'SELL'
                    },
                    'newOrderResponse': {
                        'symbol': 'BTCUSDT',
                        'orderId': 10,
                        'orderListId': -1,
                        'clientOrderId': 'wOceeeOzNORyLiQfw7jd8S',
                        'transactTime': 1507725176595,
                        'price': '0.00000000',
                        'origQty': '0.000100',
                        'executedQty': '0.00000000',
                        'cummulativeQuoteQty': '0.00000000',
                        'status': 'NEW',
                        'timeInForce': 'GTC',
                        'type': 'MARKET',
                        'side': 'BUY'
                    }
                }
        """
        ...  # pragma: no cover

    def amend_order(self, **kwargs) -> Awaitable:
        """Reduce the quantity of an existing open order, keeping its priority.

        This adds 0 orders to the ``EXCHANGE_MAX_ORDERS`` filter and the
        ``MAX_NUM_ORDERS`` filter, and does not consume the account orders
        rate-limit pool.

        Weight: 4

        Args:
            symbol (str):
            orderId (:obj:`long`, optional):
            origClientOrderId (:obj:`str`, optional):
            newClientOrderId (:obj:`str`, optional): The new client order id for
                the order after being amended. If not sent, one is randomly
                generated. The current ``clientOrderId`` may be reused.
            newQty (float): Must be greater than 0 and less than the order's quantity.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

            Either ``orderId`` or ``origClientOrderId`` must be sent.

        Returns:
            dict: The amend result including the amended order, for example::

                {
                    'transactTime': 1741923284382,
                    'executionId': 16,
                    'amendedOrder': {
                        'symbol': 'BTCUSDT',
                        'orderId': 12,
                        'orderListId': -1,
                        'origClientOrderId': 'my_test_order1',
                        'clientOrderId': '4zR9HFcEq8gM1tWUqPEUHc',
                        'price': '5.00000000',
                        'qty': '5.00000000',
                        'executedQty': '0.00000000',
                        'preventedQty': '0.00000000',
                        'quoteOrderQty': '0.00000000',
                        'cumulativeQuoteQty': '0.00000000',
                        'status': 'NEW',
                        'timeInForce': 'GTC',
                        'type': 'LIMIT',
                        'side': 'BUY'
                    }
                }
        """
        ...  # pragma: no cover

    def get_open_orders(self, **kwargs) -> Awaitable:
        """Gets all open orders on a symbol. Careful when accessing this with no symbol.

        Weight: 6 for a single symbol; 80 when the symbol parameter is omitted.

        Args:
            symbol (str):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000
            timestamp (long):

            If the ``symbol`` is not sent, orders for all symbols will be returned in an list.

        Returns:
            list: For example::

                [
                    {
                        'symbol': 'LTCBTC',
                        'orderId': 1,
                        'orderListId': -1, # Unless OCO, the value will always be - 1
                        'clientOrderId': 'myOrder1',
                        'price': '0.1',
                        'origQty': '1.0',
                        'executedQty': '0.0',
                        'cummulativeQuoteQty': '0.0',
                        'status': 'NEW',
                        'timeInForce': 'GTC',
                        'type': 'LIMIT',
                        'side': 'BUY',
                        'stopPrice': '0.0',
                        'icebergQty': '0.0',
                        'time': 1499827319559,
                        'updateTime': 1499827319559,
                        'isWorking': True,
                        'origQuoteOrderQty': '0.000000'
                    }
                ]

        """
        ...  # pragma: no cover

    def cancel_all_orders(self, **kwargs) -> Awaitable:
        """Cancel all open orders on a symbol, including OCO orders.

        Weight: 1

        Args:
            symbol (str):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            list: A list of the canceled order and order-list reports. Each
            entry has the same shape as a single ``cancel_order`` /
            ``cancel_oco`` response.
        """
        ...  # pragma: no cover

    def get_all_orders(self, **kwargs) -> Awaitable:
        """Gets all account orders, either active, or canceled, or filled.

        Weight: 20

        Args:
            symbol (str):
            orderId (:obj:`long`, optional):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000
            timestamp (long):

            If ``orderId`` is set, it will get orders >= that ``orderId``. Otherwise most recent orders are returned.
            For some historical orders `cummulativeQuoteQty` will be < 0, meaning the data is not available at this time.

        Returns:
            list: A list of dicts of all queried orders. For example::

                [
                    {
                        'symbol': 'LTCBTC',
                        'orderId': 1,
                        'orderListId': -1, # Unless OCO, the value will always be - 1
                        'clientOrderId': 'myOrder1',
                        'price': '0.1',
                        'origQty': '1.0',
                        'executedQty': '0.0',
                        'cummulativeQuoteQty': '0.0',
                        'status': 'NEW',
                        'timeInForce': 'GTC',
                        'type': 'LIMIT',
                        'side': 'BUY',
                        'stopPrice': '0.0',
                        'icebergQty': '0.0',
                        'time': 1499827319559,
                        'updateTime': 1499827319559,
                        'isWorking': True,
                        'origQuoteOrderQty': '0.000000'
                    }
                ]
        """
        ...  # pragma: no cover

    def create_sor_order(self, **kwargs) -> Awaitable:
        """Places an order using Smart Order Routing (SOR).

        Weight: 1

        Args:
            symbol (str):
            side (OrderSide):
            type (OrderType):
            timeInForce (:obj:`TimeInForce`, optional):
            quantity (float):
            price (:obj:`float`, optional):
            newClientOrderId (:obj:`str`, optional): A unique id for the order. Automatically generated if not sent.
            newOrderRespType (:obj:`OrderRespType`, optional): Set the response JSON. ``ACK``, ``RESULT``, or ``FULL``.
            icebergQty (:obj:`float`, optional):
            strategyId (:obj:`long`, optional):
            strategyType (:obj:`int`, optional): The value cannot be less than 1000000.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            list: A list of the placed order reports. For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'orderId': 2,
                        'orderListId': -1,
                        'clientOrderId': 'sBI1KM6nNtOfj5tccZSKly',
                        'transactTime': 1689149087774,
                        'price': '31000.00000000',
                        'origQty': '0.50000000',
                        'executedQty': '0.50000000',
                        'cummulativeQuoteQty': '14000.00000000',
                        'status': 'FILLED',
                        'timeInForce': 'GTC',
                        'type': 'LIMIT',
                        'side': 'BUY',
                        'workingTime': 1689149087774,
                        'fills': [
                            {
                                'matchType': 'ONE_PARTY_TRADE_REPORT',
                                'price': '28000.00000000',
                                'qty': '0.50000000',
                                'commission': '0.00000000',
                                'commissionAsset': 'BTC',
                                'tradeId': -1,
                                'allocId': 0
                            }
                        ],
                        'workingFloor': 'SOR',
                        'selfTradePreventionMode': 'NONE',
                        'usedSor': True
                    }
                ]
        """
        ...  # pragma: no cover

    def create_oco(self, **kwargs) -> Awaitable:
        """Sends in a new one-cancels-the-other order

        Args:
            symbol (str):
            listClientOrderId (:obj:`str`, optional): A unique Id for the entire orderList
            side (OrderSide):
            quantity (float):
            limitClientOrderId (:obj:`str`, optional): A unique Id for the limit order
            price (float):
            limitIcebergQty (:obj:`float`, optional): Used to make the LIMIT_MAKER leg an iceberg order.
            stopClientOrderId (:obj:`str`, optional): A unique Id for the stop loss/stop loss limit leg
            stopPrice (float):
            stopLimitPrice (:obj:`float`, optional): If provided, stopLimitTimeInForce is required.
            stopIcebergQty (:obj:`float`, optional): Used with STOP_LOSS_LIMIT leg to make an iceberg order.
            stopLimitTimeInForce (:obj:`TimeInForce`, optional): time in force.
            newOrderRespType (:obj:`OrderRespType`, optional) Set the response JSON.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000
            timestamp (long):

        Additional Info:
            Price Restrictions:
                SELL: Limit Price > Last Price > Stop Price
                BUY: Limit Price < Last Price < Stop Price
            Quantity Restrictions:
                Both legs must have the same quantity.
                ICEBERG quantities however do not have to be the same

        Returns:
            dict: A dict of the oco order. For example::

                {
                    'orderListId': 0,
                    'contingencyType': 'OCO',
                    'listStatusType': 'EXEC_STARTED',
                    'listOrderStatus': 'EXECUTING',
                    'listClientOrderId': 'JYVpp3F0f5CAG15DhtrqLp',
                    'transactionTime': 1563417480525,
                    'symbol': 'LTCBTC',
                    'orders': [
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 2,
                            'clientOrderId': 'Kk7sqHb9J6mJWTMDVW7Vos'
                        },
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 3,
                            'clientOrderId': 'xTXKaGYd4bluPVp78IVRvl'
                        }
                    ],
                    'orderReports': [
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 2,
                            'orderListId': 0,
                            'clientOrderId': 'Kk7sqHb9J6mJWTMDVW7Vos',
                            'transactTime': 1563417480525,
                            'price': '0.000000',
                            'origQty': '0.624363',
                            'executedQty': '0.000000',
                            'cummulativeQuoteQty': '0.000000',
                            'status': 'NEW',
                            'timeInForce': 'GTC',
                            'type': 'STOP_LOSS',
                            'side': 'BUY',
                            'stopPrice': '0.960664'
                        },
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 3,
                            'orderListId': 0,
                            'clientOrderId': 'xTXKaGYd4bluPVp78IVRvl',
                            'transactTime': 1563417480525,
                            'price': '0.036435',
                            'origQty': '0.624363',
                            'executedQty': '0.000000',
                            'cummulativeQuoteQty': '0.000000',
                            'status': 'NEW',
                            'timeInForce': 'GTC',
                            'type': 'LIMIT_MAKER',
                            'side': 'BUY'
                        }
                    ]
                }
        """
        ...  # pragma: no cover

    def create_oto(self, **kwargs) -> Awaitable:
        """Places an OTO (One-Triggers-the-Other) order list.

        An OTO order list contains a pending working order that is placed on the
        book and a pending order that is only placed when the working order is
        fully filled.

        Weight: 1

        Args:
            symbol (str):
            listClientOrderId (:obj:`str`, optional): A unique id for the entire orderList.
            newOrderRespType (:obj:`OrderRespType`, optional):
            workingType (OrderType): ``LIMIT`` or ``LIMIT_MAKER``.
            workingSide (OrderSide):
            workingClientOrderId (:obj:`str`, optional):
            workingPrice (float):
            workingQuantity (float):
            workingIcebergQty (:obj:`float`, optional):
            workingTimeInForce (:obj:`TimeInForce`, optional):
            pendingType (OrderType):
            pendingSide (OrderSide):
            pendingClientOrderId (:obj:`str`, optional):
            pendingPrice (:obj:`float`, optional):
            pendingStopPrice (:obj:`float`, optional):
            pendingTrailingDelta (:obj:`float`, optional):
            pendingQuantity (float):
            pendingIcebergQty (:obj:`float`, optional):
            pendingTimeInForce (:obj:`TimeInForce`, optional):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            dict: A dict describing the placed order list, with the same shape
            as ``create_oco`` (``orderListId``, ``contingencyType``: ``OTO``,
            ``orders``, ``orderReports``).
        """
        ...  # pragma: no cover

    def create_otoco(self, **kwargs) -> Awaitable:
        """Places an OTOCO (One-Triggers-One-Cancels-the-Other) order list.

        An OTOCO order list contains a pending working order that is placed on
        the book and a pending OCO pair (two orders) that is only placed when
        the working order is fully filled.

        Weight: 1

        Args:
            symbol (str):
            listClientOrderId (:obj:`str`, optional): A unique id for the entire orderList.
            newOrderRespType (:obj:`OrderRespType`, optional):
            workingType (OrderType): ``LIMIT`` or ``LIMIT_MAKER``.
            workingSide (OrderSide):
            workingClientOrderId (:obj:`str`, optional):
            workingPrice (float):
            workingQuantity (float):
            workingIcebergQty (:obj:`float`, optional):
            workingTimeInForce (:obj:`TimeInForce`, optional):
            pendingSide (OrderSide):
            pendingQuantity (float):
            pendingAboveType (OrderType):
            pendingAboveClientOrderId (:obj:`str`, optional):
            pendingAbovePrice (:obj:`float`, optional):
            pendingAboveStopPrice (:obj:`float`, optional):
            pendingAboveTrailingDelta (:obj:`float`, optional):
            pendingAboveIcebergQty (:obj:`float`, optional):
            pendingAboveTimeInForce (:obj:`TimeInForce`, optional):
            pendingBelowType (:obj:`OrderType`, optional):
            pendingBelowClientOrderId (:obj:`str`, optional):
            pendingBelowPrice (:obj:`float`, optional):
            pendingBelowStopPrice (:obj:`float`, optional):
            pendingBelowTrailingDelta (:obj:`float`, optional):
            pendingBelowIcebergQty (:obj:`float`, optional):
            pendingBelowTimeInForce (:obj:`TimeInForce`, optional):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            dict: A dict describing the placed order list, with the same shape
            as ``create_oco`` (``orderListId``, ``contingencyType``: ``OTO``,
            ``orders``, ``orderReports``).
        """
        ...  # pragma: no cover

    def cancel_oco(self, **kwargs) -> Awaitable:
        """Cancels an entire Order List

        Weight: 1

        Args:
            symbol (str):
            orderListId (:obj:`long`, optional):
            listClientOrderId (:obj:`str`, optional): A unique Id for the entire orderList.
            newClientOrderId (:obj:`str`, optional): Used to uniquely identify this cancel. Automatically generated by default.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

            Either ``orderListId`` or ``listClientOrderId`` must be provided.

            Canceling an individual leg will cancel the entire OCO

        Returns:
            dict: For example::

                {
                    'orderListId': 0,
                    'contingencyType': 'OCO',
                    'listStatusType': 'ALL_DONE',
                    'listOrderStatus': 'ALL_DONE',
                    'listClientOrderId': 'C3wyj4WVEktd7u9aVBRXcN',
                    'transactionTime': 1574040868128,
                    'symbol': 'LTCBTC',
                    'orders': [
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 2,
                            'clientOrderId': 'pO9ufTiFGg3nw2fOdgeOXa'
                        },
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 3,
                            'clientOrderId': 'TXOvglzXuaubXAaENpaRCB'
                        }
                    ],
                    'orderReports': [
                        {
                            'symbol': 'LTCBTC',
                            'origClientOrderId': 'pO9ufTiFGg3nw2fOdgeOXa',
                            'orderId': 2,
                            'orderListId': 0,
                            'clientOrderId': 'unfWT8ig8i0uj6lPuYLez6',
                            'price': '1.00000000',
                            'origQty': '10.00000000',
                            'executedQty': '0.00000000',
                            'cummulativeQuoteQty': '0.00000000',
                            'status': 'CANCELED',
                            'timeInForce': 'GTC',
                            'type': 'STOP_LOSS_LIMIT',
                            'side': 'SELL',
                            'stopPrice': '1.00000000'
                        },
                        {
                            'symbol': 'LTCBTC',
                            'origClientOrderId': 'TXOvglzXuaubXAaENpaRCB',
                            'orderId': 3,
                            'orderListId': 0,
                            'clientOrderId': 'unfWT8ig8i0uj6lPuYLez6',
                            'price': '3.00000000',
                            'origQty': '10.00000000',
                            'executedQty': '0.00000000',
                            'cummulativeQuoteQty': '0.00000000',
                            'status': 'CANCELED',
                            'timeInForce': 'GTC',
                            'type': 'LIMIT_MAKER',
                            'side': 'SELL'
                        }
                    ]
                }
        """
        ...  # pragma: no cover

    def get_oco(self, **kwargs) -> Awaitable:
        """Retrieves a specific OCO based on provided optional parameters.

        Weight: 4

        Args:
            orderListId (:obj:`long`, optional):
            origClientOrderId (:obj:`str`, optional): A unique Id for the entire orderList.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

            Either ``orderListId`` or ``listClientOrderId`` must be provided

        Returns:
            dict: For example::

                {
                    'orderListId': 27,
                    'contingencyType': 'OCO',
                    'listStatusType': 'EXEC_STARTED',
                    'listOrderStatus': 'EXECUTING',
                    'listClientOrderId': 'h2USkA5YQpaXHPIrkd96xE',
                    'transactionTime': 1565245656253,
                    'symbol': 'LTCBTC',
                    'orders': [
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 4,
                            'clientOrderId': 'qD1gy3kc3Gx0rihm9Y3xwS'
                        },
                        {
                            'symbol': 'LTCBTC',
                            'orderId': 5,
                            'clientOrderId': 'ARzZ9I00CPM8i3NhmU9Ega'
                        }
                    ]
                }

        """
        ...  # pragma: no cover

    def get_all_oco(self, **kwargs) -> Awaitable:
        """Retrieves all OCO based on provided optional parameters.

        Weight: 20

        Args:
            fromId (:obj:`long`, optional): If supplied, neither ``startTime`` or ``endTime`` can be provided
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

        Returns:
            dict: A list of dicts of queried oco. For example::

                [
                    {
                        'orderListId': 29,
                        'contingencyType': 'OCO',
                        'listStatusType': 'EXEC_STARTED',
                        'listOrderStatus': 'EXECUTING',
                        'listClientOrderId': 'amEEAXryFzFwYF1FeRpUoZ',
                        'transactionTime': 1565245913483,
                        'symbol': 'LTCBTC',
                        'orders': [
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 4,
                                'clientOrderId': 'oD7aesZqjEGlZrbtRpy5zB'
                            },
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 5,
                                'clientOrderId': 'Jr1h6xirOxgeJOUuYQS7V3'
                            }
                        ]
                    },
                    {
                        'orderListId': 28,
                        'contingencyType': 'OCO',
                        'listStatusType': 'EXEC_STARTED',
                        'listOrderStatus': 'EXECUTING',
                        'listClientOrderId': 'hG7hFNxJV6cZy3Ze4AUT4d',
                        'transactionTime': 1565245913407,
                        'symbol': 'LTCBTC',
                        'orders': [
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 2,
                                'clientOrderId': 'j6lFOfbmFMRjTYA7rRJ0LP'
                            },
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 3,
                                'clientOrderId': 'z0KCjOdditiLS5ekAFtK81'
                            }
                        ]
                    }
                ]
        """
        ...  # pragma: no cover

    def get_open_oco(self, **kwargs) -> Awaitable:
        """Retrieves open OCO.

        Weight: 6

        Args:
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

        Returns:
            list: For example::

                [
                    {
                        'orderListId': 31,
                        'contingencyType': 'OCO',
                        'listStatusType': 'EXEC_STARTED',
                        'listOrderStatus': 'EXECUTING',
                        'listClientOrderId': 'wuB13fmulKj3YjdqWEcsnp',
                        'transactionTime': 1565246080644,
                        'symbol': '1565246079109',
                        'orders': [
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 4,
                                'clientOrderId': 'r3EH2N76dHfLoSZWIUw1bT'
                            },
                            {
                                'symbol': 'LTCBTC',
                                'orderId': 5,
                                'clientOrderId': 'Cv1SnyPD3qhqpbjpYEHbd2'
                            }
                        ]
                    }
                ]
        """
        ...  # pragma: no cover


for getter_setting in WS_APIS:
    define_ws_getter(WsApiGetters, **getter_setting)
