"""Account-data WS-API endpoint stubs.

Signed user-data queries: account status, trades, commission, order rate
limits, prevented matches, allocations, order amendments and per-symbol
filters. These are pre-declared stubs whose bodies are replaced by
``define_ws_getter``.
"""

from typing import (
    Awaitable,
    Callable,
)


# pylint: disable=no-member


class WsApiAccountGetters:
    """Account/user-data mixin for ``WsApiGetters``."""

    _ws_api_request: Callable[..., Awaitable]

    # ----- account ---------------------------------------------------------

    def get_account(self, **kwargs) -> Awaitable:
        """Gets current account information.

        Weight: 20

        Args:
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

        Returns:
            dict: For example::

                {
                    'makerCommission': 15,
                    'takerCommission': 15,
                    'buyerCommission': 0,
                    'sellerCommission': 0,
                    'canTrade': True,
                    'canWithdraw': True,
                    'canDeposit': True,
                    'updateTime': 123456789,
                    'accountType': 'SPOT',
                    'balances': [
                        {
                            'asset': 'BTC',
                            'free': '4723846.89208129',
                            'locked': '0.00000000'
                        },
                        {
                            'asset': 'LTC',
                            'free': '4763368.68006011',
                            'locked': '0.00000000'
                        }
                    ]
                }
        """
        ...  # pragma: no cover

    def get_trades(self, **kwargs) -> Awaitable:
        """Gets trades for a specific account and symbol.

        Weight: 5 when scoped by ``orderId``, else 20.

        Args:
            symbol (str):
            orderId (:obj:`long`, optional): If set, fetches the trades for this
                order only (must be used together with ``symbol``).
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            fromId (:obj:`long`, optional): TradeId to fetch from. Default gets most recent trades.
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000。
            timestamp (long):

            If ``fromId`` is set, it will get orders >= that ``fromId``. Otherwise most recent orders are returned.

        Returns:
            list: For example::

                [
                    {
                        'symbol': 'BNBBTC',
                        'id': 28457,
                        'orderId': 100234,
                        'orderListId': -1,
                        'price': '4.00000100',
                        'qty': '12.00000000',
                        'quoteQty': '48.000012',
                        'commission': '10.10000000',
                        'commissionAsset': 'BNB',
                        'time': 1499865549590,
                        'isBuyer': True,
                        'isMaker': False,
                        'isBestMatch': True
                    }
                ]

        """
        ...  # pragma: no cover

    def get_commission(self, **kwargs) -> Awaitable:
        """Gets current account commission rates.

        Weight: 20

        Args:
            symbol (str):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            dict: For example::

                {
                    'symbol': 'BTCUSDT',
                    'standardCommission': {
                        'maker': '0.00000010',
                        'taker': '0.00000020',
                        'buyer': '0.00000030',
                        'seller': '0.00000040'
                    },
                    'taxCommission': {
                        'maker': '0.00000112',
                        'taker': '0.00000114',
                        'buyer': '0.00000118',
                        'seller': '0.00000116'
                    },
                    'discount': {
                        'enabledForAccount': True,
                        'enabledForSymbol': True,
                        'discountAsset': 'BNB',
                        'discount': '0.25000000'
                    }
                }
        """
        ...  # pragma: no cover

    def get_order_rate_limit(self, **kwargs) -> Awaitable:
        """Gets the current unfilled order count for all the account's order rate limits.

        Weight: 40

        Args:
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            list: For example::

                [
                    {
                        'rateLimitType': 'ORDERS',
                        'interval': 'SECOND',
                        'intervalNum': 10,
                        'limit': 50,
                        'count': 0
                    },
                    {
                        'rateLimitType': 'ORDERS',
                        'interval': 'DAY',
                        'intervalNum': 1,
                        'limit': 160000,
                        'count': 0
                    }
                ]
        """
        ...  # pragma: no cover

    def get_prevented_matches(self, **kwargs) -> Awaitable:
        """Displays the list of orders that were expired due to STP.

        Weight: 20

        Args:
            symbol (str):
            preventedMatchId (:obj:`long`, optional):
            orderId (:obj:`long`, optional):
            fromPreventedMatchId (:obj:`long`, optional):
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

            Supported parameter combinations:
                ``symbol`` + ``preventedMatchId``
                ``symbol`` + ``orderId``
                ``symbol`` + ``orderId`` + ``fromPreventedMatchId`` (+ ``limit``)

        Returns:
            list: For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'preventedMatchId': 1,
                        'takerOrderId': 5,
                        'makerOrderId': 3,
                        'tradeGroupId': 1,
                        'selfTradePreventionMode': 'EXPIRE_MAKER',
                        'price': '1.100000',
                        'makerPreventedQuantity': '1.300000',
                        'transactTime': 1669101687094
                    }
                ]
        """
        ...  # pragma: no cover

    def get_allocations(self, **kwargs) -> Awaitable:
        """Retrieves allocations resulting from SOR order placement.

        Weight: 20

        Args:
            symbol (str):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            fromAllocationId (:obj:`int`, optional):
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            orderId (:obj:`long`, optional):
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.
            timestamp (long):

        Returns:
            list: For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'allocationId': 0,
                        'allocationType': 'SOR',
                        'orderId': 1,
                        'orderListId': -1,
                        'price': '1.00000000',
                        'qty': '5.00000000',
                        'quoteQty': '5.00000000',
                        'commission': '0.00000000',
                        'commissionAsset': 'BTC',
                        'time': 1687506878118,
                        'isBuyer': True,
                        'isMaker': False,
                        'isAllocator': False
                    }
                ]
        """
        ...  # pragma: no cover

    def get_order_amendments(self, **kwargs) -> Awaitable:
        """Queries amendment history for a single order.

        Weight: 4

        Args:
            symbol (str): Required.
            orderId (long): Required.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.

        Returns:
            list: A list of amendment records for the specified order.
        """
        ...  # pragma: no cover

    def get_my_filters(self, **kwargs) -> Awaitable:
        """Gets account-relevant filters, including ``MAX_ASSET`` limits.

        Weight: 40

        Args:
            symbol (str): Required.
            recvWindow (:obj:`long`, optional): The value cannot be greater than 60000.

        Returns:
            dict: Account-specific filter data for the requested symbol.
        """
        ...  # pragma: no cover
