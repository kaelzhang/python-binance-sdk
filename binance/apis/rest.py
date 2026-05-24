from typing import (
    Awaitable
)

from binance.common.constants import (
    REST_API_VERSION,
    SecurityType,
    RequestMethod
)
from binance.rate_limit import REST_ENDPOINT_WEIGHTS, depth_weight

# Rest APIs ref:
# https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
APIS = [

    # General Endpoints

    dict(
        name='ping',
        path='ping',

        # Support params, defaults to `True`
        params=False,

        # request method, defaults to 'get'
        # method = RequestMethod.GET

        # SecurityType, defaults to NONE (False, False)
        # ref: https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#endpoint-security-type
        # security_type=SecurityType.NONE

        # api version
        # version=REST_API_VERSION
    ),

    dict(
        name='get_server_time',
        path='time',
        params=False
    ),

    dict(
        name='get_exchange_info',
        path='exchangeInfo',
        params=False
    ),

    # Market Data endpoints

    dict(
        name='get_orderbook',
        path='depth'
    ),

    dict(
        name='get_recent_trades',
        path='trades'
    ),

    dict(
        name='get_historical_trades',
        path='historicalTrades',
        security_type=SecurityType.MARKET_DATA
    ),

    dict(
        name='get_aggregate_trades',
        path='aggTrades'
    ),

    dict(
        name='get_klines',
        path='klines'
    ),

    dict(
        name='get_average_price',
        path='avgPrice'
    ),

    dict(
        name='get_ticker',
        path='ticker/24hr'
    ),

    dict(
        name='get_ticker_price',
        path='ticker/price'
    ),

    dict(
        name='get_orderbook_ticker',
        path='ticker/bookTicker'
    ),

    # Account endpoints
    #
    # NOTE: the trading endpoints (order.* / orderList.* / sor.* /
    # openOrders.*) have been migrated to the WebSocket API and now live in
    # `binance.apis.ws_api.WsApiGetters`. Only the account/trade-history
    # endpoints below remain on REST (pending migration in a later task).

    dict(
        name='get_account',
        path='account',
        security_type=SecurityType.USER_DATA
    ),

    dict(
        name='get_trades',
        path='myTrades',
        security_type=SecurityType.USER_DATA
    )
]


def define_getter(
    Target,
    name,
    path,
    params=True,
    version=REST_API_VERSION,
    method=RequestMethod.GET,
    security_type=SecurityType.NONE,
    is_order=False
):
    """Internal factory: build a concrete async REST closure and install it on ``Target``."""
    base_weight = REST_ENDPOINT_WEIGHTS.get(path, 1)

    def getter(self, **kwargs):
        uri = self._rest_uri(path, version)
        ka = kwargs if params else {}
        if path == 'depth':
            weight = depth_weight(int(kwargs.get('limit', 100)))
        else:
            weight = base_weight

        return self._request(
            method,
            uri,
            security_type,
            weight=weight,
            is_order=is_order,
            **ka
        )

    origin = getattr(Target, name)

    # Migrate the docstring to the new getter
    getter.__doc__ = origin.__doc__

    setattr(Target, name, getter)

# Google Style guide:
# https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
# Sphinx extension supports a mixed style of
#   google and reStructuredText formatting

# Neither VSCode Python language server nor Jedi server could handle
#   class methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So, however, we need to just create those methods and docstrings first,
#   then override them.

# pylint: disable=no-member


class RestAPIGetters:
    """Internal mixin providing dynamically-generated async methods for Binance ``/api/`` endpoints."""

    _api_host: str

    def _rest_uri(self, path, version=REST_API_VERSION) -> str:
        return self._api_host + '/api/' + version + '/' + path

    def ping(self) -> Awaitable:
        """Tests connectivity to the Rest API

        Returns:
            dict: An empty dict `{}`
        """
        ...  # pragma: no cover

    def get_server_time(self) -> Awaitable:
        """Tests connectivity to the Rest API and gets the current server time.

        Returns:
            dict: A dict contains only one key `serverTime`. For example::

                {"serverTime": 1499827319559}
        """
        ...  # pragma: no cover

    def get_exchange_info(self) -> Awaitable:
        """Gets Current exchange trading rules and symbol information.

        Returns:
            dict: A dict of the exchange info. For example::

                {
                    'timezone': 'UTC',
                    'serverTime': 1565246363776,
                    'rateLimits': [
                        {
                            # These are defined in the `ENUM definitions` section under `Rate Limiters (rateLimitType)`.
                            # All limits are optional
                        }
                    ],
                    'exchangeFilters': [
                        # These are the defined filters in the `Filters` section.
                        # All filters are optional.
                    ],
                    'symbols': [
                        {
                            'symbol': 'ETHBTC',
                            'status': 'TRADING',
                            'baseAsset': 'ETH',
                            'baseAssetPrecision': 8,
                            'quoteAsset': 'BTC',
                            'quotePrecision': 8,
                            'baseCommissionPrecision': 8,
                            'quoteCommissionPrecision': 8,
                            'orderTypes': [
                                'LIMIT',
                                'LIMIT_MAKER',
                                'MARKET',
                                'STOP_LOSS',
                                'STOP_LOSS_LIMIT',
                                'TAKE_PROFIT',
                                'TAKE_PROFIT_LIMIT'
                            ],
                            'icebergAllowed': True,
                            'ocoAllowed': True,
                            'quoteOrderQtyMarketAllowed': True,
                            'isSpotTradingAllowed': True,
                            'isMarginTradingAllowed': True,
                            'filters': [
                                # These are defined in the Filters section.
                                # All filters are optional
                            ]
                        }
                    ]
                }
        """
        ...  # pragma: no cover

    # Market Data endpoints

    def get_orderbook(self, **kwargs) -> Awaitable:
        """Gets the orderbook for a certain symbol.

        Args:
            symbol (str): The symbol of the orderbook.
            limit (:obj:`int`, optional): Defaults to 100; max 5000. Valid limits: [5, 10, 20, 50, 100, 500, 1000, 5000].

        Returns:
            dict: The orderbook. For example::

                {
                    'lastUpdateId': 1027024,
                    'bids': [
                        [
                            '4.00000000',  # PRICE
                            '431.00000000' # QTY
                        ]
                    ],
                    'asks': [
                        [
                            '4.00000200',
                            '12.00000000'
                        ]
                    ]
                }
        """
        ...  # pragma: no cover

    def get_recent_trades(self, **kwargs) -> Awaitable:
        """Gets recent trades.

        Args:
            symbol (str): The symbol.
            limit (:obj:`int`, optional): Defaults to 100; max 5000.

        Returns:
            list: A list of recent trade orders. For example::

                [
                    {
                        'id': 28457,
                        'price': '4.00000100',
                        'qty': '12.00000000',
                        'quoteQty': '48.000012',
                        'time': 1499865549590,
                        'isBuyerMaker': True,
                        'isBestMatch': True
                    }

                    # ...
                ]
        """
        ...  # pragma: no cover

    def get_historical_trades(self, **kwargs) -> Awaitable:
        """Get older trades.

        Args:
            symbol (str): The symbol name
            limit (:obj:`int`, optional): Defaults to 500, max 1000.
            fromId (:obj:`long`, optional): TradeId to fetch from. Default gets most recent trades.

        Returns:
            list: A list of trade orders. For example::

                [
                    {
                        'id': 28457,
                        'price': '4.00000100',
                        'qty': '12.00000000',
                        'quoteQty': '48.000012',
                        'time': 1499865549590,
                        'isBuyerMaker': True,
                        'isBestMatch': True
                    }

                    # ...
                ]
        """
        ...  # pragma: no cover

    def get_aggregate_trades(self, **kwargs) -> Awaitable:
        """Gets compressed, aggregate trades. Trades that fill at the time, from the same order, with the same price will have the quantity aggregated.

        Args:
            symbol (str): The symbol name.
            fromId (:obj:`long`, optional): ID to get aggregate trades from INCLUSIVE.
            startTime (:obj:`long`, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
            endTime (:obj:`long`, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
            limit (:obj:`int`, optional): Defaults to 500, max 1000.

            If both ``startTime`` and ``endTime`` are sent, time between ``startTime`` and ``endTime`` must be less than 1 hour.
            If ``fromId``, ``startTime``, and ``endTime`` are not sent, the most recent aggregate trades will be returned.

        Returns:
            list: A list of aggregated trade orders. For example::

                [
                    {
                        'a': 26129,         # Aggregate tradeId
                        'p': '0.01633102',  # Price
                        'q': '4.70443515',  # Quantity
                        'f': 27781,         # First tradeId
                        'l': 27781,         # Last tradeId
                        'T': 1498793709153, # Timestamp
                        'm': True,          # Was the buyer the maker?
                        'M': True           # Was the trade the best price match?
                    }
                ]
        """
        ...  # pragma: no cover

    def get_klines(self, **kwargs) -> Awaitable:
        """Gets kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.

        Args:
            symbol (str):
            interval (TimeFrame):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Defaults to 500, max 1000.

            If ``startTime`` and ``endTime`` are not sent, the most recent klines are returned.

        Returns:
            list: A list of candlesticks. For example::

                [
                    [
                        1499040000000,      # Open time
                        '0.01634790',       # Open
                        '0.80000000',       # High
                        '0.01575800',       # Low
                        '0.01577100',       # Close
                        '148976.11427815',  # Volume
                        1499644799999,      # Close time
                        '2434.19055334',    # Quote asset volume
                        308,                # Number of trades
                        '1756.87402397',    # Taker buy base asset volume
                        '28.46694368',      # Taker buy quote asset volume
                        '17928899.62484339' # Ignore.
                    ]
                ]
        """
        ...  # pragma: no cover

    def get_average_price(self, **kwargs) -> Awaitable:
        """Gets current average price for a symbol.

        Args:
            symbol (str): The symbol name.

        Returns:
            dict: For example::

                {
                    'mins': 5,
                    'price': '9.35751834'
                }
        """
        ...  # pragma: no cover

    def get_ticker(self, **kwargs) -> Awaitable:
        """Gets 24 hour rolling window price change statistics. Careful when accessing this with no symbol.

        Weight: 1 for a single symbol, 40 when the symbol parameter is omitted.

        Args:
            symbol (:obj:`str`, optional): If the ``symbol`` is not sent, tickers for all symbols will be returned in a list.

        Returns:
            dict: If the ``symbol`` parameter is specified::

                {
                    'symbol': 'BNBBTC',
                    'priceChange': '-94.99999800',
                    'priceChangePercent': '-95.960',
                    'weightedAvgPrice': '0.29628482',
                    'prevClosePrice': '0.10002000',
                    'lastPrice': '4.00000200',
                    'lastQty': '200.00000000',
                    'bidPrice': '4.00000000',
                    'askPrice': '4.00000200',
                    'openPrice': '99.00000000',
                    'highPrice': '100.00000000',
                    'lowPrice': '0.10000000',
                    'volume': '8913.30000000',
                    'quoteVolume': '15.30000000',
                    'openTime': 1499783499040,
                    'closeTime': 1499869899040,
                    'firstId': 28385,   # First tradeId
                    'lastId': 28460,    # Last tradeId
                    'count': 76         # Trade count
                }

            list: If the ``symbol`` parameter is omitted::

                [
                    {
                        'symbol': 'BNBBTC',
                        'priceChange': '-94.99999800',
                        'priceChangePercent': '-95.960',
                        'weightedAvgPrice': '0.29628482',
                        'prevClosePrice': '0.10002000',
                        'lastPrice': '4.00000200',
                        'lastQty': '200.00000000',
                        'bidPrice': '4.00000000',
                        'askPrice': '4.00000200',
                        'openPrice': '99.00000000',
                        'highPrice': '100.00000000',
                        'lowPrice': '0.10000000',
                        'volume': '8913.30000000',
                        'quoteVolume': '15.30000000',
                        'openTime': 1499783499040,
                        'closeTime': 1499869899040,
                        'firstId': 28385,   # First tradeId
                        'lastId': 28460,    # Last tradeId
                        'count': 76         # Trade count
                    }
                ]

        """
        ...  # pragma: no cover

    def get_ticker_price(self) -> Awaitable:
        """Gets latest price for a symbol or symbols.

        Weight: 1 for a single symbol; 2 when the symbol parameter is omitted.

        Args:
            symbol (:obj:`str`, optional): If the ``symbol`` is not sent, prices for all symbols will be returned in a list.

        Returns:
            dict: If the ``symbol`` parameter is specified::

                {
                    'symbol': 'LTCBTC',
                    'price': '4.00000200'
                }

            list: If the ``symbol`` parameter is omitted::

                [
                    {
                        'symbol': 'LTCBTC',
                        'price': '4.00000200'
                    },
                    {
                        'symbol': 'ETHBTC',
                        'price': '0.07946600'
                    }
                ]
        """
        ...  # pragma: no cover

    def get_orderbook_ticker(self) -> Awaitable:
        """Gets the best price/quantity on the order book for a symbol or symbols.

        Weight: 1 for a single symbol; 2 when the symbol parameter is omitted.

        Args:
            symbol (:obj:`str`, optional): If the ``symbol`` is not sent, bookTickers for all symbols will be returned in a list.

        Returns:
            dict: If the ``symbol`` parameter is specified::

                {
                    'symbol': 'LTCBTC',
                    'bidPrice': '4.00000000',
                    'bidQty': '431.00000000',
                    'askPrice': '4.00000200',
                    'askQty': '9.00000000'
                }


            list: If the ``symbol`` parameter is omitted::

                [
                    {
                        'symbol': 'LTCBTC',
                        'bidPrice': '4.00000000',
                        'bidQty': '431.00000000',
                        'askPrice': '4.00000200',
                        'askQty': '9.00000000'
                    },
                    {
                        'symbol': 'ETHBTC',
                        'bidPrice': '0.07946700',
                        'bidQty': '9.00000000',
                        'askPrice': '100000.00000000',
                        'askQty': '1000.00000000'
                    }
                ]
        """
        ...  # pragma: no cover

    # Account endpoints

    def get_account(self, **kwargs) -> Awaitable:
        """Gets current account information.

        Weight: 5

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

        Args:
            symbol (str):
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

for getter_setting in APIS:
    define_getter(RestAPIGetters, **getter_setting)
