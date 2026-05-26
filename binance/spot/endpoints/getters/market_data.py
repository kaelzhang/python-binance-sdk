"""Market-data WS-API endpoint stubs.

Public market data: depth, klines / uiKlines, trades (recent / historical /
aggregate / block), avgPrice, tickers (24hr / price / book / rolling window /
trading day), execution rules and reference price. These are pre-declared
stubs whose bodies are replaced by ``define_ws_getter``.
"""

from typing import Awaitable


class WsApiMarketDataGetters:
    """Public market-data mixin for ``WsApiGetters``."""

    # ----- market data -----------------------------------------------------

    def get_orderbook(self, **kwargs) -> Awaitable:
        """Gets the orderbook for a certain symbol.

        Args:
            symbol (str): The symbol of the orderbook.
            limit (:obj:`int`, optional): Default 100, max 5000 (any value; Binance caps at 5000).

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

        Weight: 2 for a single ``symbol``; tiered by ``symbols`` count
        (<=20 -> 2, <=100 -> 40, else 80); 80 when neither is sent.

        Args:
            symbol (:obj:`str`, optional): A single symbol.
            symbols (:obj:`list`, optional): A list of symbols. If neither
                ``symbol`` nor ``symbols`` is sent, tickers for all symbols
                will be returned in a list.

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

    def get_ticker_price(self, **kwargs) -> Awaitable:
        """Gets latest price for a symbol or symbols.

        Weight: 2 for a single symbol; 4 when the symbol parameter is omitted.

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

    def get_orderbook_ticker(self, **kwargs) -> Awaitable:
        """Gets the best price/quantity on the order book for a symbol or symbols.

        Weight: 2 for a single symbol; 4 when the symbol parameter is omitted.

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

    def get_ui_klines(self, **kwargs) -> Awaitable:
        """Gets klines optimized for chart presentation (same params as ``get_klines``).

        Weight: 2

        Returns:
            list: kline rows, same shape as ``get_klines``.
        """
        ...  # pragma: no cover

    def get_rolling_window_ticker(self, **kwargs) -> Awaitable:
        """Gets rolling-window price-change statistics for a symbol or list of symbols.

        Weight: 4 per symbol; 200 when more than 50 symbols are requested.

        Args:
            symbol (:obj:`str`, optional): A single symbol.
            symbols (:obj:`list`, optional): A list of symbols.
            windowSize (:obj:`str`, optional): Window size (e.g. ``'1m'``–``'7d'``).

        Returns:
            dict or list: Stats dict for a single symbol, or a list of dicts.
        """
        ...  # pragma: no cover

    def get_trading_day_ticker(self, **kwargs) -> Awaitable:
        """Gets trading-day price statistics for a symbol or list of symbols.

        Weight: 4 per symbol; 200 when more than 50 symbols are requested.

        Args:
            symbol (:obj:`str`, optional): A single symbol.
            symbols (:obj:`list`, optional): A list of symbols.
            timeZone (:obj:`str`, optional): UTC offset (e.g. ``'0'``, ``'8:00'``).
            type (:obj:`str`, optional): Response type: ``'FULL'`` (default) or ``'MINI'``.

        Returns:
            dict or list: Stats dict for a single symbol, or a list of dicts.
        """
        ...  # pragma: no cover

    def get_historical_block_trades(self, **kwargs) -> Awaitable:
        """Gets historical block trades for a symbol.

        Weight: 25

        Args:
            symbol (str): The symbol.
            fromId (:obj:`long`, optional): Block-trade ID to fetch from.
            limit (:obj:`int`, optional): Defaults to 500; max 1000.

        Returns:
            list: A list of historical block trade records.
        """
        ...  # pragma: no cover

    def get_execution_rules(self, **kwargs) -> Awaitable:
        """Gets per-symbol execution rules (e.g. price bands, order size limits).

        Weight: 2 for a single ``symbol``; 2 per symbol in a ``symbols`` list
        (capped at 40); 40 for an unscoped or ``symbolStatus``-only query.

        Args:
            symbol (:obj:`str`, optional): A single symbol.
            symbols (:obj:`list`, optional): A list of symbols.
            symbolStatus (:obj:`str`, optional): Filter by status (e.g. ``'TRADING'``).

        Returns:
            dict or list: Execution-rule data for the queried symbol(s).
        """
        ...  # pragma: no cover

    def get_reference_price(self, **kwargs) -> Awaitable:
        """Gets the current reference price for a symbol.

        Weight: 2

        Args:
            symbol (str): The symbol.

        Returns:
            dict: Reference price data for the symbol.
        """
        ...  # pragma: no cover

    def get_reference_price_calculation(self, **kwargs) -> Awaitable:
        """Gets the methodology used to compute the reference price for a symbol.

        Weight: 2

        Args:
            symbol (str): The symbol.

        Returns:
            dict: Reference price calculation details.
        """
        ...  # pragma: no cover
