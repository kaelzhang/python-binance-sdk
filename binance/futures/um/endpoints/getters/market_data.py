"""USDⓈ-M Futures market-data endpoint stubs (REST + WS-API).

Public market data: open interest (current and historical), funding rate /
info, premium index, and the three documented WS-API methods (``depth``,
``ticker.price``, ``ticker.book``). All stubs are replaced by
``define_getter`` at import time (see ``registry.py``).

WS-API getters carry the ``_ws`` suffix to avoid colliding with the REST
counterparts of the same name (e.g. REST ``get_orderbook`` for
``GET /fapi/v1/depth`` vs WS-API ``get_orderbook_ws`` for ``depth`` on
``ws-fapi``).
"""

from typing import Awaitable


class UMMarketDataGetters:
    """Public market-data mixin for :class:`UMFuturesGetters`."""

    # ----- WS-API: market data ----------------------------------------------

    def get_orderbook_ws(self, **kwargs) -> Awaitable:
        """Get the USDⓈ-M futures order-book snapshot over the WebSocket API.

        WS-API counterpart of the REST ``get_orderbook``
        (``GET /fapi/v1/depth``). Use this when you already have an open
        WS-API connection and want to avoid a REST round-trip.

        Weight (dynamic by ``limit``): 5/10/20/50 -> 2, 100 -> 5,
        500 -> 10, 1000 -> 20. Default ``limit`` is 500.
        Security: NONE.
        Docs:
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Order-Book

        Args:
            symbol (str): The trading pair (e.g. ``'BTCUSDT'``).
            limit (:obj:`int`, optional): Number of price levels per
                side. Valid: 5, 10, 20, 50, 100, 500, 1000. Default 500.

        Returns:
            dict: ``{'lastUpdateId': int, 'E': int, 'T': int, 'bids': [...], 'asks': [...]}``.
        """
        ...  # pragma: no cover

    def get_ticker_price_ws(self, **kwargs) -> Awaitable:
        """Get the USDⓈ-M futures latest price ticker over the WebSocket API.

        WS-API equivalent of REST ``GET /fapi/v1/ticker/price``.
        Weight: 1 with ``symbol``, 2 when omitted (returns all symbols).
        Security: NONE.
        Docs:
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Symbol-Price-Ticker

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns the latest price for every symbol.

        Returns:
            dict | list: ``{'symbol': ..., 'price': ..., 'time': ...}``
            (single symbol) or a list of such dicts.
        """
        ...  # pragma: no cover

    def get_ticker_book_ws(self, **kwargs) -> Awaitable:
        """Get the USDⓈ-M futures best bid/ask snapshot over the WebSocket API.

        WS-API equivalent of REST ``GET /fapi/v1/ticker/bookTicker``.
        Weight: 2 with ``symbol``, 5 when omitted.
        Security: NONE.
        Docs:
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Symbol-Order-Book-Ticker

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns the bid/ask snapshot for every symbol.

        Returns:
            dict | list: ``{'symbol': ..., 'bidPrice': ..., 'bidQty': ..., 'askPrice': ..., 'askQty': ..., 'time': ..., 'lastUpdateId': ...}``
            (single symbol) or a list of such dicts.

        Note:
            Retail Price Improvement (RPI) orders are not visible on this
            endpoint per docs.
        """
        ...  # pragma: no cover

    # ----- REST: market data ------------------------------------------------

    def get_orderbook(self, **kwargs) -> Awaitable:
        """Get the USDⓈ-M futures order book depth snapshot.

        Used by the high-level :class:`~binance.OrderBookHandlerBase` to seed
        a local order book before consuming the diff stream.

        Args:
            symbol (str): The trading pair (e.g. ``'BTCUSDT'``).
            limit (int, optional): Number of price levels per side. Valid
                values: 5, 10, 20, 50, 100, 500, 1000. Defaults to 500.

        Returns:
            dict: ``{'lastUpdateId': int, 'E': int, 'T': int, 'bids': [...], 'asks': [...]}``

        Endpoint: ``GET /fapi/v1/depth``  Weight: depends on ``limit`` (see ``_depth_weight``).
        """
        ...  # pragma: no cover

    def get_open_interest(self, **kwargs) -> Awaitable:
        """Gets the present open interest for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol, e.g. ``'BTCUSDT'``.

        Returns:
            dict: For example::

                {
                    'openInterest': '10659.509',
                    'symbol': 'BTCUSDT',
                    'time': 1589437530011
                }
        """
        ...  # pragma: no cover

    def get_open_interest_hist(self, **kwargs) -> Awaitable:
        """Gets historical open interest statistics for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            period (str): Statistical period -- one of
                ``'5m'``, ``'15m'``, ``'30m'``, ``'1h'``, ``'2h'``, ``'4h'``,
                ``'6h'``, ``'12h'``, ``'1d'``.
            limit (:obj:`int`, optional): Default 30; max 500.
            startTime (:obj:`long`, optional): Start timestamp in ms (inclusive).
            endTime (:obj:`long`, optional): End timestamp in ms (inclusive).

        Returns:
            list: A list of open-interest history records. For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'sumOpenInterest': '20403.63700000',
                        'sumOpenInterestValue': '150570784.07809979',
                        'timestamp': 1583127900000
                    }
                ]
        """
        ...  # pragma: no cover

    def get_funding_rate(self, **kwargs) -> Awaitable:
        """Gets historical funding rate data.

        Shares the ``500/5min/IP`` rate-limit pool with ``get_funding_info``;
        each call counts as weight 1 against the main REQUEST_WEIGHT pool.

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns the most recent records for all symbols.
            startTime (:obj:`long`, optional): Start timestamp in ms (inclusive).
            endTime (:obj:`long`, optional): End timestamp in ms (inclusive).
            limit (:obj:`int`, optional): Default 100; max 1000.

        Returns:
            list: A list of funding rate records. For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'fundingRate': '-0.03750000',
                        'fundingTime': 1570608000000,
                        'markPrice': '11758.53843548'
                    }
                ]
        """
        ...  # pragma: no cover

    def get_funding_info(self, **kwargs) -> Awaitable:
        """Gets funding rate cap/floor and funding interval for all symbols.

        Shares the ``500/5min/IP`` rate-limit pool with ``get_funding_rate``;
        each call counts as weight 1 against the main REQUEST_WEIGHT pool.

        Returns:
            list: A list of funding info records. For example::

                [
                    {
                        'symbol': 'BTCUSDT',
                        'adjustedFundingRateCap': '0.02000000',
                        'adjustedFundingRateFloor': '-0.02000000',
                        'fundingIntervalHours': 8,
                        'disclaimer': False,
                        'updateTime': 1744070609229
                    }
                ]
        """
        ...  # pragma: no cover

    def get_premium_index(self, **kwargs) -> Awaitable:
        """Gets the current mark price, index price, and funding rate for a symbol.

        Weight: 1 when ``symbol`` is given; 10 when omitted (returns all).

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                data for all symbols is returned in a list.

        Returns:
            dict: If ``symbol`` is given::

                {
                    'symbol': 'BTCUSDT',
                    'markPrice': '11793.63104562',
                    'indexPrice': '11781.80495970',
                    'estimatedSettlePrice': '11781.16138815',
                    'lastFundingRate': '0.00010000',
                    'interestRate': '0.00010000',
                    'nextFundingTime': 1595836800000,
                    'time': 1595827200000
                }

            list: If ``symbol`` is omitted, a list of the above dicts.
        """
        ...  # pragma: no cover
