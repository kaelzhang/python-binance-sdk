"""USDⓈ-M Futures market-data REST endpoint stubs.

Public market data: open interest (current and historical), funding rate /
info, premium index. These are pre-declared stubs whose bodies are replaced
by ``define_getter`` at import time (see ``registry.py``).
"""

from typing import Awaitable


class UMMarketDataGetters:
    """Public market-data mixin for :class:`UMFuturesGetters`."""

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
