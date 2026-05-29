"""COIN-M Futures market-data REST endpoint stubs.

Public market data: open interest (current and historical), funding rate /
info, premium index. These are pre-declared stubs whose bodies are replaced
by ``define_getter`` at import time (see ``registry.py``).
"""

from typing import Awaitable


class CMMarketDataGetters:
    """Public market-data mixin for :class:`CMFuturesGetters`."""

    # ----- REST: market data ------------------------------------------------

    def get_orderbook(self, **kwargs) -> Awaitable:
        """Get the COIN-M futures order book depth snapshot.

        Used by the high-level :class:`~binance.OrderBookHandlerBase` to seed
        a local order book before consuming the diff stream.

        Args:
            symbol (str): The trading pair (e.g. ``'BTCUSD_PERP'``).
            limit (int, optional): Number of price levels per side. Valid
                values: 5, 10, 20, 50, 100, 500, 1000. Defaults to 500.

        Returns:
            dict: ``{'lastUpdateId': int, 'symbol': str, 'pair': str, 'E': int, 'T': int, 'bids': [...], 'asks': [...]}``

        Endpoint: ``GET /dapi/v1/depth``  Weight: depends on ``limit`` (see ``_depth_weight``).
        """
        ...  # pragma: no cover

    def get_open_interest(self, **kwargs) -> Awaitable:
        """Gets the present open interest for a COIN-M futures symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol, e.g. ``'BTCUSD_PERP'``.

        Returns:
            dict: For example::

                {
                    'symbol': 'BTCUSD_PERP',
                    'pair': 'BTCUSD',
                    'openInterest': '11942594',
                    'contractType': 'PERPETUAL',
                    'time': 1779705310815
                }
        """
        ...  # pragma: no cover

    def get_open_interest_hist(self, **kwargs) -> Awaitable:
        """Gets historical open interest statistics for a COIN-M pair and contract type.

        Weight: 1

        Note: Unlike USDⓈ-M which takes ``symbol``, COIN-M uses ``pair`` and
        ``contractType`` as the primary filters.

        Args:
            pair (str): The underlying asset pair, e.g. ``'BTCUSD'``.
            contractType (str): Contract type -- one of ``'CURRENT_QUARTER'``,
                ``'NEXT_QUARTER'``, ``'PERPETUAL'``.
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
                        'contractType': 'PERPETUAL',
                        'sumOpenInterest': '11948062.00000000',
                        'sumOpenInterestValue': '15419.48047926',
                        'pair': 'BTCUSD',
                        'timestamp': 1779703200000
                    }
                ]
        """
        ...  # pragma: no cover

    def get_funding_rate(self, **kwargs) -> Awaitable:
        """Gets historical funding rate data for a COIN-M perpetual symbol.

        Weight: 1

        Note:
            Unlike the USDⓈ-M endpoint of the same name, on COIN-M ``symbol``
            is **required**.  Calling without it returns HTTP 400.
            Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Get-Funding-Rate-History-of-Perpetual-Futures

        Args:
            symbol (str): The COIN-M perpetual symbol (e.g. ``'BTCUSD_PERP'``).
                Required.
            startTime (:obj:`long`, optional): Start timestamp in ms (inclusive).
            endTime (:obj:`long`, optional): End timestamp in ms (inclusive).
            limit (:obj:`int`, optional): Default 100; max 1000.

        Returns:
            list: A list of funding rate records. For example::

                [
                    {
                        'symbol': 'BTCUSD_PERP',
                        'fundingTime': 1779667200001,
                        'fundingRate': '0.00009106',
                        'markPrice': '76943.88345951'
                    }
                ]
        """
        ...  # pragma: no cover

    def get_funding_info(self, **kwargs) -> Awaitable:
        """Gets funding rate cap/floor and funding interval for all COIN-M symbols.

        Weight: 1

        Returns:
            list: A list of funding info records. Currently returns an empty list
            if no data is configured. For example when populated::

                [
                    {
                        'symbol': 'BTCUSD_PERP',
                        'adjustedFundingRateCap': '0.02000000',
                        'adjustedFundingRateFloor': '-0.02000000',
                        'fundingIntervalHours': 8,
                        'disclaimer': False
                    }
                ]
        """
        ...  # pragma: no cover

    def get_premium_index(self, **kwargs) -> Awaitable:
        """Gets the current mark price, index price, and funding rate for a COIN-M symbol.

        Weight: 1 when ``symbol`` or ``pair`` is given; 10 when both are omitted (returns all).

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol,
                e.g. ``'BTCUSD_PERP'``. If omitted with ``pair`` also omitted,
                data for all symbols is returned.
            pair (:obj:`str`, optional): The underlying pair, e.g. ``'BTCUSD'``.

        Returns:
            list: A list of mark-price records (even for a single symbol). For example::

                [
                    {
                        'symbol': 'BTCUSD_PERP',
                        'pair': 'BTCUSD',
                        'markPrice': '77458.95073093',
                        'indexPrice': '77493.53787133',
                        'estimatedSettlePrice': '77504.04329360',
                        'lastFundingRate': '0.00006004',
                        'interestRate': '0.00010000',
                        'nextFundingTime': 1779724800000,
                        'time': 1779705323000
                    }
                ]
        """
        ...  # pragma: no cover
