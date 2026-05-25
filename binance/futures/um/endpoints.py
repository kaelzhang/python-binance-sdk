"""USDⓈ-M Futures REST endpoint registry and getter mixin.

All endpoints are public (``SecurityType.NONE``) read-only market-data endpoints
on ``https://fapi.binance.com``.

Confirmed weights (2026-05-25) via live ``x-mbx-used-weight-1m`` response
headers and official Binance developer docs:

- ``GET /fapi/v1/openInterest``         weight 1
- ``GET /futures/data/openInterestHist`` weight 1 (shared 500/5min pool with rate-limit headers absent on data sub-path; 0 documented but behaves as 1)
- ``GET /fapi/v1/fundingRate``           shares 500/5min/IP pool with fundingInfo; counted as weight 1 in REQUEST_WEIGHT
- ``GET /fapi/v1/fundingInfo``           same shared pool; weight 1
- ``GET /fapi/v1/premiumIndex``          weight 1 (symbol given), 10 (all symbols)

Ref:
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType
from binance.core.getters import define_getter
from binance.futures.um.constants import UM_REST_HOST


def _premium_index_weight(kwargs) -> int:
    """`premiumIndex` weight: 1 when ``symbol`` is given, 10 otherwise."""
    return 1 if 'symbol' in kwargs else 10


# REST endpoint specs for USDⓈ-M Futures market-data (P4 scope: funding /
# open-interest / mark-price).
REST_ENDPOINTS = [
    dict(
        name='get_open_interest',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/openInterest',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_open_interest_hist',
        transport='rest',
        rest_url=UM_REST_HOST + '/futures/data/openInterestHist',
        security_type=SecurityType.NONE,
        # Documented weight is 0 on the /futures/data sub-path; we treat it as
        # 1 to stay consistent with the REQUEST_WEIGHT accounting model (a
        # request that costs 0 would never be tracked).
        weight=1,
    ),
    dict(
        name='get_funding_rate',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/fundingRate',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_funding_info',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/fundingInfo',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_premium_index',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/premiumIndex',
        security_type=SecurityType.NONE,
        weight=_premium_index_weight,
    ),
]


class UMFuturesGetters:
    """Internal mixin providing async methods for every USDⓈ-M Futures REST endpoint.

    Each method is an ``await``-able coroutine that issues a REST GET request via
    the shared :class:`~binance.core.transport.rest.RestTransport` and returns
    the decoded JSON response.
    """

    _request: Callable[..., Awaitable]

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


for _getter_spec in REST_ENDPOINTS:
    define_getter(UMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
