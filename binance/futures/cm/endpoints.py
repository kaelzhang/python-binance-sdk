"""COIN-M Futures REST endpoint registry and getter mixin.

All endpoints are public (``SecurityType.NONE``) read-only market-data endpoints
on ``https://dapi.binance.com``.

Confirmed weights (2026-05-25) via live ``x-mbx-used-weight-1m`` response
header deltas and official Binance COIN-M developer docs:

- ``GET /dapi/v1/openInterest``          weight 1
- ``GET /futures/data/openInterestHist`` weight 1 (same shared data sub-path as
                                                   USDⓈ-M; no weight header on
                                                   the /futures/data sub-path;
                                                   treated as 1 for consistency)
- ``GET /dapi/v1/fundingRate``           weight 1
- ``GET /dapi/v1/fundingInfo``           weight 1 (endpoint exists; header absent
                                                   on this path; treated as 1)
- ``GET /dapi/v1/premiumIndex``          weight 1 (symbol given), 10 (all symbols)

Key COIN-M parameter difference vs USDⓈ-M:
- ``openInterestHist``: COIN-M uses ``pair`` + ``contractType`` (not ``symbol``).
  The ``symbol`` param is optional and used to filter by specific contract.
- ``openInterest``: uses ``symbol`` (e.g. ``'BTCUSD_PERP'``).
- ``premiumIndex``: uses ``symbol`` or ``pair`` (symbol is the full contract name,
  pair is the base asset, e.g. ``'BTCUSD'``).

Ref:
- https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType
from binance.core.getters import define_getter
from binance.futures.cm.constants import CM_REST_HOST


def _premium_index_weight(kwargs) -> int:
    """`premiumIndex` weight: 1 when ``symbol`` or ``pair`` is given, 10 otherwise."""
    return 1 if ('symbol' in kwargs or 'pair' in kwargs) else 10


# REST endpoint specs for COIN-M Futures market-data (read-only: funding /
# open-interest / mark-price).
REST_ENDPOINTS = [
    dict(
        name='get_open_interest',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/openInterest',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_open_interest_hist',
        transport='rest',
        rest_url=CM_REST_HOST + '/futures/data/openInterestHist',
        security_type=SecurityType.NONE,
        # Documented weight is 0 on the /futures/data sub-path (same as USDⓈ-M);
        # treated as 1 to stay consistent with the REQUEST_WEIGHT accounting model.
        weight=1,
    ),
    dict(
        name='get_funding_rate',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/fundingRate',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_funding_info',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/fundingInfo',
        security_type=SecurityType.NONE,
        weight=1,
    ),
    dict(
        name='get_premium_index',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/premiumIndex',
        security_type=SecurityType.NONE,
        weight=_premium_index_weight,
    ),
]


class CMFuturesGetters:
    """Internal mixin providing async methods for every COIN-M Futures REST endpoint.

    Each method is an ``await``-able coroutine that issues a REST GET request via
    the shared :class:`~binance.core.transport.rest.RestTransport` and returns
    the decoded JSON response.
    """

    _request: Callable[..., Awaitable]

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
        """Gets historical funding rate data for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol,
                e.g. ``'BTCUSD_PERP'``. If omitted, returns records for all symbols.
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


for _getter_spec in REST_ENDPOINTS:
    define_getter(CMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
