"""COIN-M Futures endpoint registry and getter mixin.

Covers two groups of endpoints:

**Market data** (``SecurityType.NONE``, REST GET):
All on ``https://dapi.binance.com``.

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

**Trading / account / position** (signed, WS-API + REST):
WS-API endpoints go through the shared ``wss://ws-dapi.binance.com/ws-dapi/v1``
connection; REST endpoints use ``https://dapi.binance.com``.

Confirmed COIN-M trading/account endpoint facts (2026-05-25):
- WS-API: ``order.place``, ``order.modify``, ``order.cancel``, ``order.status``,
  ``account.status``, ``account.balance`` — same method names as USDⓈ-M.
  Source: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade
  Source: https://developers.binance.com/docs/derivatives/coin-margined-futures/account
- REST paths use ``/dapi/v1/`` (not ``/fapi/v3/``).
  ``positionRisk`` is ``/dapi/v1/positionRisk`` (not ``/fapi/v3/positionRisk``).
- COIN-M does NOT have ``multiAssetsMargin`` (USDⓈ-M-only endpoint; omitted here).
- COIN-M rate limits: no 10-second ORDERS pool (only 1-min ORDERS pool).
- User-data stream: ``userDataStream.start/ping/stop`` on ws-dapi; events on
  ``wss://dstream.binance.com/ws/<listenKey>`` (``FuturesUserStreamMixin`` already
  reads ``_stream_host`` at runtime so both UM and CM share the same mixin).

Key COIN-M parameter difference vs USDⓈ-M:
- ``openInterestHist``: COIN-M uses ``pair`` + ``contractType`` (not ``symbol``).
  The ``symbol`` param is optional and used to filter by specific contract.
- ``openInterest``: uses ``symbol`` (e.g. ``'BTCUSD_PERP'``).
- ``premiumIndex``: uses ``symbol`` or ``pair`` (symbol is the full contract name,
  pair is the base asset, e.g. ``'BTCUSD'``).

Ref:
- https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data
- https://developers.binance.com/docs/derivatives/coin-margined-futures/trade
- https://developers.binance.com/docs/derivatives/coin-margined-futures/account
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType, RequestMethod
from binance.core.getters import define_getter
from binance.futures.cm.constants import CM_REST_HOST


def _premium_index_weight(kwargs) -> int:
    """`premiumIndex` weight: 1 when ``symbol`` or ``pair`` is given, 10 otherwise."""
    return 1 if ('symbol' in kwargs or 'pair' in kwargs) else 10


def _cm_open_orders_weight(kwargs) -> int:
    """`openOrders` weight: 1 when scoped to a ``symbol``, else 40."""
    return 1 if 'symbol' in kwargs else 40


# WS-API endpoint specs for COIN-M Futures trading / account (signed).
# Confirmed 2026-05-25 — same WS-API method names as USDⓈ-M on ws-dapi.
WS_API_ENDPOINTS = [
    dict(
        name='create_order',
        transport='ws_api',
        ws_method='order.place',
        security_type=SecurityType.TRADE,
        weight=1,
        is_order=True,
    ),
    dict(
        name='modify_order',
        transport='ws_api',
        ws_method='order.modify',
        security_type=SecurityType.TRADE,
        weight=1,
        is_order=True,
    ),
    dict(
        name='cancel_order',
        transport='ws_api',
        ws_method='order.cancel',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='get_order',
        transport='ws_api',
        ws_method='order.status',
        security_type=SecurityType.USER_DATA,
        weight=1,
    ),
    dict(
        name='get_account',
        transport='ws_api',
        ws_method='account.status',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_balance',
        transport='ws_api',
        ws_method='account.balance',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_position',
        transport='ws_api',
        ws_method='account.position',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
]

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
    # ----- Trading -----------------------------------------------------------
    dict(
        name='create_test_order',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/order/test',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='cancel_all_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=CM_REST_HOST + '/dapi/v1/allOpenOrders',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='get_open_orders',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/openOrders',
        security_type=SecurityType.USER_DATA,
        weight=_cm_open_orders_weight,
    ),
    dict(
        name='get_all_orders',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/allOrders',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='create_batch_orders',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
    ),
    dict(
        name='cancel_batch_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=CM_REST_HOST + '/dapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
    ),
    # ----- Account / Position ------------------------------------------------
    dict(
        name='get_position_risk',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/positionRisk',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_user_trades',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/userTrades',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_commission',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/commissionRate',
        security_type=SecurityType.USER_DATA,
        weight=20,
    ),
    dict(
        name='get_income',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/income',
        security_type=SecurityType.USER_DATA,
        weight=30,
    ),
    dict(
        name='get_leverage_bracket',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/leverageBracket',
        security_type=SecurityType.USER_DATA,
        weight=1,
    ),
    dict(
        name='set_leverage',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/leverage',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='set_margin_type',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/marginType',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='set_position_margin',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/positionMargin',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='get_position_mode',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/positionSide/dual',
        security_type=SecurityType.USER_DATA,
        weight=30,
    ),
    dict(
        name='set_position_mode',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/positionSide/dual',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
]


class CMFuturesGetters:
    """Internal mixin providing async methods for every COIN-M Futures endpoint.

    Covers two transports:

    - **WS-API** (trading / account): coroutines that issue a single
      id-correlated request over the shared WS-API connection via
      :meth:`_ws_api_request` — ``create_order``, ``modify_order``,
      ``cancel_order``, ``get_order``, ``get_account``, ``get_balance``,
      ``get_position``.
    - **REST** (market-data + trading/account/position): coroutines that issue
      an HTTP request via :meth:`_request` (RestTransport) and return the
      decoded JSON response.
    """

    _request: Callable[..., Awaitable]
    _ws_api_request: Callable[..., Awaitable]

    # ----- WS-API: trading / account ----------------------------------------

    def create_order(self, **kwargs) -> Awaitable:
        """Places a new COIN-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Note:
            Pass prices and quantities as strings; the SDK rejects ``float``
            values to avoid scientific notation or precision loss.

        Args:
            symbol (str): The COIN-M futures symbol, e.g. ``'BTCUSD_PERP'``.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            type (FuturesOrderType): e.g. ``'LIMIT'``, ``'MARKET'``, ``'STOP'``.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'`` (default),
                ``'LONG'``, or ``'SHORT'`` for hedge mode.
            timeInForce (:obj:`FuturesTimeInForce`, optional): Required for
                ``LIMIT`` orders.
            quantity (:obj:`str`, optional): Order quantity (number of contracts).
            reduceOnly (:obj:`bool`, optional): Cannot be set to ``true`` in
                hedge mode; defaults to ``false``.
            price (:obj:`str`, optional): Required for ``LIMIT`` / stop-limit orders.
            newClientOrderId (:obj:`str`, optional): Unique client order id.
            stopPrice (:obj:`str`, optional): Required for ``STOP`` /
                ``TAKE_PROFIT`` orders.
            workingType (:obj:`WorkingType`, optional): ``'MARK_PRICE'`` or
                ``'CONTRACT_PRICE'``.
            priceProtect (:obj:`bool`, optional): Activates price protection
                for conditional orders.
            newOrderRespType (:obj:`str`, optional): ``'ACK'`` or ``'RESULT'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Order acknowledgement or result.
        """
        ...  # pragma: no cover

    def modify_order(self, **kwargs) -> Awaitable:
        """Modifies an existing COIN-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Args:
            symbol (str): The COIN-M futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            orderId (:obj:`long`, optional): Order to modify.
            origClientOrderId (:obj:`str`, optional): Client order id to modify.
            quantity (str): New quantity (number of contracts).
            price (str): New price.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The modified order.
        """
        ...  # pragma: no cover

    def cancel_order(self, **kwargs) -> Awaitable:
        """Cancels an active COIN-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The cancelled order.
        """
        ...  # pragma: no cover

    def get_order(self, **kwargs) -> Awaitable:
        """Checks the status of a COIN-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The order.
        """
        ...  # pragma: no cover

    def get_account(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures account status over the WebSocket API.

        Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account status including assets and positions.
        """
        ...  # pragma: no cover

    def get_balance(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures account balance over the WebSocket API.

        Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-asset balance records.
        """
        ...  # pragma: no cover

    def get_position(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures position information over the WebSocket API.

        Distinct from REST ``get_position_risk`` (``/dapi/v1/positionRisk``);
        this uses WS-API ``account.position`` for a no-REST-round-trip query.
        Weight: 5.

        Args:
            marginAsset (:obj:`str`, optional): The margin asset (e.g. ``'BTC'``).
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position information records.
        """
        ...  # pragma: no cover

    # ----- REST: market data ------------------------------------------------

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

    # ----- REST: trading ----------------------------------------------------

    def create_test_order(self, **kwargs) -> Awaitable:
        """Tests a new COIN-M futures order without submitting it.

        Weight: 1

        Args:
            Same parameters as ``create_order``.

        Returns:
            dict: An empty dict ``{}`` on success.
        """
        ...  # pragma: no cover

    def cancel_all_orders(self, **kwargs) -> Awaitable:
        """Cancels all open orders for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_open_orders(self, **kwargs) -> Awaitable:
        """Gets all open orders, or open orders for a specific COIN-M symbol.

        Weight: 1 when ``symbol`` is given; 40 otherwise.

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. If omitted,
                returns open orders for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Open order records.
        """
        ...  # pragma: no cover

    def get_all_orders(self, **kwargs) -> Awaitable:
        """Gets all orders (active, cancelled, or filled) for a COIN-M symbol.

        Weight: 5

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Fetch orders >= this id.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 500; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order records.
        """
        ...  # pragma: no cover

    def create_batch_orders(self, **kwargs) -> Awaitable:
        """Places multiple COIN-M orders in a single request.

        Weight: 5. Consumes the account ORDERS pool.

        Args:
            batchOrders (list): List of order parameter dicts.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order results (one per input order).
        """
        ...  # pragma: no cover

    def cancel_batch_orders(self, **kwargs) -> Awaitable:
        """Cancels multiple COIN-M orders in a single request.

        Weight: 5

        Args:
            symbol (str): The COIN-M futures symbol.
            orderIdList (:obj:`list`, optional): List of order ids to cancel.
            origClientOrderIdList (:obj:`list`, optional): List of client
                order ids to cancel.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Cancellation results.
        """
        ...  # pragma: no cover

    # ----- REST: account / position -----------------------------------------

    def get_position_risk(self, **kwargs) -> Awaitable:
        """Gets position risk information for COIN-M contracts.

        Weight: 5

        Args:
            marginAsset (:obj:`str`, optional): The margin asset (e.g. ``'BTC'``).
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position risk records.
        """
        ...  # pragma: no cover

    def get_user_trades(self, **kwargs) -> Awaitable:
        """Gets trades for a specific COIN-M account and symbol.

        Weight: 5

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            fromId (:obj:`long`, optional): Trade id to fetch from.
            limit (:obj:`int`, optional): Default 500; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Trade records.
        """
        ...  # pragma: no cover

    def get_commission(self, **kwargs) -> Awaitable:
        """Gets commission rates for a COIN-M symbol.

        Weight: 20

        Args:
            symbol (str): The COIN-M futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Commission rate information.
        """
        ...  # pragma: no cover

    def get_income(self, **kwargs) -> Awaitable:
        """Gets income history for COIN-M.

        Weight: 30

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol.
            incomeType (:obj:`str`, optional): Income type filter.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 1000; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Income history records.
        """
        ...  # pragma: no cover

    def get_leverage_bracket(self, **kwargs) -> Awaitable:
        """Gets leverage bracket information for COIN-M.

        Weight: 1

        Args:
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
                If omitted, returns brackets for all pairs.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Leverage bracket records.
        """
        ...  # pragma: no cover

    def set_leverage(self, **kwargs) -> Awaitable:
        """Changes the initial leverage for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            leverage (int): Target leverage (1–125).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation including ``leverage`` and ``maxQty``.
        """
        ...  # pragma: no cover

    def set_margin_type(self, **kwargs) -> Awaitable:
        """Changes the margin type (ISOLATED or CROSSED) for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            marginType (MarginType): ``'ISOLATED'`` or ``'CROSSED'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_margin(self, **kwargs) -> Awaitable:
        """Adjusts isolated position margin for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'``,
                ``'LONG'``, or ``'SHORT'``.
            amount (str): Margin amount.
            type (int): 1 = add; 2 = reduce.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_position_mode(self, **kwargs) -> Awaitable:
        """Gets the current position mode (one-way vs hedge) for COIN-M.

        Weight: 30

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'dualSidePosition': True/False}``
        """
        ...  # pragma: no cover

    def set_position_mode(self, **kwargs) -> Awaitable:
        """Changes the position mode (one-way vs hedge) for COIN-M.

        Weight: 1

        Args:
            dualSidePosition (bool): ``true`` for hedge mode;
                ``false`` for one-way.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover


for _getter_spec in WS_API_ENDPOINTS:
    define_getter(CMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]

for _getter_spec in REST_ENDPOINTS:
    define_getter(CMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
