"""USDⓈ-M Futures endpoint registry and getter mixin.

Covers two groups of endpoints:

**Market data** (``SecurityType.NONE``, REST GET):
All on ``https://fapi.binance.com``.

Confirmed weights (2026-05-25) via live ``x-mbx-used-weight-1m`` response
headers and official Binance developer docs:

- ``GET /fapi/v1/openInterest``         weight 1
- ``GET /futures/data/openInterestHist`` weight 1 (shared 500/5min pool with rate-limit headers absent on data sub-path; 0 documented but behaves as 1)
- ``GET /fapi/v1/fundingRate``           shares 500/5min/IP pool with fundingInfo; counted as weight 1 in REQUEST_WEIGHT
- ``GET /fapi/v1/fundingInfo``           same shared pool; weight 1
- ``GET /fapi/v1/premiumIndex``          weight 1 (symbol given), 10 (all symbols)

**Trading / account / position** (signed, WS-API + REST):
WS-API endpoints go through the shared ``wss://ws-fapi.binance.com/ws-fapi/v1``
connection; REST endpoints use ``https://fapi.binance.com``.

Ref:
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data
- https://developers.binance.com/docs/derivatives/usds-margined-futures/trade
- https://developers.binance.com/docs/derivatives/usds-margined-futures/account
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType, RequestMethod
from binance.core.getters import define_getter
from binance.futures.um.constants import UM_REST_HOST


def _premium_index_weight(kwargs) -> int:
    """`premiumIndex` weight: 1 when ``symbol`` is given, 10 otherwise."""
    return 1 if 'symbol' in kwargs else 10


def _um_open_orders_weight(kwargs) -> int:
    """`openOrders` weight: 1 when scoped to a ``symbol``, else 40."""
    return 1 if 'symbol' in kwargs else 40


# WS-API endpoint specs for USDⓈ-M Futures trading / account (signed).
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
        ws_method='v2/account.status',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_balance',
        transport='ws_api',
        ws_method='v2/account.balance',
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
    dict(
        name='get_position_mode',
        transport='ws_api',
        ws_method='positionSide.dual.get',
        security_type=SecurityType.USER_DATA,
        weight=30,
    ),
    dict(
        name='create_algo_order',
        transport='ws_api',
        ws_method='algoOrder.place',
        security_type=SecurityType.TRADE,
        weight=0,
        # algoOrder draws from a separate algo-orders quota, NOT the standard
        # ORDERS pool; is_order=False to avoid double-counting.
        is_order=False,
    ),
    dict(
        name='cancel_algo_order',
        transport='ws_api',
        ws_method='algoOrder.cancel',
        security_type=SecurityType.TRADE,
        weight=0,
    ),
]

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
    # ----- Trading -----------------------------------------------------------
    dict(
        name='create_test_order',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/order/test',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='cancel_all_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=UM_REST_HOST + '/fapi/v1/allOpenOrders',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='get_open_orders',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/openOrders',
        security_type=SecurityType.USER_DATA,
        weight=_um_open_orders_weight,
    ),
    dict(
        name='get_all_orders',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/allOrders',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='create_batch_orders',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
    ),
    dict(
        name='cancel_batch_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=UM_REST_HOST + '/fapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
    ),
    # ----- Account / Position ------------------------------------------------
    dict(
        name='get_position_risk',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v3/positionRisk',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_user_trades',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/userTrades',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_commission',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/commissionRate',
        security_type=SecurityType.USER_DATA,
        weight=20,
    ),
    dict(
        name='get_income',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/income',
        security_type=SecurityType.USER_DATA,
        weight=30,
    ),
    dict(
        name='get_leverage_bracket',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/leverageBracket',
        security_type=SecurityType.USER_DATA,
        weight=1,
    ),
    dict(
        name='set_leverage',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/leverage',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='set_margin_type',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/marginType',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='set_position_margin',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/positionMargin',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='set_position_mode',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/positionSide/dual',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    dict(
        name='get_multi_assets_mode',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/multiAssetsMargin',
        security_type=SecurityType.USER_DATA,
        weight=30,
    ),
    dict(
        name='set_multi_assets_mode',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/multiAssetsMargin',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
]


class UMFuturesGetters:
    """Internal mixin providing async methods for every USDⓈ-M Futures endpoint.

    Covers two transports:

    - **WS-API** (trading / account): coroutines that issue a single
      id-correlated request over the shared WS-API connection via
      :meth:`_ws_api_request` — ``create_order``, ``modify_order``,
      ``cancel_order``, ``get_order``, ``get_account`` (v2),
      ``get_balance`` (v2), ``get_position``, ``get_position_mode``,
      ``create_algo_order``, ``cancel_algo_order``.
    - **REST** (market-data + trading/account/position): coroutines that issue
      an HTTP request via :meth:`_request` (RestTransport) and return the
      decoded JSON response.
    """

    _request: Callable[..., Awaitable]
    _ws_api_request: Callable[..., Awaitable]

    # ----- WS-API: trading / account ----------------------------------------

    def create_order(self, **kwargs) -> Awaitable:
        """Places a new USDⓈ-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Note:
            Pass prices and quantities as strings; the SDK rejects ``float``
            values to avoid scientific notation or precision loss.

        Args:
            symbol (str): The futures symbol, e.g. ``'BTCUSDT'``.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            type (FuturesOrderType): e.g. ``'LIMIT'``, ``'MARKET'``, ``'STOP'``.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'`` (default),
                ``'LONG'``, or ``'SHORT'`` for hedge mode.
            timeInForce (:obj:`FuturesTimeInForce`, optional): Required for
                ``LIMIT`` orders.
            quantity (:obj:`str`, optional): Order quantity (base asset).
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
        """Modifies an existing USDⓈ-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Args:
            symbol (str): The futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            orderId (:obj:`long`, optional): Order to modify.
            origClientOrderId (:obj:`str`, optional): Client order id to modify.
            quantity (str): New quantity.
            price (str): New price.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The modified order.
        """
        ...  # pragma: no cover

    def cancel_order(self, **kwargs) -> Awaitable:
        """Cancels an active USDⓈ-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The cancelled order.
        """
        ...  # pragma: no cover

    def get_order(self, **kwargs) -> Awaitable:
        """Checks the status of a USDⓈ-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The order.
        """
        ...  # pragma: no cover

    def get_account(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures account status over the WebSocket API (v2).

        Uses ``v2/account.status`` (richer field set than the deprecated v1
        ``account.status``). Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account status including balances and positions.
        """
        ...  # pragma: no cover

    def get_balance(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures account balance over the WebSocket API (v2).

        Uses ``v2/account.balance`` (richer field set than the deprecated v1
        ``account.balance``). Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-asset balance records.
        """
        ...  # pragma: no cover

    def get_position(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures position information over the WebSocket API.

        Distinct from REST ``get_position_risk`` (``/fapi/v3/positionRisk``);
        this uses WS-API ``account.position`` for a no-REST-round-trip query.
        Weight: 5.

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position information records.
        """
        ...  # pragma: no cover

    def get_position_mode(self, **kwargs) -> Awaitable:
        """Gets the current position mode (one-way vs hedge) via WebSocket API.

        Uses WS-API ``positionSide.dual.get``. Weight: 30.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'dualSidePosition': True/False}``
        """
        ...  # pragma: no cover

    def create_algo_order(self, **kwargs) -> Awaitable:
        """Places an algo (TWAP/VP) order over the WebSocket API.

        Uses WS-API ``algoOrder.place`` (TRADE). Weight: 0.

        Note:
            Algo orders draw from a separate algo-orders quota, not the
            standard ORDERS pool (``is_order=False`` — no ORDERS-pool charge).

        Args:
            symbol (str): The futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            positionSide (:obj:`PositionSide`, optional): Hedge-mode direction.
            quantity (str): Total quantity to execute.
            duration (int): Execution duration in seconds.
            clientAlgoId (:obj:`str`, optional): Client algo order identifier.
            limitPrice (:obj:`str`, optional): Price limit for the algo.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Algo order placement acknowledgement.
        """
        ...  # pragma: no cover

    def cancel_algo_order(self, **kwargs) -> Awaitable:
        """Cancels an active algo order over the WebSocket API.

        Uses WS-API ``algoOrder.cancel`` (TRADE). Weight: 0.

        Args:
            algoId (long): The algo order id to cancel.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Cancellation confirmation.
        """
        ...  # pragma: no cover

    # ----- REST: market data ------------------------------------------------

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

    # ----- REST: trading ----------------------------------------------------

    def create_test_order(self, **kwargs) -> Awaitable:
        """Tests a new futures order without submitting it.

        Weight: 1

        Args:
            Same parameters as ``create_order``.

        Returns:
            dict: An empty dict ``{}`` on success.
        """
        ...  # pragma: no cover

    def cancel_all_orders(self, **kwargs) -> Awaitable:
        """Cancels all open orders for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_open_orders(self, **kwargs) -> Awaitable:
        """Gets all open orders, or open orders for a specific symbol.

        Weight: 1 when ``symbol`` is given; 40 otherwise.

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns open orders for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Open order records.
        """
        ...  # pragma: no cover

    def get_all_orders(self, **kwargs) -> Awaitable:
        """Gets all orders (active, cancelled, or filled) for a symbol.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
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
        """Places multiple orders in a single request.

        Weight: 5. Consumes the account ORDERS pool.

        Args:
            batchOrders (list): List of order parameter dicts.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order results (one per input order).
        """
        ...  # pragma: no cover

    def cancel_batch_orders(self, **kwargs) -> Awaitable:
        """Cancels multiple orders in a single request.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
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
        """Gets position risk information.

        Weight: 5

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position risk records.
        """
        ...  # pragma: no cover

    def get_user_trades(self, **kwargs) -> Awaitable:
        """Gets trades for a specific account and symbol.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
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
        """Gets commission rates for a symbol.

        Weight: 20

        Args:
            symbol (str): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Commission rate information.
        """
        ...  # pragma: no cover

    def get_income(self, **kwargs) -> Awaitable:
        """Gets income history.

        Weight: 30

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
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
        """Gets leverage bracket information.

        Weight: 1

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns brackets for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Leverage bracket records.
        """
        ...  # pragma: no cover

    def set_leverage(self, **kwargs) -> Awaitable:
        """Changes the initial leverage for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            leverage (int): Target leverage (1–125).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation including ``leverage`` and ``maxNotionalValue``.
        """
        ...  # pragma: no cover

    def set_margin_type(self, **kwargs) -> Awaitable:
        """Changes the margin type (ISOLATED or CROSSED) for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            marginType (MarginType): ``'ISOLATED'`` or ``'CROSSED'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_margin(self, **kwargs) -> Awaitable:
        """Adjusts isolated position margin.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'``,
                ``'LONG'``, or ``'SHORT'``.
            amount (str): Margin amount.
            type (int): 1 = add; 2 = reduce.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_mode(self, **kwargs) -> Awaitable:
        """Changes the position mode (one-way vs hedge).

        Weight: 1

        Args:
            dualSidePosition (bool): ``true`` for hedge mode;
                ``false`` for one-way.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_multi_assets_mode(self, **kwargs) -> Awaitable:
        """Gets the current multi-assets margin mode.

        Weight: 30

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'multiAssetsMargin': True/False}``
        """
        ...  # pragma: no cover

    def set_multi_assets_mode(self, **kwargs) -> Awaitable:
        """Changes the multi-assets margin mode.

        Weight: 1

        Args:
            multiAssetsMargin (bool): ``true`` to enable; ``false`` to disable.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover


for _getter_spec in WS_API_ENDPOINTS:
    define_getter(UMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]

for _getter_spec in REST_ENDPOINTS:
    define_getter(UMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
