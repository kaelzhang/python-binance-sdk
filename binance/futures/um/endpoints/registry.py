"""USDⓈ-M Futures endpoint registry and stub-to-getter injection.

Maps every public python method name on :class:`UMFuturesGetters` to its
Binance WebSocket-API method name OR REST URL, security level, request
weight, and whether it consumes the account ORDERS pool. Importing this
module triggers the ``define_getter`` loop at the bottom, which patches each
stub on the combined :class:`UMFuturesGetters` class with a real coroutine.

Each entry's ``weight`` may be an ``int`` OR a callable ``(kwargs) -> int``
for endpoints whose weight depends on the request params (see ``weights.py``).

Covers two groups of endpoints:

**Market data** (``SecurityType.NONE``, REST GET):
All on ``https://fapi.binance.com``.

Confirmed weights (2026-05-30) against developers.binance.com:

- ``GET /fapi/v1/openInterest``         weight 1
- ``GET /futures/data/openInterestHist`` weight 0 (shares the 1000/5min/IP
  ``/futures/data`` sub-path pool; SDK bucket clamps cost to max(1, weight))
- ``GET /fapi/v1/fundingRate``          weight 1; shares 500/5min/IP pool with fundingInfo
- ``GET /fapi/v1/fundingInfo``          weight 0; shares the same 500/5min/IP pool
- ``GET /fapi/v1/premiumIndex``          weight 1 (symbol given), 10 (all symbols)

**Trading / account / position** (signed, WS-API + REST):
WS-API endpoints go through the shared ``wss://ws-fapi.binance.com/ws-fapi/v1``
connection; REST endpoints use ``https://fapi.binance.com``.

Ref:
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api
- https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api
- https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api
"""

from binance.core.common.constants import SecurityType, RequestMethod
from binance.core.getters import define_getter
from binance.futures.um.constants import UM_REST_HOST
from binance.futures.um.endpoints.getters import UMFuturesGetters
from binance.futures.um.endpoints.weights import (
    _depth_weight,
    _premium_index_weight,
    _um_open_orders_weight,
)


# WS-API endpoint specs for USDⓈ-M Futures trading / account (signed).
WS_API_ENDPOINTS = [
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/New-Order
        # Request Weight: 0 (IP rate limit / x-mbx-used-weight-1m); 1 on the
        # 10s and 1min ORDERS rate limits. ``is_order=True`` consumes the
        # ORDERS pool; the SDK bucket clamps cost to max(1, weight) so a
        # single request still records 1 unit in the local REQUEST_WEIGHT
        # window even though the server-side IP weight is 0.
        name='create_order',
        transport='ws_api',
        ws_method='order.place',
        security_type=SecurityType.TRADE,
        weight=0,
        is_order=True,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Modify-Order
        # Request Weight: 0 (IP rate limit / x-mbx-used-weight-1m); 1 on the
        # 10s and 1min ORDERS rate limits. Same pattern as ``order.place``:
        # ORDERS pool is consumed via ``is_order=True`` and the SDK bucket
        # clamps cost to max(1, weight) for the local REQUEST_WEIGHT window.
        name='modify_order',
        transport='ws_api',
        ws_method='order.modify',
        security_type=SecurityType.TRADE,
        weight=0,
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
        # Position Info V2 -- docs:
        # https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Position-Info-V2
        name='get_position',
        transport='ws_api',
        ws_method='v2/account.position',
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Cancel-Algo-Order
        # Request Weight: 1.
        name='cancel_algo_order',
        transport='ws_api',
        ws_method='algoOrder.cancel',
        security_type=SecurityType.TRADE,
        weight=1,
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
        name='get_orderbook',
        transport='rest',
        method=RequestMethod.GET,
        rest_url=UM_REST_HOST + '/fapi/v1/depth',
        security_type=SecurityType.NONE,
        weight=_depth_weight,
    ),
    dict(
        name='get_open_interest_hist',
        transport='rest',
        rest_url=UM_REST_HOST + '/futures/data/openInterestHist',
        security_type=SecurityType.NONE,
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
        # Request Weight: 0 (shares the 1000/5min/IP /futures/data sub-path
        # pool); the SDK bucket clamps cost to max(1, weight) so a single
        # request still consumes 1 unit in the local REQUEST_WEIGHT window.
        weight=0,
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info
        # Request Weight: 0 (shares the 500/5min/IP pool with fundingRate);
        # SDK bucket clamps cost to max(1, weight) so consumed usage is 1.
        weight=0,
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders
        # Request Weight: 10. Dead-man's switch: the server auto-cancels
        # all open orders for ``symbol`` once ``countdownTime`` ms elapse
        # without another call. Call again to refresh the timer; send
        # ``countdownTime=0`` to disarm. Critical safety mechanism for
        # live trading.
        name='countdown_cancel_all_orders',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=UM_REST_HOST + '/fapi/v1/countdownCancelAll',
        security_type=SecurityType.TRADE,
        weight=10,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Current-Open-Order
        # Request Weight: 1. Returns ONE open order (singular ``/openOrder``,
        # not plural ``/openOrders``); either ``orderId`` or
        # ``origClientOrderId`` MUST be supplied. Returns an "Order does not
        # exist" error if the order is filled or cancelled.
        name='get_open_order',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/openOrder',
        security_type=SecurityType.USER_DATA,
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Get-Order-Modify-History
        # Request Weight: 1. Returns the chain of price/quantity
        # amendments for one order. Either ``orderId`` or
        # ``origClientOrderId`` MUST be supplied; ``orderId`` wins if
        # both are sent. Modifications older than 3 months are not
        # retained.
        name='get_order_modify_history',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/orderAmendment',
        security_type=SecurityType.USER_DATA,
        weight=1,
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Modify-Multiple-Orders
        # Request Weight: 5 IP, 5 ORDERS-10s, 1 ORDERS-1m. TRADE.
        # Companion to create_batch_orders / cancel_batch_orders; modifies
        # up to 5 existing orders atomically. Consumes the ORDERS pool
        # (`is_order=True`) — Binance counts modifies the same way as new
        # orders against the 10s/1m ORDERS windows.
        name='modify_batch_orders',
        transport='rest',
        method=RequestMethod.PUT,
        rest_url=UM_REST_HOST + '/fapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
        is_order=True,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Multiple-Orders
        # Request Weight: 1.
        name='cancel_batch_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=UM_REST_HOST + '/fapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=1,
    ),
    # ----- Account / Position ------------------------------------------------
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3
        # Request Weight: 5. REST V3 returns a richer aggregated-account
        # field set than the v2 WS-API ``v2/account.status``; the SDK
        # keeps WS-API V2 as the low-latency primary and exposes REST V3
        # as a richer fallback. Distinct method name to avoid shadowing
        # the WS-API ``get_account``. CM has no V3 — UM only.
        name='get_account_rest_v3',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v3/account',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Futures-Account-Balance-V3
        # Request Weight: 5. Same V3-vs-V2 rationale as ``get_account_rest_v3``.
        name='get_balance_rest_v3',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v3/balance',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        name='get_position_risk',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v3/positionRisk',
        security_type=SecurityType.USER_DATA,
        weight=5,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation
        # Request Weight: 5. Returns ADL quantile (queue position 0-4) per
        # symbol/side; used for risk monitoring. Server-side cache refreshes
        # every 30s.
        name='get_adl_quantile',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/adlQuantile',
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
        # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Get-Position-Margin-Change-History
        # Request Weight: 1; security TRADE (docs heading: "(TRADE)").
        # Returns isolated-margin add/reduce events for ``symbol`` over a
        # 30-day window. Companion read endpoint to ``set_position_margin``.
        name='get_position_margin_history',
        transport='rest',
        rest_url=UM_REST_HOST + '/fapi/v1/positionMargin/history',
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


# Neither VSCode Python language server nor Jedi server could handle class
# methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So we declare those methods (as stubs) in the per-mixin modules first, then
# override them here at import time.

for _getter_spec in WS_API_ENDPOINTS:
    define_getter(UMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]

for _getter_spec in REST_ENDPOINTS:
    define_getter(UMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
