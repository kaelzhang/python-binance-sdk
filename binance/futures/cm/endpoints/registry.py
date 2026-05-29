"""COIN-M Futures endpoint registry and stub-to-getter injection.

Maps every public python method name on :class:`CMFuturesGetters` to its
Binance WebSocket-API method name OR REST URL, security level, request
weight, and whether it consumes the account ORDERS pool. Importing this
module triggers the ``define_getter`` loop at the bottom, which patches each
stub on the combined :class:`CMFuturesGetters` class with a real coroutine.

Each entry's ``weight`` may be an ``int`` OR a callable ``(kwargs) -> int``
for endpoints whose weight depends on the request params (see ``weights.py``).

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
- ``GET /dapi/v1/premiumIndex``          weight 10 (flat; CM docs do not list
                                                    the dynamic 1/10 split that
                                                    UM has)

**Trading / account / position** (signed, WS-API + REST):
WS-API endpoints go through the shared ``wss://ws-dapi.binance.com/ws-dapi/v1``
connection; REST endpoints use ``https://dapi.binance.com``.

Confirmed COIN-M trading/account endpoint facts (2026-05-25):
- WS-API: ``order.place``, ``order.modify``, ``order.cancel``, ``order.status``,
  ``account.status``, ``account.balance`` — same method names as USDⓈ-M.
  Source: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api
  Source: https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api
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
- https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api
- https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api
- https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api
"""

from binance.core.common.constants import SecurityType, RequestMethod
from binance.core.getters import define_getter
from binance.futures.cm.constants import CM_REST_HOST
from binance.futures.cm.endpoints.getters import CMFuturesGetters
from binance.futures.cm.endpoints.weights import (
    _cm_all_orders_weight,
    _cm_open_orders_weight,
    _cm_user_trades_weight,
    _depth_weight,
)


# WS-API endpoint specs for COIN-M Futures trading / account (signed).
# Confirmed 2026-05-25 — same WS-API method names as USDⓈ-M on ws-dapi.
WS_API_ENDPOINTS = [
    dict(
        name='create_order',
        transport='ws_api',
        ws_method='order.place',
        security_type=SecurityType.TRADE,
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/websocket-api
        # IP Request Weight: 0 (ORDERS pool consumed separately via is_order=True);
        # the SDK bucket clamps cost to max(1, weight) so the local
        # REQUEST_WEIGHT window still counts ≥1 — intentional defensive behavior.
        weight=0,
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
        name='get_orderbook',
        transport='rest',
        method=RequestMethod.GET,
        rest_url=CM_REST_HOST + '/dapi/v1/depth',
        security_type=SecurityType.NONE,
        weight=_depth_weight,
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
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Index-Price-and-Mark-Price
        # CM premiumIndex is flat weight 10 (CM docs do not list the dynamic
        # 1/10 split that UM has).
        weight=10,
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
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/All-Orders
        # 20 with `symbol`; 40 with `pair`.
        weight=_cm_all_orders_weight,
    ),
    dict(
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Place-Multiple-Orders
        # Request Weight 5; consumes the account ORDERS pool (parity with the
        # UM equivalent — the CM "Place Multiple Orders" page omits the order
        # rate-limit clause from its Request Weight block but every CM
        # order-placing endpoint goes through the CM 1-min ORDERS pool, as
        # documented on "New Order" and the CM common rate-limit page).
        name='create_batch_orders',
        transport='rest',
        method=RequestMethod.POST,
        rest_url=CM_REST_HOST + '/dapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        weight=5,
        is_order=True,
    ),
    dict(
        name='cancel_batch_orders',
        transport='rest',
        method=RequestMethod.DELETE,
        rest_url=CM_REST_HOST + '/dapi/v1/batchOrders',
        security_type=SecurityType.TRADE,
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Cancel-Multiple-Orders
        weight=1,
    ),
    # ----- Account / Position ------------------------------------------------
    dict(
        name='get_position_risk',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/positionRisk',
        security_type=SecurityType.USER_DATA,
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Position-Information
        weight=1,
    ),
    dict(
        name='get_user_trades',
        transport='rest',
        rest_url=CM_REST_HOST + '/dapi/v1/userTrades',
        security_type=SecurityType.USER_DATA,
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Account-Trade-List
        # 20 with `symbol`; 40 with `pair`.
        weight=_cm_user_trades_weight,
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
        # Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api/Get-Income-History
        weight=20,
    ),
    dict(
        name='get_leverage_bracket',
        transport='rest',
        # v2 supersedes v1; v1 is explicitly "not recommended" per docs.
        # v2 takes optional `symbol` (not v1's `pair`).
        # https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api/Notional-Bracket-for-Symbol
        rest_url=CM_REST_HOST + '/dapi/v2/leverageBracket',
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


# Neither VSCode Python language server nor Jedi server could handle class
# methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So we declare those methods (as stubs) in the per-mixin modules first, then
# override them here at import time.

for _getter_spec in WS_API_ENDPOINTS:
    define_getter(CMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]

for _getter_spec in REST_ENDPOINTS:
    define_getter(CMFuturesGetters, **_getter_spec)  # type: ignore[arg-type]
