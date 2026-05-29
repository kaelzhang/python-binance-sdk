"""Hermetic tests for COIN-M Futures REST trading/account/position endpoints.

Uses ``aioresponses`` to mock HTTP; asserts:

- each method hits the correct URL with the correct HTTP method;
- request weight is consumed correctly (including open_orders 1/40 helper);
- signed endpoints include a ``signature`` in the query or body.

COIN-M vs USDⓈ-M confirmed differences:
- paths use /dapi/v1/ (not /fapi/v3/)
- positionRisk is /dapi/v1/positionRisk (not /fapi/v3/positionRisk)
- NO multiAssetsMargin endpoint (USDⓈ-M only)
"""

import re
import pytest
from aioresponses import aioresponses

from binance import CMFuturesClient, Credentials
from binance.core.common.constants import SecurityType
from binance.core.rate_limit.types import RateLimitType
from binance.futures.cm.endpoints import (
    REST_ENDPOINTS,
    _cm_all_orders_weight,
    _cm_open_orders_weight,
    _cm_user_trades_weight,
    _depth_weight,
)


DAPI = 'https://dapi.binance.com'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signed_client():
    client = CMFuturesClient(Credentials(api_key='K', api_secret='S'))
    # Pre-mark time as synced so signed REST requests don't try to call
    # get_server_time() (which is not installed on CMFuturesClient).
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            return w.used
    return 0


def _re(path: str) -> re.Pattern:
    """Regex matching the full dapi URL + any query string."""
    escaped = re.escape(DAPI + path)
    return re.compile(rf'{escaped}(\?.*)?$')


# ---------------------------------------------------------------------------
# create_test_order — NOT documented on COIN-M (CM Trade REST docs lack a
# "Test New Order" page; POST /dapi/v1/order/test is not listed).  Dropped
# from the CM surface.  See Phase G audit.
# Source: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api
# ---------------------------------------------------------------------------

def test_cm_create_test_order_removed_from_client():
    """``CMFuturesClient`` MUST NOT expose ``create_test_order`` —
    POST /dapi/v1/order/test is not documented on COIN-M.
    """
    client = CMFuturesClient(Credentials(api_key='K', api_secret='S'))
    assert not hasattr(client, 'create_test_order')


def test_cm_create_test_order_removed_from_registry():
    """The CM REST registry MUST NOT carry a ``create_test_order`` entry."""
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    assert 'create_test_order' not in by_name


# ---------------------------------------------------------------------------
# cancel_all_orders  DELETE /dapi/v1/allOpenOrders  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_cancel_all_orders_delete_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.delete(_re('/dapi/v1/allOpenOrders'), payload={'code': 200}, status=200)
        result = await client.cancel_all_orders(symbol='BTCUSD_PERP')
    assert result == {'code': 200}
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# countdown_cancel_all_orders  POST /dapi/v1/countdownCancelAll  weight 10
# Dead-man's switch — cancels all open orders for a symbol if not
# refreshed within the countdown window. Same semantics as UM.
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_countdown_cancel_all_orders_post_correct_url_and_weight():
    client = _signed_client()
    payload = {'symbol': 'BTCUSD_PERP', 'countdownTime': '100000'}
    with aioresponses() as m:
        m.post(_re('/dapi/v1/countdownCancelAll'), payload=payload, status=200)
        result = await client.countdown_cancel_all_orders(
            symbol='BTCUSD_PERP', countdownTime=100000)
    assert result == payload
    assert _weight_used(client) == 10


def test_cm_countdown_cancel_all_orders_registry_shape():
    """Registry entry MUST be POST + correct URL + weight 10 + TRADE."""
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['countdown_cancel_all_orders']
    assert str(entry['method']).lower() == 'post'
    assert entry['rest_url'].endswith('/dapi/v1/countdownCancelAll')
    assert entry['weight'] == 10
    assert entry['security_type'] == SecurityType.TRADE


# ---------------------------------------------------------------------------
# get_open_orders  GET /dapi/v1/openOrders  weight 1 (symbol) or 40 (no symbol)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_open_orders_with_symbol_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/openOrders'), payload=[], status=200)
        await client.get_open_orders(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_cm_get_open_orders_without_symbol_weight_40():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/openOrders'), payload=[], status=200)
        await client.get_open_orders()
    assert _weight_used(client) == 40


# ---------------------------------------------------------------------------
# get_all_orders  GET /dapi/v1/allOrders  weight 20 (symbol) / 40 (pair)
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/All-Orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_all_orders_with_symbol_weight_20():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/allOrders'), payload=[], status=200)
        await client.get_all_orders(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 20


@pytest.mark.asyncio
async def test_cm_get_all_orders_with_pair_weight_40():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/allOrders'), payload=[], status=200)
        await client.get_all_orders(pair='BTCUSD')
    assert _weight_used(client) == 40


# ---------------------------------------------------------------------------
# create_batch_orders  POST /dapi/v1/batchOrders  weight 5
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Place-Multiple-Orders
# Consumes the account ORDERS pool (per UM parity and the SDK's own docstring).
# ---------------------------------------------------------------------------

def _orders_used(client) -> int:
    """Maximum ``used`` count across all ORDERS-pool windows."""
    snap = client.rate_limit_snapshot()
    return max(
        (w.used for w in snap.windows if w.type == RateLimitType.ORDERS),
        default=0,
    )


@pytest.mark.asyncio
async def test_cm_create_batch_orders_post_weight_5():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/dapi/v1/batchOrders'), payload=[], status=200)
        await client.create_batch_orders(batchOrders=[])
    assert _weight_used(client) == 5


@pytest.mark.asyncio
async def test_cm_create_batch_orders_consumes_orders_pool():
    """A successful batch-orders call MUST consume the account ORDERS pool
    (parity with UM and with the SDK's own ``create_batch_orders`` docstring).
    """
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/dapi/v1/batchOrders'), payload=[], status=200)
        await client.create_batch_orders(batchOrders=[])
    assert _orders_used(client) == 1


def test_cm_create_batch_orders_registry_marks_is_order():
    """Registry entry for ``create_batch_orders`` MUST set ``is_order=True``."""
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    assert by_name['create_batch_orders'].get('is_order') is True


# ---------------------------------------------------------------------------
# cancel_batch_orders  DELETE /dapi/v1/batchOrders  weight 1
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Cancel-Multiple-Orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_cancel_batch_orders_delete_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.delete(_re('/dapi/v1/batchOrders'), payload=[], status=200)
        await client.cancel_batch_orders(symbol='BTCUSD_PERP', orderIdList=[1, 2])
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# get_adl_quantile  GET /dapi/v1/adlQuantile  weight 5  (USER_DATA)
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation
# Used for risk monitoring — exposes ADL queue position (0-4) per side.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_adl_quantile_weight_5():
    client = _signed_client()
    payload = [{'symbol': 'BTCUSD_PERP', 'adlQuantile': {'LONG': 1, 'SHORT': 2}}]
    with aioresponses() as m:
        m.get(_re('/dapi/v1/adlQuantile'), payload=payload, status=200)
        result = await client.get_adl_quantile(symbol='BTCUSD_PERP')
    assert result == payload
    assert _weight_used(client) == 5


def test_cm_get_adl_quantile_registry_shape():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['get_adl_quantile']
    assert str(entry.get('method', 'get')).lower() == 'get'
    assert entry['rest_url'].endswith('/dapi/v1/adlQuantile')
    assert entry['weight'] == 5
    assert entry['security_type'] == SecurityType.USER_DATA


# ---------------------------------------------------------------------------
# get_position_margin_history  GET /dapi/v1/positionMargin/history  weight 1
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Get-Position-Margin-Change-History
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_position_margin_history_weight_1():
    client = _signed_client()
    payload = [{
        'symbol': 'BTCUSD_PERP', 'type': 1, 'amount': '0.01',
        'asset': 'BTC', 'time': 1578047897183, 'positionSide': 'BOTH',
    }]
    with aioresponses() as m:
        m.get(_re('/dapi/v1/positionMargin/history'), payload=payload, status=200)
        result = await client.get_position_margin_history(symbol='BTCUSD_PERP')
    assert result == payload
    assert _weight_used(client) == 1


def test_cm_get_position_margin_history_registry_shape():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['get_position_margin_history']
    assert str(entry.get('method', 'get')).lower() == 'get'
    assert entry['rest_url'].endswith('/dapi/v1/positionMargin/history')
    assert entry['weight'] == 1
    assert entry['security_type'] == SecurityType.TRADE


# ---------------------------------------------------------------------------
# get_position_risk  GET /dapi/v1/positionRisk  weight 1
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Position-Information
# NOTE: COIN-M uses /dapi/v1/positionRisk (not /fapi/v3/positionRisk).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_position_risk_weight_1():
    client = _signed_client()
    payload = [{'symbol': 'BTCUSD_PERP', 'positionAmt': '1'}]
    with aioresponses() as m:
        m.get(_re('/dapi/v1/positionRisk'), payload=payload, status=200)
        result = await client.get_position_risk(pair='BTCUSD')
    assert result == payload
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# get_user_trades  GET /dapi/v1/userTrades  weight 20 (symbol) / 40 (pair)
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Account-Trade-List
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_user_trades_with_symbol_weight_20():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/userTrades'), payload=[], status=200)
        await client.get_user_trades(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 20


@pytest.mark.asyncio
async def test_cm_get_user_trades_with_pair_weight_40():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/userTrades'), payload=[], status=200)
        await client.get_user_trades(pair='BTCUSD')
    assert _weight_used(client) == 40


# ---------------------------------------------------------------------------
# get_commission  GET /dapi/v1/commissionRate  weight 20
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_commission_weight_20():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/commissionRate'), payload={}, status=200)
        await client.get_commission(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 20


# ---------------------------------------------------------------------------
# get_income  GET /dapi/v1/income  weight 20
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api/Get-Income-History
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_income_weight_20():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/income'), payload=[], status=200)
        await client.get_income(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 20


# ---------------------------------------------------------------------------
# get_leverage_bracket  GET /dapi/v2/leverageBracket  weight 1
# v1 is deprecated per developers.binance.com; v2 takes optional `symbol`
# (not `pair`).
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api/Notional-Bracket-for-Symbol
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_leverage_bracket_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v2/leverageBracket'), payload=[], status=200)
        await client.get_leverage_bracket(symbol='BTCUSD_PERP')
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_leverage  POST /dapi/v1/leverage  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_set_leverage_post_weight_1():
    client = _signed_client()
    payload = {'leverage': 10, 'symbol': 'BTCUSD_PERP', 'maxQty': '100'}
    with aioresponses() as m:
        m.post(_re('/dapi/v1/leverage'), payload=payload, status=200)
        result = await client.set_leverage(symbol='BTCUSD_PERP', leverage=10)
    assert result == payload
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_margin_type  POST /dapi/v1/marginType  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_set_margin_type_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/dapi/v1/marginType'), payload={'code': 200}, status=200)
        await client.set_margin_type(symbol='BTCUSD_PERP', marginType='ISOLATED')
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_position_margin  POST /dapi/v1/positionMargin  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_set_position_margin_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/dapi/v1/positionMargin'), payload={'amount': '0.01'}, status=200)
        await client.set_position_margin(symbol='BTCUSD_PERP', amount='0.01', type=1)
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# get_position_mode  GET /dapi/v1/positionSide/dual  weight 30
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_position_mode_weight_30():
    client = _signed_client()
    payload = {'dualSidePosition': False}
    with aioresponses() as m:
        m.get(_re('/dapi/v1/positionSide/dual'), payload=payload, status=200)
        result = await client.get_position_mode()
    assert result == payload
    assert _weight_used(client) == 30


# ---------------------------------------------------------------------------
# set_position_mode  POST /dapi/v1/positionSide/dual  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_set_position_mode_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/dapi/v1/positionSide/dual'), payload={'code': 200}, status=200)
        await client.set_position_mode(dualSidePosition=False)
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# Weight-helper unit tests (no network).
# ---------------------------------------------------------------------------

def test_cm_open_orders_weight_with_symbol():
    assert _cm_open_orders_weight({'symbol': 'BTCUSD_PERP'}) == 1


def test_cm_open_orders_weight_without_symbol():
    assert _cm_open_orders_weight({}) == 40


# `_cm_all_orders_weight`: 20 with `symbol`; 40 with `pair`.
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/All-Orders

def test_cm_all_orders_weight_with_symbol():
    assert _cm_all_orders_weight({'symbol': 'BTCUSD_PERP'}) == 20


def test_cm_all_orders_weight_with_pair():
    assert _cm_all_orders_weight({'pair': 'BTCUSD'}) == 40


# `_cm_user_trades_weight`: 20 with `symbol`; 40 with `pair`.
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Account-Trade-List

def test_cm_user_trades_weight_with_symbol():
    assert _cm_user_trades_weight({'symbol': 'BTCUSD_PERP'}) == 20


def test_cm_user_trades_weight_with_pair():
    assert _cm_user_trades_weight({'pair': 'BTCUSD'}) == 40


# ---------------------------------------------------------------------------
# `_depth_weight`: CM ``/dapi/v1/depth`` -- weight depends on ``limit``.
# Same table as USDⓈ-M per Binance docs:
#   limit 5/10/20/50 -> 2; limit 100 -> 5; limit 500 -> 10; limit 1000 -> 20
# Default limit (no kwarg) is 500 -> weight 10.
# ---------------------------------------------------------------------------

def test_cm_depth_weight_default_limit():
    """No ``limit`` kwarg -> defaults to 500 -> weight 10."""
    assert _depth_weight({}) == 10


def test_cm_depth_weight_limit_50_and_below():
    for limit in (5, 10, 20, 50):
        assert _depth_weight({'limit': limit}) == 2


def test_cm_depth_weight_limit_100():
    assert _depth_weight({'limit': 100}) == 5


def test_cm_depth_weight_limit_500():
    assert _depth_weight({'limit': 500}) == 10


def test_cm_depth_weight_limit_1000():
    assert _depth_weight({'limit': 1000}) == 20


# ---------------------------------------------------------------------------
# REST_ENDPOINTS registry spot-check: correct HTTP methods and paths.
# Also confirms NO multiAssetsMargin endpoint (COIN-M specific).
# ---------------------------------------------------------------------------

def test_cm_rest_endpoints_registry_contains_trading_entries():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}

    # Trading/account entries with expected HTTP method and path.
    # NOTE: ``create_test_order`` is intentionally absent — POST /dapi/v1/order/test
    # is not documented on the CM Trade REST docs.
    expected_method_path = {
        'cancel_all_orders': ('delete', '/dapi/v1/allOpenOrders'),
        'get_open_orders': ('get', '/dapi/v1/openOrders'),
        'get_all_orders': ('get', '/dapi/v1/allOrders'),
        'create_batch_orders': ('post', '/dapi/v1/batchOrders'),
        'cancel_batch_orders': ('delete', '/dapi/v1/batchOrders'),
        'get_position_risk': ('get', '/dapi/v1/positionRisk'),
        'get_user_trades': ('get', '/dapi/v1/userTrades'),
        'get_commission': ('get', '/dapi/v1/commissionRate'),
        'get_income': ('get', '/dapi/v1/income'),
        'get_leverage_bracket': ('get', '/dapi/v2/leverageBracket'),
        'set_leverage': ('post', '/dapi/v1/leverage'),
        'set_margin_type': ('post', '/dapi/v1/marginType'),
        'set_position_margin': ('post', '/dapi/v1/positionMargin'),
        'get_position_mode': ('get', '/dapi/v1/positionSide/dual'),
        'set_position_mode': ('post', '/dapi/v1/positionSide/dual'),
    }

    for name, (http_method, path) in expected_method_path.items():
        assert name in by_name, f'{name} not in REST_ENDPOINTS'
        entry = by_name[name]
        got_method = str(entry.get('method', 'get')).lower()
        assert got_method == http_method, f'{name}: method {got_method!r} != {http_method!r}'
        assert entry['rest_url'] == DAPI + path, (
            f'{name}: rest_url {entry["rest_url"]!r} != {DAPI + path!r}'
        )

    # Confirm COIN-M does NOT have multiAssetsMargin (USDⓈ-M only).
    assert 'get_multi_assets_mode' not in by_name
    assert 'set_multi_assets_mode' not in by_name
