"""Hermetic tests for USDⓈ-M Futures REST trading/account/position endpoints.

Uses ``aioresponses`` to mock HTTP; asserts:

- each method hits the correct URL with the correct HTTP method;
- request weight is consumed correctly (including open_orders 1/40 helper);
- signed endpoints include a ``signature`` in the query or body.

Note: ``get_position_mode`` was migrated from REST to WS-API
(``positionSide.dual.get``) in 2026-05-26; its REST test now lives in
``test_um_ws_api_trading.py``.
"""

import re
import pytest
from aioresponses import aioresponses

from binance import UMFuturesClient, Credentials
from binance.core.common.constants import SecurityType
from binance.core.rate_limit.types import RateLimitType
from binance.futures.um.endpoints import (
    REST_ENDPOINTS,
    _depth_weight,
    _um_open_orders_weight,
)


FAPI = 'https://fapi.binance.com'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signed_client():
    client = UMFuturesClient(Credentials(api_key='K', api_secret='S'))
    # Pre-mark time as synced so signed REST requests don't try to call
    # get_server_time() (which is not installed on UMFuturesClient).
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            return w.used
    return 0


def _re(path: str) -> re.Pattern:
    """Regex matching the full fapi URL + any query string."""
    escaped = re.escape(FAPI + path)
    return re.compile(rf'{escaped}(\?.*)?$')


# ---------------------------------------------------------------------------
# create_test_order  POST /fapi/v1/order/test  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_test_order_post_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/order/test'), payload={}, status=200)
        result = await client.create_test_order(
            symbol='BTCUSDT', side='BUY', type='MARKET', quantity='0.01')
    assert result == {}
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# cancel_all_orders  DELETE /fapi/v1/allOpenOrders  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_all_orders_delete_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.delete(_re('/fapi/v1/allOpenOrders'), payload={'code': 200}, status=200)
        result = await client.cancel_all_orders(symbol='BTCUSDT')
    assert result == {'code': 200}
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# countdown_cancel_all_orders  POST /fapi/v1/countdownCancelAll  weight 10
# Dead-man's switch — cancels all open orders for a symbol if not
# refreshed within the countdown window. Critical safety mechanism for
# live trading; the SDK MUST expose it.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_countdown_cancel_all_orders_post_correct_url_and_weight():
    client = _signed_client()
    payload = {'symbol': 'BTCUSDT', 'countdownTime': '100000'}
    with aioresponses() as m:
        m.post(_re('/fapi/v1/countdownCancelAll'), payload=payload, status=200)
        result = await client.countdown_cancel_all_orders(
            symbol='BTCUSDT', countdownTime=100000)
    assert result == payload
    assert _weight_used(client) == 10


def test_countdown_cancel_all_orders_registry_shape():
    """Registry entry MUST be POST + correct URL + weight 10 + TRADE."""
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['countdown_cancel_all_orders']
    assert str(entry['method']).lower() == 'post'
    assert entry['rest_url'].endswith('/fapi/v1/countdownCancelAll')
    assert entry['weight'] == 10
    assert entry['security_type'] == SecurityType.TRADE


# ---------------------------------------------------------------------------
# get_open_orders  GET /fapi/v1/openOrders  weight 1 (symbol) or 40 (no symbol)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_orders_with_symbol_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/openOrders'), payload=[], status=200)
        await client.get_open_orders(symbol='BTCUSDT')
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_get_open_orders_without_symbol_weight_40():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/openOrders'), payload=[], status=200)
        await client.get_open_orders()
    assert _weight_used(client) == 40


# ---------------------------------------------------------------------------
# get_all_orders  GET /fapi/v1/allOrders  weight 5
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_all_orders_weight_5():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/allOrders'), payload=[], status=200)
        await client.get_all_orders(symbol='BTCUSDT')
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# create_batch_orders  POST /fapi/v1/batchOrders  weight 5
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_batch_orders_post_weight_5():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/batchOrders'), payload=[], status=200)
        await client.create_batch_orders(batchOrders=[])
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# cancel_batch_orders  DELETE /fapi/v1/batchOrders  weight 1
# (Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/
# trade/rest-api/Cancel-Multiple-Orders — Request Weight: 1.)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_batch_orders_delete_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.delete(_re('/fapi/v1/batchOrders'), payload=[], status=200)
        await client.cancel_batch_orders(symbol='BTCUSDT', orderIdList=[1, 2])
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# get_adl_quantile  GET /fapi/v1/adlQuantile  weight 5  (USER_DATA)
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation
# Used for risk monitoring — exposes ADL queue position (0-4) per side.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_adl_quantile_weight_5():
    client = _signed_client()
    payload = [{'symbol': 'BTCUSDT', 'adlQuantile': {'LONG': 1, 'SHORT': 2}}]
    with aioresponses() as m:
        m.get(_re('/fapi/v1/adlQuantile'), payload=payload, status=200)
        result = await client.get_adl_quantile(symbol='BTCUSDT')
    assert result == payload
    assert _weight_used(client) == 5


def test_get_adl_quantile_registry_shape():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['get_adl_quantile']
    # GET is the default; explicit `method` may be absent.
    assert str(entry.get('method', 'get')).lower() == 'get'
    assert entry['rest_url'].endswith('/fapi/v1/adlQuantile')
    assert entry['weight'] == 5
    assert entry['security_type'] == SecurityType.USER_DATA


# ---------------------------------------------------------------------------
# get_position_margin_history  GET /fapi/v1/positionMargin/history  weight 1
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Get-Position-Margin-Change-History
# TRADE security per the doc page heading "(TRADE)".
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_position_margin_history_weight_1():
    client = _signed_client()
    payload = [{
        'symbol': 'BTCUSDT', 'type': 1, 'amount': '23',
        'asset': 'USDT', 'time': 1578047897183, 'positionSide': 'BOTH',
    }]
    with aioresponses() as m:
        m.get(_re('/fapi/v1/positionMargin/history'), payload=payload, status=200)
        result = await client.get_position_margin_history(symbol='BTCUSDT')
    assert result == payload
    assert _weight_used(client) == 1


def test_get_position_margin_history_registry_shape():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}
    entry = by_name['get_position_margin_history']
    assert str(entry.get('method', 'get')).lower() == 'get'
    assert entry['rest_url'].endswith('/fapi/v1/positionMargin/history')
    assert entry['weight'] == 1
    assert entry['security_type'] == SecurityType.TRADE


# ---------------------------------------------------------------------------
# get_position_risk  GET /fapi/v3/positionRisk  weight 5
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_position_risk_weight_5():
    client = _signed_client()
    payload = [{'symbol': 'BTCUSDT', 'positionAmt': '0.01'}]
    with aioresponses() as m:
        m.get(_re('/fapi/v3/positionRisk'), payload=payload, status=200)
        result = await client.get_position_risk(symbol='BTCUSDT')
    assert result == payload
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# get_user_trades  GET /fapi/v1/userTrades  weight 5
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_trades_weight_5():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/userTrades'), payload=[], status=200)
        await client.get_user_trades(symbol='BTCUSDT')
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# get_commission  GET /fapi/v1/commissionRate  weight 20
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_commission_weight_20():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/commissionRate'), payload={}, status=200)
        await client.get_commission(symbol='BTCUSDT')
    assert _weight_used(client) == 20


# ---------------------------------------------------------------------------
# get_income  GET /fapi/v1/income  weight 30
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_income_weight_30():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/income'), payload=[], status=200)
        await client.get_income(symbol='BTCUSDT')
    assert _weight_used(client) == 30


# ---------------------------------------------------------------------------
# get_leverage_bracket  GET /fapi/v1/leverageBracket  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_leverage_bracket_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/leverageBracket'), payload=[], status=200)
        await client.get_leverage_bracket(symbol='BTCUSDT')
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_leverage  POST /fapi/v1/leverage  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_leverage_post_weight_1():
    client = _signed_client()
    payload = {'leverage': 10, 'symbol': 'BTCUSDT'}
    with aioresponses() as m:
        m.post(_re('/fapi/v1/leverage'), payload=payload, status=200)
        result = await client.set_leverage(symbol='BTCUSDT', leverage=10)
    assert result == payload
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_margin_type  POST /fapi/v1/marginType  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_margin_type_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/marginType'), payload={'code': 200}, status=200)
        await client.set_margin_type(symbol='BTCUSDT', marginType='ISOLATED')
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_position_margin  POST /fapi/v1/positionMargin  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_position_margin_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/positionMargin'), payload={'amount': '100'}, status=200)
        await client.set_position_margin(symbol='BTCUSDT', amount='100', type=1)
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# set_position_mode  POST /fapi/v1/positionSide/dual  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_position_mode_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/positionSide/dual'), payload={'code': 200}, status=200)
        await client.set_position_mode(dualSidePosition=False)
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# get_multi_assets_mode  GET /fapi/v1/multiAssetsMargin  weight 30
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_multi_assets_mode_weight_30():
    client = _signed_client()
    payload = {'multiAssetsMargin': False}
    with aioresponses() as m:
        m.get(_re('/fapi/v1/multiAssetsMargin'), payload=payload, status=200)
        result = await client.get_multi_assets_mode()
    assert result == payload
    assert _weight_used(client) == 30


# ---------------------------------------------------------------------------
# set_multi_assets_mode  POST /fapi/v1/multiAssetsMargin  weight 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_multi_assets_mode_post_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/multiAssetsMargin'), payload={'code': 200}, status=200)
        await client.set_multi_assets_mode(multiAssetsMargin=False)
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# Weight-helper unit tests (no network).
# ---------------------------------------------------------------------------

def test_um_open_orders_weight_with_symbol():
    assert _um_open_orders_weight({'symbol': 'BTCUSDT'}) == 1


def test_um_open_orders_weight_without_symbol():
    assert _um_open_orders_weight({}) == 40


# ---------------------------------------------------------------------------
# `_depth_weight`: UM ``/fapi/v1/depth`` -- weight depends on ``limit``.
# Table per Binance UM docs:
#   limit 5/10/20/50 -> 2; limit 100 -> 5; limit 500 -> 10; limit 1000 -> 20
# Default limit (no kwarg) is 500 per Binance docs -> weight 10.
# ---------------------------------------------------------------------------

def test_um_depth_weight_default_limit():
    """No ``limit`` kwarg -> defaults to 500 -> weight 10."""
    assert _depth_weight({}) == 10


def test_um_depth_weight_limit_50_and_below():
    for limit in (5, 10, 20, 50):
        assert _depth_weight({'limit': limit}) == 2


def test_um_depth_weight_limit_100():
    assert _depth_weight({'limit': 100}) == 5


def test_um_depth_weight_limit_500():
    assert _depth_weight({'limit': 500}) == 10


def test_um_depth_weight_limit_1000():
    assert _depth_weight({'limit': 1000}) == 20


# ---------------------------------------------------------------------------
# REST_ENDPOINTS registry spot-check: correct HTTP methods and paths.
# ---------------------------------------------------------------------------

def test_rest_endpoints_registry_contains_trading_entries():
    by_name = {entry['name']: entry for entry in REST_ENDPOINTS}

    # Trading entries with expected HTTP method and path.
    expected_method_path = {
        'create_test_order': ('post', '/fapi/v1/order/test'),
        'cancel_all_orders': ('delete', '/fapi/v1/allOpenOrders'),
        'countdown_cancel_all_orders': ('post', '/fapi/v1/countdownCancelAll'),
        'get_adl_quantile': ('get', '/fapi/v1/adlQuantile'),
        'get_position_margin_history': ('get', '/fapi/v1/positionMargin/history'),
        'get_open_orders': ('get', '/fapi/v1/openOrders'),
        'get_all_orders': ('get', '/fapi/v1/allOrders'),
        'create_batch_orders': ('post', '/fapi/v1/batchOrders'),
        'cancel_batch_orders': ('delete', '/fapi/v1/batchOrders'),
        'get_position_risk': ('get', '/fapi/v3/positionRisk'),
        'get_user_trades': ('get', '/fapi/v1/userTrades'),
        'get_commission': ('get', '/fapi/v1/commissionRate'),
        'get_income': ('get', '/fapi/v1/income'),
        'get_leverage_bracket': ('get', '/fapi/v1/leverageBracket'),
        'set_leverage': ('post', '/fapi/v1/leverage'),
        'set_margin_type': ('post', '/fapi/v1/marginType'),
        'set_position_margin': ('post', '/fapi/v1/positionMargin'),
        'set_position_mode': ('post', '/fapi/v1/positionSide/dual'),
        'get_multi_assets_mode': ('get', '/fapi/v1/multiAssetsMargin'),
        'set_multi_assets_mode': ('post', '/fapi/v1/multiAssetsMargin'),
    }

    for name, (http_method, path) in expected_method_path.items():
        assert name in by_name, f'{name} not in REST_ENDPOINTS'
        entry = by_name[name]
        got_method = str(entry.get('method', 'get')).lower()
        assert got_method == http_method, f'{name}: method {got_method!r} != {http_method!r}'
        assert entry['rest_url'] == FAPI + path, (
            f'{name}: rest_url {entry["rest_url"]!r} != {FAPI + path!r}'
        )
