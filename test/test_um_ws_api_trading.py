"""Hermetic tests for USDⓈ-M Futures WS-API trading/account endpoints.

Drives ``UMFuturesClient`` against the local :class:`WSAPIServer` harness and
asserts:

- each method sends the correct WS-API ``method`` and forwards params;
- order-placing endpoints consume the ORDERS pool (``is_order=True``);
- cancel / query endpoints do NOT consume the ORDERS pool;
- weights are correct per the spec.

v2 migration (2026-05-26): ``get_account`` now uses ``v2/account.status``;
``get_balance`` now uses ``v2/account.balance``.  v1 entries are dropped.

v2 migration (2026-05-30): ``get_position`` now uses ``v2/account.position``
per developers.binance.com Position-Info-V2 docs; v1 entry is dropped.
"""

import pytest

from binance import UMFuturesClient, Credentials
from binance.core.common.constants import SecurityType
from binance.futures.um.endpoints import WS_API_ENDPOINTS
from binance.core.rate_limit.types import RateLimitType

from test.test_ws_api import WSAPIServer


_PORT = 9091


def _make_client(server) -> UMFuturesClient:
    client = UMFuturesClient(
        Credentials(api_key='K', api_secret='S'),
        ws_api_host=server.uri,
    )
    # Pre-mark time as synced so signed requests don't emit a prior `time` frame.
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0].used


def _orders_used(client) -> int:
    snap = client.rate_limit_snapshot()
    orders = [w for w in snap.windows if w.type == RateLimitType.ORDERS]
    assert orders
    assert len({w.used for w in orders}) == 1
    return orders[0].used


# ---------------------------------------------------------------------------
# create_order: is_order=True, weight=1, ws_method='order.place'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_create_order_places_via_order_place():
    server = WSAPIServer(port=_PORT)
    server.on('order.place', result={'orderId': 1, 'status': 'NEW'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.create_order(
            symbol='BTCUSDT', side='BUY', type='LIMIT',
            timeInForce='GTC', quantity='0.01', price='30000')
        assert result == {'orderId': 1, 'status': 'NEW'}

        sent = server.received[0]
        assert sent['method'] == 'order.place'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['side'] == 'BUY'
        assert sent['params']['quantity'] == '0.01'
        assert sent['params']['price'] == '30000'
        # SIGNED: auth fields attached.
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert 'timestamp' in sent['params']

        # order.place is an order-placing endpoint -> ORDERS pool consumed.
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# modify_order: is_order=True, weight=1, ws_method='order.modify'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_modify_order_via_order_modify_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('order.modify', result={'orderId': 2, 'status': 'NEW'})
    await server.run()
    try:
        client = _make_client(server)
        await client.modify_order(
            symbol='BTCUSDT', side='BUY', orderId=2,
            quantity='0.02', price='31000')
        sent = server.received[0]
        assert sent['method'] == 'order.modify'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['orderId'] == 2
        # modify_order is is_order=True -> ORDERS pool consumed.
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# cancel_order: is_order=False, weight=1, ws_method='order.cancel'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_cancel_order_via_order_cancel_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.cancel', result={'orderId': 3, 'status': 'CANCELED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.cancel_order(symbol='BTCUSDT', orderId=3)
        sent = server.received[0]
        assert sent['method'] == 'order.cancel'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['orderId'] == 3
        # cancel_order does NOT consume the ORDERS pool.
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_order: is_order=False, weight=1, ws_method='order.status'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_order_via_order_status_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.status', result={'orderId': 4, 'status': 'FILLED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_order(symbol='BTCUSDT', orderId=4)
        sent = server.received[0]
        assert sent['method'] == 'order.status'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['orderId'] == 4
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_account: is_order=False, weight=5, ws_method='v2/account.status'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_account_via_v2_account_status():
    server = WSAPIServer(port=_PORT)
    server.on('v2/account.status', result={'totalWalletBalance': '1000.00'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_account()
        assert result == {'totalWalletBalance': '1000.00'}
        sent = server.received[0]
        assert sent['method'] == 'v2/account.status'
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_balance: is_order=False, weight=5, ws_method='v2/account.balance'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_balance_via_v2_account_balance():
    server = WSAPIServer(port=_PORT)
    server.on('v2/account.balance', result=[{'asset': 'USDT', 'balance': '500.00'}])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_balance()
        assert result == [{'asset': 'USDT', 'balance': '500.00'}]
        sent = server.received[0]
        assert sent['method'] == 'v2/account.balance'
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_position: is_order=False, weight=5, ws_method='v2/account.position'
# (Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/
# trade/websocket-api/Position-Info-V2 — V2 is the documented latest variant,
# legacy v1 ``account.position`` retired.)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_position_via_v2_account_position():
    server = WSAPIServer(port=_PORT)
    server.on('v2/account.position', result=[{'symbol': 'BTCUSDT', 'positionAmt': '0.1'}])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_position(symbol='BTCUSDT')
        assert result == [{'symbol': 'BTCUSDT', 'positionAmt': '0.1'}]
        sent = server.received[0]
        assert sent['method'] == 'v2/account.position'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_position_mode: is_order=False, weight=30, ws_method='positionSide.dual.get'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_position_mode_via_positionside_dual_get():
    server = WSAPIServer(port=_PORT)
    server.on('positionSide.dual.get', result={'dualSidePosition': False})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_position_mode()
        assert result == {'dualSidePosition': False}
        sent = server.received[0]
        assert sent['method'] == 'positionSide.dual.get'
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        assert _weight_used(client) == 30
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# create_algo_order: is_order=False, weight=0, ws_method='algoOrder.place'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_create_algo_order_via_algo_order_place():
    server = WSAPIServer(port=_PORT)
    server.on('algoOrder.place', result={'algoId': 123, 'clientAlgoId': 'myAlgo'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.create_algo_order(
            symbol='BTCUSDT', side='BUY', quantity='0.5', duration=3600)
        assert result == {'algoId': 123, 'clientAlgoId': 'myAlgo'}
        sent = server.received[0]
        assert sent['method'] == 'algoOrder.place'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['side'] == 'BUY'
        assert sent['params']['quantity'] == '0.5'
        assert sent['params']['duration'] == 3600
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        # algoOrder.place draws from a separate algo quota, NOT ORDERS pool.
        assert _orders_used(client) == 0
        # Documented weight is 0; the SDK's rate-limit bucket clamps to
        # max(1, weight), so the recorded cost is 1.
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# cancel_algo_order: is_order=False, weight=1, ws_method='algoOrder.cancel'
# (Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/
# trade/websocket-api/Cancel-Algo-Order — Request Weight: 1.)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_cancel_algo_order_via_algo_order_cancel():
    server = WSAPIServer(port=_PORT)
    server.on('algoOrder.cancel', result={'algoId': 123, 'success': True})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.cancel_algo_order(algoId=123)
        assert result == {'algoId': 123, 'success': True}
        sent = server.received[0]
        assert sent['method'] == 'algoOrder.cancel'
        assert sent['params']['algoId'] == 123
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        # Documented weight is 1 (developers.binance.com).
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Declarative registry: WS_API_ENDPOINTS matches the spec.
# ---------------------------------------------------------------------------

def test_ws_api_endpoints_registry_matches_spec():
    by_name = {entry['name']: entry for entry in WS_API_ENDPOINTS}

    # Entries with dynamic weight (callable) are validated by their own
    # tests in `test_um_ws_api_market_data.py`; exclude them here so the
    # static-weight assertion stays simple.
    dynamic_weight_entries = {
        'get_orderbook_ws',
        'get_ticker_price_ws',
        'get_ticker_book_ws',
    }
    static_by_name = {n: e for n, e in by_name.items() if n not in dynamic_weight_entries}

    expected = {
        # name: (ws_method, security, is_order, weight)
        # session.status: weight 2, security NONE per general-info docs.
        # https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info
        'get_session_status': ('session.status', SecurityType.NONE, False, 2),
        # order.place + order.modify: docs say IP weight 0 (ORDERS pool still
        # consumed via is_order=True); SDK bucket clamps to max(1, weight) so
        # local request_weight window still records >=1.
        # https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/New-Order
        # https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Modify-Order
        'create_order': ('order.place', SecurityType.TRADE, True, 0),
        'modify_order': ('order.modify', SecurityType.TRADE, True, 0),
        'cancel_order': ('order.cancel', SecurityType.TRADE, False, 1),
        'get_order': ('order.status', SecurityType.USER_DATA, False, 1),
        'get_account': ('v2/account.status', SecurityType.USER_DATA, False, 5),
        'get_balance': ('v2/account.balance', SecurityType.USER_DATA, False, 5),
        'get_position': ('v2/account.position', SecurityType.USER_DATA, False, 5),
        'get_position_mode': ('positionSide.dual.get', SecurityType.USER_DATA, False, 30),
        'create_algo_order': ('algoOrder.place', SecurityType.TRADE, False, 0),
        'cancel_algo_order': ('algoOrder.cancel', SecurityType.TRADE, False, 1),
    }

    assert set(expected) == set(static_by_name)
    for name, (ws_method, security, is_order, weight) in expected.items():
        entry = static_by_name[name]
        assert entry['ws_method'] == ws_method, f'{name}: ws_method mismatch'
        assert entry['security_type'] == security, f'{name}: security mismatch'
        assert entry.get('is_order', False) is is_order, f'{name}: is_order mismatch'
        assert entry['weight'] == weight, f'{name}: weight mismatch'

    # Dynamic-weight entries exist with callable weights — full
    # validation lives in `test_um_ws_api_market_data.py`.
    for name in dynamic_weight_entries:
        assert name in by_name, f'{name}: expected in registry'
        assert callable(by_name[name]['weight']), f'{name}: weight should be callable'


def test_create_order_registry_weight_is_zero():
    """``order.place`` IP weight is 0 per docs; SDK bucket clamps to 1.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/New-Order
    """
    entry = next(e for e in WS_API_ENDPOINTS if e['name'] == 'create_order')
    assert entry['weight'] == 0
    assert entry['is_order'] is True


def test_modify_order_registry_weight_is_zero():
    """``order.modify`` IP weight is 0 per docs; SDK bucket clamps to 1.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Modify-Order
    """
    entry = next(e for e in WS_API_ENDPOINTS if e['name'] == 'modify_order')
    assert entry['weight'] == 0
    assert entry['is_order'] is True
