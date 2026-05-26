"""Hermetic tests for COIN-M Futures WS-API trading/account endpoints.

Drives ``CMFuturesClient`` against the local :class:`WSAPIServer` harness and
asserts:

- each method sends the correct WS-API ``method`` and forwards params;
- order-placing endpoints consume the ORDERS pool (``is_order=True``);
- cancel / query endpoints do NOT consume the ORDERS pool;
- weights are correct per the spec.

Transport confirmed 2026-05-25:
- WS-API host: wss://ws-dapi.binance.com/ws-dapi/v1
- Methods: order.place, order.modify, order.cancel, order.status,
           account.status, account.balance

CM WS-API additions (2026-05-26):
- account.position added (C-W1 confirmed gap).
- v2/account.balance and v2/account.status NOT added (unconfirmed for CM).
- positionSide.dual.get NOT added (not documented on ws-dapi for CM).
- algoOrder.place/cancel NOT added (CM algo orders not on ws-dapi).
"""

import pytest

from binance import CMFuturesClient, Credentials
from binance.core.common.constants import SecurityType
from binance.futures.cm.endpoints import WS_API_ENDPOINTS
from binance.core.rate_limit.types import RateLimitType

from test.test_ws_api import WSAPIServer


_PORT = 9096


def _make_client(server) -> CMFuturesClient:
    client = CMFuturesClient(
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
async def test_cm_create_order_places_via_order_place():
    server = WSAPIServer(port=_PORT)
    server.on('order.place', result={'orderId': 1, 'status': 'NEW'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.create_order(
            symbol='BTCUSD_PERP', side='BUY', type='LIMIT',
            timeInForce='GTC', quantity='1', price='30000')
        assert result == {'orderId': 1, 'status': 'NEW'}

        sent = server.received[0]
        assert sent['method'] == 'order.place'
        assert sent['params']['symbol'] == 'BTCUSD_PERP'
        assert sent['params']['side'] == 'BUY'
        assert sent['params']['quantity'] == '1'
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
async def test_cm_modify_order_via_order_modify_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('order.modify', result={'orderId': 2, 'status': 'NEW'})
    await server.run()
    try:
        client = _make_client(server)
        await client.modify_order(
            symbol='BTCUSD_PERP', side='BUY', orderId=2,
            quantity='2', price='31000')
        sent = server.received[0]
        assert sent['method'] == 'order.modify'
        assert sent['params']['symbol'] == 'BTCUSD_PERP'
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
async def test_cm_cancel_order_via_order_cancel_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.cancel', result={'orderId': 3, 'status': 'CANCELED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.cancel_order(symbol='BTCUSD_PERP', orderId=3)
        sent = server.received[0]
        assert sent['method'] == 'order.cancel'
        assert sent['params']['symbol'] == 'BTCUSD_PERP'
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
async def test_cm_get_order_via_order_status_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.status', result={'orderId': 4, 'status': 'FILLED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_order(symbol='BTCUSD_PERP', orderId=4)
        sent = server.received[0]
        assert sent['method'] == 'order.status'
        assert sent['params']['symbol'] == 'BTCUSD_PERP'
        assert sent['params']['orderId'] == 4
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_account: is_order=False, weight=5, ws_method='account.status'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_account_via_account_status():
    server = WSAPIServer(port=_PORT)
    server.on('account.status', result={'totalInitialMargin': '0.00000000'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_account()
        assert result == {'totalInitialMargin': '0.00000000'}
        sent = server.received[0]
        assert sent['method'] == 'account.status'
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_balance: is_order=False, weight=5, ws_method='account.balance'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_balance_via_account_balance():
    server = WSAPIServer(port=_PORT)
    server.on('account.balance', result=[{'asset': 'BTC', 'balance': '0.50000000'}])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_balance()
        assert result == [{'asset': 'BTC', 'balance': '0.50000000'}]
        sent = server.received[0]
        assert sent['method'] == 'account.balance'
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_position: is_order=False, weight=5, ws_method='account.position'
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_position_via_account_position():
    server = WSAPIServer(port=_PORT)
    server.on('account.position', result=[{'symbol': 'BTCUSD_PERP', 'positionAmt': '10'}])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_position(pair='BTCUSD')
        assert result == [{'symbol': 'BTCUSD_PERP', 'positionAmt': '10'}]
        sent = server.received[0]
        assert sent['method'] == 'account.position'
        assert sent['params']['pair'] == 'BTCUSD'
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert _orders_used(client) == 0
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Declarative registry: WS_API_ENDPOINTS matches the spec.
# ---------------------------------------------------------------------------

def test_cm_ws_api_endpoints_registry_matches_spec():
    by_name = {entry['name']: entry for entry in WS_API_ENDPOINTS}

    expected = {
        # name: (ws_method, security, is_order, weight)
        'create_order': ('order.place', SecurityType.TRADE, True, 1),
        'modify_order': ('order.modify', SecurityType.TRADE, True, 1),
        'cancel_order': ('order.cancel', SecurityType.TRADE, False, 1),
        'get_order': ('order.status', SecurityType.USER_DATA, False, 1),
        'get_account': ('account.status', SecurityType.USER_DATA, False, 5),
        'get_balance': ('account.balance', SecurityType.USER_DATA, False, 5),
        'get_position': ('account.position', SecurityType.USER_DATA, False, 5),
    }

    assert set(expected) == set(by_name)
    for name, (ws_method, security, is_order, weight) in expected.items():
        entry = by_name[name]
        assert entry['ws_method'] == ws_method, f'{name}: ws_method mismatch'
        assert entry['security_type'] == security, f'{name}: security mismatch'
        assert entry.get('is_order', False) is is_order, f'{name}: is_order mismatch'
        assert entry['weight'] == weight, f'{name}: weight mismatch'
