"""Hermetic tests for the WS-API trading endpoints (G-04).

The trading surface (``order.*`` / ``orderList.*`` / ``sor.*`` /
``openOrders.*``) was migrated from REST to the WebSocket API. These drive the
public ``Client`` trading methods against the local :class:`WSAPIServer`
request/response harness (reused from ``test_ws_api``) and assert:

- each method sends the correct WS-API ``method`` and forwards its params;
- order-PLACING endpoints consume the account ORDERS pool (``is_order``) while
  cancels / amends / queries do not;
- params-dependent weights resolve per call (``order.test`` /
  ``openOrders.status``);
- the declarative registry matches the documented spec.
"""

import pytest

from binance import Client
from binance.core.common.constants import SecurityType
from binance.apis.ws_api import (
    WS_APIS,
    define_ws_getter,
    _order_test_weight,
    _open_orders_status_weight,
)
from binance.core.rate_limit.types import RateLimitType

from test.test_ws_api import WSAPIServer


# Each trading method runs its own server on a dedicated port so a bind race
# with the other WS-API test modules is impossible even if collected together.
_PORT = 9087


def _make_client(server) -> Client:
    client = Client(ws_api_host=server.uri, api_key='K', api_secret='S')
    # Pre-mark the server-time offset as synced so the endpoint under test is
    # the FIRST frame sent (otherwise the lazy `time` sync would precede the
    # first signed request). The lazy-sync arming itself is covered in
    # test_time_sync.py.
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0].used


def _orders_used(client) -> int:
    snap = client.rate_limit_snapshot()
    orders = [w for w in snap.windows if w.type == RateLimitType.ORDERS]
    assert orders
    # both the 10s and 1d ORDERS buckets move together
    assert len({w.used for w in orders}) == 1
    return orders[0].used


# ---------------------------------------------------------------------------
# Each migrated/new method sends the right WS-API `method` + forwards params.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_order_places_via_order_place():
    server = WSAPIServer(port=_PORT)
    server.on('order.place', result={'orderId': 28, 'status': 'NEW'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.create_order(
            symbol='BTCUSDT', side='BUY', type='LIMIT',
            timeInForce='GTC', quantity='1', price='10000')
        assert result == {'orderId': 28, 'status': 'NEW'}

        sent = server.received[0]
        assert sent['method'] == 'order.place'
        # Caller params are forwarded verbatim (plus signing fields).
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['side'] == 'BUY'
        assert sent['params']['type'] == 'LIMIT'
        assert sent['params']['quantity'] == '1'
        assert sent['params']['price'] == '10000'
        # SIGNED endpoint: auth fields attached.
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert 'timestamp' in sent['params']

        # order.place is an order-placing endpoint -> ORDERS pool consumed.
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_order_queries_via_order_status_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.status', result={'orderId': 1, 'status': 'FILLED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_order(symbol='BTCUSDT', orderId=1)
        sent = server.received[0]
        assert sent['method'] == 'order.status'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert sent['params']['orderId'] == 1
        # A query must not touch the ORDERS pool; weight is 4.
        assert _orders_used(client) == 0
        assert _weight_used(client) == 4
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_cancel_order_via_order_cancel_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('order.cancel', result={'orderId': 1, 'status': 'CANCELED'})
    await server.run()
    try:
        client = _make_client(server)
        await client.cancel_order(symbol='BTCUSDT', orderId=1)
        sent = server.received[0]
        assert sent['method'] == 'order.cancel'
        # A cancel must not consume the ORDERS pool.
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_cancel_replace_order_places_via_cancel_replace_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('order.cancelReplace',
              result={'cancelResult': 'SUCCESS', 'newOrderResult': 'SUCCESS'})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.cancel_replace_order(
            symbol='BTCUSDT', side='SELL', type='LIMIT',
            cancelReplaceMode='STOP_ON_FAILURE', cancelOrderId=9,
            timeInForce='GTC', quantity='1', price='10000')
        assert result['newOrderResult'] == 'SUCCESS'
        sent = server.received[0]
        assert sent['method'] == 'order.cancelReplace'
        assert sent['params']['cancelReplaceMode'] == 'STOP_ON_FAILURE'
        assert sent['params']['cancelOrderId'] == 9
        # cancelReplace PLACES a new order -> consumes the ORDERS pool.
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_amend_order_via_keep_priority_does_not_consume_orders():
    server = WSAPIServer(port=_PORT)
    server.on('order.amend.keepPriority',
              result={'transactTime': 1, 'amendedOrder': {'orderId': 12}})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.amend_order(
            symbol='BTCUSDT', orderId=12, newQty='5')
        assert result['amendedOrder']['orderId'] == 12
        sent = server.received[0]
        assert sent['method'] == 'order.amend.keepPriority'
        assert sent['params']['newQty'] == '5'
        # Order Amend Keep Priority modifies (not places) an order:
        # documented Unfilled Order Count is 0 -> ORDERS pool untouched.
        assert _orders_used(client) == 0
        # Documented weight is 4.
        assert _weight_used(client) == 4
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_cancel_all_orders_via_open_orders_cancel_all_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('openOrders.cancelAll', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.cancel_all_orders(symbol='BTCUSDT')
        sent = server.received[0]
        assert sent['method'] == 'openOrders.cancelAll'
        assert sent['params']['symbol'] == 'BTCUSDT'
        # Cancelling is not a placement -> ORDERS pool untouched; weight 1.
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_all_orders_via_all_orders():
    server = WSAPIServer(port=_PORT)
    server.on('allOrders', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_all_orders(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'allOrders'
        assert _orders_used(client) == 0
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_sor_order_via_sor_order_place_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('sor.order.place', result=[{'orderId': 2}])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.create_sor_order(
            symbol='BTCUSDT', side='BUY', type='LIMIT',
            quantity='0.5', price='31000')
        assert result == [{'orderId': 2}]
        assert server.received[0]['method'] == 'sor.order.place'
        # SOR placement consumes the ORDERS pool.
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_oco_via_order_list_place_oco_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.place.oco', result={'orderListId': 0})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_oco(
            symbol='BTCUSDT', side='SELL', quantity='1',
            price='12000', stopPrice='9000')
        assert server.received[0]['method'] == 'orderList.place.oco'
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_oto_via_order_list_place_oto_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.place.oto', result={'orderListId': 1})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_oto(
            symbol='BTCUSDT',
            workingType='LIMIT', workingSide='BUY',
            workingPrice='10000', workingQuantity='1',
            pendingType='LIMIT', pendingSide='SELL', pendingQuantity='1')
        assert server.received[0]['method'] == 'orderList.place.oto'
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_otoco_via_order_list_place_otoco_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.place.otoco', result={'orderListId': 2})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_otoco(
            symbol='BTCUSDT',
            workingType='LIMIT', workingSide='BUY',
            workingPrice='10000', workingQuantity='1',
            pendingSide='SELL', pendingQuantity='1',
            pendingAboveType='LIMIT_MAKER', pendingAbovePrice='12000')
        assert server.received[0]['method'] == 'orderList.place.otoco'
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_cancel_oco_via_order_list_cancel_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.cancel', result={'orderListId': 0})
    await server.run()
    try:
        client = _make_client(server)
        await client.cancel_oco(symbol='BTCUSDT', orderListId=0)
        assert server.received[0]['method'] == 'orderList.cancel'
        assert _orders_used(client) == 0
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_oco_via_order_list_status():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.status', result={'orderListId': 27})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_oco(orderListId=27)
        assert server.received[0]['method'] == 'orderList.status'
        assert _orders_used(client) == 0
        assert _weight_used(client) == 4
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_all_oco_via_all_order_lists():
    server = WSAPIServer(port=_PORT)
    server.on('allOrderLists', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_all_oco()
        assert server.received[0]['method'] == 'allOrderLists'
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_open_oco_via_open_order_lists_status():
    server = WSAPIServer(port=_PORT)
    server.on('openOrderLists.status', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_open_oco()
        assert server.received[0]['method'] == 'openOrderLists.status'
        assert _weight_used(client) == 6
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Dynamic weight: resolved per call from the request params.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_test_order_weight_without_compute_commission_rates():
    server = WSAPIServer(port=_PORT)
    server.on('order.test', result={})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_test_order(
            symbol='BTCUSDT', side='BUY', type='MARKET', quantity='1')
        assert server.received[0]['method'] == 'order.test'
        # order.test is NOT an order placement -> ORDERS pool untouched.
        assert _orders_used(client) == 0
        # Default weight is 1 (no computeCommissionRates).
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_test_order_weight_with_compute_commission_rates():
    server = WSAPIServer(port=_PORT)
    server.on('order.test', result={})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_test_order(
            symbol='BTCUSDT', side='BUY', type='MARKET', quantity='1',
            computeCommissionRates=True)
        # computeCommissionRates raises the weight from 1 to 20.
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_open_orders_weight_with_symbol():
    server = WSAPIServer(port=_PORT)
    server.on('openOrders.status', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_open_orders(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'openOrders.status'
        # Scoped to a single symbol -> weight 6.
        assert _weight_used(client) == 6
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_open_orders_weight_without_symbol():
    server = WSAPIServer(port=_PORT)
    server.on('openOrders.status', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_open_orders()
        # No symbol -> account-wide query -> weight 80.
        assert _weight_used(client) == 80
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Pure-unit coverage of the dynamic-weight helpers (no network).
# ---------------------------------------------------------------------------

def test_order_test_weight_helper():
    assert _order_test_weight({}) == 1
    assert _order_test_weight({'computeCommissionRates': False}) == 1
    assert _order_test_weight({'computeCommissionRates': True}) == 20


def test_open_orders_status_weight_helper():
    assert _open_orders_status_weight({}) == 80
    assert _open_orders_status_weight({'symbol': 'BTCUSDT'}) == 6


# ---------------------------------------------------------------------------
# The declarative registry matches the documented (method, security, is_order).
# ---------------------------------------------------------------------------

def test_ws_apis_registry_matches_spec():
    by_name = {entry['name']: entry for entry in WS_APIS}

    expected = {
        # name: (ws_method, security, is_order)
        'create_order': ('order.place', SecurityType.TRADE, True),
        'create_test_order': ('order.test', SecurityType.TRADE, False),
        'get_order': ('order.status', SecurityType.USER_DATA, False),
        'cancel_order': ('order.cancel', SecurityType.TRADE, False),
        'cancel_replace_order': (
            'order.cancelReplace', SecurityType.TRADE, True),
        'amend_order': (
            'order.amend.keepPriority', SecurityType.TRADE, False),
        'get_open_orders': (
            'openOrders.status', SecurityType.USER_DATA, False),
        'cancel_all_orders': (
            'openOrders.cancelAll', SecurityType.TRADE, False),
        'get_all_orders': ('allOrders', SecurityType.USER_DATA, False),
        'create_sor_order': ('sor.order.place', SecurityType.TRADE, True),
        'create_test_sor_order': ('sor.order.test', SecurityType.TRADE, False),
        'create_oco': ('orderList.place.oco', SecurityType.TRADE, True),
        'create_oto': ('orderList.place.oto', SecurityType.TRADE, True),
        'create_otoco': ('orderList.place.otoco', SecurityType.TRADE, True),
        'create_opo': ('orderList.place.opo', SecurityType.TRADE, True),
        'create_opoco': ('orderList.place.opoco', SecurityType.TRADE, True),
        'cancel_oco': ('orderList.cancel', SecurityType.TRADE, False),
        'get_oco': ('orderList.status', SecurityType.USER_DATA, False),
        'get_all_oco': ('allOrderLists', SecurityType.USER_DATA, False),
        'get_open_oco': (
            'openOrderLists.status', SecurityType.USER_DATA, False),
    }

    # The trading endpoints are a subset of the full WS-API registry (which
    # also carries the general / market-data / account endpoints).
    assert set(expected) <= set(by_name)
    for name, (ws_method, security, is_order) in expected.items():
        entry = by_name[name]
        assert entry['ws_method'] == ws_method
        assert entry['security_type'] == security
        assert entry.get('is_order', False) is is_order


def test_static_weights_match_spec():
    by_name = {entry['name']: entry for entry in WS_APIS}
    static_weights = {
        'create_order': 1,
        'get_order': 4,
        'cancel_order': 1,
        'cancel_replace_order': 1,
        'amend_order': 4,
        'cancel_all_orders': 1,
        'get_all_orders': 20,
        'create_sor_order': 1,
        'create_oco': 1,
        'create_oto': 1,
        'create_otoco': 1,
        'create_opo': 1,
        'create_opoco': 1,
        'cancel_oco': 1,
        'get_oco': 4,
        'get_all_oco': 20,
        'get_open_oco': 6,
    }
    for name, weight in static_weights.items():
        assert by_name[name]['weight'] == weight
    # The params-dependent endpoints carry callables, not ints.
    assert callable(by_name['create_test_order']['weight'])
    assert callable(by_name['create_test_sor_order']['weight'])
    assert callable(by_name['get_open_orders']['weight'])


# ---------------------------------------------------------------------------
# New: sor.order.test, OPO, OPOCO
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_test_sor_order_via_sor_order_test_no_orders_pool():
    server = WSAPIServer(port=_PORT)
    server.on('sor.order.test', result={})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_test_sor_order(
            symbol='BTCUSDT', side='BUY', type='LIMIT',
            quantity='0.5', price='31000')
        sent = server.received[0]
        assert sent['method'] == 'sor.order.test'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert 'apiKey' in sent['params']
        # sor.order.test does NOT place an order (is_order=False)
        assert _orders_used(client) == 0
        # default weight 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_opo_via_order_list_place_opo_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.place.opo', result={'orderListId': 10})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_opo(
            symbol='BTCUSDT',
            workingType='LIMIT', workingSide='BUY',
            workingPrice='10000', workingQuantity='1',
            workingTimeInForce='GTC',
            pendingType='LIMIT', pendingSide='SELL')
        sent = server.received[0]
        assert sent['method'] == 'orderList.place.opo'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert 'apiKey' in sent['params']
        # OPO places orders -> ORDERS pool consumed
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_create_opoco_via_order_list_place_opoco_consumes_orders():
    server = WSAPIServer(port=_PORT)
    server.on('orderList.place.opoco', result={'orderListId': 11})
    await server.run()
    try:
        client = _make_client(server)
        await client.create_opoco(
            symbol='BTCUSDT',
            workingType='LIMIT', workingSide='BUY',
            workingPrice='10000', workingQuantity='1',
            workingTimeInForce='GTC',
            pendingAboveType='LIMIT_MAKER', pendingAbovePrice='12000',
            pendingBelowType='STOP_LOSS_LIMIT', pendingBelowPrice='9000')
        sent = server.received[0]
        assert sent['method'] == 'orderList.place.opoco'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert 'apiKey' in sent['params']
        # OPOCO places orders -> ORDERS pool consumed
        assert _orders_used(client) == 1
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# define_ws_getter: factory contract (params=False path + docstring migration).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_define_ws_getter_params_false_sends_no_params():
    # A getter declared with params=False must call _ws_api_request with None
    # params regardless of kwargs, so the wire frame omits the `params` key.
    class Target:
        def noparams(self, **kwargs):
            """original docstring"""
            ...

    define_ws_getter(
        Target, 'noparams', 'some.method', params=False,
        security_type=SecurityType.NONE, weight=2)

    captured = {}

    class FakeReq(Target):
        async def _ws_api_request(self, method, params, *,
                                  security, weight, is_order):
            captured.update(method=method, params=params, security=security,
                            weight=weight, is_order=is_order)
            return 'ok'

    obj = FakeReq()
    result = await obj.noparams(ignored='x')
    assert result == 'ok'
    assert captured['method'] == 'some.method'
    assert captured['params'] is None         # params=False -> None
    assert captured['security'] is SecurityType.NONE
    assert captured['weight'] == 2
    assert captured['is_order'] is False
    # The original stub docstring is migrated onto the generated getter.
    assert Target.noparams.__doc__ == 'original docstring'
