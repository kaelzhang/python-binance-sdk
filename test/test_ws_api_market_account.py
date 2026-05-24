"""Hermetic tests for the WS-API general / market-data / account endpoints (G-05).

The general (``ping``/``time``/``exchangeInfo``), market-data
(``depth``/``klines``/``trades.*``/``ticker.*``/...) and account
(``account.*``/``myTrades``/...) surface was migrated from REST to the
WebSocket API. These drive the public ``Client`` methods against the local
:class:`WSAPIServer` request/response harness (reused from ``test_ws_api``) and
assert:

- each method sends the correct WS-API ``method`` and forwards its params;
- public (``NONE``) endpoints attach no auth fields; account (``USER_DATA``)
  endpoints are signed;
- the ``params=False`` general endpoints send no ``params`` key;
- params-dependent weights resolve per call (depth / ticker.* / myTrades);
- the declarative registry matches the documented spec.
"""

import pytest

from binance import Client
from binance.common.constants import SecurityType
from binance.apis.ws_api import (
    WS_APIS,
    _depth_weight,
    _ticker_24hr_weight,
    _ticker_price_weight,
    _ticker_book_weight,
    _my_trades_weight,
)

from test.test_ws_api import WSAPIServer


# A dedicated port avoids any bind race with the other WS-API test modules.
_PORT = 9093


def _make_client(server, signed: bool = False) -> Client:
    kwargs = dict(ws_api_host=server.uri)
    if signed:
        kwargs.update(api_key='K', api_secret='S')
    client = Client(**kwargs)
    # The signed-endpoint tests isolate the endpoint under test from the lazy
    # `time` sync (covered in test_time_sync.py).
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == 'request_weight'][0].used


def _orders_used(client) -> int:
    snap = client.rate_limit_snapshot()
    orders = [w for w in snap.windows if w.type == 'orders']
    assert orders
    return orders[0].used


# ---------------------------------------------------------------------------
# General endpoints (NONE; params=False)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ping_via_ping_sends_no_params():
    server = WSAPIServer(port=_PORT)
    server.on('ping', result={})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.ping()
        assert result == {}
        sent = server.received[0]
        assert sent['method'] == 'ping'
        # params=False -> the wire frame omits the `params` key entirely.
        assert 'params' not in sent
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_server_time_via_time():
    server = WSAPIServer(port=_PORT)
    server.on('time', result={'serverTime': 1499827319559})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_server_time()
        assert result == {'serverTime': 1499827319559}
        sent = server.received[0]
        assert sent['method'] == 'time'
        assert 'params' not in sent
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_exchange_info_via_exchange_info():
    server = WSAPIServer(port=_PORT)
    server.on('exchangeInfo', result={'symbols': []})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_exchange_info()
        sent = server.received[0]
        assert sent['method'] == 'exchangeInfo'
        assert 'params' not in sent
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Market-data endpoints (NONE; public, no auth fields)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_orderbook_via_depth_is_public_with_dynamic_weight():
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_orderbook(symbol='BTCUSDT', limit=100)
        sent = server.received[0]
        assert sent['method'] == 'depth'
        assert sent['params'] == {'symbol': 'BTCUSDT', 'limit': 100}
        # NONE endpoint: no auth fields.
        assert 'apiKey' not in sent['params']
        assert 'signature' not in sent['params']
        # limit=100 -> weight 5.
        assert _weight_used(client) == 5
        assert _orders_used(client) == 0
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_orderbook_depth_weight_large_limit():
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_orderbook(symbol='BTCUSDT', limit=5000)
        # limit=5000 -> weight 250.
        assert _weight_used(client) == 250
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_orderbook_depth_weight_default_limit():
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server)
        # No `limit` -> defaults to 100 -> weight 5.
        await client.get_orderbook(symbol='BTCUSDT')
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_recent_trades_via_trades_recent():
    server = WSAPIServer(port=_PORT)
    server.on('trades.recent', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_recent_trades(symbol='BTCUSDT', limit=100)
        sent = server.received[0]
        assert sent['method'] == 'trades.recent'
        assert sent['params'] == {'symbol': 'BTCUSDT', 'limit': 100}
        assert _weight_used(client) == 25
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_historical_trades_via_trades_historical_is_public():
    server = WSAPIServer(port=_PORT)
    server.on('trades.historical', result=[])
    await server.run()
    try:
        # F-03: historical trades is NONE over the WS-API -> no api_key needed.
        client = _make_client(server)
        await client.get_historical_trades(symbol='BTCUSDT', fromId=1)
        sent = server.received[0]
        assert sent['method'] == 'trades.historical'
        assert 'apiKey' not in sent['params']
        assert _weight_used(client) == 25
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_aggregate_trades_via_trades_aggregate():
    server = WSAPIServer(port=_PORT)
    server.on('trades.aggregate', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_aggregate_trades(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'trades.aggregate'
        assert _weight_used(client) == 4
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_klines_via_klines():
    server = WSAPIServer(port=_PORT)
    server.on('klines', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_klines(symbol='BTCUSDT', interval='1d')
        sent = server.received[0]
        assert sent['method'] == 'klines'
        assert sent['params'] == {'symbol': 'BTCUSDT', 'interval': '1d'}
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_average_price_via_avg_price():
    server = WSAPIServer(port=_PORT)
    server.on('avgPrice', result={'mins': 5, 'price': '9.35'})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_average_price(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'avgPrice'
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


# ----- ticker.* dynamic weights ----------------------------------------

@pytest.mark.asyncio
async def test_get_ticker_via_ticker_24hr_single_symbol():
    server = WSAPIServer(port=_PORT)
    server.on('ticker.24hr', result={})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'ticker.24hr'
        # Single symbol -> weight 2.
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_ticker_via_ticker_24hr_all_symbols():
    server = WSAPIServer(port=_PORT)
    server.on('ticker.24hr', result=[])
    await server.run()
    try:
        client = _make_client(server)
        # Neither symbol nor symbols -> account-wide -> weight 80.
        await client.get_ticker()
        assert _weight_used(client) == 80
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_ticker_price_via_ticker_price_single_vs_all():
    server = WSAPIServer(port=_PORT)
    server.on('ticker.price', result={})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker_price(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'ticker.price'
        assert _weight_used(client) == 2          # single symbol
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_orderbook_ticker_via_ticker_book_all_symbols():
    server = WSAPIServer(port=_PORT)
    server.on('ticker.book', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_orderbook_ticker()
        assert server.received[0]['method'] == 'ticker.book'
        assert _weight_used(client) == 4          # no symbol
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Account endpoints (USER_DATA; signed)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_account_via_account_status_is_signed():
    server = WSAPIServer(port=_PORT)
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_account()
        sent = server.received[0]
        assert sent['method'] == 'account.status'
        # USER_DATA endpoint: auth fields attached.
        assert sent['params']['apiKey'] == 'K'
        assert 'signature' in sent['params']
        assert 'timestamp' in sent['params']
        assert _weight_used(client) == 20
        assert _orders_used(client) == 0
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_trades_via_my_trades_dynamic_weight():
    server = WSAPIServer(port=_PORT)
    server.on('myTrades', result=[])
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_trades(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'myTrades'
        # No orderId -> weight 20.
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_trades_via_my_trades_with_order_id():
    server = WSAPIServer(port=_PORT)
    server.on('myTrades', result=[])
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_trades(symbol='BTCUSDT', orderId=123)
        # orderId present -> weight 5.
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_commission_via_account_commission():
    server = WSAPIServer(port=_PORT)
    server.on('account.commission', result={'symbol': 'BTCUSDT'})
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_commission(symbol='BTCUSDT')
        sent = server.received[0]
        assert sent['method'] == 'account.commission'
        assert 'signature' in sent['params']
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_order_rate_limit_via_account_rate_limits_orders():
    server = WSAPIServer(port=_PORT)
    server.on('account.rateLimits.orders', result=[])
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_order_rate_limit()
        assert server.received[0]['method'] == 'account.rateLimits.orders'
        assert _weight_used(client) == 40
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_prevented_matches_via_my_prevented_matches():
    server = WSAPIServer(port=_PORT)
    server.on('myPreventedMatches', result=[])
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_prevented_matches(symbol='BTCUSDT', orderId=1)
        assert server.received[0]['method'] == 'myPreventedMatches'
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_get_allocations_via_my_allocations():
    server = WSAPIServer(port=_PORT)
    server.on('myAllocations', result=[])
    await server.run()
    try:
        client = _make_client(server, signed=True)
        await client.get_allocations(symbol='BTCUSDT')
        assert server.received[0]['method'] == 'myAllocations'
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Pure-unit coverage of the dynamic-weight helpers (no network).
# ---------------------------------------------------------------------------

def test_depth_weight_helper():
    assert _depth_weight({}) == 5                       # default 100
    assert _depth_weight({'limit': 100}) == 5
    assert _depth_weight({'limit': 500}) == 25
    assert _depth_weight({'limit': 1000}) == 50
    assert _depth_weight({'limit': 5000}) == 250


def test_ticker_24hr_weight_helper():
    assert _ticker_24hr_weight({'symbol': 'BTCUSDT'}) == 2
    assert _ticker_24hr_weight({}) == 80                # all symbols
    assert _ticker_24hr_weight({'symbols': ['A'] * 20}) == 2
    assert _ticker_24hr_weight({'symbols': ['A'] * 21}) == 40
    assert _ticker_24hr_weight({'symbols': ['A'] * 100}) == 40
    assert _ticker_24hr_weight({'symbols': ['A'] * 101}) == 80


def test_ticker_price_weight_helper():
    assert _ticker_price_weight({'symbol': 'BTCUSDT'}) == 2
    assert _ticker_price_weight({}) == 4
    assert _ticker_price_weight({'symbols': ['A', 'B']}) == 4


def test_ticker_book_weight_helper():
    assert _ticker_book_weight({'symbol': 'BTCUSDT'}) == 2
    assert _ticker_book_weight({}) == 4


def test_my_trades_weight_helper():
    assert _my_trades_weight({'symbol': 'BTCUSDT'}) == 20
    assert _my_trades_weight({'symbol': 'BTCUSDT', 'orderId': 1}) == 5


# ---------------------------------------------------------------------------
# The declarative registry matches the documented (method, security, weight).
# ---------------------------------------------------------------------------

def test_ws_apis_registry_market_account_matches_spec():
    by_name = {entry['name']: entry for entry in WS_APIS}

    expected = {
        # name: (ws_method, security)
        'ping': ('ping', SecurityType.NONE),
        'get_server_time': ('time', SecurityType.NONE),
        'get_exchange_info': ('exchangeInfo', SecurityType.NONE),
        'get_orderbook': ('depth', SecurityType.NONE),
        'get_recent_trades': ('trades.recent', SecurityType.NONE),
        'get_historical_trades': ('trades.historical', SecurityType.NONE),
        'get_aggregate_trades': ('trades.aggregate', SecurityType.NONE),
        'get_klines': ('klines', SecurityType.NONE),
        'get_average_price': ('avgPrice', SecurityType.NONE),
        'get_ticker': ('ticker.24hr', SecurityType.NONE),
        'get_ticker_price': ('ticker.price', SecurityType.NONE),
        'get_orderbook_ticker': ('ticker.book', SecurityType.NONE),
        'get_account': ('account.status', SecurityType.USER_DATA),
        'get_trades': ('myTrades', SecurityType.USER_DATA),
        'get_commission': ('account.commission', SecurityType.USER_DATA),
        'get_order_rate_limit': (
            'account.rateLimits.orders', SecurityType.USER_DATA),
        'get_prevented_matches': (
            'myPreventedMatches', SecurityType.USER_DATA),
        'get_allocations': ('myAllocations', SecurityType.USER_DATA),
    }

    assert set(expected) <= set(by_name)
    for name, (ws_method, security) in expected.items():
        entry = by_name[name]
        assert entry['ws_method'] == ws_method
        assert entry['security_type'] == security
        # None of these are order-placing endpoints.
        assert entry.get('is_order', False) is False


def test_market_account_static_weights_match_spec():
    by_name = {entry['name']: entry for entry in WS_APIS}
    static_weights = {
        'ping': 1,
        'get_server_time': 1,
        'get_exchange_info': 20,
        'get_recent_trades': 25,
        'get_historical_trades': 25,
        'get_aggregate_trades': 4,
        'get_klines': 2,
        'get_average_price': 2,
        'get_account': 20,
        'get_commission': 20,
        'get_order_rate_limit': 40,
        'get_prevented_matches': 20,
        'get_allocations': 20,
    }
    for name, weight in static_weights.items():
        assert by_name[name]['weight'] == weight
    # The params-dependent endpoints carry callables, not ints.
    for name in (
        'get_orderbook', 'get_ticker', 'get_ticker_price',
        'get_orderbook_ticker', 'get_trades',
    ):
        assert callable(by_name[name]['weight'])


def test_params_false_general_endpoints():
    by_name = {entry['name']: entry for entry in WS_APIS}
    # The three general endpoints historically took no params.
    for name in ('ping', 'get_server_time', 'get_exchange_info'):
        assert by_name[name].get('params') is False
