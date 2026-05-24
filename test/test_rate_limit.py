import pytest

from aioresponses import aioresponses

from binance import Client, RateLimitException, IPBannedException
from binance.rate_limit import parse_retry_after, depth_weight


class _Resp:
    def __init__(self, headers):
        from multidict import CIMultiDict
        self.headers = CIMultiDict(headers)


def test_parse_retry_after_reads_header_case_insensitively():
    assert parse_retry_after(_Resp({'Retry-After': '120'})) == 120
    assert parse_retry_after(_Resp({'retry-after': '7'})) == 7
    assert parse_retry_after(_Resp({})) is None
    assert parse_retry_after(_Resp({'Retry-After': 'nope'})) is None


def test_depth_weight_matches_documented_tiers():
    assert depth_weight(1) == 5
    assert depth_weight(100) == 5
    assert depth_weight(101) == 25
    assert depth_weight(500) == 25
    assert depth_weight(1000) == 50
    assert depth_weight(5000) == 250


_URL = 'https://api.binance.com/api/v3/depth'


@pytest.mark.asyncio
async def test_429_raises_rate_limit_with_retry_after():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=429,
              headers={'Retry-After': '42', 'X-MBX-USED-WEIGHT-1M': '6001'},
              payload={'code': -1003, 'msg': 'Too many requests'})
        with pytest.raises(RateLimitException) as info:
            await client.get(_URL, symbol='BTCUSDT')
    assert info.value.retry_after == 42
    assert client.used_weight.get('1m') == 6001


@pytest.mark.asyncio
async def test_418_raises_ip_banned_with_retry_after():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=418,
              headers={'Retry-After': '120'},
              payload={'code': -1003, 'msg': 'banned'})
        with pytest.raises(IPBannedException) as info:
            await client.get(_URL, symbol='BTCUSDT')
    assert info.value.retry_after == 120


@pytest.mark.asyncio
async def test_success_captures_used_weight_and_order_count():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=200,
              headers={'X-MBX-USED-WEIGHT-1M': '12', 'X-MBX-ORDER-COUNT-10S': '3'},
              payload={'lastUpdateId': 1, 'bids': [], 'asks': []})
        await client.get(_URL, symbol='BTCUSDT')
    assert client.used_weight.get('1m') == 12
    assert client.order_count.get('10s') == 3


def test_default_retry_policy_has_floor_and_ceiling():
    from types import SimpleNamespace
    from binance.common.constants import DEFAULT_RETRY_POLICY, RETRY_MAX_DELAY

    delays = []
    for fails in range(1, 12):
        abandon, delay = DEFAULT_RETRY_POLICY(SimpleNamespace(fails=fails, exception=None))
        assert abandon is False
        assert delay >= 0.5            # floor: never a 0s busy-reconnect
        assert delay <= RETRY_MAX_DELAY
        delays.append(delay)
    # backoff grows then caps
    assert delays[-1] >= delays[0]


@pytest.mark.asyncio
async def test_rate_limit_snapshot_reflects_used_weight():
    from binance import Client
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=200,
              headers={'X-MBX-USED-WEIGHT-1M': '4321'},
              payload={'lastUpdateId': 1, 'bids': [], 'asks': []})
        await client.get(_URL, symbol='BTCUSDT')
    snap = client.rate_limit_snapshot()
    weight = [w for w in snap.windows if w.type == 'request_weight'][0]
    assert weight.used == 4321
    assert weight.source == 'header'


@pytest.mark.asyncio
async def test_429_sets_snapshot_retry_after():
    from binance import Client, RateLimitException
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=429,
              headers={'Retry-After': '30'},
              payload={'code': -1003, 'msg': 'too many'})
        with pytest.raises(RateLimitException):
            await client.get(_URL, symbol='BTCUSDT')
    snap = client.rate_limit_snapshot()
    assert snap.retry_after is not None and snap.retry_after <= 30
    assert snap.throttled is True


@pytest.mark.asyncio
async def test_order_endpoint_consumes_orders_pool():
    from binance import Client
    from test.test_ws_api import WSAPIServer
    # create_order is a TRADE (signed) endpoint now served over the WebSocket
    # API (order.place); supply credentials so the request reaches the
    # rate-limiter core instead of raising on missing keys. The canned response
    # carries NO `rateLimits` array, so the ORDERS-pool usage can only come from
    # the proactive is_order=True consumption -- isolating the tagging path.
    server = WSAPIServer(port=9086)
    server.on('order.place', result={'orderId': 1, 'status': 'NEW'})
    await server.run()
    try:
        client = Client(ws_api_host=server.uri, api_key='k', api_secret='s')
        await client.create_order(symbol='BTCUSDT', side='BUY', type='MARKET',
                                  quantity=1)
        snap = client.rate_limit_snapshot()
        orders = [w for w in snap.windows if w.type == 'orders']
        assert orders and all(w.used == 1 for w in orders)  # proactively consumed
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_non_order_endpoint_does_not_consume_orders_pool():
    from binance import Client
    from test.test_ws_api import WSAPIServer
    # get_orderbook is now a public WS-API `depth` request.
    server = WSAPIServer(port=9091)
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = Client(ws_api_host=server.uri)
        await client.get_orderbook(symbol='BTCUSDT', limit=100)
        snap = client.rate_limit_snapshot()
        orders = [w for w in snap.windows if w.type == 'orders']
        # A plain market-data request must never touch the ORDERS pool.
        assert orders and all(w.used == 0 for w in orders)
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_exchange_info_autoconfigures_pool_limits():
    from binance import Client
    from test.test_ws_api import WSAPIServer
    # exchangeInfo is now served over the WebSocket API; its `result` carries
    # the pool caps, which the on_response reconciler folds into the core.
    server = WSAPIServer(port=9090)
    server.on('exchangeInfo', result={
        'rateLimits': [
            {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
             'intervalNum': 1, 'limit': 12000},
        ],
        'symbols': []
    })
    await server.run()
    try:
        client = Client(ws_api_host=server.uri)
        await client.get_exchange_info()
        snap = client.rate_limit_snapshot()
        weight = [w for w in snap.windows if w.type == 'request_weight'][0]
        # configured cap 12000 * 0.9 safety ratio = 10800 effective
        assert weight.limit == 10800
    finally:
        await client.close()
        await server.shutdown()
