import time
import pytest

from aioresponses import aioresponses

from binance import Client, RateLimitException, IPBannedException
from binance.common.rate_limit import (
    parse_retry_after,
    depth_weight,
    SlidingWindowRateLimiter,
    WeightRateLimiter
)


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


@pytest.mark.asyncio
async def test_sliding_window_blocks_when_full():
    limiter = SlidingWindowRateLimiter(max_count=2, window=0.3)
    start = time.monotonic()
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()  # third must wait ~window
    assert time.monotonic() - start >= 0.25


@pytest.mark.asyncio
async def test_weight_limiter_blocks_over_budget():
    limiter = WeightRateLimiter(limit=10, window=0.3, safety_ratio=1.0)
    start = time.monotonic()
    await limiter.acquire(6)
    await limiter.acquire(6)  # 12 > 10 -> must wait
    assert time.monotonic() - start >= 0.25


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
    import re
    from binance import Client
    # create_order is a TRADE (signed) endpoint; supply credentials so the
    # request reaches the rate-limiter core instead of raising on missing keys.
    client = Client(api_key='k', api_secret='s')
    with aioresponses() as m:
        # The order body (incl. signature) is POSTed as form data, so the URL
        # is just `.../api/v3/order`; match it with a regex.
        m.post(re.compile(r'.*/api/v3/order(\?.*)?$'),
               payload={'orderId': 1, 'status': 'NEW'}, status=200,
               headers={'X-MBX-ORDER-COUNT-10S': '1'})
        await client.create_order(symbol='BTCUSDT', side='BUY', type='MARKET',
                                  quantity=1)
    snap = client.rate_limit_snapshot()
    orders = [w for w in snap.windows if w.type == 'orders']
    assert orders and all(w.used >= 1 for w in orders)   # order pool consumed


@pytest.mark.asyncio
async def test_non_order_endpoint_does_not_consume_orders_pool():
    from binance import Client
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT&limit=100', status=200,
              payload={'lastUpdateId': 1, 'bids': [], 'asks': []})
        await client.get_orderbook(symbol='BTCUSDT', limit=100)
    snap = client.rate_limit_snapshot()
    orders = [w for w in snap.windows if w.type == 'orders']
    # A plain market-data GET must never touch the ORDERS pool.
    assert orders and all(w.used == 0 for w in orders)
