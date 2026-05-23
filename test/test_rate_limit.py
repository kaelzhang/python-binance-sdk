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
