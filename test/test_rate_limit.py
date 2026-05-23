import asyncio
import time
import pytest

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
