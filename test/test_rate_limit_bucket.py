import time
import pytest

from binance.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)
from binance.rate_limit.bucket import RateLimitBucket
from binance.common.exceptions import (
    RateLimitReachedException, TooManyStreamsException
)


def _rule(kind=RateLimitKind.COUNT, enforce=EnforceMode.SLEEP,
          interval=0.3, limit=2, safety=1.0, type_=RateLimitType.RAW_REQUESTS):
    return RateLimitRule(RateLimitScope.IP, type_, interval, limit, kind, enforce, safety)


def test_used_and_remaining_via_record():
    b = RateLimitBucket(_rule(limit=5))
    assert b.used == 0
    b.record(2)
    assert b.used == 2


def test_sync_uses_max_of_estimate_and_authoritative():
    b = RateLimitBucket(_rule(limit=6000, type_=RateLimitType.REQUEST_WEIGHT))
    b.record(10)
    b.sync(5)            # header lower than estimate -> keep estimate
    assert b.used == 10
    b.sync(50)           # header higher -> authoritative wins
    assert b.used == 50


def test_set_limit_and_effective_limit_safety():
    b = RateLimitBucket(_rule(limit=100, safety=0.9))
    assert b.effective_limit == 90
    b.set_limit(1000)
    assert b.effective_limit == 900


@pytest.mark.asyncio
async def test_sleep_mode_blocks_when_full():
    b = RateLimitBucket(_rule(enforce=EnforceMode.SLEEP, interval=0.3, limit=2))
    start = time.monotonic()
    await b.acquire()
    await b.acquire()
    await b.acquire()                       # third must wait ~interval
    assert time.monotonic() - start >= 0.25


@pytest.mark.asyncio
async def test_raise_mode_fails_fast_when_full():
    b = RateLimitBucket(_rule(enforce=EnforceMode.RAISE, interval=10, limit=2))
    await b.acquire()
    await b.acquire()
    with pytest.raises(RateLimitReachedException):
        await b.acquire()


@pytest.mark.asyncio
async def test_track_mode_records_but_never_blocks():
    b = RateLimitBucket(_rule(enforce=EnforceMode.TRACK, interval=10, limit=2))
    await b.acquire()                        # never blocks/raises
    await b.acquire()
    await b.acquire()
    assert b.used == 3                       # over limit, still accounted


def test_cap_reserve_and_release():
    b = RateLimitBucket(_rule(kind=RateLimitKind.CAP, interval=0,
                              limit=1024, type_=RateLimitType.WS_STREAMS))
    b.reserve(1024)
    assert b.used == 1024
    with pytest.raises(TooManyStreamsException):
        b.reserve(1025)
    b.release(24)
    assert b.used == 1000
