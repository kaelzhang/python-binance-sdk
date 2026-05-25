import asyncio
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


@pytest.mark.asyncio
async def test_acquire_cost_over_limit_fails_fast_not_hang():
    b = RateLimitBucket(_rule(enforce=EnforceMode.SLEEP, interval=10, limit=10))
    with pytest.raises(RateLimitReachedException):
        await asyncio.wait_for(b.acquire(20), timeout=1.0)


@pytest.mark.asyncio
async def test_acquire_after_high_sync_does_not_hang():
    b = RateLimitBucket(_rule(enforce=EnforceMode.SLEEP, interval=0.3, limit=100))
    b.sync(200)                       # authoritative >= limit, empty window
    # must not spin; admits once the authoritative reading goes stale
    await asyncio.wait_for(b.acquire(1), timeout=2.0)


@pytest.mark.asyncio
async def test_authoritative_decays_after_window():
    b = RateLimitBucket(_rule(interval=0.3, limit=1000,
                              type_=RateLimitType.REQUEST_WEIGHT))
    b.record(5)
    b.sync(80)
    assert b.used == 80
    assert b.has_authoritative is True
    await asyncio.sleep(0.4)
    assert b.used == 0                # event pruned + authoritative gone stale
    assert b.has_authoritative is False


def test_bucket_exposes_rule_and_pending():
    rule = _rule(limit=5)
    b = RateLimitBucket(rule)
    assert b.rule is rule
    assert b.pending == 0


def test_blocked_wait_floors_at_min_when_nothing_to_expire():
    # No events and no authoritative reading: the loop must still yield a
    # positive sleep (the busy-wait guard) rather than 0.
    from binance.rate_limit.bucket import _MIN_WAIT
    b = RateLimitBucket(_rule(enforce=EnforceMode.SLEEP, interval=10, limit=10))
    assert b._blocked_wait(time.monotonic()) == _MIN_WAIT


@pytest.mark.asyncio
async def test_sleep_lock_released_during_wait_allows_concurrent_small(monkeypatch):
    """F-73: a small request must NOT be blocked by a large sleeping waiter.

    Before the fix the lock was held across the asyncio.sleep, so a small
    request that fits the bucket would be head-of-line blocked by a large
    one sleeping its window away. After the fix the lock is released before
    sleep, so the small request acquires the lock and completes immediately.
    """
    # Bucket: limit=3, interval=0.3s, SLEEP.  Fill it with 2 units so there
    # is 1 unit of headroom but not 3 (the large-cost waiter needs 3 + existing
    # 2 = 5 > limit, so it must sleep).
    b = RateLimitBucket(_rule(enforce=EnforceMode.SLEEP, interval=0.3, limit=3))
    # Pre-fill 2 units so only 1 headroom remains.
    b.record(1)
    b.record(1)

    start = time.monotonic()

    # Large-cost waiter (cost=3): bucket has only 1 headroom -> must sleep.
    large_task = asyncio.create_task(b.acquire(3))

    # Yield so large_task runs and is now inside asyncio.sleep (lock released).
    await asyncio.sleep(0.01)

    # Small waiter (cost=1): fits the 1-unit gap -> must NOT be blocked.
    small_task = asyncio.create_task(b.acquire(1))

    # The small task must finish well before the large task's window expires.
    try:
        await asyncio.wait_for(asyncio.shield(small_task), timeout=0.2)
    except asyncio.TimeoutError:
        large_task.cancel()
        pytest.fail("Small request was head-of-line blocked by large sleeping waiter")

    elapsed = time.monotonic() - start
    assert elapsed < 0.2, (
        f"Small request took {elapsed:.3f}s — still blocked by large waiter"
    )

    # Clean up the still-sleeping large task.
    large_task.cancel()
    try:
        await large_task
    except (asyncio.CancelledError, Exception):
        pass
