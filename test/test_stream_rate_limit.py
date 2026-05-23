import asyncio
import pytest

from binance import Stream
from binance.common.rate_limit import SlidingWindowRateLimiter
from logging import getLogger

logger = getLogger(__name__)


@pytest.mark.asyncio
async def test_stream_connect_is_gated_by_connection_limiter():
    # 2 connections allowed per 0.4s window
    limiter = SlidingWindowRateLimiter(max_count=2, window=0.4)

    async def on_message(_):
        return None

    def policy(info):
        return False, 0.0  # retry immediately; the limiter must throttle

    # Point at a port nobody is listening on so connect() fails fast and retries
    stream = Stream(
        'ws://localhost:9099/stream',
        on_message=on_message,
        retry_policy=policy,
        timeout=0.1,
        logger=logger,
        connection_limiter=limiter
    )
    stream.connect()
    await asyncio.sleep(0.5)
    await stream.close()
    # With 2/0.4s, far fewer than the unbounded storm would produce
    assert len(limiter._events) <= 4


@pytest.mark.asyncio
async def test_close_while_parked_in_connection_throttle_is_clean():
    # Pre-fill a 1-slot, long-window limiter so the stream's _connect blocks
    # inside acquire()'s sleep (not in connect()).
    limiter = SlidingWindowRateLimiter(max_count=1, window=30.0)
    await limiter.acquire()  # consume the only slot

    async def on_message(_):
        return None

    stream = Stream(
        'ws://localhost:9097/stream',
        on_message=on_message,
        timeout=0.1,
        logger=logger,
        connection_limiter=limiter
    )
    stream.connect()
    # Give the connect task time to reach acquire() and park in its sleep
    await asyncio.sleep(0.1)
    # close() cancels the parked task; the CancelledError guard must absorb it
    await stream.close()  # must NOT raise CancelledError
    assert stream._socket is None
