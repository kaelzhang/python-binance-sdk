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
    limiter._events.clear()
    stream.connect()
    await asyncio.sleep(0.5)
    await stream.close()
    # With 2/0.4s, far fewer than the unbounded storm would produce
    assert len(limiter._events) <= 4
