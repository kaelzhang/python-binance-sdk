import asyncio
import pytest

from binance import Stream
from binance.core.rate_limit import (
    RateLimiter,
    RateLimitRule,
    RateLimitScope,
    RateLimitType,
    RateLimitKind,
    EnforceMode,
)
from logging import getLogger

logger = getLogger(__name__)


def _ws_connections_used(rate_limiter):
    """Read the ws_connections window's `used` count from a core snapshot."""
    for window in rate_limiter.snapshot().windows:
        if window.type == RateLimitType.WS_CONNECTIONS.value:
            return window.used
    raise AssertionError('no ws_connections window in snapshot')


# A tiny ws_connections rule: 2 attempts per 0.4s window, SLEEP-enforced so the
# core throttles a connect storm instead of raising.
_TINY_WS_CONNECTIONS_RULE = RateLimitRule(
    RateLimitScope.IP, RateLimitType.WS_CONNECTIONS, 0.4, 2,
    RateLimitKind.COUNT, EnforceMode.SLEEP)


@pytest.mark.asyncio
async def test_stream_connect_is_gated_by_connection_limiter():
    # Core configured with a TINY ws_connections rule: 2 attempts / 0.4s.
    rate_limiter = RateLimiter(rules=(_TINY_WS_CONNECTIONS_RULE,))

    async def on_message(_):
        return None

    def policy(info):
        return False, 0.0  # retry immediately; the core must throttle

    # Point at a port nobody is listening on so connect() fails fast and retries
    stream = Stream(
        'ws://localhost:9099/stream',
        on_message=on_message,
        retry_policy=policy,
        timeout=0.1,
        logger=logger,
        rate_limiter=rate_limiter
    )
    stream.connect()
    await asyncio.sleep(0.5)
    await stream.close()
    # With 2/0.4s, far fewer than the unbounded storm would produce
    assert _ws_connections_used(rate_limiter) <= 4


@pytest.mark.asyncio
async def test_close_while_parked_in_connection_throttle_is_clean():
    # Pre-fill a 1-slot, long-window ws_connections bucket so the stream's
    # _connect blocks inside acquire_connection()'s sleep (not in connect()).
    rate_limiter = RateLimiter(rules=(
        RateLimitRule(
            RateLimitScope.IP, RateLimitType.WS_CONNECTIONS, 30.0, 1,
            RateLimitKind.COUNT, EnforceMode.SLEEP),
    ))
    await rate_limiter.acquire_connection()  # consume the only slot

    async def on_message(_):
        return None

    stream = Stream(
        'ws://localhost:9097/stream',
        on_message=on_message,
        timeout=0.1,
        logger=logger,
        rate_limiter=rate_limiter
    )
    stream.connect()
    # Give the connect task time to reach acquire_connection() and park
    await asyncio.sleep(0.1)
    # close() cancels the parked task; the CancelledError guard must absorb it
    await stream.close()  # must NOT raise CancelledError
    assert stream._socket is None


def test_extract_event_type_handles_documented_shapes():
    from binance.subscribe.manager import _extract_event_type
    assert _extract_event_type({'e': 'serverShutdown'}) == 'serverShutdown'
    assert _extract_event_type(
        {'stream': 'x', 'data': {'e': 'serverShutdown'}}) == 'serverShutdown'
    assert _extract_event_type(
        {'event': {'e': 'eventStreamTerminated'}}) == 'eventStreamTerminated'
    assert _extract_event_type({'data': {'e': 'depthUpdate'}}) == 'depthUpdate'
    assert _extract_event_type('not-a-dict') is None


@pytest.mark.asyncio
async def test_recycle_closes_socket_without_setting_closing_flag():
    # recycle() must close the socket but leave _closing False so the
    # _connect loop reconnects (unlike close(), which is terminal).
    stream = Stream.__new__(Stream)   # bypass __init__/connect
    stream._closing = False
    closed = []

    class FakeSock:
        async def close(self, code):
            closed.append(code)

    stream._socket = FakeSock()
    await stream.recycle()
    assert closed == [4999]
    assert stream._closing is False


@pytest.mark.asyncio
async def test_ws_api_error_minus_1003_raises_stream_rate_limit():
    from binance.core.common.exceptions import StreamRateLimitException
    from binance.core.common.utils import create_future

    async def on_message(_):
        return None

    stream = Stream('ws://localhost:9098/ws', on_message=on_message,
                    timeout=0.1, logger=logger)
    fut = create_future()
    stream._message_futures[7] = fut
    await stream._handle_message({
        'id': 7,
        'status': 418,
        'error': {'code': -1003, 'msg': 'Too much request weight used',
                  'data': {'retryAfter': 88}}
    })
    with pytest.raises(StreamRateLimitException) as info:
        await fut
    assert info.value.retry_after == 88
    assert info.value.code == -1003
