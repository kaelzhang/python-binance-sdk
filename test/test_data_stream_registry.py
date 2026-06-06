from logging import getLogger

import pytest

from binance.core.rate_limit import RateLimiter, RateLimitType
from binance.core.transport.data_streams import DataStreamRegistry


logger = getLogger(__name__)


class FakeStream:
    def __init__(
        self,
        uri,
        *,
        on_message,
        on_connected,
        retry_policy,
        timeout,
        logger,
        rate_limiter,
        connection_id,
        connection_lease,
    ):
        self.uri = uri
        self.on_message = on_message
        self.on_connected = on_connected
        self.connection_id = connection_id
        self.connection_lease = connection_lease
        self.closed = []
        self.connected = False

    def connect(self):
        self.connected = True
        return self

    async def close(self, code=4999):
        self.closed.append(code)


def _connection_windows(rate_limiter):
    return [
        w for w in rate_limiter.snapshot().windows
        if w.type == RateLimitType.WS_STREAMS and w.connection_id is not None
    ]


class CloseFailingStream(FakeStream):
    async def close(self, code=4999):
        self.closed.append(code)
        raise RuntimeError('close failed')


def _registry(rate_limiter=None, stream_factory=FakeStream):
    received = []

    async def on_message(stream, msg):
        received.append((stream, msg))

    def on_connected(path):
        async def connected():
            return path

        return connected

    return (
        DataStreamRegistry(
            stream_host='wss://example.test',
            retry_policy=lambda _info: (False, 0),
            timeout=1.0,
            logger=logger,
            rate_limiter=rate_limiter or RateLimiter(enabled=False),
            on_message=on_message,
            on_connected=on_connected,
            stream_factory=stream_factory,
        ),
        received,
    )


@pytest.mark.asyncio
async def test_data_stream_registry_reuses_stream_and_binds_origin():
    registry, received = _registry()

    first = registry.get_stream('/stream')
    second = registry.get_stream('/stream')

    assert first is second
    assert first.connected
    assert first.uri == 'wss://example.test/stream'
    assert first.connection_id == 'data'
    assert first.connection_lease is registry.get_lease('/stream')

    await first.on_message({'e': 'serverShutdown'})

    assert received == [(first, {'e': 'serverShutdown'})]


def test_data_stream_registry_drops_only_unopened_leases():
    rate_limiter = RateLimiter(enabled=False)
    registry, _received = _registry(rate_limiter)

    lease = registry.get_lease('/stream')
    assert registry.leases == {'/stream': lease}
    assert len(_connection_windows(rate_limiter)) == 1

    registry.drop_unopened_lease('/stream')

    assert registry.leases == {}
    assert _connection_windows(rate_limiter) == []

    stream = registry.get_stream('/stream')
    open_lease = registry.get_lease('/stream')

    registry.drop_unopened_lease('/stream')

    assert registry.streams == {'/stream': stream}
    assert registry.leases == {'/stream': open_lease}


@pytest.mark.asyncio
async def test_data_stream_registry_close_all_unregisters_leases():
    rate_limiter = RateLimiter(enabled=False)
    registry, _received = _registry(rate_limiter)
    first = registry.get_stream('/stream')
    second = registry.get_stream('/alt')

    assert len(_connection_windows(rate_limiter)) == 2

    await registry.close_all(code=4001)

    assert first.closed == [4001]
    assert second.closed == [4001]
    assert registry.streams == {}
    assert registry.leases == {}
    assert _connection_windows(rate_limiter) == []


@pytest.mark.asyncio
async def test_data_stream_registry_close_all_cleans_leases_after_close_error():
    rate_limiter = RateLimiter(enabled=False)

    def stream_factory(uri, **kwargs):
        if uri.endswith('/stream'):
            return CloseFailingStream(uri, **kwargs)
        return FakeStream(uri, **kwargs)

    registry, _received = _registry(rate_limiter, stream_factory)
    first = registry.get_stream('/stream')
    second = registry.get_stream('/alt')

    with pytest.raises(RuntimeError, match='close failed'):
        await registry.close_all(code=4001)

    assert first.closed == [4001]
    assert second.closed == [4001]
    assert registry.streams == {}
    assert registry.leases == {}
    assert _connection_windows(rate_limiter) == []
