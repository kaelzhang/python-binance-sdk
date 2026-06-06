import asyncio
from typing import ClassVar

import pytest

from binance import CMFuturesClient, Credentials, SubType, UMFuturesClient


class _FakeStream:
    instances: ClassVar[list['_FakeStream']] = []
    block_first_close = False
    close_started = None
    release_close = None
    raise_on_init = False

    @classmethod
    def reset(cls):
        cls.instances = []
        cls.block_first_close = False
        cls.close_started = None
        cls.release_close = None
        cls.raise_on_init = False

    def __init__(self, uri, *, connection_lease=None, **_kwargs):
        if self.__class__.raise_on_init:
            raise RuntimeError('stream init failed')
        self.uri = uri
        self.connection_lease = connection_lease
        self.closed = False
        self.raise_on_close = False
        self.__class__.instances.append(self)

    def connect(self):
        return self

    async def send(self, _req):
        return None

    async def close(self, code=4999):
        if self.__class__.block_first_close and self is self.__class__.instances[0]:
            self.__class__.close_started.set()
            await self.__class__.release_close.wait()
        if self.raise_on_close:
            raise RuntimeError('close failed')
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('client_cls', 'stream_host'),
    (
        (UMFuturesClient, 'wss://fstream.binance.com'),
        (CMFuturesClient, 'wss://dstream.binance.com'),
    ),
    ids=('um', 'cm'),
)
async def test_repeated_futures_user_subscribe_reuses_existing_lifecycle(
    monkeypatch,
    client_cls,
    stream_host,
):
    _FakeStream.reset()
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)

    client = client_cls(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host='ws://unused',
        stream_host=stream_host,
    ).start()

    start_methods = []

    async def fake_ws_api_request(method, *_args, **_kwargs):
        if method == 'userDataStream.start':
            start_methods.append(method)
            return {'listenKey': f'listen-key-{len(start_methods)}'}
        return None

    async def no_keepalive_loop():
        return None

    monkeypatch.setattr(client, '_ws_api_request', fake_ws_api_request)
    monkeypatch.setattr(client, '_futures_keepalive_loop', no_keepalive_loop)

    try:
        await client.subscribe(SubType.USER)
        first_stream = client._futures_user_stream
        first_lease = client._futures_user_connection_lease

        await client.subscribe(SubType.USER)

        assert start_methods == ['userDataStream.start']
        assert len(_FakeStream.instances) == 1
        assert client._futures_user_stream is first_stream
        assert client._futures_user_connection_lease is first_lease
        assert client._futures_listen_key == 'listen-key-1'
        futures_connections = [
            state
            for state in client._rate_limiter._connections.values()
            if state.lease.kind == 'futures_user'
        ]
        assert len(futures_connections) == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_futures_user_stream_start_failure_rolls_back_listen_key_and_lease(
    monkeypatch,
):
    _FakeStream.reset()
    _FakeStream.raise_on_init = True
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)

    client = UMFuturesClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host='ws://unused',
        stream_host='wss://fstream.binance.com',
    ).start()

    requests = []

    async def fake_ws_api_request(method, params=None, **_kwargs):
        requests.append((method, params))
        if method == 'userDataStream.start':
            return {'listenKey': 'listen-key-1'}
        return None

    monkeypatch.setattr(client, '_ws_api_request', fake_ws_api_request)

    with pytest.raises(RuntimeError, match='stream init failed'):
        await client.subscribe(SubType.USER)

    assert requests == [
        ('userDataStream.start', None),
        ('userDataStream.stop', {'listenKey': 'listen-key-1'}),
    ]
    assert client._futures_user_stream is None
    assert client._futures_listen_key is None
    assert client._futures_user_connection_lease is None
    futures_connections = [
        state
        for state in client._rate_limiter._connections.values()
        if state.lease.kind == 'futures_user'
    ]
    assert futures_connections == []
    assert (SubType.USER,) not in client._subscribed


@pytest.mark.asyncio
async def test_futures_user_close_ignores_listen_key_expired_during_cleanup(
    monkeypatch,
):
    _FakeStream.reset()
    _FakeStream.block_first_close = True
    _FakeStream.close_started = asyncio.Event()
    _FakeStream.release_close = asyncio.Event()
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)

    client = UMFuturesClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host='ws://unused',
        stream_host='wss://fstream.binance.com',
    ).start()

    start_count = 0

    async def fake_ws_api_request(method, *_args, **_kwargs):
        nonlocal start_count
        if method == 'userDataStream.start':
            start_count += 1
            return {'listenKey': f'listen-key-{start_count}'}
        return None

    async def no_keepalive_loop():
        return None

    monkeypatch.setattr(client, '_ws_api_request', fake_ws_api_request)
    monkeypatch.setattr(client, '_futures_keepalive_loop', no_keepalive_loop)

    await client.subscribe(SubType.USER)

    close_task = asyncio.create_task(client.close())
    await asyncio.wait_for(_FakeStream.close_started.wait(), timeout=0.1)

    await client._receive({'data': {'e': 'listenKeyExpired'}})
    await asyncio.sleep(0)
    _FakeStream.release_close.set()
    await asyncio.wait_for(close_task, timeout=0.5)
    await asyncio.sleep(0)

    assert start_count == 1
    assert len(_FakeStream.instances) == 1
    assert _FakeStream.instances[0].closed is True
    assert client._futures_user_stream is None
    assert client._futures_listen_key is None
    assert client._futures_user_connection_lease is None


@pytest.mark.asyncio
async def test_futures_listen_key_expired_releases_old_lease_when_close_fails(
    monkeypatch,
):
    _FakeStream.reset()
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)

    client = UMFuturesClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host='ws://unused',
        stream_host='wss://fstream.binance.com',
    ).start()

    start_count = 0

    async def fake_ws_api_request(method, *_args, **_kwargs):
        nonlocal start_count
        if method == 'userDataStream.start':
            start_count += 1
            return {'listenKey': f'listen-key-{start_count}'}
        return None

    async def no_keepalive_loop():
        return None

    monkeypatch.setattr(client, '_ws_api_request', fake_ws_api_request)
    monkeypatch.setattr(client, '_futures_keepalive_loop', no_keepalive_loop)

    try:
        await client.subscribe(SubType.USER)
        old_stream = _FakeStream.instances[0]
        old_stream.raise_on_close = True
        old_lease = client._futures_user_connection_lease

        await client._on_futures_listen_key_expired()

        assert start_count == 2
        assert len(_FakeStream.instances) == 2
        assert client._futures_user_connection_lease is not old_lease
        assert old_lease.id not in client._rate_limiter._connections
        futures_connections = [
            state
            for state in client._rate_limiter._connections.values()
            if state.lease.kind == 'futures_user'
        ]
        assert len(futures_connections) == 1
    finally:
        await client.close()
