import asyncio
import json
from contextlib import suppress
from logging import getLogger

import pytest

from binance import Credentials, SpotClient, Stream, SubType, UMFuturesClient
from binance.core.common.exceptions import StreamDisconnectedException
from binance.core.rate_limit import RateLimiter


logger = getLogger(__name__)


class _NoopHandlerContext:
    def __init__(self):
        self.received = []

    async def receive(self, msg):
        self.received.append(msg)


@pytest.mark.asyncio
async def test_event_stream_terminated_recovery_does_not_self_deadlock():
    client = SpotClient(Credentials('key')).start()
    handler_ctx = _NoopHandlerContext()
    client._handler_ctx = handler_ctx
    client._want_user_stream = True
    client._subscribed.add((SubType.USER,))

    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()
    calls = []

    async def fake_subscribe_user_only(subscribe, subscriptions):
        calls.append((subscribe, tuple(subscriptions)))
        recovery_started.set()
        await release_recovery.wait()

    client._subscribe_user_only = fake_subscribe_user_only

    receive_task = asyncio.create_task(client._receive({
        'subscriptionId': 0,
        'event': {'e': 'eventStreamTerminated'},
    }))

    try:
        await asyncio.wait_for(receive_task, timeout=0.05)
    finally:
        release_recovery.set()
        if not receive_task.done():
            receive_task.cancel()
            with suppress(asyncio.CancelledError):
                await receive_task
        await client.close()

    assert recovery_started.is_set()
    assert calls == [(True, ((SubType.USER,),))]
    assert handler_ctx.received


@pytest.mark.asyncio
async def test_server_shutdown_receive_does_not_wait_for_recycle_close():
    client = SpotClient(Credentials('key')).start()
    client._handler_ctx = _NoopHandlerContext()

    class SlowRecycleStream:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def recycle(self):
            self.started.set()
            await self.release.wait()

    origin = SlowRecycleStream()
    receive_task = asyncio.create_task(client._receive(
        {'event': {'e': 'serverShutdown'}},
        origin=origin,
    ))

    try:
        await asyncio.wait_for(receive_task, timeout=0.05)
    finally:
        origin.release.set()
        if not receive_task.done():
            receive_task.cancel()
            with suppress(asyncio.CancelledError):
                await receive_task
        await client.close()

    assert origin.started.is_set()


@pytest.mark.asyncio
async def test_raw_stream_on_message_long_task_does_not_block_recv():
    received = []

    async def on_message(msg):
        received.append(msg)
        if len(received) == 1:
            await asyncio.sleep(0.2)

    stream = Stream(
        'ws://fake',
        on_message=on_message,
        logger=logger,
        rate_limiter=RateLimiter(enabled=False),
    )
    stream._connection_error = False
    stream._timeout = 1.0

    class FakeSocket:
        def __init__(self):
            self.recv_count = 0
            self.messages = [
                json.dumps({'stream': 'one', 'data': {'e': '24hrTicker'}}),
                json.dumps({'stream': 'two', 'data': {'e': '24hrTicker'}}),
            ]

        async def recv(self):
            self.recv_count += 1
            return self.messages.pop(0)

    socket = FakeSocket()
    stream._socket = socket

    async def reader():
        await stream._receive()
        await stream._receive()

    task = asyncio.create_task(reader())
    try:
        await asyncio.sleep(0.05)
        assert socket.recv_count == 2
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_stream_send_response_timeout_rejects_and_cleans_future():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._uri = 'ws://fake'
    stream._message_futures = {}
    stream._message_id = 0
    stream._rate_limiter = RateLimiter(enabled=False)
    stream._connection_id = 'default'
    stream._timeout = 0.05
    stream._open_future = None

    class FakeSocket:
        async def send(self, _data):
            pass

    stream._socket = FakeSocket()

    with pytest.raises(Exception) as exc:
        await asyncio.wait_for(
            stream.send({'method': 'never_replies'}),
            timeout=0.2,
        )

    assert type(exc.value).__name__ == 'StreamResponseTimeoutException'
    assert stream._message_futures == {}


@pytest.mark.asyncio
async def test_stream_close_rejects_send_waiting_for_open_future():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._uri = 'ws://fake'
    stream._message_futures = {}
    stream._message_id = 0
    stream._rate_limiter = RateLimiter(enabled=False)
    stream._connection_id = 'default'
    stream._open_future = asyncio.get_running_loop().create_future()
    stream._socket = None
    stream._connected_task = None
    stream._closing = False

    async def conn_task():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return

    stream._conn_task = asyncio.create_task(conn_task())
    send_task = asyncio.create_task(stream.send({'method': 'wait_open'}))
    await asyncio.sleep(0)

    await stream.close()

    with pytest.raises(StreamDisconnectedException):
        await asyncio.wait_for(send_task, timeout=0.1)


@pytest.mark.asyncio
async def test_spot_ws_api_callback_captures_origin_instance(monkeypatch):
    captured = []

    async def fake_receive(_msg, *, origin=None):
        captured.append(origin)

    class CapturingStream:
        def __init__(self, uri, *, on_message, **_kwargs):
            self.uri = uri
            self.on_message = on_message

        def connect(self):
            return self

        async def close(self, code=4999):
            pass

    monkeypatch.setattr(
        'binance.core.transport.subscription.Stream',
        CapturingStream,
    )

    client = SpotClient(Credentials('key')).start()
    monkeypatch.setattr(client, '_receive', fake_receive)

    old_stream = client._get_ws_api_stream()
    new_stream = object()
    client._user_stream = new_stream

    await old_stream.on_message({'event': {'e': 'serverShutdown'}})

    client._user_stream = None
    assert captured == [old_stream]


@pytest.mark.asyncio
async def test_event_stream_terminated_recovery_is_single_flight():
    client = SpotClient(Credentials('key')).start()
    handler_ctx = _NoopHandlerContext()
    client._handler_ctx = handler_ctx
    client._want_user_stream = True
    client._subscribed.add((SubType.USER,))

    release_recovery = asyncio.Event()
    calls = []

    async def fake_subscribe_user_only(subscribe, subscriptions):
        calls.append((subscribe, tuple(subscriptions)))
        await release_recovery.wait()

    client._subscribe_user_only = fake_subscribe_user_only

    receive_tasks = [
        asyncio.create_task(client._receive({
            'subscriptionId': 0,
            'event': {'e': 'eventStreamTerminated'},
        }))
        for _ in range(2)
    ]

    try:
        await asyncio.wait_for(asyncio.gather(*receive_tasks), timeout=0.05)
    finally:
        release_recovery.set()
        await client.close()

    assert calls == [(True, ((SubType.USER,),))]
    assert len(handler_ctx.received) == 2


@pytest.mark.asyncio
async def test_server_shutdown_without_origin_does_not_wait_for_all_recycles():
    client = SpotClient(Credentials('key')).start()
    client._handler_ctx = _NoopHandlerContext()

    class SlowRecycleStream:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def recycle(self):
            self.started.set()
            await self.release.wait()

    streams = [SlowRecycleStream(), SlowRecycleStream()]
    client._data_streams = {'/stream': streams[0], '/alt': streams[1]}

    try:
        await asyncio.wait_for(
            client._receive({'event': {'e': 'serverShutdown'}}),
            timeout=0.05,
        )
    finally:
        for stream in streams:
            stream.release.set()
        client._data_streams = {}
        await client.close()

    assert all(stream.started.is_set() for stream in streams)


@pytest.mark.asyncio
async def test_data_stream_callback_captures_origin_instance(monkeypatch):
    captured = []

    async def fake_receive(_msg, *, origin=None):
        captured.append(origin)

    class CapturingStream:
        def __init__(self, uri, *, on_message, **_kwargs):
            self.uri = uri
            self.on_message = on_message

        def connect(self):
            return self

        async def close(self, code=4999):
            pass

    monkeypatch.setattr(
        'binance.core.transport.subscription.Stream',
        CapturingStream,
    )

    client = SpotClient(Credentials('key')).start()
    monkeypatch.setattr(client, '_receive', fake_receive)

    old_stream = client._get_data_stream()
    client._data_streams['/stream'] = object()

    await old_stream.on_message({'data': {'e': '24hrTicker'}})

    client._data_streams = {}
    assert captured == [old_stream]


@pytest.mark.asyncio
async def test_futures_user_stream_callback_captures_origin_instance(monkeypatch):
    captured = []

    async def fake_receive(_msg, *, origin=None):
        captured.append(origin)

    class CapturingStream:
        def __init__(self, uri, *, on_message, **_kwargs):
            self.uri = uri
            self.on_message = on_message

        def connect(self):
            return self

        async def close(self, code=4999):
            pass

    monkeypatch.setattr('binance.futures.user_stream.Stream', CapturingStream)

    client = UMFuturesClient(Credentials('key', 'secret')).start()

    async def fake_ws_api_request(*_args, **_kwargs):
        return {'listenKey': 'listen-key'}

    monkeypatch.setattr(client, '_ws_api_request', fake_ws_api_request)
    monkeypatch.setattr(client, '_receive', fake_receive)

    await client._futures_user_stream_start()
    old_stream = client._futures_user_stream
    client._futures_user_stream = object()

    await old_stream.on_message({'data': {'e': 'ACCOUNT_UPDATE'}})

    client._futures_user_stream = None
    client._cancel_futures_keepalive()
    assert captured == [old_stream]


@pytest.mark.asyncio
async def test_futures_listen_key_expired_recovery_does_not_block_receive(monkeypatch):
    client = UMFuturesClient(Credentials('key', 'secret')).start()
    handler_ctx = _NoopHandlerContext()
    client._handler_ctx = handler_ctx
    client._want_user_stream = True

    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()

    async def slow_listen_key_recovery():
        recovery_started.set()
        await release_recovery.wait()

    monkeypatch.setattr(
        client,
        '_on_futures_listen_key_expired',
        slow_listen_key_recovery,
    )

    try:
        await asyncio.wait_for(
            client._receive({'e': 'listenKeyExpired'}),
            timeout=0.05,
        )
    finally:
        release_recovery.set()
        await client.close()

    assert recovery_started.is_set()
    assert handler_ctx.received == [{'e': 'listenKeyExpired'}]


@pytest.mark.asyncio
async def test_stream_late_response_after_timeout_is_ignored():
    delivered = []

    async def on_message(msg):
        delivered.append(msg)

    stream = Stream(
        'ws://fake',
        on_message=on_message,
        logger=logger,
        rate_limiter=RateLimiter(enabled=False),
        timeout=0.05,
    )

    class FakeSocket:
        async def send(self, _data):
            pass

    stream._socket = FakeSocket()

    with pytest.raises(Exception) as exc:
        await stream.send({'method': 'never_replies'})

    assert type(exc.value).__name__ == 'StreamResponseTimeoutException'

    await stream._handle_message({'id': 0, 'result': 'late'})
    await asyncio.sleep(0)

    assert delivered == []
    assert stream._message_futures == {}
