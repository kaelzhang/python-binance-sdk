"""``serverShutdown`` event handling for both market-data and WS-API streams.

Per the 2026-05-06 Spot changelog the ``serverShutdown`` event is sent on
BOTH the WS-API connection AND the market-data WS stream. The previous
behaviour unconditionally recycled the market-data ``_data_stream`` even
when the event arrived on the WS-API connection -- recycling the wrong
stream and leaving the actually-shutting-down connection up until aioretry
noticed the disconnect.

The fix: the stream that delivered the message is the one that gets
recycled. Each :class:`Stream` is constructed with an ``on_message``
callback that, after the SDK's normal dispatch, knows which connection it
came from; on ``serverShutdown`` it recycles that connection only.

Docs:
- 2026-05-06 Spot changelog:
  https://developers.binance.com/docs/binance-spot-api-docs/CHANGELOG
"""

import pytest

from binance import SpotClient, UMFuturesClient, Credentials


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RecordingStream:
    """Stream stub: tracks every ``recycle()`` call."""

    def __init__(self, uri='', **kwargs):
        self.uri = uri
        self.on_message = kwargs.get('on_message')
        self.recycle_calls = 0

    def connect(self):
        return self

    async def recycle(self):
        self.recycle_calls += 1

    async def send(self, _req):
        return None

    async def close(self, code=4999):
        pass


@pytest.fixture
def client():
    return SpotClient(Credentials('k')).start()


# ---------------------------------------------------------------------------
# Spot data stream receives serverShutdown -> recycles ONLY the data stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_shutdown_on_data_stream_recycles_data_stream(client):
    data = _RecordingStream(uri='wss://stream.binance.com/stream')
    user = _RecordingStream(uri='wss://ws-api.binance.com/ws-api/v3')

    client._data_streams = {'/stream': data}
    client._user_stream = user

    # Simulate a serverShutdown delivered by the data stream's on_message hook.
    await client._receive(
        {'data': {'e': 'serverShutdown'}}, origin=data,
    )

    assert data.recycle_calls == 1
    assert user.recycle_calls == 0


# ---------------------------------------------------------------------------
# WS-API connection receives serverShutdown -> recycles ONLY the WS-API stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_shutdown_on_ws_api_recycles_ws_api_stream(client):
    data = _RecordingStream(uri='wss://stream.binance.com/stream')
    user = _RecordingStream(uri='wss://ws-api.binance.com/ws-api/v3')

    client._data_streams = {'/stream': data}
    client._user_stream = user

    # Simulate a serverShutdown delivered by the WS-API connection.
    await client._receive({'event': {'e': 'serverShutdown'}}, origin=user)

    assert user.recycle_calls == 1
    assert data.recycle_calls == 0


# ---------------------------------------------------------------------------
# UM split data streams: shutdown on /public/stream recycles only it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_shutdown_on_um_public_stream_recycles_only_that_path():
    client = UMFuturesClient().start()
    public = _RecordingStream(uri='wss://fstream.binance.com/public/stream')
    market = _RecordingStream(uri='wss://fstream.binance.com/market/stream')
    user = _RecordingStream(uri='wss://ws-fapi.binance.com/ws-fapi/v1')

    client._data_streams = {
        '/public/stream': public,
        '/market/stream': market,
    }
    client._user_stream = user

    await client._receive({'event': {'e': 'serverShutdown'}}, origin=public)

    assert public.recycle_calls == 1
    assert market.recycle_calls == 0
    assert user.recycle_calls == 0


# ---------------------------------------------------------------------------
# UM private fstream user-data: shutdown recycles ONLY that connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_shutdown_on_um_futures_user_stream_recycles_only_it(
    monkeypatch,
):
    """Shutdown delivered by the dedicated /private/ws/<key> stream recycles only it."""
    client = UMFuturesClient(Credentials('k')).start()

    public = _RecordingStream(uri='wss://fstream.binance.com/public/stream')
    market = _RecordingStream(uri='wss://fstream.binance.com/market/stream')
    user = _RecordingStream(uri='wss://ws-fapi.binance.com/ws-fapi/v1')
    private = _RecordingStream(
        uri='wss://fstream.binance.com/private/ws/abc')

    client._data_streams = {
        '/public/stream': public,
        '/market/stream': market,
    }
    client._user_stream = user
    client._futures_user_stream = private

    # Deliver via the private fstream as origin.
    await client._receive(
        {'data': {'e': 'serverShutdown'}}, origin=private,
    )

    assert private.recycle_calls == 1
    assert public.recycle_calls == 0
    assert market.recycle_calls == 0
    assert user.recycle_calls == 0


# ---------------------------------------------------------------------------
# Backwards-compat: when no origin is passed (e.g. legacy test mocks),
# the behaviour falls back to recycling all known data streams.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_shutdown_without_origin_recycles_all_data_streams(client):
    data = _RecordingStream(uri='wss://stream.binance.com/stream')
    client._data_streams = {'/stream': data}

    await client._receive({'data': {'e': 'serverShutdown'}})

    assert data.recycle_calls == 1


# ---------------------------------------------------------------------------
# Each stream's on_message callback passes itself as origin
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_data_stream_callback_binds_origin(monkeypatch):
    """The on_message wrapper passed to Stream() resolves to the same stream instance."""
    client = SpotClient(Credentials('k')).start()

    captured = []

    async def fake_receive(msg, *, origin=None):
        captured.append((msg, origin))

    monkeypatch.setattr(client, '_receive', fake_receive)

    # Replace Stream with one that exposes on_message so we can drive it directly.
    class CapturingStream:
        captured_callbacks = []

        def __init__(self, uri, *, on_message, **kwargs):
            self.uri = uri
            self.on_message = on_message
            CapturingStream.captured_callbacks.append((uri, on_message, self))

        def connect(self):
            return self

        async def send(self, _r):
            return None

        async def close(self, code=4999):
            pass

        async def recycle(self):
            pass

    monkeypatch.setattr(
        'binance.core.transport.subscription.Stream', CapturingStream)

    s = client._get_data_stream()
    # Invoke the bound on_message callback as if a message arrived.
    await s.on_message({'data': {'e': '24hrTicker', 's': 'BTCUSDT'}})

    assert len(captured) == 1
    _msg, origin = captured[0]
    assert origin is s

    await client.close()


@pytest.mark.asyncio
async def test_ws_api_stream_callback_binds_origin(monkeypatch):
    """The WS-API stream's on_message wrapper passes the WS-API stream as origin."""
    client = SpotClient(Credentials('k')).start()

    captured = []

    async def fake_receive(msg, *, origin=None):
        captured.append((msg, origin))

    monkeypatch.setattr(client, '_receive', fake_receive)

    class CapturingStream:
        def __init__(self, uri, *, on_message, **kwargs):
            self.uri = uri
            self.on_message = on_message

        def connect(self):
            return self

        async def send(self, _r):
            return None

        async def close(self, code=4999):
            pass

        async def recycle(self):
            pass

    monkeypatch.setattr(
        'binance.core.transport.subscription.Stream', CapturingStream)

    s = client._get_ws_api_stream()
    await s.on_message({'event': {'e': 'serverShutdown'}})

    assert len(captured) == 1
    _msg, origin = captured[0]
    assert origin is s
    await client.close()


@pytest.mark.asyncio
async def test_um_futures_user_stream_callback_binds_origin(monkeypatch):
    """UM futures user-data fstream binds its own ``origin`` via on_message wrapper."""
    client = UMFuturesClient(Credentials('k', 'sec')).start()

    captured = []

    async def fake_receive(msg, *, origin=None):
        captured.append((msg, origin))

    monkeypatch.setattr(client, '_receive', fake_receive)

    class CapturingStream:
        def __init__(self, uri, *, on_message, **kwargs):
            self.uri = uri
            self.on_message = on_message

        def connect(self):
            return self

        async def send(self, _r):
            return None

        async def close(self, code=4999):
            pass

        async def recycle(self):
            pass

    # Patch Stream in the futures.user_stream module (separate import).
    monkeypatch.setattr(
        'binance.futures.user_stream.Stream', CapturingStream)

    # Drive the futures user-stream start path by stubbing _ws_api_request.
    async def fake_start(*_a, **_k):
        return {'listenKey': 'abc'}

    monkeypatch.setattr(client, '_ws_api_request', fake_start)
    await client._futures_user_stream_start()
    client._cancel_futures_keepalive()

    # Now drive the bound on_message wrapper.
    s = client._futures_user_stream
    assert s is not None
    await s.on_message({'data': {'e': 'serverShutdown'}})

    assert len(captured) == 1
    _msg, origin = captured[0]
    assert origin is s
    await client.close()
