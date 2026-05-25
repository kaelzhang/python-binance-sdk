"""Tests for the COIN-M Futures user-data-stream lifecycle.

Confirms that CMFuturesClient:
- inherits FuturesUserStreamMixin and uses the same listenKey flow as UM;
- subscribe(SubType.USER) calls userDataStream.start over ws-dapi (USER_STREAM, weight 1);
- the dedicated stream is opened at dstream.binance.com/ws/<listenKey>;
- keepalive calls userDataStream.ping with the listenKey;
- close() calls userDataStream.stop;
- FuturesUserProcessor is in CM PROCESSORS so user events route correctly.

SAFETY: MOCK-only — no live API calls.
"""

import asyncio
import pytest

from binance import CMFuturesClient, Credentials, SubType
from binance import (
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
)
from binance.futures.user_stream import FuturesUserStreamMixin

from test.test_ws_api import WSAPIServer


# Port dedicated to CM user-stream tests (must not conflict with other tests).
_DAPI_PORT = 9097


def _make_cm_client_for_lifecycle(server) -> CMFuturesClient:
    """Create a CMFuturesClient pointing at the local mock ws-dapi server."""
    client = CMFuturesClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host=server.uri,
        stream_host='wss://dstream.binance.com',  # real value; Stream is patched
    )
    client._time_synced = True
    return client


class _FakeStream:
    """Minimal Stream stub: records connections and supports send()/close()."""

    connected_uris = []
    _all_streams = []

    @classmethod
    def reset(cls):
        cls.connected_uris = []
        cls._all_streams = []

    def __init__(self, uri, *, on_message=None, **kwargs):
        self.uri = uri
        self._on_message = on_message
        self.closed = False
        _FakeStream.connected_uris.append(uri)
        _FakeStream._all_streams.append(self)

    def connect(self):
        return self

    async def send(self, req):
        return None

    async def close(self, code=4999):
        self.closed = True


@pytest.fixture(autouse=False)
def patch_dstream(monkeypatch):
    """Monkeypatch Stream in binance.futures.user_stream to use _FakeStream."""
    _FakeStream.reset()
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)
    return _FakeStream


# ---------------------------------------------------------------------------
# FuturesUserStreamMixin inheritance
# ---------------------------------------------------------------------------

def test_cm_client_inherits_futures_user_stream_mixin():
    """CMFuturesClient must be a subclass of FuturesUserStreamMixin."""
    assert issubclass(CMFuturesClient, FuturesUserStreamMixin)


# ---------------------------------------------------------------------------
# FuturesUserProcessor in CM PROCESSORS
# ---------------------------------------------------------------------------

def test_cm_processors_includes_futures_user_processor():
    """CM PROCESSORS list must include FuturesUserProcessor."""
    from binance.futures.cm.streams import PROCESSORS
    from binance.futures.user_processor import FuturesUserProcessor

    assert FuturesUserProcessor in PROCESSORS


# ---------------------------------------------------------------------------
# subscribe(SubType.USER) → userDataStream.start on ws-dapi
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_subscribe_user_calls_userDataStream_start(patch_dstream):
    """subscribe(SubType.USER) calls userDataStream.start over ws-dapi (USER_STREAM, weight 1)."""
    listen_key = 'cm-test-listen-key-abc123'
    server = WSAPIServer(port=_DAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    await server.run()
    try:
        client = _make_cm_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # Verify userDataStream.start was sent over ws-api.
        methods = [r['method'] for r in server.received]
        assert 'userDataStream.start' in methods

        # USER_STREAM security: apiKey only, no signature.
        start_req = next(r for r in server.received if r['method'] == 'userDataStream.start')
        assert start_req.get('params', {}).get('apiKey') == 'TESTAPIKEY'
        assert 'signature' not in start_req.get('params', {})
        assert 'timestamp' not in start_req.get('params', {})

        # The listenKey was stored on the client.
        assert client._futures_listen_key == listen_key
        assert client._want_user_stream is True

        # A keepalive task was started.
        assert client._futures_keepalive_task is not None
        assert not client._futures_keepalive_task.done()
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Dedicated stream opened at dstream.binance.com/ws/<listenKey>
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_subscribe_user_opens_dedicated_stream_at_dstream_uri(patch_dstream):
    """subscribe(SubType.USER) opens a dedicated Stream to dstream.binance.com/ws/<listenKey>."""
    listen_key = 'cm-futures-listen-key-xyz'
    server = WSAPIServer(port=_DAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    await server.run()
    try:
        client = _make_cm_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # The dstream connection was opened at the COIN-M correct URI.
        expected_uri = 'wss://dstream.binance.com/ws/' + listen_key
        assert expected_uri in _FakeStream.connected_uris
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# close() calls userDataStream.stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_close_calls_userDataStream_stop(patch_dstream):
    """close() calls userDataStream.stop with the listenKey."""
    listen_key = 'cm-close-test-listen-key'
    server = WSAPIServer(port=_DAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_cm_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # Cancel the keepalive before close so the test is fast.
        client._cancel_futures_keepalive()

        await client.close()

        methods = [r['method'] for r in server.received]
        assert 'userDataStream.stop' in methods

        # userDataStream.stop carries the listenKey param.
        stop_req = next(r for r in server.received if r['method'] == 'userDataStream.stop')
        assert stop_req.get('params', {}).get('listenKey') == listen_key

        # USER_STREAM security: apiKey only, no signature.
        assert stop_req.get('params', {}).get('apiKey') == 'TESTAPIKEY'
        assert 'signature' not in stop_req.get('params', {})

        # The dedicated stream was closed.
        dstream = _FakeStream._all_streams[-1]
        assert dstream.closed is True

        # listen_key was cleared.
        assert client._futures_listen_key is None
    finally:
        await server.shutdown()


# ---------------------------------------------------------------------------
# keepalive calls userDataStream.ping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_keepalive_calls_userDataStream_ping(monkeypatch, patch_dstream):
    """The keepalive loop sends userDataStream.ping with the listenKey."""
    listen_key = 'cm-ping-test-listen-key'
    server = WSAPIServer(port=_DAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.ping', result=None)
    await server.run()
    try:
        monkeypatch.setattr('binance.futures.user_stream._KEEPALIVE_INTERVAL', 0)

        client = _make_cm_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # Let the keepalive loop run at least one iteration.
        await asyncio.sleep(0.05)

        methods = [r['method'] for r in server.received]
        assert 'userDataStream.ping' in methods

        ping_req = next(r for r in server.received if r['method'] == 'userDataStream.ping')
        assert ping_req.get('params', {}).get('listenKey') == listen_key

        # USER_STREAM security: apiKey only, no signature.
        assert ping_req.get('params', {}).get('apiKey') == 'TESTAPIKEY'
        assert 'signature' not in ping_req.get('params', {})
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Events routed correctly via FuturesUserProcessor on CM client
# ---------------------------------------------------------------------------

ACCOUNT_UPDATE_PAYLOAD = {
    'e': 'ACCOUNT_UPDATE',
    'E': 1564745798939,
    'T': 1564745798938,
    'a': {
        'm': 'ORDER',
        'B': [{'a': 'BTC', 'wb': '1.00000000', 'cw': '0.50000000', 'bc': '0.01'}],
        'P': [{'s': 'BTCUSD_PERP', 'pa': '1', 'ep': '30000', 'bep': '30000',
               'cr': '0', 'up': '0', 'mt': 'isolated', 'iw': '0.01', 'ps': 'BOTH'}],
    },
}

ORDER_TRADE_UPDATE_PAYLOAD = {
    'e': 'ORDER_TRADE_UPDATE',
    'E': 1568879465651,
    'T': 1568879465650,
    'o': {
        's': 'BTCUSD_PERP',
        'c': 'abc123',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'GTC',
        'q': '1',
        'p': '30000',
        'ap': '30000',
        'sp': '0',
        'x': 'TRADE',
        'X': 'FILLED',
        'i': 8886774,
        'l': '1',
        'z': '1',
        'L': '30000',
        'N': 'BTC',
        'n': '0.00001',
        'T': 1568879465651,
        't': 1,
        'b': '0',
        'a': '0',
        'm': False,
        'R': False,
        'wt': 'CONTRACT_PRICE',
        'ot': 'LIMIT',
        'ps': 'BOTH',
        'cp': False,
        'rp': '0',
        'pP': False,
        'V': 'NONE',
        'pm': 'NONE',
        'gtd': 0,
    },
}


@pytest.fixture
def cm_client():
    return CMFuturesClient(Credentials('api_key', 'api_secret')).start()


async def _run_cm_futures_handler(client, HandlerBase, payload, envelope='data'):
    """Drive a payload through client._receive and return what the handler sees."""
    from binance.core.common.utils import create_future

    future = create_future()

    class Handler(HandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.start()
    client.handler(Handler())

    if envelope == 'event':
        msg = {'subscriptionId': 0, 'event': payload}
    else:
        msg = {'data': payload, 'stream': 'fake'}

    await client._receive(msg)
    return await future


@pytest.mark.asyncio
async def test_cm_account_update_routed_to_handler(cm_client):
    """ACCOUNT_UPDATE routes to FuturesAccountUpdateHandlerBase on CM client."""
    received = await _run_cm_futures_handler(
        cm_client, FuturesAccountUpdateHandlerBase, ACCOUNT_UPDATE_PAYLOAD
    )
    assert received['e'] == 'ACCOUNT_UPDATE'
    assert received['a']['m'] == 'ORDER'


@pytest.mark.asyncio
async def test_cm_order_trade_update_routed_to_handler(cm_client):
    """ORDER_TRADE_UPDATE routes to FuturesOrderUpdateHandlerBase on CM client."""
    received = await _run_cm_futures_handler(
        cm_client, FuturesOrderUpdateHandlerBase, ORDER_TRADE_UPDATE_PAYLOAD
    )
    assert received['e'] == 'ORDER_TRADE_UPDATE'
    assert received['o']['s'] == 'BTCUSD_PERP'


# ---------------------------------------------------------------------------
# No Spot subscribe method sent (correct futures lifecycle)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_no_spot_subscribe_method_sent(patch_dstream):
    """subscribe(SubType.USER) must NOT send userDataStream.subscribe.signature (Spot-only)."""
    server = WSAPIServer(port=_DAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': 'cm-correct-key'})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_cm_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()
        await client.unsubscribe(SubType.USER)
        await client.close()

        methods = [r['method'] for r in server.received]
        assert 'userDataStream.subscribe.signature' not in methods
        assert 'userDataStream.unsubscribe' not in methods
    finally:
        await server.shutdown()
