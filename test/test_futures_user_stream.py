"""Tests for the USDⓈ-M Futures user-data-stream handlers, processor, and mixin.

Covers:
- Each event type (ACCOUNT_UPDATE, ORDER_TRADE_UPDATE, MARGIN_CALL,
  ACCOUNT_CONFIG_UPDATE, listenKeyExpired, eventStreamTerminated) is routed to
  the correct handler base via client._receive().
- FuturesUserProcessor routes by 'e' key (event type) within both WS-API
  'event' envelope and data-stream 'data' envelope.
- subscribe_param tracks subscribed state (returns {} — listenKey flow handled by mixin).
- Layering: all handler bases are importable from the top-level 'binance'
  package.
- The CORRECT futures user-stream lifecycle:
    - subscribe(SubType.USER) calls userDataStream.start (USER_STREAM security,
      weight 1) over the ws-fapi connection, then opens a dedicated Stream to
      stream_host/ws/<listenKey>.
    - The keepalive task calls userDataStream.ping every ~50 min.
    - close() calls userDataStream.stop and closes the dedicated stream.
    - listenKeyExpired event triggers recreation of the listenKey + stream.

SAFETY: MOCK-only — no live API calls.
"""

import asyncio
import json
import pytest

from aiohttp import web

from binance import (
    UMFuturesClient,
    Credentials,
    SubType,
    # Public handler bases importable from binance root:
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesEventStreamTerminatedHandlerBase,
)
from binance.core.common.utils import create_future

# Re-use the WSAPIServer from test_ws_api (registered under /ws-api/v3 path,
# but the path is overridden per test via _PORT_UM).
from test.test_ws_api import WSAPIServer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return UMFuturesClient(Credentials('api_key', 'api_secret')).start()


# ---------------------------------------------------------------------------
# Canonical payloads (confirmed from Binance USDⓈ-M docs, 2026-05-25)
# ---------------------------------------------------------------------------

ACCOUNT_UPDATE_PAYLOAD = {
    'e': 'ACCOUNT_UPDATE',
    'E': 1564745798939,
    'T': 1564745798938,
    'a': {
        'm': 'ORDER',
        'B': [
            {
                'a': 'USDT',
                'wb': '122624.12345678',
                'cw': '100.12345678',
                'bc': '50.12345678',
            }
        ],
        'P': [
            {
                's': 'BTCUSDT',
                'pa': '0',
                'ep': '0.00000',
                'bep': '0',
                'cr': '200',
                'up': '0',
                'mt': 'isolated',
                'iw': '0.00000000',
                'ps': 'BOTH',
            }
        ],
    },
}

ORDER_TRADE_UPDATE_PAYLOAD = {
    'e': 'ORDER_TRADE_UPDATE',
    'E': 1568879465651,
    'T': 1568879465650,
    'o': {
        's': 'BTCUSDT',
        'c': 'abc123',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'GTC',
        'q': '0.001',
        'p': '9910',
        'ap': '9910',
        'sp': '0',
        'x': 'TRADE',
        'X': 'FILLED',
        'i': 8886774,
        'l': '0.001',
        'z': '0.001',
        'L': '9910',
        'N': 'BNB',
        'n': '0.01',
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

MARGIN_CALL_PAYLOAD = {
    'e': 'MARGIN_CALL',
    'E': 1587727187525,
    'cw': '3.16812045',
    'p': [
        {
            's': 'ETHUSDT',
            'ps': 'LONG',
            'pa': '1.327',
            'mt': 'CROSSED',
            'iw': '0',
            'mp': '187.17127',
            'up': '-1.166074',
            'mm': '1.614445',
        }
    ],
}

ACCOUNT_CONFIG_UPDATE_LEVERAGE_PAYLOAD = {
    'e': 'ACCOUNT_CONFIG_UPDATE',
    'E': 1611646737479,
    'T': 1611646737476,
    'ac': {
        's': 'BTCUSDT',
        'l': 25,
    },
}

ACCOUNT_CONFIG_UPDATE_MULTIASSETS_PAYLOAD = {
    'e': 'ACCOUNT_CONFIG_UPDATE',
    'E': 1611646737479,
    'T': 1611646737476,
    'ai': {
        'j': True,
    },
}

LISTEN_KEY_EXPIRED_PAYLOAD = {
    'e': 'listenKeyExpired',
    'E': 1736996475556,
    'listenKey': 'WsCMN0a4KHUPTQuX6IUnqEZfB1inxmv1qR4kbf1Luabcd',
}

EVENT_STREAM_TERMINATED_PAYLOAD = {
    'e': 'eventStreamTerminated',
    'E': 1700000000000,
}


# ---------------------------------------------------------------------------
# Helper: drive a payload through client._receive and capture what the handler
# receives.  Mirrors test_handlers.py `run_handler` but for futures clients.
# ---------------------------------------------------------------------------

async def run_futures_handler(client, HandlerBase, payload, envelope='event'):
    """Drive *payload* through client._receive and return what the handler sees.

    Args:
        client: a started UMFuturesClient
        HandlerBase: the handler base class to instantiate
        payload: the inner event dict (with 'e' key)
        envelope: 'event' wraps in {'event': payload} (WS-API form);
                  'data' wraps in {'data': payload, 'stream': 'fake'}
    """
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


# ---------------------------------------------------------------------------
# Routing tests: each event type → correct handler
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_account_update_routed_ws_api_envelope(client):
    """ACCOUNT_UPDATE in WS-API 'event' envelope routes to FuturesAccountUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountUpdateHandlerBase, ACCOUNT_UPDATE_PAYLOAD, envelope='event'
    )
    assert received['e'] == 'ACCOUNT_UPDATE'
    assert received['a']['m'] == 'ORDER'
    assert received['a']['B'][0]['a'] == 'USDT'
    assert received['a']['P'][0]['s'] == 'BTCUSDT'


@pytest.mark.asyncio
async def test_account_update_routed_data_envelope(client):
    """ACCOUNT_UPDATE in data-stream 'data' envelope routes to FuturesAccountUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountUpdateHandlerBase, ACCOUNT_UPDATE_PAYLOAD, envelope='data'
    )
    assert received['e'] == 'ACCOUNT_UPDATE'
    assert received['a']['m'] == 'ORDER'


@pytest.mark.asyncio
async def test_order_trade_update_routed(client):
    """ORDER_TRADE_UPDATE routes to FuturesOrderUpdateHandlerBase; raw dict returned."""
    received = await run_futures_handler(
        client, FuturesOrderUpdateHandlerBase, ORDER_TRADE_UPDATE_PAYLOAD
    )
    assert received['e'] == 'ORDER_TRADE_UPDATE'
    assert received['o']['s'] == 'BTCUSDT'
    assert received['o']['S'] == 'SELL'
    assert received['o']['X'] == 'FILLED'


@pytest.mark.asyncio
async def test_margin_call_routed(client):
    """MARGIN_CALL routes to FuturesMarginCallHandlerBase."""
    received = await run_futures_handler(
        client, FuturesMarginCallHandlerBase, MARGIN_CALL_PAYLOAD
    )
    assert received['e'] == 'MARGIN_CALL'
    assert received['p'][0]['s'] == 'ETHUSDT'
    assert received['p'][0]['mt'] == 'CROSSED'


@pytest.mark.asyncio
async def test_account_config_update_leverage_routed(client):
    """ACCOUNT_CONFIG_UPDATE (leverage variant) routes to FuturesAccountConfigUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountConfigUpdateHandlerBase,
        ACCOUNT_CONFIG_UPDATE_LEVERAGE_PAYLOAD
    )
    assert received['e'] == 'ACCOUNT_CONFIG_UPDATE'
    assert received['ac']['s'] == 'BTCUSDT'
    assert received['ac']['l'] == 25


@pytest.mark.asyncio
async def test_account_config_update_multiassets_routed(client):
    """ACCOUNT_CONFIG_UPDATE (multi-assets variant) routes to FuturesAccountConfigUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountConfigUpdateHandlerBase,
        ACCOUNT_CONFIG_UPDATE_MULTIASSETS_PAYLOAD
    )
    assert received['e'] == 'ACCOUNT_CONFIG_UPDATE'
    assert received['ai']['j'] is True


@pytest.mark.asyncio
async def test_listen_key_expired_routed(client):
    """listenKeyExpired routes to FuturesListenKeyExpiredHandlerBase."""
    received = await run_futures_handler(
        client, FuturesListenKeyExpiredHandlerBase, LISTEN_KEY_EXPIRED_PAYLOAD
    )
    assert received['e'] == 'listenKeyExpired'
    assert 'listenKey' in received


@pytest.mark.asyncio
async def test_event_stream_terminated_routed(client):
    """eventStreamTerminated routes to FuturesEventStreamTerminatedHandlerBase."""
    received = await run_futures_handler(
        client, FuturesEventStreamTerminatedHandlerBase, EVENT_STREAM_TERMINATED_PAYLOAD
    )
    assert received['e'] == 'eventStreamTerminated'


# ---------------------------------------------------------------------------
# Unrelated payloads are NOT delivered to futures user handlers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unrelated_payload_not_delivered(client):
    """A non-user-stream payload must not reach a futures user handler."""
    delivered = []

    class Handler(FuturesAccountUpdateHandlerBase):
        def receive(self, p):
            delivered.append(p)

    client.start()
    client.handler(Handler())

    # A spot ticker payload — not a futures user event.
    await client._receive({'data': {'e': '24hrTicker', 's': 'BTCUSDT'}})

    assert delivered == []


# ---------------------------------------------------------------------------
# subscribe_param contract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_futures_user_processor_subscribe_param_returns_empty_dict(client):
    """subscribe_param(True, ...) returns {} (listenKey flow is handled by the mixin)."""
    from binance.futures.user_processor import FuturesUserProcessor

    proc = FuturesUserProcessor(client)
    params = proc.subscribe_param(True, SubType.USER)

    assert params == {}
    assert proc._subscribed is True


@pytest.mark.asyncio
async def test_futures_user_processor_unsubscribe_param_returns_empty(client):
    """subscribe_param(False, ...) after subscribe returns {}."""
    from binance.futures.user_processor import FuturesUserProcessor

    proc = FuturesUserProcessor(client)
    proc.subscribe_param(True, SubType.USER)  # subscribe first
    params = proc.subscribe_param(False, SubType.USER)

    assert params == {}
    assert proc._subscribed is False


@pytest.mark.asyncio
async def test_futures_user_processor_unsubscribe_before_subscribe_raises(client):
    """subscribe_param(False, ...) without prior subscribe raises UserStreamNotSubscribedException."""
    from binance.futures.user_processor import FuturesUserProcessor
    from binance import UserStreamNotSubscribedException

    proc = FuturesUserProcessor(client)

    with pytest.raises(UserStreamNotSubscribedException):
        proc.subscribe_param(False, SubType.USER)


# ---------------------------------------------------------------------------
# CORRECT futures user-stream lifecycle tests (listenKey flow)
# ---------------------------------------------------------------------------

# Port for the ws-fapi mock server used in user-stream lifecycle tests.
# 9094 is used only by this file; other ports (9085-9093) are taken by other tests.
_FAPI_PORT = 9094


def _make_um_client_for_lifecycle(server) -> UMFuturesClient:
    """Create a UMFuturesClient pointing at the local mock ws-api server."""
    # stream_host must be set to a non-connectable URL for tests that monkeypatch
    # binance.futures.user_stream.Stream; when Stream is patched, the URI is
    # never actually connected so any value works.
    client = UMFuturesClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host=server.uri,
        stream_host='wss://fstream.binance.com',  # real value; Stream is patched
    )
    # Mark time as synced: USER_STREAM requests don't sign so no time sync
    # is needed, but marking it avoids an extra 'time' request on the first
    # USER_DATA/TRADE call if tests are chained.
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
def patch_fstream(monkeypatch):
    """Monkeypatch Stream in binance.futures.user_stream to use _FakeStream."""
    _FakeStream.reset()
    monkeypatch.setattr('binance.futures.user_stream.Stream', _FakeStream)
    return _FakeStream


@pytest.mark.asyncio
async def test_subscribe_user_calls_userDataStream_start(patch_fstream):
    """subscribe(SubType.USER) calls userDataStream.start over ws-fapi (USER_STREAM, weight 1)."""
    listen_key = 'test-listen-key-abc123'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
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
        # Cancel the keepalive before closing to prevent asyncio warnings.
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_subscribe_user_opens_dedicated_stream_at_correct_uri(patch_fstream):
    """subscribe(SubType.USER) opens a dedicated Stream to stream_host/ws/<listenKey>."""
    listen_key = 'my-futures-listen-key-xyz'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # The fstream was opened at the correct URI.
        expected_uri = 'wss://fstream.binance.com/ws/' + listen_key
        assert expected_uri in _FakeStream.connected_uris
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_close_calls_userDataStream_stop(patch_fstream):
    """close() calls userDataStream.stop with the listenKey and closes the dedicated stream."""
    listen_key = 'close-test-listen-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
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
        fstream = _FakeStream._all_streams[-1]
        assert fstream.closed is True

        # listen_key was cleared.
        assert client._futures_listen_key is None
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_unsubscribe_user_calls_stop_and_clears_state(patch_fstream):
    """unsubscribe(SubType.USER) stops the listenKey flow and clears state."""
    listen_key = 'unsub-test-listen-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        client._cancel_futures_keepalive()

        await client.unsubscribe(SubType.USER)

        methods = [r['method'] for r in server.received]
        assert 'userDataStream.stop' in methods
        assert client._want_user_stream is False
        assert client._futures_listen_key is None
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_keepalive_calls_userDataStream_ping(monkeypatch, patch_fstream):
    """The keepalive loop sends userDataStream.ping with the listenKey."""
    listen_key = 'ping-test-listen-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.ping', result=None)
    await server.run()
    try:
        # Shorten the keepalive interval to 0 for the test.
        monkeypatch.setattr('binance.futures.user_stream._KEEPALIVE_INTERVAL', 0)

        client = _make_um_client_for_lifecycle(server)
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


@pytest.mark.asyncio
async def test_events_delivered_on_futures_user_stream(patch_fstream):
    """Events arriving on the dedicated fstream are dispatched to the correct handler."""
    # The fstream calls client._receive; simulate that directly.
    listen_key = 'events-test-listen-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        client.start()

        received_payloads = []

        class Handler(FuturesAccountUpdateHandlerBase):
            def receive(self, p):
                received_payloads.append(p)

        client.handler(Handler())

        await client.subscribe(SubType.USER)

        # Simulate an event arriving on the fstream via client._receive.
        await client._receive({'data': ACCOUNT_UPDATE_PAYLOAD})

        assert len(received_payloads) == 1
        assert received_payloads[0]['e'] == 'ACCOUNT_UPDATE'
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_listen_key_expired_triggers_stream_restart(monkeypatch, patch_fstream):
    """listenKeyExpired event triggers a new userDataStream.start and opens a new stream."""
    old_key = 'old-listen-key-111'
    new_key = 'new-listen-key-222'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.stop', result=None)

    # First call returns old_key; subsequent calls return new_key.
    call_count = {'n': 0}

    async def _patched_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for raw in ws:
            if raw.type != web.WSMsgType.TEXT:
                continue
            msg = json.loads(raw.data)
            server.received.append(msg)
            mid = msg.get('id')
            method = msg.get('method')
            if method == 'userDataStream.start':
                call_count['n'] += 1
                k = old_key if call_count['n'] == 1 else new_key
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': {'listenKey': k}}))
            elif method == 'userDataStream.stop':
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': None}))
            elif method == 'time':
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': {'serverTime': 1_700_000_000_000}}))
            else:
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': None}))
        return ws

    # Replace the server handler.
    app = web.Application()
    app.add_routes([web.get('/ws-api/v3', _patched_handler)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', _FAPI_PORT)
    await site.start()

    try:
        uri = f'ws://localhost:{_FAPI_PORT}/ws-api/v3'
        client = UMFuturesClient(
            Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
            ws_api_host=uri,
            stream_host='wss://fstream.binance.com',
        )
        client._time_synced = True
        client.start()
        await client.subscribe(SubType.USER)

        assert client._futures_listen_key == old_key

        # Simulate listenKeyExpired arriving on the dedicated stream.
        await client._receive({'data': LISTEN_KEY_EXPIRED_PAYLOAD})

        # Allow the background task spawned by _receive to complete.
        await asyncio.sleep(0.1)

        # The client should have re-obtained a new listen key.
        assert client._futures_listen_key == new_key
        assert 'wss://fstream.binance.com/ws/' + new_key in _FakeStream.connected_uris

    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_resubscribe_user_is_noop(patch_fstream):
    """_resubscribe_user() is a no-op for futures (dedicated stream reconnects itself)."""
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': 'noop-test-key'})
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        before_received = len(server.received)

        # _resubscribe_user is called by _on_ws_api_connected after reconnect.
        await client._resubscribe_user()

        # No extra ws-api requests should have been sent.
        assert len(server.received) == before_received
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_no_spot_subscribe_method_sent(patch_fstream):
    """subscribe(SubType.USER) must NOT send userDataStream.subscribe.signature (Spot-only method)."""
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': 'correct-key'})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()
        await client.unsubscribe(SubType.USER)
        await client.close()

        methods = [r['method'] for r in server.received]
        assert 'userDataStream.subscribe.signature' not in methods
        assert 'userDataStream.unsubscribe' not in methods
    finally:
        await server.shutdown()


# ---------------------------------------------------------------------------
# Public API: all handler bases importable from top-level 'binance' package
# ---------------------------------------------------------------------------

def test_handler_bases_importable_from_binance():
    """All futures user handler bases are importable from the binance root package."""
    import binance

    assert hasattr(binance, 'FuturesAccountUpdateHandlerBase')
    assert hasattr(binance, 'FuturesOrderUpdateHandlerBase')
    assert hasattr(binance, 'FuturesMarginCallHandlerBase')
    assert hasattr(binance, 'FuturesAccountConfigUpdateHandlerBase')
    assert hasattr(binance, 'FuturesListenKeyExpiredHandlerBase')
    assert hasattr(binance, 'FuturesEventStreamTerminatedHandlerBase')


# ---------------------------------------------------------------------------
# ACCOUNT_CONFIG_UPDATE docstring must clarify that ``ai`` (multi-assets-mode
# change) is delivered ONLY on USDⓈ-M.  CM has no multi-assets margin mode
# (only USDⓈ-M supports the multi-assets account), so CM payloads NEVER
# carry the ``ai`` variant.
#
# UM docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Account-Configuration-Update-previous-Leverage-Update
# CM docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/user-data-streams/Event-Account-Configuration-Update
# ---------------------------------------------------------------------------


def test_account_config_update_docstring_marks_ai_as_um_only():
    """FuturesAccountConfigUpdateHandlerBase docstring must mark the ``ai`` variant as USDⓈ-M-only."""
    doc = FuturesAccountConfigUpdateHandlerBase.__doc__ or ''
    # ``ai`` (multi-assets-mode change) is documented as USDⓈ-M-only.
    assert 'UM-only' in doc or 'USDⓈ-M-only' in doc or 'UM only' in doc or 'USDⓈ-M only' in doc
    # The docstring must explicitly mention CM has no multi-assets mode.
    assert 'multi-assets' in doc.lower()
    assert 'CM' in doc


# ---------------------------------------------------------------------------
# ``eventStreamTerminated`` is SERVER-PUSHED by Binance on the ws-fapi
# connection (mirrors the spot WS-API behaviour). The dedicated futures
# user-data fstream uses ``listenKeyExpired`` instead for listenKey
# invalidation — that is the SDK's recovery trigger, NOT a synthesized
# eventStreamTerminated. The handler docstring must reflect this.
#
# Source: https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
# Futures listenKey flow: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams
# ---------------------------------------------------------------------------


def test_futures_event_stream_terminated_docstring_marks_event_as_server_pushed():
    """FuturesEventStreamTerminatedHandlerBase docstring must mark the event as server-pushed on the ws-fapi connection, NOT SDK-synthesized."""
    doc = FuturesEventStreamTerminatedHandlerBase.__doc__ or ''
    # Stale wording must be gone (the SDK does not synthesize this event
    # anywhere in source — it is always pushed by the Binance WS-API).
    assert 'synthesized by the SDK' not in doc
    assert 'SDK-synthesized' not in doc
    # The docstring must say it is server-pushed by Binance.
    assert 'server-pushed' in doc or 'pushed by Binance' in doc
    # It must also point at the listenKeyExpired recovery channel for the
    # dedicated futures user-data fstream so users do not confuse the two.
    assert 'listenKeyExpired' in doc


def test_handler_bases_are_handler_subclasses():
    """All futures user handler bases are subclasses of core Handler."""
    from binance.core.handlers.base import Handler

    assert issubclass(FuturesAccountUpdateHandlerBase, Handler)
    assert issubclass(FuturesOrderUpdateHandlerBase, Handler)
    assert issubclass(FuturesMarginCallHandlerBase, Handler)
    assert issubclass(FuturesAccountConfigUpdateHandlerBase, Handler)
    assert issubclass(FuturesListenKeyExpiredHandlerBase, Handler)
    assert issubclass(FuturesEventStreamTerminatedHandlerBase, Handler)


# ---------------------------------------------------------------------------
# Layering: futures handlers must NOT import from binance.spot
# ---------------------------------------------------------------------------

def test_futures_user_handlers_no_spot_import():
    """futures/user_handlers.py must not import from binance.spot."""
    import importlib
    import sys

    # Reload the module fresh to check its actual imports
    mod_name = 'binance.futures.user_handlers'
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    mod = importlib.import_module(mod_name)

    # binance.spot may already be loaded via other imports; what matters is
    # that user_handlers itself does not depend on spot.
    assert not any('spot' in str(dep) for dep in getattr(mod, '__dict__', {}).values()
                   if hasattr(dep, '__module__') and dep.__module__ is not None
                   and 'spot' in dep.__module__)


def test_futures_user_processor_no_spot_import():
    """futures/user_processor.py must not import from binance.spot."""
    import importlib
    import sys

    mod_name = 'binance.futures.user_processor'
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    mod = importlib.import_module(mod_name)

    # No binance.spot dependency in the module's own namespace
    assert not any(
        hasattr(v, '__module__') and v.__module__ is not None and 'spot' in v.__module__
        for v in mod.__dict__.values()
    )


# ---------------------------------------------------------------------------
# FuturesUserProcessor.is_message_type correctness
# ---------------------------------------------------------------------------

def test_is_message_type_event_envelope():
    """is_message_type matches WS-API 'event' envelope for ACCOUNT_UPDATE."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'subscriptionId': 0, 'event': {'e': 'ACCOUNT_UPDATE', 'T': 1}}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'ACCOUNT_UPDATE'


def test_is_message_type_data_envelope():
    """is_message_type matches data-stream 'data' envelope for ORDER_TRADE_UPDATE."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'ORDER_TRADE_UPDATE', 'T': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'ORDER_TRADE_UPDATE'


def test_is_message_type_no_match():
    """is_message_type returns (False, None) for non-user-stream payloads."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'markPriceUpdate', 's': 'BTCUSDT'}}
    matched, payload = proc.is_message_type(msg)

    assert matched is False
    assert payload is None


# ---------------------------------------------------------------------------
# Coverage: edge-case paths in FuturesUserStreamMixin
# ---------------------------------------------------------------------------

def test_extract_event_type_non_dict_returns_none():
    """_extract_event_type returns None for non-dict messages (line 49)."""
    from binance.futures.user_stream import _extract_event_type
    assert _extract_event_type('not-a-dict') is None
    assert _extract_event_type(None) is None
    assert _extract_event_type(42) is None


def test_extract_event_type_top_level_e():
    """_extract_event_type returns msg['e'] when not nested in data/event (line 54)."""
    from binance.futures.user_stream import _extract_event_type
    assert _extract_event_type({'e': 'ACCOUNT_UPDATE', 'E': 1}) == 'ACCOUNT_UPDATE'


@pytest.mark.asyncio
async def test_listen_key_expired_when_not_want_user_stream(patch_fstream):
    """_on_futures_listen_key_expired is a no-op when _want_user_stream is False (line 183)."""
    client = UMFuturesClient(Credentials('key', 'secret'))
    client._want_user_stream = False
    # Should return immediately without any side effects.
    await client._on_futures_listen_key_expired()
    assert client._futures_listen_key is None


@pytest.mark.asyncio
async def test_listen_key_expired_old_stream_close_error_is_logged(patch_fstream):
    """If the old stream.close() raises in _on_futures_listen_key_expired, the error is logged (lines 197-198)."""
    listen_key = 'error-close-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()

        # Replace the dedicated stream with one whose close() raises.
        class BrokenStream:
            async def close(self, code=4999):
                raise RuntimeError('close failed')

        client._futures_user_stream = BrokenStream()
        client._futures_listen_key = listen_key
        client._want_user_stream = True

        # Should not raise; error is logged.
        await client._on_futures_listen_key_expired()

        # A new stream was opened after the error.
        assert client._futures_listen_key is not None
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_listen_key_expired_start_failure_dispatches_stream_error(patch_fstream):
    """If _futures_user_stream_start fails in _on_futures_listen_key_expired, the error is dispatched (lines 205-216)."""
    listen_key = 'start-fail-key'
    server = WSAPIServer(port=_FAPI_PORT)
    # First call succeeds; second call (after listenKeyExpired) fails.
    call_count = {'n': 0}

    async def _patched_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for raw in ws:
            if raw.type != web.WSMsgType.TEXT:
                continue
            msg = json.loads(raw.data)
            server.received.append(msg)
            mid = msg.get('id')
            method = msg.get('method')
            if method == 'userDataStream.start':
                call_count['n'] += 1
                if call_count['n'] == 1:
                    await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': {'listenKey': listen_key}}))
                else:
                    await ws.send_str(json.dumps({'id': mid, 'status': 400, 'error': {'code': -1100, 'msg': 'fail'}}))
            elif method == 'time':
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': {'serverTime': 1_700_000_000_000}}))
            else:
                await ws.send_str(json.dumps({'id': mid, 'status': 200, 'result': None}))
        return ws

    app = web.Application()
    app.add_routes([web.get('/ws-api/v3', _patched_handler)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', _FAPI_PORT)
    await site.start()

    try:
        uri = f'ws://localhost:{_FAPI_PORT}/ws-api/v3'
        client = UMFuturesClient(
            Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
            ws_api_host=uri,
            stream_host='wss://fstream.binance.com',
        )
        client._time_synced = True
        client.start()

        errors_dispatched = []
        from binance.core.handlers.framework import StreamErrorHandlerBase
        class ErrHandler(StreamErrorHandlerBase):
            def receive(self, err):
                errors_dispatched.append(err)
        client.handler(ErrHandler())

        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()

        # Simulate listenKeyExpired; the restart will fail.
        await client._receive({'data': LISTEN_KEY_EXPIRED_PAYLOAD})
        await asyncio.sleep(0.1)

        # A stream error was dispatched.
        assert len(errors_dispatched) > 0
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_futures_cleanup_stop_error_is_logged(patch_fstream):
    """If userDataStream.stop raises in _futures_cleanup, the error is logged (lines 268-269)."""
    listen_key = 'stop-error-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on_error('userDataStream.stop', code=-1000)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()

        # Should not raise; error is logged.
        await client._futures_cleanup()

        assert client._futures_listen_key is None
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_futures_cleanup_stream_close_error_is_logged(patch_fstream):
    """If the dedicated stream close() raises in _futures_cleanup, the error is logged (lines 277-278)."""
    listen_key = 'close-error-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on('userDataStream.stop', result=None)
    await server.run()
    try:
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)
        client._cancel_futures_keepalive()

        # Replace the dedicated stream with one whose close() raises.
        class BrokenStream:
            async def close(self, code=4999):
                raise RuntimeError('close error')

        client._futures_user_stream = BrokenStream()

        # Should not raise; error is logged.
        await client._futures_cleanup()

        assert client._futures_user_stream is None
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_keepalive_returns_when_listen_key_cleared(monkeypatch, patch_fstream):
    """The keepalive loop returns early when _futures_listen_key is None (line 295)."""
    listen_key = 'early-return-key'
    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    await server.run()
    try:
        monkeypatch.setattr('binance.futures.user_stream._KEEPALIVE_INTERVAL', 0)
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # Clear the listen key so the keepalive exits on next iteration.
        client._futures_listen_key = None

        # Allow the keepalive to run its next iteration.
        await asyncio.sleep(0.05)

        # Keepalive task should have exited.
        task = client._futures_keepalive_task
        if task is not None:
            await asyncio.sleep(0.05)
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_keepalive_ping_error_is_logged_not_raised(monkeypatch, patch_fstream):
    """A ping failure in the keepalive loop is logged but the loop continues (lines 306-307)."""
    listen_key = 'ping-error-key'

    server = WSAPIServer(port=_FAPI_PORT)
    server.on('userDataStream.start', result={'listenKey': listen_key})
    server.on_error('userDataStream.ping', code=-1000)
    await server.run()
    try:
        monkeypatch.setattr('binance.futures.user_stream._KEEPALIVE_INTERVAL', 0)
        client = _make_um_client_for_lifecycle(server)
        await client.subscribe(SubType.USER)

        # Allow the keepalive to run and hit the ping error.
        await asyncio.sleep(0.1)

        # Keepalive should still be running (error was caught, not re-raised).
        assert client._futures_keepalive_task is not None
        assert not client._futures_keepalive_task.done()
    finally:
        client._cancel_futures_keepalive()
        await client.close()
        await server.shutdown()
