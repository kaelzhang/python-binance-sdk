"""End-to-end tests for the WS-API request subsystem (G-03 infra).

These drive a real :class:`binance.subscribe.stream.Stream` against a local
aiohttp WebSocket server that speaks the Binance WS-API request/response
protocol (``{id, method, params}`` -> ``{id, status, result, rateLimits}`` or
``{id, status, error}``). This exercises the full path: ``_ws_api_request`` ->
auth assembly -> rate-limit acquire -> the shared connection -> the real
``_handle_message`` + ``on_response`` reconcile hook.
"""

import asyncio
import json

import pytest
from aiohttp import web

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding, PrivateFormat, NoEncryption,
)

from binance import SpotClient, Credentials
from binance.core.common.constants import SecurityType
from binance.core.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    StreamSubscribeException,
    StreamRateLimitException,
)
from binance.core.rate_limit.types import RateLimitType, RateLimitSource


WS_API_PORT = 9085


def _ed25519_pem() -> str:
    key = Ed25519PrivateKey.generate()
    return key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    ).decode('utf-8')


def _rsa_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    ).decode('utf-8')


class WSAPIServer:
    """A minimal Binance WS-API request/response server for tests.

    Responds to each ``{id, method, params}`` frame with a canned reply keyed
    by ``method``. Records every received request so tests can assert the auth
    fields the client attached (apiKey/signature/timestamp, session.logon).
    """

    def __init__(self, port: int = WS_API_PORT) -> None:
        self._port = port
        self.received = []                 # every received request frame
        # method -> (status, result, rateLimits)
        self._results = {}
        # method -> (code, msg, status, data)
        self._errors = {}
        # default rateLimits array attached to every success unless overridden
        self.default_rate_limits = None

        # The lazy server-time sync (`sync_time`) issues a public WS-API `time`
        # request before the FIRST signed request on a connection. Pre-register
        # a canned reply so every signed-request test works without having to
        # opt in; individual tests may still override it via `.on('time', ...)`.
        self.on('time', result={'serverTime': 1_700_000_000_000})

        app = web.Application()
        app.add_routes([web.get('/ws-api/v3', self._handler)])
        self._runner = web.AppRunner(app)

    def on(self, method, result=None, rate_limits=None):
        self._results[method] = (200, result, rate_limits)
        # `on` and `on_error` are mutually exclusive per method: registering a
        # success clears any prior error so re-presetting works in sequence.
        self._errors.pop(method, None)
        return self

    def on_error(self, method, code, msg='boom', status=400, data=None):
        self._errors[method] = (code, msg, status, data)
        self._results.pop(method, None)
        return self

    @property
    def uri(self) -> str:
        return f'ws://localhost:{self._port}/ws-api/v3'

    async def run(self):
        await self._runner.setup()
        site = web.TCPSite(self._runner, 'localhost', self._port)
        await site.start()

    async def shutdown(self):
        await self._runner.cleanup()

    async def _handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async for raw in ws:
            if raw.type != web.WSMsgType.TEXT:
                continue
            msg = json.loads(raw.data)
            self.received.append(msg)

            mid = msg.get('id')
            method = msg.get('method')

            if method in self._errors:
                code, em, status, data = self._errors[method]
                error = {'code': code, 'msg': em}
                if data is not None:
                    error['data'] = data
                await ws.send_str(json.dumps(
                    {'id': mid, 'status': status, 'error': error}))
                continue

            status, result, rate_limits = self._results.get(
                method, (200, None, None))
            if rate_limits is None:
                rate_limits = self.default_rate_limits
            payload = {'id': mid, 'status': status, 'result': result}
            if rate_limits is not None:
                payload['rateLimits'] = rate_limits
            await ws.send_str(json.dumps(payload))

        return ws


def _make_client(server, **kwargs) -> SpotClient:
    cred_kwargs = {
        k: kwargs.pop(k)
        for k in ('api_key', 'api_secret', 'private_key', 'private_key_pass')
        if k in kwargs
    }
    return SpotClient(Credentials(**cred_kwargs), ws_api_host=server.uri, **kwargs)


# ---------------------------------------------------------------------------
# NONE security: public request, no auth fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_request_none_security_no_auth():
    server = WSAPIServer()
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server)
        result = await client._ws_api_request(
            'depth',
            {'symbol': 'BTCUSDT', 'limit': None},   # None dropped
            security=SecurityType.NONE,
            weight=5,
        )
        assert result == {'lastUpdateId': 1, 'bids': [], 'asks': []}

        sent = server.received[0]
        assert sent['method'] == 'depth'
        assert sent['params'] == {'symbol': 'BTCUSDT'}     # `limit=None` dropped
        # No auth fields on a NONE request.
        assert 'apiKey' not in sent['params']
        assert 'signature' not in sent['params']
        assert 'timestamp' not in sent['params']

        # weight was accounted in the shared core
        snap = client.rate_limit_snapshot()
        assert [w for w in snap.windows
                if w.type == RateLimitType.REQUEST_WEIGHT][0].used == 5
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_request_none_security_empty_params_omits_params_key():
    server = WSAPIServer()
    server.on('ping', result={})
    await server.run()
    try:
        client = _make_client(server)
        result = await client._ws_api_request(
            'ping', None, security=SecurityType.NONE, weight=1)
        assert result == {}
        # No params at all -> the 'params' key is omitted entirely.
        assert 'params' not in server.received[0]
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# SIGNED security with per-request signing (HMAC, no session)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_request_signed_per_request_signing():
    server = WSAPIServer()
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        client._time_synced = True   # isolate from the lazy `time` sync
        result = await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        assert result == {'canTrade': True}

        params = server.received[0]['params']
        # Per-request signing: apiKey + timestamp + signature all present.
        assert params['apiKey'] == 'K'
        assert 'timestamp' in params
        assert isinstance(params['signature'], str) and params['signature']
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_request_trade_is_order_consumes_orders_pool():
    server = WSAPIServer()
    server.on('order.place', result={'orderId': 1})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        await client._ws_api_request(
            'order.place', {'symbol': 'BTCUSDT'},
            security=SecurityType.TRADE, weight=1, is_order=True)
        snap = client.rate_limit_snapshot()
        assert all(w.used == 1 for w in snap.windows if w.type == RateLimitType.ORDERS)
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# USER_STREAM security: apiKey + timestamp + signature
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_request_user_stream_security_signs():
    server = WSAPIServer()
    server.on('userDataStream.subscribe.signature', result=None)
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        await client._ws_api_request(
            'userDataStream.subscribe.signature', None,
            security=SecurityType.USER_STREAM, weight=2)
        params = server.received[0]['params']
        assert params['apiKey'] == 'K'
        assert 'timestamp' in params
        assert isinstance(params['signature'], str) and params['signature']
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# session.logon (Ed25519): later SIGNED requests omit apiKey/signature
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_logon_then_signed_requests_omit_apikey_signature():
    server = WSAPIServer()
    server.on('session.logon', result={'apiKey': 'K', 'authorizedSince': 1})
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='K', private_key=_ed25519_pem())
        # First request opens the connection -> on_connected runs session.logon
        # before the request is served.
        result = await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        assert result == {'canTrade': True}

        # Wait until the session is marked authenticated (logon completed).
        for _ in range(100):
            if client._ws_api_authenticated:
                break
            await asyncio.sleep(0.02)
        assert client._ws_api_authenticated is True

        methods = [m['method'] for m in server.received]
        assert 'session.logon' in methods

        logon = next(m for m in server.received
                     if m['method'] == 'session.logon')
        # session.logon itself is fully signed.
        assert logon['params']['apiKey'] == 'K'
        assert 'signature' in logon['params']

        # A SIGNED request AFTER logon must omit apiKey + signature, keep timestamp.
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        last = server.received[-1]
        assert last['method'] == 'account.status'
        assert 'apiKey' not in last['params']
        assert 'signature' not in last['params']
        assert 'timestamp' in last['params']
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_non_ed25519_key_does_not_logon():
    server = WSAPIServer()
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        # RSA key -> no session.logon; every request is signed per-request.
        client = _make_client(server, api_key='K', private_key=_rsa_pem())
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        # give the connection a moment in case a stray logon were attempted
        await asyncio.sleep(0.1)
        assert client._ws_api_authenticated is False
        methods = [m['method'] for m in server.received]
        assert 'session.logon' not in methods
        params = server.received[-1]['params']
        assert params['apiKey'] == 'K' and 'signature' in params
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# rateLimits reconciliation (the on_response hook, real Stream path)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_response_rate_limits_are_reconciled():
    server = WSAPIServer()
    server.on(
        'account.status',
        result={'canTrade': True},
        rate_limits=[
            {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
             'intervalNum': 1, 'limit': 6000, 'count': 1234},
        ],
    )
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        # The authoritative server count overrides the local estimate.
        snap = client.rate_limit_snapshot()
        weight = [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0]
        assert weight.used == 1234
        assert weight.source == RateLimitSource.HEADER
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Error response -> exception
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_error_response_raises():
    server = WSAPIServer()
    server.on_error('order.status', code=-2013, msg='Order does not exist.')
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        with pytest.raises(StreamSubscribeException, match='Order does not exist'):
            await client._ws_api_request(
                'order.status', {'symbol': 'BTCUSDT', 'orderId': 1},
                security=SecurityType.USER_DATA, weight=4)
    finally:
        await client.close()
        await server.shutdown()


class _RaisingStream:
    """A pre-connected fake stream whose ``send`` always raises ``exc``.

    Injected as ``client._user_stream`` so ``_get_ws_api_stream`` returns it
    without opening a real connection (and without firing ``on_connected``),
    isolating ``_ws_api_request``'s error handling.
    """

    def __init__(self, exc):
        self._exc = exc

    async def send(self, _req):
        raise self._exc


@pytest.mark.asyncio
async def test_ws_api_unauthorized_resets_session_auth_flag():
    # -2015 (session revoked/expired) must drop the authenticated flag so the
    # next request re-signs per-request (self-healing).
    client = SpotClient(Credentials(api_key='K', api_secret='S'))
    client._user_stream = _RaisingStream(
        StreamSubscribeException(-2015, 'Invalid API-key, IP, or permissions.'))
    client._ws_api_authenticated = True       # pretend a session existed

    with pytest.raises(StreamSubscribeException) as exc:
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
    assert exc.value.code == -2015
    # The flag was reset by the -2015 handler.
    assert client._ws_api_authenticated is False


@pytest.mark.asyncio
async def test_ws_api_non_unauthorized_error_keeps_session_auth_flag():
    # A non -2015 error must NOT touch the authenticated flag.
    client = SpotClient(Credentials(api_key='K', api_secret='S'))
    client._user_stream = _RaisingStream(
        StreamSubscribeException(-1100, 'Illegal characters.'))
    client._ws_api_authenticated = True

    with pytest.raises(StreamSubscribeException):
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
    # Unrelated error: flag untouched.
    assert client._ws_api_authenticated is True


@pytest.mark.asyncio
async def test_ws_api_rate_limit_error_raises_rate_limit_exception():
    server = WSAPIServer()
    server.on_error('order.place', code=-1003, msg='Too many requests.',
                    status=429, data={'retryAfter': 5000})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S')
        with pytest.raises(StreamRateLimitException) as exc:
            await client._ws_api_request(
                'order.place', {'symbol': 'BTCUSDT'},
                security=SecurityType.TRADE, weight=1, is_order=True)
        assert exc.value.retry_after == 5000
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Credential guards (raised before any send)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_signed_request_without_api_key_raises():
    client = SpotClient()
    with pytest.raises(APIKeyNotDefinedException):
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)


@pytest.mark.asyncio
async def test_ws_api_signed_request_without_secret_raises():
    client = SpotClient(Credentials(api_key='K'))   # key but no secret / private key
    with pytest.raises(APISecretNotDefinedException):
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)


# ---------------------------------------------------------------------------
# The user-stream subscription shares the SAME WS-API connection.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_user_stream_and_ws_api_request_share_one_connection():
    server = WSAPIServer()
    server.on('userDataStream.subscribe.signature', result=None)
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        from binance import SubType
        client = _make_client(server, api_key='K', api_secret='S')

        await client.subscribe(SubType.USER)
        stream_after_subscribe = client._user_stream
        assert stream_after_subscribe is not None

        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        # Same Stream object reused -> one shared WS-API connection.
        assert client._user_stream is stream_after_subscribe
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# F-07 — float param rejection on WS-API path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_float_param_rejected_before_network():
    """A float param on a WS-API call raises ValueError before any connection."""
    server = WSAPIServer()
    await server.run()
    try:
        client = _make_client(server)
        with pytest.raises(ValueError, match="float"):
            await client._ws_api_request(
                'depth',
                {'symbol': 'BTCUSDT', 'limit': 100.0},
                security=SecurityType.NONE,
                weight=5,
            )
        # No frame should have been sent.
        assert server.received == []
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_int_and_str_params_accepted():
    """int and str params on WS-API calls are accepted without error."""
    server = WSAPIServer()
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server)
        result = await client._ws_api_request(
            'depth',
            {'symbol': 'BTCUSDT', 'limit': 100},   # int is fine
            security=SecurityType.NONE,
            weight=5,
        )
        assert result is not None
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# F-48 — client-level recv_window injection on WS-API signed requests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_api_recv_window_injected_when_not_supplied():
    """F-48: SpotClient(recv_window=5000) injects recvWindow into signed requests
    that do not already carry one."""
    server = WSAPIServer()
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S',
                              recv_window=5000)
        client._time_synced = True
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        params = server.received[0]['params']
        assert params.get('recvWindow') == 5000
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_recv_window_clamped_to_60000():
    """F-48: recv_window values above 60000 are clamped to 60000."""
    server = WSAPIServer()
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S',
                              recv_window=999999)
        client._time_synced = True
        await client._ws_api_request(
            'account.status', None,
            security=SecurityType.USER_DATA, weight=20)
        params = server.received[0]['params']
        assert params.get('recvWindow') == 60000
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_recv_window_not_overridden_when_caller_supplies():
    """F-48: an explicit per-call recvWindow overrides the client-level default."""
    server = WSAPIServer()
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='K', api_secret='S',
                              recv_window=5000)
        client._time_synced = True
        await client._ws_api_request(
            'account.status', {'recvWindow': 1000},
            security=SecurityType.USER_DATA, weight=20)
        params = server.received[0]['params']
        # Caller wins; the client-level default must not override.
        assert params.get('recvWindow') == 1000
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_ws_api_recv_window_not_injected_on_none_security():
    """F-48: recv_window must NOT be injected for NONE (public) endpoints."""
    server = WSAPIServer()
    server.on('depth', result={'lastUpdateId': 1, 'bids': [], 'asks': []})
    await server.run()
    try:
        client = _make_client(server, recv_window=5000)
        await client._ws_api_request(
            'depth', {'symbol': 'BTCUSDT'},
            security=SecurityType.NONE, weight=5)
        params = server.received[0].get('params', {})
        assert 'recvWindow' not in params
    finally:
        await client.close()
        await server.shutdown()
