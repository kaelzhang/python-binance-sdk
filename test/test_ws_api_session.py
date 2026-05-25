"""Hermetic tests for the WS-API session management endpoints (F-65).

Covers ``get_session_status`` (session.status, NONE, no params),
``get_session_subscriptions`` (session.subscriptions, NONE, no params),
and the real ``session_logout`` method which sends ``session.logout`` and
clears the ``_ws_api_authenticated`` flag on the client.
"""

import pytest

from binance import Client
from test.test_ws_api import WSAPIServer


_PORT = 9091


def _make_client(server) -> Client:
    client = Client(ws_api_host=server.uri)
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == 'request_weight'][0].used


# ---------------------------------------------------------------------------
# get_session_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_session_status_sends_no_params():
    server = WSAPIServer(port=_PORT)
    server.on('session.status', result={'apiKey': None, 'authorizedSince': None})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_session_status()
        assert result == {'apiKey': None, 'authorizedSince': None}
        sent = server.received[0]
        assert sent['method'] == 'session.status'
        # params=False -> no 'params' key on the wire
        assert 'params' not in sent
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_session_subscriptions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_session_subscriptions_sends_no_params():
    server = WSAPIServer(port=_PORT)
    server.on('session.subscriptions', result=[])
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_session_subscriptions()
        assert result == []
        sent = server.received[0]
        assert sent['method'] == 'session.subscriptions'
        assert 'params' not in sent
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# session_logout — sends session.logout and clears _ws_api_authenticated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_logout_clears_auth_flag():
    server = WSAPIServer(port=_PORT)
    server.on('session.logout', result={})
    await server.run()
    try:
        client = _make_client(server)
        # Simulate the client having a session-authenticated connection.
        client._ws_api_authenticated = True
        result = await client.session_logout()
        assert result == {}
        sent = server.received[0]
        assert sent['method'] == 'session.logout'
        # Critical: the SDK's auth flag is cleared after logout.
        assert client._ws_api_authenticated is False
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()
