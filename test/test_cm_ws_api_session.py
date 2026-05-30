"""Hermetic tests for the COIN-M Futures WS-API session-management endpoints.

Covers ``get_session_status`` (session.status, NONE, no params) and
``session_logout`` (session.logout, NONE, no params; also clears
``_ws_api_authenticated``).

Docs:
https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-api-general-info
"""

import pytest

from binance import CMFuturesClient
from binance.core.common.constants import SecurityType
from binance.core.rate_limit.types import RateLimitType
from binance.futures.cm.endpoints import WS_API_ENDPOINTS
from test.test_ws_api import WSAPIServer


_PORT = 9100


def _make_client(server) -> CMFuturesClient:
    client = CMFuturesClient(ws_api_host=server.uri)
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0].used


# ---------------------------------------------------------------------------
# get_session_status (NONE, no params, weight=2)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_session_status_sends_no_params():
    server = WSAPIServer(port=_PORT)
    server.on('session.status', result={'apiKey': None, 'authorizedSince': None})
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_session_status()
        assert result == {'apiKey': None, 'authorizedSince': None}
        sent = server.received[0]
        assert sent['method'] == 'session.status'
        # params=False -> no 'params' key on the wire.
        assert 'params' not in sent
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# session_logout — sends session.logout and clears _ws_api_authenticated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_session_logout_clears_auth_flag():
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


# ---------------------------------------------------------------------------
# Registry shape: session methods are present with correct fields.
# ---------------------------------------------------------------------------

def test_cm_session_registry_shape():
    by_name = {entry['name']: entry for entry in WS_API_ENDPOINTS}

    # session.status — getter
    status = by_name['get_session_status']
    assert status['ws_method'] == 'session.status'
    assert status['security_type'] == SecurityType.NONE
    assert status['weight'] == 2
    assert status['params'] is False
    assert status['transport'] == 'ws_api'

    # session_logout is implemented inline, not via define_getter.
    assert 'session_logout' not in by_name


# Binance docs do NOT list session.subscriptions for CM futures.
def test_cm_session_subscriptions_not_in_registry():
    by_name = {entry['name']: entry for entry in WS_API_ENDPOINTS}
    assert 'get_session_subscriptions' not in by_name
