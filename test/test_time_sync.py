"""Tests for the server-time offset sync feature (prevent -1021 errors).

``_sync_time`` and every signed request now travel over the WebSocket API, so
these drive the local :class:`WSAPIServer` request/response harness instead of
mocking REST. ``_sync_time()`` resolves via the WS-API ``time`` request
(``get_server_time``); the lazy arming fires the unsigned ``time`` request once
before the first signed request, and a ``-1021`` re-arms it.
"""

import time

import pytest

from binance import SpotClient, Credentials
from binance.core.common.exceptions import StreamSubscribeException

from test.test_ws_api import WSAPIServer


_PORT = 9088


def _make_client(server, **kwargs) -> SpotClient:
    cred_kwargs = {
        k: kwargs.pop(k)
        for k in ('api_key', 'api_secret', 'private_key', 'private_key_pass')
        if k in kwargs
    }
    return SpotClient(Credentials(**cred_kwargs), ws_api_host=server.uri, **kwargs)


@pytest.mark.asyncio
async def test_sync_time_stores_positive_offset_and_sets_flag():
    """_sync_time() computes roughly server_time - local_time and marks synced."""
    local_now = int(time.time() * 1000)
    fake_server_time = local_now + 50_000  # server is 50 s ahead

    server = WSAPIServer(port=_PORT)
    server.on('time', result={'serverTime': fake_server_time})
    await server.run()
    try:
        client = _make_client(server)
        offset = await client._sync_time()

        # The offset should be close to +50 000 ms; allow +/-2 000 ms of
        # wall-clock drift between the fake_server_time capture above and
        # _sync_time's own time.time() call.
        assert abs(offset - 50_000) < 2_000
        assert client._time_offset == offset
        assert client._time_synced is True

        # _sync_time issues the public (unsigned) `time` request.
        assert server.received[-1]['method'] == 'time'
        assert 'params' not in server.received[-1]
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_lazy_auto_sync_on_first_signed_request():
    """A signed endpoint triggers _sync_time once before the first request."""
    local_now = int(time.time() * 1000)

    server = WSAPIServer(port=_PORT)
    server.on('time', result={'serverTime': local_now + 100})
    server.on('account.status', result={
        'makerCommission': 10, 'takerCommission': 10,
        'buyerCommission': 0, 'sellerCommission': 0,
        'canTrade': True, 'canWithdraw': True, 'canDeposit': True,
        'balances': []
    })
    await server.run()
    try:
        client = _make_client(server, api_key='k', api_secret='s')
        assert client._time_synced is False

        await client.get_account()

        assert client._time_synced is True
        methods = [m['method'] for m in server.received]
        # The lazy `time` sync precedes the first signed `account.status`.
        assert methods == ['time', 'account.status']
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_lazy_sync_only_runs_once():
    """_sync_time fires once; subsequent signed requests do not re-sync."""
    server = WSAPIServer(port=_PORT)
    server.on('account.status', result={'canTrade': True})
    await server.run()
    try:
        client = _make_client(server, api_key='k', api_secret='s')
        await client.get_account()
        await client.get_account()

        methods = [m['method'] for m in server.received]
        # Only ONE `time` request despite two signed calls.
        assert methods.count('time') == 1
        assert methods.count('account.status') == 2
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_1021_response_rearms_resync():
    """-1021 response sets _time_synced=False so the next request re-syncs."""
    server = WSAPIServer(port=_PORT)
    server.on_error(
        'account.status', code=-1021,
        msg='Timestamp for this request is outside of the recvWindow.')
    await server.run()
    try:
        client = _make_client(server, api_key='k', api_secret='s')
        client._time_synced = True   # pretend we already synced

        with pytest.raises(StreamSubscribeException) as exc_info:
            await client.get_account()

        assert exc_info.value.code == -1021
        # The -1021 handler re-armed the lazy sync.
        assert client._time_synced is False
    finally:
        await client.close()
        await server.shutdown()
