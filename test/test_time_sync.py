"""Tests for the server-time offset sync feature (prevent -1021 errors)."""

import re
import time

import pytest
from aioresponses import aioresponses

from binance import Client
from binance.common.exceptions import StatusException

_TIME_URL = 'https://api.binance.com/api/v3/time'
_ACCOUNT_URL_RE = re.compile(r'https://api\.binance\.com/api/v3/account(\?.*)?$')


@pytest.mark.asyncio
async def test_sync_time_stores_positive_offset_and_sets_flag():
    """sync_time() computes roughly server_time - local_time and marks synced."""
    client = Client()
    local_now = int(time.time() * 1000)
    fake_server_time = local_now + 50_000  # server is 50 s ahead

    with aioresponses() as m:
        m.get(_TIME_URL, payload={'serverTime': fake_server_time})
        offset = await client.sync_time()

    # The offset should be close to +50 000 ms; allow ±2 000 ms of wall-clock
    # drift between the fake_server_time capture above and sync_time's own
    # time.time() call.
    assert abs(offset - 50_000) < 2_000
    assert client._time_offset == offset
    assert client._time_synced is True


@pytest.mark.asyncio
async def test_lazy_auto_sync_on_first_signed_request():
    """A signed endpoint triggers sync_time once before the first request."""
    client = Client(api_key='k', api_secret='s')
    assert client._time_synced is False

    local_now = int(time.time() * 1000)
    with aioresponses() as m:
        # sync_time hits /api/v3/time
        m.get(_TIME_URL, payload={'serverTime': local_now + 100})
        # the actual signed GET /api/v3/account
        m.get(_ACCOUNT_URL_RE, payload={
            'makerCommission': 10, 'takerCommission': 10,
            'buyerCommission': 0, 'sellerCommission': 0,
            'canTrade': True, 'canWithdraw': True, 'canDeposit': True,
            'balances': []
        })
        await client.get_account()

    assert client._time_synced is True


@pytest.mark.asyncio
async def test_1021_response_rearms_resync():
    """-1021 response sets _time_synced=False so the next request re-syncs."""
    client = Client(api_key='k', api_secret='s')
    client._time_synced = True   # pretend we already synced

    with aioresponses() as m:
        m.get(_ACCOUNT_URL_RE, status=400, payload={
            'code': -1021,
            'msg': 'Timestamp for this request is outside of the recvWindow.'
        })
        with pytest.raises(StatusException) as exc_info:
            await client.get_account()

    assert exc_info.value.code == -1021
    assert client._time_synced is False
