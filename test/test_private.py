import os

import pytest

from binance import (
    Client,
    SubType,
    UserStreamNotSubscribedException
)

from .common import print_json, get_api_credentials


API_KEY, API_SECRET = get_api_credentials()

# These tests talk to Binance directly (network + valid credentials). They are
# skipped by default so `make test` stays hermetic and can never hang on an
# unreachable Binance (e.g. a region-blocked network). The user-stream and
# signed-REST code paths they exercise are covered hermetically in
# test_internals.py. Opt in to the live checks with BINANCE_LIVE_TEST=1.
_RUN_LIVE = (
    os.environ.get('BINANCE_LIVE_TEST') == '1'
    and API_KEY is not None
    and API_SECRET is not None
)

live_only = pytest.mark.skipif(
    not _RUN_LIVE,
    reason='live Binance test; set BINANCE_LIVE_TEST=1 with valid credentials'
)


@live_only
@pytest.mark.asyncio
async def test_user_stream():
    client = Client(API_KEY, API_SECRET)

    with pytest.raises(
        UserStreamNotSubscribedException,
        match='not subscribed'
    ):
        await client.unsubscribe(SubType.USER)

    await client.subscribe(SubType.USER)

    await client.unsubscribe(SubType.USER)

    await client.close()


@live_only
@pytest.mark.asyncio
async def test_user_trades():
    client = Client(API_KEY, API_SECRET)

    res = await client.get_trades(symbol='BTCUSDT')

    print_json('get_trades:', res)
