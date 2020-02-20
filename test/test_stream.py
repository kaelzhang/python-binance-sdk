import pytest
import asyncio

from binance import Stream, StreamDisconnectedException
from binance.common.constants import STREAM_HOST

async def run_stream():
    f = asyncio.Future()

    async def on_message(msg):
        f.set_result(msg)

    stream = Stream(
        STREAM_HOST + '/stream',
        on_message
    ).connect()

    params = ['btcusdt@ticker']

    print('\nsubscribed', await stream.subscribe(params))

    assert await stream.list_subscriptions() == params

    msg = await f

    assert msg['stream'] == 'btcusdt@ticker'

    print('before close')
    await stream.close()

@pytest.mark.asyncio
async def test_binance_stream():
    await run_stream()

@pytest.mark.asyncio
async def test_stream_never_connect():
    def on_message():
        pass

    with pytest.raises(StreamDisconnectedException, match='never connected'):
        await Stream(
            STREAM_HOST + '/stream',
            on_message
        ).send({})
