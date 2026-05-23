import pytest

from binance import (
    Client,
    SubType,
    UserStreamNotSubscribedException
)

from .common import print_json, get_api_credentials


API_KEY, API_SECRET = get_api_credentials()
has_private_config = API_KEY is not None and API_SECRET is not None


@pytest.mark.asyncio
async def test_user_stream():
    if not has_private_config:
        return

    client = Client(API_KEY, API_SECRET)

    with pytest.raises(
        UserStreamNotSubscribedException,
        match='not subscribed'
    ):
        await client.unsubscribe(SubType.USER)

    await client.subscribe(SubType.USER)

    await client.unsubscribe(SubType.USER)

    await client.close()


@pytest.mark.asyncio
async def test_user_trades():
    if not has_private_config:
        return

    client = Client(API_KEY, API_SECRET)

    res = await client.get_trades(symbol='BTCUSDT')

    print_json('get_trades:', res)
