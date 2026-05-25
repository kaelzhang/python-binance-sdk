import os

import pytest

# pylint: disable=no-member

from binance import SpotClient, TimeFrame

from .common import print_json

mock = False

# Live test: hits Binance directly. Skipped by default (keeps `make test`
# hermetic / non-hanging); opt in with BINANCE_LIVE_TEST=1.
_RUN_LIVE = os.environ.get('BINANCE_LIVE_TEST') == '1'

FREE_CASES = [
    dict(
        name='ping'
    ),
    dict(
        # name of the spot method
        name='get_server_time',
        # arguments, defaults to ()
        # a=tuple(),
        # keyworded arguments, defaults to {}
        # ka={},
        #########################################
        # uri to be requested
        # uri='https://api.binance.com/api/v3/time',
        # request method, defaults to 'get'
        # method='get'
    ),
    dict(
        name='get_exchange_info'
        # uri='https://www'
    ),
    # dict(
    #     name='get_system_status'
    # ),
    dict(
        name='get_orderbook',
        ka=dict(
            symbol='BTCUSDT',
            limit=100
        )
    ),
    dict(
        name='get_recent_trades',
        ka=dict(
            symbol='BTCUSDT',
            limit=100
        )
    ),
    dict(
        name='get_aggregate_trades',
        ka=dict(
            symbol='BTCUSDT'
        )
    ),
    dict(
        name='get_klines',
        ka=dict(
            symbol='BTCUSDT',
            interval=TimeFrame.D1
        )
    ),
    dict(
        name='get_average_price',
        ka=dict(
            symbol='BTCUSDT'
        )
    ),
    dict(
        name='get_ticker',
        ka=dict(
            symbol='BTCUSDT'
        )
    ),
    dict(
        name='get_ticker_price',
        ka=dict(
            symbol='BTCUSDT'
        )
    ),
    dict(
        name='get_orderbook_ticker',
        ka=dict(
            symbol='BTCUSDT'
        )
    )
]

# def callback(method, url, **kwargs):
#     def real_callback(url, **kwargs):
#         print(url, kwargs)
#         return CallbackResult(status=200)
#     return real_callback


@pytest.mark.skipif(
    not _RUN_LIVE,
    reason='live Binance test; set BINANCE_LIVE_TEST=1')
@pytest.mark.asyncio
async def test_free_apis():
    client = SpotClient()

    print('')

    async def go():
        for case in FREE_CASES:
            name = case['name']
            args = case.get('a', tuple())
            kwargs = case.get('ka', {})

            ret = await getattr(client, name)(*args, **kwargs)

            print_json(name + ':', ret)

    await go()

    # if not mock:
    #     await go()
    #     return

    # with aioresponses() as m:
    #     pattern = re.compile(r'^https://(?:api|www).binance.com')

    #     if mock:
    #         m.get(pattern, callback=callback('get'), repeat=True)

    #     await go()
