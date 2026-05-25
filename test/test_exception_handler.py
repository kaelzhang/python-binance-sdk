import pytest

from binance import (
    SpotClient,
    Credentials,
    HandlerExceptionHandlerBase,
    AccountPositionHandlerBase
)

from binance.core.common.utils import create_future

ACCOUNT_POSITION = {
    'e': 'outboundAccountPosition',
    'E': 1499405658849,
    'u': 1499405658073,
    'B': []
}


@pytest.mark.asyncio
async def test_handler_exception_handler(capsys):
    client = SpotClient(Credentials('api_key'))

    future = create_future()

    e = ValueError('this is an exception for testing, not a bug')

    class ExceptionHandler(HandlerExceptionHandlerBase):
        def receive(self, e):
            e = super().receive(e)
            future.set_exception(e)

    class AccountPositionHandler(AccountPositionHandlerBase):
        def receive(self, payload):
            raise e

    client.start()
    client.handler(ExceptionHandler())
    client.handler(AccountPositionHandler())

    await client._receive({
        'data': ACCOUNT_POSITION
    })

    try:
        await future
    except Exception as catched:
        assert catched is e

    # captured = capsys.readouterr()

    # assert captured.err == 'haha'
    # assert not captured.out
