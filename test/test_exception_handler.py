import pytest

from binance import (
    Client,
    HandlerExceptionHandlerBase,
    AccountInfoHandlerBase
)

from .common import ACCOUNT_INFO

from binance.common.utils import create_future


@pytest.mark.asyncio
async def test_handler_exception_handler(capsys):
    client = Client('api_key')

    future = create_future()

    e = ValueError('this is an exception for testing, not a bug')

    class ExceptionHandler(HandlerExceptionHandlerBase):
        def receive(self, e):
            e = super().receive(e)
            future.set_exception(e)

    class AccountInfoHandler(AccountInfoHandlerBase):
        def receive(self, payload):
            raise e

    client.start()
    client.handler(ExceptionHandler())
    client.handler(AccountInfoHandler())

    await client._receive({
        'data': ACCOUNT_INFO
    })

    try:
        await future
    except Exception as catched:
        assert catched is e

    # captured = capsys.readouterr()

    # assert captured.err == 'haha'
    # assert not captured.out
