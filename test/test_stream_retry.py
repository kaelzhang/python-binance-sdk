import pytest
import asyncio

from binance import Stream
from binance.core.common.utils import create_future

from .common import (
    PORT,
    SocketServer
)
from logging import getLogger

logger = getLogger(__name__)


# This integration test exercises the SDK's recv-timeout -> ping -> reconnect
# path against a local mock server; the SDK's pong wait is 10s, so the test is
# legitimately slow (~10-25s). Give it more headroom than the global 30s
# timeout (which exists to catch genuine hangs in the fast tests).
@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_stream_timeout_disconnect_reconnect():
    class Handler:
        def __init__(self):
            self.reset()

        def reset(self):
            self.f = create_future()

        def receive(self, msg):
            if not self.f.done():
                self.f.set_result(msg)

        async def received(self):
            return await self.f

    handler = Handler()

    async def on_message(msg):
        handler.receive(msg)

    def on_connected():
        return None

    def retry_policy(info):
        return False, 0.05

    server = SocketServer()

    await server.run()

    uri = f'ws://localhost:{PORT}/stream'

    stream = Stream(
        uri,
        on_message=on_message,
        on_connected=on_connected,
        retry_policy=retry_policy,
        timeout=0.1,
        logger=logger
    ).connect()

    # During the 500ms, there might be a lot of disconnection
    await asyncio.sleep(0.5)

    server.start()

    await asyncio.sleep(0.5)
    server.no_timeout()

    msg = await handler.received()

    assert msg['ok']

    await server.shutdown()

    handler.reset()

    await server.run()
    server.start()

    msg = await handler.received()

    assert msg['ok']

    await stream.close()
    await server.shutdown()


@pytest.mark.asyncio
async def test_stream_warning_should_be_captured():
    async def on_message(_):
        return None

    def on_connected():
        raise RuntimeError(
            'this warning is expected and should be captured by pytest'
        )

    server = SocketServer()
    await server.start().run()

    uri = f'ws://localhost:{PORT}/stream'

    with pytest.warns(RuntimeWarning, match='on_connected'):
        stream = Stream(
            uri,
            on_message=on_message,
            on_connected=on_connected,
            timeout=0.1,
            logger=logger
        ).connect()
        await asyncio.sleep(0.2)

    await stream.close()
    await server.shutdown()
