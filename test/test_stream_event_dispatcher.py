import subprocess
import sys


def test_event_dispatcher_can_close_from_own_worker():
    script = """
import asyncio
from logging import getLogger

from binance.core.transport.event_dispatcher import StreamEventDispatcher


async def main():
    started = asyncio.Event()
    returned = asyncio.Event()
    dispatcher = None

    async def on_message(_msg):
        started.set()
        await dispatcher.close()
        returned.set()

    dispatcher = StreamEventDispatcher(
        on_message,
        getLogger(__name__),
        max_queue_size=2,
        max_workers=1,
    )

    assert dispatcher.submit({'stream': 'one'})
    await asyncio.wait_for(started.wait(), timeout=0.1)
    await asyncio.wait_for(returned.wait(), timeout=0.2)
    await asyncio.sleep(0)


asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        text=True,
        capture_output=True,
        timeout=1.0,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_stream_can_close_from_own_connected_callback_task():
    script = """
import asyncio
from logging import getLogger

from binance import Stream
from binance.core.rate_limit import RateLimiter


async def main():
    started = asyncio.Event()
    returned = asyncio.Event()
    task_cancelling_after_close = None

    async def on_message(_msg):
        pass

    async def conn_task():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return

    class FakeSocket:
        def __init__(self):
            self.closed = False

        async def close(self, _code=4999):
            self.closed = True

    stream = Stream(
        'ws://fake',
        on_message=on_message,
        logger=getLogger(__name__),
        rate_limiter=RateLimiter(enabled=False),
    )
    stream._conn_task = asyncio.create_task(conn_task())
    socket = FakeSocket()
    stream._socket = socket

    async def connected_callback_task():
        nonlocal task_cancelling_after_close
        started.set()
        await stream.close()
        task_cancelling_after_close = asyncio.current_task().cancelling()
        returned.set()

    stream._connected_task = asyncio.create_task(connected_callback_task())

    await asyncio.wait_for(started.wait(), timeout=0.1)
    await asyncio.wait_for(returned.wait(), timeout=0.2)

    assert task_cancelling_after_close == 0
    assert socket.closed is True
    assert stream._socket is None
    assert stream._closing is False


asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        text=True,
        capture_output=True,
        timeout=1.0,
        check=False,
    )

    assert result.returncode == 0, result.stderr
