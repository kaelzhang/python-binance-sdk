import asyncio
import json
import time
from logging import getLogger

import pytest

from binance import Stream
from binance.core.rate_limit import RateLimiter


logger = getLogger(__name__)


@pytest.mark.asyncio
async def test_stream_ingest_latency_budget_with_slow_callback():
    delivered = []

    async def on_message(msg):
        delivered.append(msg)
        if msg['seq'] == 0:
            await asyncio.sleep(0.05)

    stream = Stream(
        'ws://fake',
        on_message=on_message,
        logger=logger,
        rate_limiter=RateLimiter(enabled=False),
    )
    stream._connection_error = False
    stream._timeout = 1.0

    class FakeSocket:
        def __init__(self):
            self.messages = [
                json.dumps({'seq': seq, 'data': {'e': '24hrTicker'}})
                for seq in range(128)
            ]
            self.recv_times = []

        async def recv(self):
            self.recv_times.append(time.perf_counter())
            return self.messages.pop(0)

    socket = FakeSocket()
    stream._socket = socket

    started_at = time.perf_counter()
    for _ in range(128):
        await stream._receive()
    ingest_elapsed = time.perf_counter() - started_at

    await asyncio.sleep(0.06)

    gaps = [
        right - left
        for left, right in zip(socket.recv_times, socket.recv_times[1:])
    ]
    p99_gap = sorted(gaps)[int(len(gaps) * 0.99)]

    assert ingest_elapsed < 0.1
    assert p99_gap < 0.02
    assert len(delivered) == 128
