import asyncio
from logging import getLogger

import pytest

from binance import Stream
from binance.core.transport.request_registry import RequestRegistry


logger = getLogger(__name__)


@pytest.mark.asyncio
async def test_request_registry_timeout_removes_pending_future():
    registry = RequestRegistry()
    message_id, future = registry.create()

    with pytest.raises(asyncio.TimeoutError):
        await registry.wait_for(message_id, timeout=0.01)

    assert message_id not in registry.pending
    assert future.done()


@pytest.mark.asyncio
async def test_request_registry_reject_all_clears_and_rejects_pending():
    registry = RequestRegistry()
    _, first = registry.create()
    _, second = registry.create()
    disconnected = RuntimeError('disconnected')

    registry.reject_all(disconnected)

    assert registry.pending == {}
    with pytest.raises(RuntimeError) as first_error:
        await first
    with pytest.raises(RuntimeError) as second_error:
        await second
    assert first_error.value is disconnected
    assert second_error.value is disconnected


@pytest.mark.asyncio
async def test_stream_legacy_message_future_facade_uses_request_registry():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._on_response = None
    future = asyncio.get_running_loop().create_future()

    stream._message_id = 9
    stream._message_futures = {7: future}

    await stream._handle_message({'id': 7, 'result': 'ok'})

    assert future.result() == 'ok'
    assert stream._message_id == 9
    assert stream._message_futures == {}
