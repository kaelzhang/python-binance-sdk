import asyncio
import time

import pytest

from binance import OrderBookHandlerBase
from binance.spot.orderbook import SpotOrderBook


def _diff(update_id: int, symbol: str = 'BTCUSDT') -> dict:
    return {
        'e': 'depthUpdate',
        's': symbol,
        'U': update_id,
        'u': update_id,
        'a': [],
        'b': [],
    }


class _SnapshotClient:
    def __init__(self, last_update_id: int = 1):
        self._last_update_id = last_update_id

    async def get_orderbook(self, *, symbol, limit):
        return {
            'lastUpdateId': self._last_update_id,
            'asks': [],
            'bids': [],
        }


def test_orderbook_unsolved_queue_is_bounded_while_snapshot_fetch_is_slow():
    orderbook = SpotOrderBook('BTCUSDT')
    orderbook._fetching = True

    for update_id in range(1, 6001):
        orderbook.update(_diff(update_id))

    assert len(orderbook._unsolved_queue) <= 4096
    assert orderbook._unsolved_queue[0]['U'] == 6000 - 4096 + 1


@pytest.mark.asyncio
async def test_orderbook_replay_yields_to_event_loop_between_batches():
    orderbook = SpotOrderBook('BTCUSDT')
    orderbook._client = _SnapshotClient(last_update_id=1)
    orderbook._unsolved_queue = [_diff(update_id) for update_id in range(1, 129)]

    def slow_update(payload):
        time.sleep(0.002)
        return True

    orderbook._update = slow_update

    started_at = time.perf_counter()
    task = asyncio.create_task(orderbook._fetch_snapshot())
    try:
        await asyncio.sleep(0.05)
        assert time.perf_counter() - started_at < 0.12
    finally:
        await asyncio.wait_for(task, timeout=1.0)

    assert orderbook._unsolved_queue == []


@pytest.mark.asyncio
async def test_orderbook_handler_update_long_task_does_not_block_event_loop():
    handler = OrderBookHandlerBase()

    class SlowBook:
        def update(self, payload):
            time.sleep(0.2)
            return True

    handler._orderbooks = {'btcusdt': SlowBook()}

    started_at = time.perf_counter()
    task = asyncio.create_task(handler.receiveDispatch(_diff(1)))
    try:
        await asyncio.sleep(0.05)
        assert time.perf_counter() - started_at < 0.12
    finally:
        await asyncio.wait_for(task, timeout=0.5)
