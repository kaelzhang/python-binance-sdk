import time

from binance.spot.orderbook import SpotOrderBook


def _diff(update_id: int) -> dict:
    return {
        'U': update_id,
        'u': update_id,
        'a': [],
        'b': [],
    }


def test_orderbook_slow_snapshot_buffer_budget_metrics():
    orderbook = SpotOrderBook('BTCUSDT')
    orderbook._fetching = True

    latencies = []
    for update_id in range(1, 12001):
        started_at = time.perf_counter()
        orderbook.update(_diff(update_id))
        latencies.append(time.perf_counter() - started_at)

    p95 = sorted(latencies)[int(len(latencies) * 0.95)]
    p99 = sorted(latencies)[int(len(latencies) * 0.99)]

    assert len(orderbook._unsolved_queue) <= 4096
    assert orderbook._unsolved_queue[0]['U'] == 12000 - 4096 + 1
    assert p95 < 0.005
    assert p99 < 0.01
