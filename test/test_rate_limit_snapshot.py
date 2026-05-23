from binance.rate_limit.snapshot import RateLimitWindow, RateLimitSnapshot


def _w(util):
    return RateLimitWindow('ip', 'request_weight', '1m', int(util * 6000),
                           6000, 6000 - int(util * 6000), util, 0, 'header')


def test_max_utilization():
    snap = RateLimitSnapshot(windows=(_w(0.1), _w(0.8), _w(0.3)),
                             pending=0, retry_after=None, throttled=False, at=1.0)
    assert snap.max_utilization == 0.8


def test_max_utilization_empty():
    snap = RateLimitSnapshot(windows=(), pending=0, retry_after=None,
                             throttled=False, at=1.0)
    assert snap.max_utilization == 0.0
