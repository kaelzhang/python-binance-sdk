from binance.rate_limit.snapshot import RateLimitWindow, RateLimitSnapshot
from binance.rate_limit import RateLimitScope, RateLimitType, RateLimitSource
from binance.rate_limit.core import RateLimiter


def _w(util):
    return RateLimitWindow(
        RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT, '1m',
        int(util * 6000), 6000, 6000 - int(util * 6000), util, 0,
        RateLimitSource.HEADER
    )


def test_max_utilization():
    snap = RateLimitSnapshot(windows=(_w(0.1), _w(0.8), _w(0.3)),
                             pending=0, retry_after=None, throttled=False, at=1.0)
    assert snap.max_utilization == 0.8


def test_max_utilization_empty():
    snap = RateLimitSnapshot(windows=(), pending=0, retry_after=None,
                             throttled=False, at=1.0)
    assert snap.max_utilization == 0.0


def test_window_fields_are_enum_instances():
    """Snapshot windows must carry enum types, not raw strings."""
    limiter = RateLimiter()
    snap = limiter.snapshot()
    assert len(snap.windows) > 0
    w = snap.windows[0]
    assert isinstance(w.scope, RateLimitScope)
    assert isinstance(w.type, RateLimitType)
    assert isinstance(w.source, RateLimitSource)
    # str-enum equality with raw strings still works (backward-compat)
    assert w.scope == w.scope.value
    assert w.type == w.type.value
    assert w.source == w.source.value
