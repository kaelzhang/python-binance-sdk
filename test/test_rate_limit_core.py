import time
import pytest

from binance.rate_limit.core import RateLimiter
from binance.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode,
    RateLimitSource
)
from binance.common.exceptions import (
    RateLimitReachedException, TooManyStreamsException
)


def _windows_by(snapshot, type_: RateLimitType):
    return [w for w in snapshot.windows if w.type == type_]


@pytest.mark.asyncio
async def test_acquire_rest_consumes_weight_raw_orders():
    rl = RateLimiter()
    await rl.acquire_rest(weight=10, is_order=False)
    snap = rl.snapshot()
    assert _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0].used == 10
    assert _windows_by(snap, RateLimitType.RAW_REQUESTS)[0].used == 1
    assert all(w.used == 0 for w in _windows_by(snap, RateLimitType.ORDERS))
    await rl.acquire_rest(weight=5, is_order=True)
    snap = rl.snapshot()
    assert _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0].used == 15
    assert all(w.used == 1 for w in _windows_by(snap, RateLimitType.ORDERS))   # both intervals


@pytest.mark.asyncio
async def test_acquire_request_is_canonical_and_acquire_rest_aliases_it():
    # acquire_request is the canonical gate; acquire_rest must delegate to it
    # with identical effect (so REST callers keep working unchanged).
    rl = RateLimiter()
    await rl.acquire_request(weight=7, is_order=True)
    snap = rl.snapshot()
    assert _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0].used == 7
    assert _windows_by(snap, RateLimitType.RAW_REQUESTS)[0].used == 1
    assert all(w.used == 1 for w in _windows_by(snap, RateLimitType.ORDERS))

    rl2 = RateLimiter()
    await rl2.acquire_rest(weight=7, is_order=True)
    snap2 = rl2.snapshot()
    assert _windows_by(snap2, RateLimitType.REQUEST_WEIGHT)[0].used == 7
    assert _windows_by(snap2, RateLimitType.RAW_REQUESTS)[0].used == 1
    assert all(w.used == 1 for w in _windows_by(snap2, RateLimitType.ORDERS))


def test_sync_from_ws_rate_limits_reconciles_matching_buckets():
    rl = RateLimiter()
    rl.sync_from_ws_rate_limits([
        # weight pool (MINUTE x1 -> 60s -> the request_weight bucket)
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
         'intervalNum': 1, 'limit': 6000, 'count': 4321},
        # orders pool (SECOND x10 -> 10s)
        {'rateLimitType': 'ORDERS', 'interval': 'SECOND',
         'intervalNum': 10, 'limit': 100, 'count': 7},
        # raw requests (5 MINUTES -> 300s)
        {'rateLimitType': 'RAW_REQUESTS', 'interval': 'MINUTE',
         'intervalNum': 5, 'limit': 300000, 'count': 99},
    ])
    snap = rl.snapshot()
    weight = _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0]
    assert weight.used == 4321 and weight.source == RateLimitSource.HEADER
    orders10 = [w for w in _windows_by(snap, RateLimitType.ORDERS) if w.interval == '10s'][0]
    assert orders10.used == 7 and orders10.source == RateLimitSource.HEADER
    raw = _windows_by(snap, RateLimitType.RAW_REQUESTS)[0]
    assert raw.used == 99 and raw.source == RateLimitSource.HEADER


def test_sync_from_ws_rate_limits_tolerates_garbage_and_none():
    rl = RateLimiter()
    rl.sync_from_ws_rate_limits(None)               # None -> no-op
    rl.sync_from_ws_rate_limits([
        'not-a-dict',                               # non-dict entry skipped
        {'rateLimitType': 'CONNECTIONS', 'interval': 'MINUTE',
         'intervalNum': 5, 'count': 1},            # unknown type skipped
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'WEEK',
         'intervalNum': 1, 'count': 1},            # unknown interval skipped
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'HOUR',
         'intervalNum': 1, 'count': 1},            # no matching bucket -> skip
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
         'intervalNum': 1},                        # missing count -> skip
        {'rateLimitType': 'ORDERS', 'interval': 'SECOND',
         'intervalNum': 10, 'count': None},        # None count -> skip
    ])
    # nothing became authoritative
    snap = rl.snapshot()
    assert _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0].source == RateLimitSource.CLIENT
    assert all(w.source == RateLimitSource.CLIENT for w in _windows_by(snap, RateLimitType.ORDERS))


@pytest.mark.asyncio
async def test_disabled_records_but_never_blocks_or_raises():
    rl = RateLimiter(enabled=False)
    for _ in range(5):
        await rl.acquire_rest(weight=1, is_order=True)
    assert all(w.used == 5 for w in _windows_by(rl.snapshot(), RateLimitType.ORDERS))


@pytest.mark.asyncio
async def test_orders_raise_when_enabled_and_over_limit():
    rules = (
        RateLimitRule(RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT, 60.0, 6000,
                      RateLimitKind.WEIGHT, EnforceMode.SLEEP),
        RateLimitRule(RateLimitScope.IP, RateLimitType.RAW_REQUESTS, 300.0, 300000,
                      RateLimitKind.COUNT, EnforceMode.SLEEP),
        RateLimitRule(RateLimitScope.ACCOUNT, RateLimitType.ORDERS, 10.0, 2,
                      RateLimitKind.COUNT, EnforceMode.RAISE),
    )
    rl = RateLimiter(rules=rules, enabled=True)
    await rl.acquire_rest(weight=1, is_order=True)
    await rl.acquire_rest(weight=1, is_order=True)
    with pytest.raises(RateLimitReachedException):
        await rl.acquire_rest(weight=1, is_order=True)


def test_sync_from_headers_sets_authoritative():
    rl = RateLimiter()
    rl.sync_from_headers({'1m': 5000}, {'10s': 50})
    snap = rl.snapshot()
    weight = _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0]
    assert weight.used == 5000 and weight.source == RateLimitSource.HEADER
    orders10 = [w for w in _windows_by(snap, RateLimitType.ORDERS) if w.interval == '10s'][0]
    assert orders10.used == 50 and orders10.source == RateLimitSource.HEADER
    # a pool with no header reads as client-sourced
    assert _windows_by(snap, RateLimitType.RAW_REQUESTS)[0].source == RateLimitSource.CLIENT


def test_configure_from_exchange_info_sets_limits_and_skips_unknown():
    rl = RateLimiter()
    rl.configure_from_exchange_info([
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 1200},
        {'rateLimitType': 'ORDERS', 'interval': 'SECOND', 'intervalNum': 10, 'limit': 50},
        {'rateLimitType': 'CONNECTIONS', 'interval': 'MINUTE', 'intervalNum': 5, 'limit': 300},  # unknown type
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'WEEK', 'intervalNum': 1, 'limit': 9},   # unknown interval
        {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'HOUR', 'intervalNum': 1, 'limit': 9},   # no matching bucket
    ])
    rl.configure_from_exchange_info(None)   # tolerates None
    snap = rl.snapshot()
    assert _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0].limit == int(1200 * 0.9)
    orders10 = [w for w in _windows_by(snap, RateLimitType.ORDERS) if w.interval == '10s'][0]
    assert orders10.limit == 50


@pytest.mark.asyncio
async def test_acquire_connection_and_per_connection_message():
    rl = RateLimiter()
    await rl.acquire_connection()
    assert _windows_by(rl.snapshot(), RateLimitType.WS_CONNECTIONS)[0].used == 1
    # auto-registers an unknown connection on first acquire_message
    await rl.acquire_message('c1')
    await rl.acquire_message('c1')
    msgs = _windows_by(rl.snapshot(), RateLimitType.WS_MESSAGES)
    assert any(w.used == 2 and w.scope == RateLimitScope.CONNECTION for w in msgs)
    rl.unregister_connection('c1')
    assert not _windows_by(rl.snapshot(), RateLimitType.WS_MESSAGES)


@pytest.mark.asyncio
async def test_acquire_message_throttles_at_per_connection_limit():
    # The per-connection ws-messages bucket (5/1s, SLEEP) must actually block
    # the message past its budget, not just account it.
    rl = RateLimiter()
    rl.register_connection('c1')
    limit = _windows_by(rl.snapshot(), RateLimitType.WS_MESSAGES)[0].limit
    start = time.monotonic()
    for _ in range(limit):
        await rl.acquire_message('c1')
    assert time.monotonic() - start < 0.3      # the first `limit` are immediate
    # all accounted while still inside the window (before it rolls)
    assert _windows_by(rl.snapshot(), RateLimitType.WS_MESSAGES)[0].used == limit
    await rl.acquire_message('c1')             # one past budget -> waits the window
    assert time.monotonic() - start >= 0.5


@pytest.mark.asyncio
async def test_disabled_message_records_only():
    rl = RateLimiter(enabled=False)
    rl.register_connection('c1')
    await rl.acquire_message('c1')
    assert any(w.used == 1 for w in _windows_by(rl.snapshot(), RateLimitType.WS_MESSAGES))


def test_reserve_and_release_streams():
    rl = RateLimiter()
    rl.reserve_streams('c1', 1024)
    assert _windows_by(rl.snapshot(), RateLimitType.WS_STREAMS)[0].used == 1024
    with pytest.raises(TooManyStreamsException):
        rl.reserve_streams('c1', 1025)
    rl.release_streams('c1', 24)
    assert _windows_by(rl.snapshot(), RateLimitType.WS_STREAMS)[0].used == 1000
    rl.release_streams('nope', 5)        # unknown connection -> no-op, no error


def test_note_retry_after_and_throttled():
    rl = RateLimiter()
    snap = rl.snapshot()
    assert snap.retry_after is None and snap.throttled is False
    rl.note_retry_after(120, 429)
    snap = rl.snapshot()
    assert snap.retry_after is not None and snap.retry_after <= 120
    assert snap.throttled is True
    rl.note_retry_after(0, 200)          # zero -> ignored
    assert rl.snapshot().throttled is True


def test_retry_after_expires():
    rl = RateLimiter()
    rl._retry_after_until = time.time() - 1   # already elapsed
    snap = rl.snapshot()
    assert snap.retry_after is None and snap.throttled is False


def test_snapshot_window_fields_and_max_utilization():
    rl = RateLimiter()
    rl.sync_from_headers({'1m': 3000}, {})   # 50% of 6000
    snap = rl.snapshot()
    weight = _windows_by(snap, RateLimitType.REQUEST_WEIGHT)[0]
    assert weight.limit == int(6000 * 0.9)
    assert weight.remaining == weight.limit - weight.used
    assert 0.0 <= weight.utilization <= 1.0
    assert snap.max_utilization >= weight.utilization
    assert snap.at > 0


# ---------------------------------------------------------------------------
# F-74 — _await_retry_after blocks acquire paths during 429/418 ban window
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_request_waits_out_retry_after_ban():
    """After a 429 ban, acquire_request blocks until the ban elapses (F-74).

    Monkey-patches _retry_after so a short (0.1s) measured wait is used instead
    of the integer-rounded real window, keeping the test fast.
    """
    rl = RateLimiter()
    deadline = time.time() + 0.1
    original = rl._retry_after

    def _fake_retry_after():
        remaining = deadline - time.time()
        if remaining <= 0:
            return None
        return remaining  # float; asyncio.sleep accepts floats

    rl._retry_after = _fake_retry_after

    start = time.monotonic()
    await rl.acquire_request(weight=1, is_order=False)
    elapsed = time.monotonic() - start

    rl._retry_after = original
    assert elapsed >= 0.09, (
        f"acquire_request returned in {elapsed:.3f}s — "
        "did not wait out the retry-after ban"
    )


@pytest.mark.asyncio
async def test_acquire_connection_waits_out_retry_after_ban():
    """After a 429 ban, acquire_connection blocks until the ban elapses (F-74)."""
    rl = RateLimiter()
    deadline = time.time() + 0.1

    def _fake_retry_after():
        remaining = deadline - time.time()
        return remaining if remaining > 0 else None

    rl._retry_after = _fake_retry_after

    start = time.monotonic()
    await rl.acquire_connection()
    elapsed = time.monotonic() - start

    assert elapsed >= 0.09, (
        f"acquire_connection returned in {elapsed:.3f}s — "
        "did not wait out the retry-after ban"
    )


@pytest.mark.asyncio
async def test_acquire_message_waits_out_retry_after_ban():
    """After a 429 ban, acquire_message blocks until the ban elapses (F-74)."""
    rl = RateLimiter()
    deadline = time.time() + 0.1
    rl.register_connection('c1')

    def _fake_retry_after():
        remaining = deadline - time.time()
        return remaining if remaining > 0 else None

    rl._retry_after = _fake_retry_after

    start = time.monotonic()
    await rl.acquire_message('c1')
    elapsed = time.monotonic() - start

    assert elapsed >= 0.09, (
        f"acquire_message returned in {elapsed:.3f}s — "
        "did not wait out the retry-after ban"
    )


@pytest.mark.asyncio
async def test_disabled_limiter_does_not_block_on_retry_after():
    """A disabled limiter must NOT block on retry-after (guard-disabled path, F-74)."""
    rl = RateLimiter(enabled=False)
    # A large ban; _await_retry_after must short-circuit when disabled.
    def _always_ban():
        return 60  # always returns 60s remaining

    rl._retry_after = _always_ban

    start = time.monotonic()
    await rl.acquire_request(weight=1, is_order=False)
    elapsed = time.monotonic() - start

    # Must return essentially immediately (well under the 60s ban).
    assert elapsed < 1.0, (
        f"Disabled limiter blocked for {elapsed:.3f}s on retry-after"
    )


@pytest.mark.asyncio
async def test_no_ban_acquire_request_returns_immediately():
    """Without a ban, acquire_request returns without delay (F-74 no-ban path)."""
    rl = RateLimiter()
    # No ban: _retry_after returns None immediately.
    start = time.monotonic()
    await rl.acquire_request(weight=1, is_order=False)
    elapsed = time.monotonic() - start
    assert elapsed < 0.5
