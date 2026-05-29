import pytest
import asyncio

from binance import (
    SpotClient,
    Credentials,
    OrderBook,
    OrderBookFetchAbandonedException
)
from binance.core.common.exceptions import SnapshotTooOldException
from binance.spot.orderbook import SpotOrderBook

from test.test_ws_api import WSAPIServer


def test_order_book_no_client():
    orderbook = SpotOrderBook('BTCUSDT')
    assert not orderbook._fetching
    # The publicly re-exported ``OrderBook`` is the abstract base; the
    # spot-concrete subclass we instantiated must be a subclass of it.
    assert isinstance(orderbook, OrderBook)


def test_orderbook_handler_per_symbol_limit():
    from binance import OrderBookHandlerBase
    # No client attached -> no snapshot fetch is triggered (hermetic).
    handler = OrderBookHandlerBase(limit=100)

    custom = handler.orderbook('BTCUSDT', limit=500)
    assert custom._limit == 500                 # per-symbol override

    default = handler.orderbook('ETHUSDT')
    assert default._limit == 100                # handler-level default

    # The book is cached; a later get returns the same instance unchanged.
    assert handler.orderbook('BTCUSDT') is custom
    assert custom._limit == 500


def test_pending_orderbook_attribute_before_materialise_raises():
    """Forwarding wrapper raises ``AttributeError`` for live-book attributes
    that are read before ``set_client`` materialises the real book.

    The wrapper is only meant to act as an opaque reference between
    ``handler.orderbook(symbol)`` and the subsequent
    ``client.handler(handler)`` call; touching live-book state before then
    is a programming error and should surface as such, not silently
    return ``None``.
    """
    from binance.core.handlers.orderbook import _PendingOrderBook

    pending = _PendingOrderBook('BTCUSDT', 500)

    # The two slot fields are readable directly.
    assert pending._symbol == 'BTCUSDT'
    assert pending._limit == 500

    # Any other ``OrderBook`` attribute raises until ``_materialise``.
    with pytest.raises(AttributeError, match='before the handler was bound'):
        _ = pending.ready


@pytest.mark.asyncio
async def test_order_book():
    # The depth snapshot is now fetched over the WebSocket API (`depth`); drive
    # it through the local WS-API request/response harness instead of REST.
    server = WSAPIServer(port=9092)
    await server.run()

    try:
        # Prepare
        # -----------------------------------------------------------
        a00, b00, b01, a10 = [100, 10], [99, 100], [98, 2], [101, 3]
        asks = [a00]
        bids = [b00, b01]
        bids_sort = [b01, b00]

        asks1 = [a10, a00]
        asks1_sort = [a00, a10]

        client = SpotClient(Credentials('api_key'), ws_api_host=server.uri)

        def preset_10():
            server.on('depth', result=dict(
                lastUpdateId=10,
                asks=asks,
                bids=bids
            ))

        def preset_11():
            # Variant of preset_10 used by scenarios whose first buffered diff
            # has U=11; lastUpdateId must be >= 11 to satisfy the docs' step-4
            # pre-check (snapshot must cover the first buffered event's U).
            server.on('depth', result=dict(
                lastUpdateId=11,
                asks=asks,
                bids=bids
            ))

        def assert_state_a():
            assert orderbook.asks == asks
            assert orderbook.bids == bids_sort

        def preset_13():
            server.on('depth', result=dict(
                lastUpdateId=13,
                asks=asks1,
                bids=bids
            ))

        def preset_14():
            # Variant of preset_13 used by scenarios whose first buffered diff
            # has U=14 so the snapshot must have lastUpdateId >= 14 to satisfy
            # the docs' step-4 pre-check
            # (developers.binance.com/docs/binance-spot-api-docs/web-socket-streams,
            # section "How to manage a local order book correctly").
            server.on('depth', result=dict(
                lastUpdateId=14,
                asks=asks1,
                bids=bids
            ))

        def preset_unavailable():
            # Mimic a not-yet-available snapshot: the refetch fails (and the
            # retry policy keeps retrying) until a `preset_*` restores success.
            server.on_error('depth', code=-1000, msg='unavailable', status=503)

        def assert_state_b():
            assert orderbook.asks == asks1_sort
            assert orderbook.bids == bids_sort

        def assert_state_c():
            assert orderbook.asks == [[95, 1], *asks1_sort]
            assert orderbook.bids == bids_sort

        # Start testing
        # -----------------------------------------------------------

        print('\nround one  : normal initialization')

        preset_10()

        orderbook = SpotOrderBook('BTCUSDT', client)

        assert not orderbook.ready
        await orderbook.updated()
        assert orderbook.ready

        assert_state_a()

        print('round two  : wrong update, refetch, retry policy and finally fetched')

        # The snapshot is not yet available -> the refetch keeps retrying.
        preset_unavailable()

        f = orderbook.updated()

        # wrong stream message,
        # and orderbook will fetch the snapshot again
        updated = orderbook.update(dict(
            # U=11 is missing
            U=12,
            u=13,
            a=[],
            b=[]
        ))

        assert not updated
        assert not orderbook.ready

        await asyncio.sleep(0.5)
        # delay initialize preset b
        preset_13()

        await f
        assert orderbook.ready

        assert_state_b()

        # valid stream message
        assert orderbook.update(dict(
            U=14,
            u=15,
            a=[[95, 1]],
            b=[]
        ))

        assert orderbook.asks == [[95, 1], *asks1_sort]

        print('round three: new update when still refetching')

        # The first event the refetch will see in the buffer is U=14, so the
        # snapshot's lastUpdateId must be >= 14 to satisfy the docs' step-4
        # pre-check.  preset_13 would be a step-4 violation here (13 < 14) and
        # would correctly trigger SnapshotTooOldException + refetch.
        preset_14()

        f = orderbook.updated()

        updated = orderbook.update(dict(
            # U=16 is missing
            U=17,
            u=18,
            a=[],
            b=[]
        ))

        assert not updated

        # orderbook is fetching
        updated = orderbook.update(dict(
            U=14,
            u=15,
            a=[[95, 1]],
            b=[]
        ))

        assert not updated

        await f

        assert_state_c()

        print('round four : retry policy -> abandon')

        def no_retry_policy(_):
            return True, 0

        orderbook.set_retry_policy(no_retry_policy)

        async def test_no_retry_policy():
            # Snapshot unavailable -> the triggered refetch fails and, with no
            # retry, is abandoned.
            preset_unavailable()

            orderbook.update(dict(
                # U=16 is missing
                U=17,
                u=18,
                a=[],
                b=[]
            ))

            exc = None

            try:
                await orderbook.updated()
            except Exception as e:
                exc = e

            assert exc is not None

            preset_13()

            await orderbook.fetch()

            assert orderbook.ready

            orderbook.update(dict(
                U=14,
                u=15,
                a=[[95, 1]],
                b=[]
            ))

            assert_state_c()

        await test_no_retry_policy()

        print('round five : no retry policy')

        orderbook.set_retry_policy(None)

        await test_no_retry_policy()

        print('round six  : part of unsolved_queue is invalid')

        if not orderbook.ready:
            await orderbook.updated()

        # The first buffered diff below has U=11, so the snapshot's
        # lastUpdateId must be >= 11 to clear the docs' step-4 pre-check.
        # The intent of this round is to exercise the "queue has an invalid
        # *intermediate* entry" path -- not the step-4 path -- so the
        # snapshot must be allowed past step 4 first.
        preset_11()

        def allow_retry_once(info):
            if info.fails > 1:
                return True, 0

            return False, 0

        orderbook.set_retry_policy(allow_retry_once)

        # however, use private method, do not do this unless for testing

        orderbook._fetching = True

        asyncio.create_task(orderbook._fetch())

        # valid, but now is fetching
        orderbook.update(dict(
            U=11,
            u=12,
            a=[],
            b=[]
        ))

        # invalid, and it will also clean the previous one
        orderbook.update(dict(
            U=14,
            u=15,
            a=[[95, 1]],
            b=[]
        ))

        # orderbook will refetch
        # now the state reset to preset_10

        orderbook.update(dict(
            U=11,
            u=12,
            a=[a10],
            b=[]
        ))

        await orderbook.updated()

        assert_state_b()

        print('round seven: fetch abandon')

        # A server error on the depth request -> the snapshot fetch is abandoned.
        server.on_error('depth', code=-1000, msg='boom', status=500)

        orderbook.set_retry_policy(None)

        f = orderbook.updated()

        asyncio.create_task(orderbook._fetch())

        with pytest.raises(
            OrderBookFetchAbandonedException,
            match='abandoned'
        ):
            await f
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Spot docs step-4 pre-check: snapshot too old -> discard before merge.
#
# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
# (section "How to manage a local order book correctly", step 4):
#
#   "If the lastUpdateId from the snapshot is strictly less than the U from
#    step 2 [first buffered event], go back to step 3 [refetch snapshot]."
#
# The base class must NOT install the snapshot before validating step 4.
# Installing a known-too-old snapshot would briefly populate the book with
# bad state visible to callers.
# ---------------------------------------------------------------------------


def _no_retry(_info):
    """A retry policy that abandons immediately so ``@retry``-decorated calls
    re-raise the original exception on the first failure -- used by unit
    tests to assert raw ``_fetch_snapshot`` semantics."""
    return True, 0


def _allow_one_retry(info):
    """Retry once with no delay, abandon afterwards."""
    if info.fails > 1:
        return True, 0
    return False, 0


def _make_spot_orderbook_for_unit_test(retry_policy=_no_retry) -> SpotOrderBook:
    """Build a ``SpotOrderBook`` with no client attached.

    No client means ``set_client`` is not called, so the automatic snapshot
    fetch is not triggered.  Tests can then drive ``_fetch_snapshot`` themselves
    through a stub client.
    """
    return SpotOrderBook('BTCUSDT', retry_policy=retry_policy)


class _StubClient:
    """Drives ``_fetch_snapshot`` deterministically without a real WS-API."""

    def __init__(self, snapshots):
        # Each call to get_orderbook pops the next prepared snapshot.
        self._snapshots = list(snapshots)
        self.calls = 0

        class _Logger:
            def error(self, *_args, **_kwargs) -> None:
                pass

        self.logger = _Logger()

    async def get_orderbook(self, *, symbol, limit):  # noqa: ARG002 - signature parity
        self.calls += 1
        return self._snapshots.pop(0)


@pytest.mark.asyncio
async def test_spot_orderbook_step4_snapshot_too_old_discards_before_merge():
    """Step 4: snapshot.lastUpdateId < first buffered U -> discard, do NOT merge.

    Setup:
      * Buffer a diff event with U=100, u=110.
      * Provide a snapshot with lastUpdateId=50  (50 < 100 -> step-4 violation).

    Expected:
      * SnapshotTooOldException is raised by _fetch_snapshot.
      * The book is NEVER populated with the too-old snapshot
        (asks/bids stay empty; _last_update_id stays 0).
      * The buffered queue is cleared (the original buffered event is gone --
        on a refetch the client will receive fresh events from the WS stream).
    """
    # _no_retry abandons after the first failure -> the original
    # SnapshotTooOldException propagates out unwrapped.
    orderbook = _make_spot_orderbook_for_unit_test()

    # Simulate the buffering step: a diff event arrived while fetching.
    orderbook._unsolved_queue.append(dict(U=100, u=110, a=[[101, 1]], b=[]))

    orderbook._client = _StubClient([
        dict(lastUpdateId=50, asks=[[200, 1]], bids=[[199, 1]]),
    ])

    with pytest.raises(SnapshotTooOldException) as exc_info:
        await orderbook._fetch_snapshot()

    # Exception carries the diagnostic details required to debug stuck books.
    assert exc_info.value.symbol == 'BTCUSDT'
    assert exc_info.value.snapshot_last_update_id == 50
    assert exc_info.value.first_buffered_U == 100
    # Make sure __str__ is exercised (matches the human-readable message).
    msg = str(exc_info.value)
    assert 'BTCUSDT' in msg
    assert '50' in msg
    assert '100' in msg

    # The book must be untouched: no bad data installed.
    assert orderbook.asks == []
    assert orderbook.bids == []
    assert orderbook._last_update_id == 0

    # The stale buffered event is gone -- step 4 requires a *fresh* fetch and
    # any subsequently arriving events; replaying old ones against a new
    # snapshot is meaningless.
    assert orderbook._unsolved_queue == []


@pytest.mark.asyncio
async def test_spot_orderbook_step4_recovers_via_retry():
    """The retry policy MUST catch SnapshotTooOldException and refetch.

    Flow:
      * First fetch returns lastUpdateId=50 (too old) -> step-4 raises.
      * Retry policy allows one retry; second fetch returns lastUpdateId=105
        which is >= the first buffered U (100) so step 4 passes.
      * Book state is taken from the *new* snapshot; the original buffered
        event was already discarded by step-4 (per the docs).
    """
    orderbook = _make_spot_orderbook_for_unit_test(_allow_one_retry)

    # Pre-populate the buffer with a diff event.
    orderbook._unsolved_queue.append(dict(U=100, u=110, a=[[101, 1]], b=[]))

    orderbook._client = _StubClient([
        # First call -> too old (50 < 100).
        dict(lastUpdateId=50, asks=[[200, 1]], bids=[[199, 1]]),
        # Second call -> covers the buffered event (105 >= 100).
        dict(lastUpdateId=105, asks=[[100, 5]], bids=[[99, 5]]),
    ])

    await orderbook._fetch_snapshot()

    # After the retry, the book reflects the *new* snapshot.  The buffered
    # event (U=100, u=110) was dropped on the first attempt (per the docs:
    # "go back to step 3" implies starting fresh, not replaying the old
    # buffered events against the new snapshot).
    assert orderbook._last_update_id == 105
    assert orderbook.asks == [[100, 5]]
    assert orderbook.bids == [[99, 5]]
    # The stub client served exactly two snapshots (one too-old + one good).
    assert orderbook._client.calls == 2


@pytest.mark.asyncio
async def test_spot_orderbook_step4_passes_when_snapshot_covers_buffered():
    """When lastUpdateId >= first buffered U, step 4 passes and merge proceeds."""
    orderbook = _make_spot_orderbook_for_unit_test()

    # Buffered event: U=100, u=110.
    orderbook._unsolved_queue.append(dict(U=100, u=110, a=[[101, 1]], b=[]))

    orderbook._client = _StubClient([
        # lastUpdateId=105 >= U=100 -> step-4 OK; the buffered event will be
        # applied since U=100 <= 105+1 = 106.
        dict(lastUpdateId=105, asks=[[100, 5]], bids=[[99, 5]]),
    ])

    await orderbook._fetch_snapshot()

    # Buffered event was applied -> last update id advanced to 110.
    assert orderbook._last_update_id == 110
    assert orderbook._unsolved_queue == []
    # asks include the snapshot's [100, 5] and the diff's [101, 1].
    assert [100, 5] in orderbook.asks
    assert [101, 1] in orderbook.asks


@pytest.mark.asyncio
async def test_spot_orderbook_step4_no_op_when_queue_empty():
    """Step 4 only applies when the buffer is non-empty.

    With an empty buffer, the first event has not yet been received, so the
    snapshot is installed unconditionally and the WS straddle is validated
    later by the subscription's first arriving diff.
    """
    orderbook = _make_spot_orderbook_for_unit_test()
    # No buffered events.
    assert orderbook._unsolved_queue == []

    orderbook._client = _StubClient([
        # Any lastUpdateId is fine; nothing to compare it against.
        dict(lastUpdateId=42, asks=[[100, 5]], bids=[[99, 5]]),
    ])

    await orderbook._fetch_snapshot()

    assert orderbook._last_update_id == 42
    assert orderbook.asks == [[100, 5]]
    assert orderbook.bids == [[99, 5]]


@pytest.mark.asyncio
async def test_spot_orderbook_step4_equal_ids_is_acceptable():
    """The docs say *strictly less* triggers refetch.

    A snapshot whose ``lastUpdateId`` exactly equals the first buffered ``U``
    is acceptable: the buffered event picks up at ``current_last + 1`` so
    ``U == lastUpdateId`` means the diff was generated immediately after the
    snapshot and bridges into the live stream.

    (Equality is acceptable for the step-4 gate; per-event ``U <= last+1``
    validation in ``_update`` then governs the merge itself.)
    """
    orderbook = _make_spot_orderbook_for_unit_test()
    # Buffered event with U == lastUpdateId.
    orderbook._unsolved_queue.append(dict(U=100, u=110, a=[[101, 1]], b=[]))

    orderbook._client = _StubClient([
        dict(lastUpdateId=100, asks=[[100, 5]], bids=[[99, 5]]),
    ])

    await orderbook._fetch_snapshot()

    # The buffered event (U=100, u=110) satisfies U <= last+1 (100 <= 101),
    # so it should be applied; final last_update_id is 110.
    assert orderbook._last_update_id == 110
    assert orderbook._unsolved_queue == []


def test_orderbook_set_limit_docstring_distinguishes_spot_and_futures_caps():
    """The Binance depth endpoint accepts different ``limit`` shapes per
    market.  Per developers.binance.com:

    * Spot WebSocket-API ``depth``: 1–5000, default 100; weight scales with
      bracket; Binance hard-caps at 5000.
    * UM REST ``/fapi/v1/depth``: discrete values
      ``{5, 10, 20, 50, 100, 500, 1000}``; max 1000.
    * CM REST ``/dapi/v1/depth``: same discrete values
      ``{5, 10, 20, 50, 100, 500, 1000}``; max 1000.

    The previous docstring claimed "max 5000 (any value; Binance caps at
    5000)" — only true for Spot — which would silently mislead any user
    pointing the handler at UM/CM (where the server rejects out-of-set
    values).  Pin the market-aware wording.
    """
    import inspect
    from binance.core.orderbook import OrderBook
    doc = inspect.getdoc(OrderBook.set_limit) or ''
    assert 'Spot' in doc
    assert '5000' in doc
    assert 'Futures' in doc
    assert '{5, 10, 20, 50, 100, 500, 1000}' in doc
    assert '1000' in doc


def test_orderbook_handler_orderbook_docstring_distinguishes_spot_and_futures_caps():
    """``OrderBookHandlerBase.orderbook(..., limit=...)`` carries the same
    cap claim — pin the same market-aware wording so handler-level users
    see the cap difference too.
    """
    import inspect
    from binance import OrderBookHandlerBase
    doc = inspect.getdoc(OrderBookHandlerBase.orderbook) or ''
    assert 'Spot' in doc
    assert '5000' in doc
    assert 'Futures' in doc
    assert '{5, 10, 20, 50, 100, 500, 1000}' in doc


def test_orderbook_handler_init_docstring_distinguishes_spot_and_futures_caps():
    """``OrderBookHandlerBase.__init__`` ``limit`` argument docs must also
    carry the market-aware cap.  Pin the same wording.
    """
    import inspect
    from binance import OrderBookHandlerBase
    doc = inspect.getdoc(OrderBookHandlerBase) or ''
    assert 'Spot' in doc
    assert '5000' in doc
    assert 'Futures' in doc
    assert '{5, 10, 20, 50, 100, 500, 1000}' in doc
