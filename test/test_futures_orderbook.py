"""Unit tests for :class:`binance.futures.orderbook.FuturesOrderBook`.

The futures ``OrderBook`` differs from the spot implementation in two ways:

1. Snapshot is fetched over REST (``GET /fapi/v1/depth`` or ``/dapi/v1/depth``)
   instead of over the WebSocket API.  Both transports share the same
   ``client.get_orderbook(symbol=, limit=)`` Python interface, so this test
   uses a tiny mock client whose ``get_orderbook`` coroutine returns a fake
   snapshot -- transport-level concerns belong in the endpoint tests.
2. Every diff event after the snapshot must satisfy the ``pu`` chain rule
   (``payload['pu'] == previous_event_u``) in addition to the standard
   ``U`` / ``u`` ordering.  This file exhaustively covers that rule.

The base ``OrderBook`` lifecycle (fetch retry / buffering / re-fetch loop) is
already covered by ``test_order_book.py``; here we only test what is *new*
in the futures subclass.
"""

from logging import getLogger
from typing import Any, Dict, List, Optional

import pytest

from binance.core.orderbook import OrderBook
from binance.futures.orderbook import (
    FuturesOrderBook,
    KEY_PREVIOUS_LAST_UPDATE_ID,
)


# ---------------------------------------------------------------------------
# Mock client
# ---------------------------------------------------------------------------


class _MockFuturesClient:
    """Tiny stand-in for ``UMFuturesClient`` / ``CMFuturesClient`` that only
    serves ``get_orderbook``.

    The futures subclass relies on the inherited base-class fetcher which
    calls ``client.get_orderbook(symbol=, limit=)``; we control the
    snapshot it returns by setting ``snapshot`` on the mock instance.  We
    also count how many times the coroutine was invoked so that re-fetch
    behaviour can be asserted.
    """

    logger = getLogger('test.futures.orderbook')

    def __init__(self, snapshot: Optional[Dict[str, Any]] = None) -> None:
        self.snapshot = snapshot or {
            'lastUpdateId': 100,
            'bids': [[99, 1], [98, 2]],
            'asks': [[101, 3], [102, 4]],
        }
        self.calls: List[Dict[str, Any]] = []

    async def get_orderbook(self, **kwargs):
        self.calls.append(kwargs)
        # Return a fresh copy so mutating the returned lists in the SUT does
        # not leak across test cases that share the mock snapshot reference.
        snap = self.snapshot
        return {
            'lastUpdateId': snap['lastUpdateId'],
            'bids': [list(p) for p in snap['bids']],
            'asks': [list(p) for p in snap['asks']],
        }


def _make_book() -> FuturesOrderBook:
    """Construct a bare ``FuturesOrderBook`` with no client attached.

    The base class's ``set_client`` schedules ``_fetch_snapshot`` as an
    ``asyncio.create_task``, which requires a running event loop.  Tests
    that exercise ``_update`` in isolation don't need an event loop, so we
    leave the client unset and poke ``_last_update_id`` /
    ``_post_snapshot_first`` directly via :func:`_seed`.
    """
    return FuturesOrderBook('BTCUSDT')


# ---------------------------------------------------------------------------
# Subclass relationship
# ---------------------------------------------------------------------------


def test_subclass_of_orderbook() -> None:
    """Sanity: the new concrete class is registered as an ``OrderBook``."""
    book = FuturesOrderBook('BTCUSDT')
    assert isinstance(book, OrderBook)


# ---------------------------------------------------------------------------
# _update algorithm -- exercised in isolation
#
# These tests directly poke ``_last_update_id`` and ``_post_snapshot_first``
# to set up the state we want to validate, without going through the full
# snapshot-fetch lifecycle.  This is the most direct way to assert the
# venue-specific rule encoded in ``_update``.
# ---------------------------------------------------------------------------


def _seed(book: FuturesOrderBook, last_update_id: int, *, post_snapshot_first: bool) -> None:
    """Place the book into the state that would normally follow a
    snapshot merge: ``_last_update_id`` set, no in-flight fetch."""
    book._last_update_id = last_update_id
    book._post_snapshot_first = post_snapshot_first
    book._fetching = False


@pytest.mark.asyncio
async def test_update_first_event_straddles_snapshot() -> None:
    """First event after snapshot applies when ``U <= lastUpdateId <= u``.

    The straddle condition is the futures-specific anchor that picks up the
    chain right at the snapshot boundary.  Marked ``async`` because
    successful merges call ``_emit_updated`` which lazily creates an
    ``asyncio.Future`` bound to the running loop.
    """
    book = _make_book()
    _seed(book, last_update_id=100, post_snapshot_first=True)

    payload = {
        'U': 95,
        'u': 110,
        'pu': 94,  # ignored on first event
        'b': [[99, 5]],
        'a': [[101, 0]],  # qty 0 deletes the level
    }
    assert book._update(payload) is True
    assert book._last_update_id == 110
    # The flag must clear so subsequent events use the pu chain.
    assert book._post_snapshot_first is False
    # Asks: 101 was deleted (qty 0); only the lower-priced 101 remains gone.
    # Bids: 99 quantity updated to 5.
    # We avoid asserting the full ladder here -- the merge semantics belong
    # in SequencedList's own tests.  Spot-checking the update id is enough.


def test_update_stale_event_skipped() -> None:
    """``u < lastUpdateId`` is a no-op success: the diff has already been
    superseded.  The book MUST NOT be marked out-of-sync, otherwise we'd
    re-fetch on every late event."""
    book = _make_book()
    _seed(book, last_update_id=200, post_snapshot_first=False)

    payload = {
        'U': 50, 'u': 100,  # entirely below the current state
        'pu': 49,
        'b': [], 'a': [],
    }
    assert book._update(payload) is True
    # Stale skip must NOT advance the chain.
    assert book._last_update_id == 200


@pytest.mark.asyncio
async def test_update_pu_chain_valid() -> None:
    """Subsequent event with ``pu == previous_u`` advances the chain.

    Marked ``async`` so the loop-bound future created by ``_emit_updated``
    has a loop to bind to.
    """
    book = _make_book()
    _seed(book, last_update_id=110, post_snapshot_first=False)

    payload = {
        'U': 111, 'u': 120, 'pu': 110,
        'b': [], 'a': [],
    }
    assert book._update(payload) is True
    assert book._last_update_id == 120


def test_update_pu_chain_broken() -> None:
    """``pu != previous_u`` is unrecoverable from streaming alone -- signal
    a resync to the caller by returning False."""
    book = _make_book()
    _seed(book, last_update_id=110, post_snapshot_first=False)

    payload = {
        'U': 111, 'u': 120,
        'pu': 109,  # off-by-one: chain broken
        'b': [], 'a': [],
    }
    assert book._update(payload) is False
    # Chain break must NOT advance _last_update_id; otherwise we'd lose
    # the resync trigger on the next call too.
    assert book._last_update_id == 110


def test_update_missing_pu_field_on_subsequent_event() -> None:
    """A non-first event missing ``pu`` is a protocol violation -- resync.

    We deliberately do NOT special-case absent ``pu`` as ``0`` or treat it
    as ``current_last``: that would silently mask malformed payloads.
    """
    # Sanity: the wire-field constant is exactly what the Binance docs
    # specify -- assert here so the constant rename would be caught.
    assert KEY_PREVIOUS_LAST_UPDATE_ID == 'pu'

    book = _make_book()
    _seed(book, last_update_id=110, post_snapshot_first=False)

    payload = {
        'U': 111, 'u': 120,
        # KEY_PREVIOUS_LAST_UPDATE_ID intentionally omitted
        'b': [], 'a': [],
    }
    assert book._update(payload) is False
    assert book._last_update_id == 110


def test_update_first_event_does_not_straddle() -> None:
    """First event that overshoots the snapshot (``U > lastUpdateId``) is
    a resync trigger: the gap between snapshot and first event cannot be
    bridged, so the chain anchor failed."""
    book = _make_book()
    _seed(book, last_update_id=100, post_snapshot_first=True)

    payload = {
        'U': 105,  # > 100 -- doesn't straddle
        'u': 110,
        'pu': 104,
        'b': [], 'a': [],
    }
    assert book._update(payload) is False
    # State must remain anchored to the snapshot so the resync can pick up
    # cleanly from the same anchor.
    assert book._last_update_id == 100
    assert book._post_snapshot_first is True


def test_update_first_event_undershoots() -> None:
    """First event whose ``u`` is below the snapshot id (``u < lastUpdateId``)
    falls through the stale-skip branch -- still a success (no-op), the
    chain remains unanchored, and we keep waiting for the straddle event."""
    book = _make_book()
    _seed(book, last_update_id=100, post_snapshot_first=True)

    payload = {
        'U': 50, 'u': 80,
        'pu': 49,
        'b': [], 'a': [],
    }
    assert book._update(payload) is True
    assert book._last_update_id == 100
    # The flag must remain True since we still need a straddle event.
    assert book._post_snapshot_first is True


# ---------------------------------------------------------------------------
# Snapshot lifecycle (integration with the inherited base-class machinery)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_then_first_then_chain_end_to_end() -> None:
    """Drive the full lifecycle through ``await book.fetch()`` so the
    interplay between ``_fetch_snapshot`` (which resets the post-snapshot
    flag) and ``_update`` (which consumes it) is exercised end-to-end."""
    client = _MockFuturesClient({
        'lastUpdateId': 1000,
        'bids': [[99, 1]],
        'asks': [[101, 1]],
    })
    # Build without a client so set_client doesn't schedule a background
    # fetch; we drive the fetch synchronously to avoid asyncio races.
    book = FuturesOrderBook('BTCUSDT')
    book._client = client
    book._fetching = True
    await book._fetch()

    assert book.ready
    assert book._last_update_id == 1000
    # No live events processed yet, so the post-snapshot flag is still set.
    assert book._post_snapshot_first is True

    # First post-snapshot event must straddle 1000.
    assert book.update({
        'U': 999, 'u': 1005, 'pu': 998,
        'b': [[99, 2]], 'a': [],
    }) is True
    assert book._last_update_id == 1005
    assert book._post_snapshot_first is False

    # Second event picks up via pu chain.
    assert book.update({
        'U': 1006, 'u': 1010, 'pu': 1005,
        'b': [], 'a': [[101, 0]],
    }) is True
    assert book._last_update_id == 1010


@pytest.mark.asyncio
async def test_refetch_resets_post_snapshot_flag() -> None:
    """A re-fetch must reset ``_post_snapshot_first`` so the NEXT live event
    is required to straddle the new snapshot id, not naively chain off the
    pre-resync state.  This is the property that distinguishes a real
    resync from a no-op."""
    client = _MockFuturesClient({
        'lastUpdateId': 500,
        'bids': [],
        'asks': [],
    })
    book = FuturesOrderBook('BTCUSDT')
    book._client = client
    book._fetching = True
    await book._fetch()
    assert book._post_snapshot_first is True

    # Advance the chain past the snapshot so we can detect the reset.
    assert book.update({
        'U': 499, 'u': 510, 'pu': 498,
        'b': [], 'a': [],
    }) is True
    assert book._post_snapshot_first is False
    assert book._last_update_id == 510

    # Repoint the snapshot and re-fetch.
    client.snapshot = {
        'lastUpdateId': 2000,
        'bids': [],
        'asks': [],
    }
    book._fetching = True
    await book._fetch()

    assert book._last_update_id == 2000
    # The flag must be reset; otherwise the next event would be evaluated
    # against the pu chain and skip the straddle anchor.
    assert book._post_snapshot_first is True

    # Any event that doesn't straddle 2000 -> resync.
    assert book.update({
        'U': 2100, 'u': 2200, 'pu': 1999,
        'b': [], 'a': [],
    }) is False
