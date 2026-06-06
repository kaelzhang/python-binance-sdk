"""Market-agnostic local order book.

This module defines :class:`OrderBook`, the abstract base for every market's
managed local order book. The class encapsulates the *common* lifecycle of a
locally maintained Binance order book:

* fetching the initial depth snapshot via the bound client
  (``client.get_orderbook(symbol=..., limit=...)``);
* buffering live diff events while the snapshot is in flight;
* applying ready diff events to the local bid / ask ladders;
* re-fetching the snapshot when an out-of-band update is detected.

The *market-specific* sequence-id validation algorithm — the rule that decides
whether a given diff payload can be merged on top of the current state — is
deliberately left :py:func:`abstractmethod`. Concrete subclasses
(:class:`~binance.spot.orderbook.SpotOrderBook` and the upcoming
``FuturesOrderBook``) implement ``_update`` to encode their venue's rules:

* **Spot** uses the ``U`` / ``u`` rule: a diff is acceptable when
  ``U <= last_update_id + 1``.
* **Futures** additionally validates the ``pu`` chain field — each diff event
  must reference the ``u`` of the *previous* event applied to the local book,
  forming a strict chain.

Per-market wiring of the concrete subclass happens through
:attr:`~binance.core.market.MarketSpec.orderbook_impl`, which the
:class:`~binance.core.handlers.orderbook.OrderBookHandlerBase` reads at handler
attach time so that a single, market-agnostic handler can produce the correct
order book implementation for whatever client it is registered with.
"""

import asyncio
from abc import ABC, abstractmethod
from asyncio import Future
from concurrent.futures import Future as ConcurrentFuture
from threading import RLock
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional
)

from aioretry import (
    retry,
    RetryPolicy
)

from binance.core.common.sequenced_list import (
    SequencedList,
    Pair
)
from binance.core.common.constants import (
    DEFAULT_DEPTH_LIMIT,
    DEFAULT_RETRY_POLICY,
    NO_RETRY_POLICY
)

from binance.core.common.utils import (
    normalize_symbol,
    create_future,
    format_msg,
    repr_exception
)
from binance.core.common.exceptions import (
    OrderBookFetchAbandonedException,
    SnapshotTooOldException,
)
from binance.core.orderbook_buffer import OrderBookBufferPolicy

# Diff-event field keys.  Identical on every Binance market venue (Spot,
# USDⓈ-M, COIN-M) per the public WebSocket reference documentation.
KEY_FIRST_UPDATE_ID = 'U'
KEY_LAST_UPDATE_ID = 'u'

# Snapshot field keys returned by the depth endpoint (REST on futures, WS-API
# on spot).  Identical across all markets.
KEY_REST_LAST_UPDATE_ID = 'lastUpdateId'
KEY_REST_BIDS = 'bids'
KEY_REST_ASKS = 'asks'

# Diff-event bid / ask payload keys.  Identical across all markets.
KEY_BIDS = 'b'
KEY_ASKS = 'a'


class OrderBook(ABC):
    """Abstract local order book for a single symbol, kept in sync with Binance.

    Maintains two ``SequencedList`` objects -- ``bids`` and ``asks`` -- that
    reflect the current state of the Binance order book for the configured
    symbol.  On initialisation (or whenever synchronisation is lost) the book
    automatically fetches a fresh depth snapshot via the associated ``Client``
    (``client.get_orderbook``), then applies any buffered diff events that have
    accumulated while the snapshot was in flight.

    The market-agnostic lifecycle (snapshot fetch, buffering, re-fetch on
    sync loss, futures-style emit semantics) lives in this class.  The
    venue-specific *sequence-id validation* rule, however, varies between Spot
    (uses ``U`` / ``u``) and Futures (additionally validates ``pu``), so the
    actual per-event merge decision is delegated to :meth:`_update`, which
    subclasses MUST implement.

    Subclasses should additionally override :meth:`_fetch_snapshot` only if
    their snapshot transport differs from the default ``client.get_orderbook``
    call (e.g. a market that needs a REST-only snapshot rather than a WS-API
    one but still exposes the same ``get_orderbook`` signature on its client
    does *not* need to override this method).

    You normally create a concrete ``OrderBook`` subclass indirectly through
    ``OrderBookHandlerBase.orderbook(symbol)`` rather than instantiating it
    directly.  If you do instantiate it directly, call ``set_client`` with a
    connected ``Client`` instance so the automatic snapshot fetching can work.

    Attributes:
        asks (SequencedList): Current ask levels, ordered ascending by price;
            the best (lowest) ask is ``asks[0]``.
        bids (SequencedList): Current bid levels, ordered ascending by price;
            the best (highest) bid is ``bids[-1]``.
    """

    asks: SequencedList
    bids: SequencedList
    _retry_policy: RetryPolicy
    _limit: int
    _last_update_id: int
    __updated_future: Optional[Future]
    _loop: Optional[asyncio.AbstractEventLoop]

    # We redundant define the default value of limit,
    #   because OrderBook is also a public class
    def __init__(
        self,
        symbol: str,
        client=None,
        limit: int = DEFAULT_DEPTH_LIMIT,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    ) -> None:
        self.asks = SequencedList()
        self.bids = SequencedList()

        self._symbol = normalize_symbol(symbol, True)
        self._client = None

        self._last_update_id = 0
        # The queue to save messages that are not continuous
        self._unsolved_queue: List[Dict[str, Any]] = []
        self._onchange_callbacks = None
        self.__updated_future = None

        # Whether we are still fetching the depth snapshot
        self._fetching = False
        self._state_lock = RLock()
        self._loop = None
        self._buffer_policy = OrderBookBufferPolicy()

        self.set_retry_policy(retry_policy)
        self.set_limit(limit)
        self.set_client(client)

    @property
    def _updated_future(self) -> Future:
        """Internal: the pending future resolved on the next book update.

        Lazily created and cached; ``updated()`` awaits it and ``_emit_updated``
        resolves it then swaps in a fresh one. Private -- consumers should use
        the public ``updated()`` coroutine instead.
        """
        future = self.__updated_future
        if future is None:
            future = create_future()
            self.__updated_future = future

        return future

    def _capture_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return self._loop
        self._loop = loop
        return loop

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = self._capture_loop()
        if loop is None or loop.is_closed():
            raise RuntimeError('orderbook needs a running event loop')
        return loop

    def _is_owning_loop_thread(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    @property
    def ready(self) -> bool:
        """bool: Whether the orderbook is updated. `False` indicates that the orderbook has not been initialized yet or is still fetching new snapshot.

        Most usually, you should not rely on this property or polling the value of this property. `await orderbook.updated()` is recommended for this scenario.
        """
        with self._state_lock:
            return not self._fetching and self._last_update_id != 0

    async def updated(self) -> None:
        """Await for the NEXT time when the orderbook is updated.

        Awaiting for this method is the recommended way to notify your program to do something when the orderbook changes::

            while True:
                await orderbook.updated()
                await doSomethingWith(order)

        Another use case is that if we want to do something only if the orderbook has finished initialization::

            if not orderbook.ready:
                await orderbook.updated()

            await doSomething(order)
        """

        await self._updated_future

    def set_retry_policy(
        self,
        retry_policy: Optional[RetryPolicy] = None
    ) -> None:
        """Sets the retry policy for the orderbook.

        Args:
            retry_policy (Callable): the function retry policy
        """

        if retry_policy is None:
            retry_policy = NO_RETRY_POLICY

        self._retry_policy = retry_policy

    def set_limit(
        self,
        limit: int
    ) -> None:
        """Set the depth-snapshot limit (number of price levels to fetch).

        This controls how many price levels are requested via the depth
        endpoint when (re-)initialising the order book.  The accepted shape
        differs by market per developers.binance.com:

        * **Spot** WebSocket-API ``depth``: any integer ``1``–``5000``
          (5000 hard cap; server caps at 5000).  Default per docs: 100.
        * **Futures** REST ``/fapi/v1/depth`` (UM) and ``/dapi/v1/depth``
          (CM): discrete value from ``{5, 10, 20, 50, 100, 500, 1000}``;
          max 1000.  Non-listed values are rejected by the server.

        The SDK default is ``DEFAULT_DEPTH_LIMIT`` (1000), valid on every
        market.

        Args:
            limit (int): Number of price levels per side to include in each
                depth snapshot request.  Spot accepts 1–5000 (hard cap
                5000); Futures (UM + CM) accepts one of the discrete values
                ``{5, 10, 20, 50, 100, 500, 1000}`` (max 1000).

        Docs:
        - Spot: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/market-data-requests
        - UM:   https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
        - CM:   https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Order-Book
        """
        self._limit = limit

    def set_client(self, client) -> None:
        """Attach a ``Client`` instance and trigger an initial depth snapshot fetch.

        Once a client is set the order book begins fetching its initial depth
        snapshot automatically if it is not already ``ready``.  This method is
        called automatically by ``OrderBookHandlerBase`` when a client is
        registered with the handler; you only need to call it directly if you
        are managing an ``OrderBook`` outside of a handler.

        Passing ``None`` or a falsy value is a no-op.

        Args:
            client: A connected ``binance.Client`` instance used to fetch the
                depth snapshot (via ``get_orderbook``).
        """
        if not client:
            return

        self._capture_loop()
        self._client = client

        if not self.ready:
            self._start_fetching()

    def _emit_updated(self, exc: Optional[Exception] = None) -> None:
        with self._state_lock:
            fetching = self._fetching

        if fetching:
            # If the orderbook is still fetching,
            # which means the orderbook is not completely updated,
            # we will not emit the event
            return

        if not self._is_owning_loop_thread():
            self._get_loop().call_soon_threadsafe(self._emit_updated_in_loop, exc)
            return

        self._emit_updated_in_loop(exc)

    def _emit_updated_in_loop(self, exc: Optional[Exception] = None) -> None:
        if exc is None:
            self._updated_future.set_result(None)
        else:
            self._updated_future.set_exception(exc)

        self.__updated_future = create_future()

    @retry('_retry_policy')
    async def _fetch_snapshot(self):
        """Fetch the depth snapshot and apply any buffered diff events.

        The default implementation calls ``self._client.get_orderbook`` which
        is provided by every market client.  Markets whose snapshot transport
        diverges from this signature may override.

        Step 4 of the Spot "How to manage a local order book correctly"
        procedure (`developers.binance.com
        <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams>`__)
        is enforced *before* the snapshot is installed: if the snapshot's
        ``lastUpdateId`` is strictly less than the ``U`` of the first
        buffered diff event, the snapshot is too old to bridge to the live
        stream.  In that case the buffered queue is cleared (those events
        will be lost; the next refetch starts a fresh straddle) and
        :class:`SnapshotTooOldException` is raised so the configured retry
        policy refetches.  This guarantees callers never observe a
        transiently-bad book state.
        """
        snapshot = await self._client.get_orderbook(
            symbol=self._symbol,
            limit=self._limit
        )

        self._capture_loop()
        return await asyncio.to_thread(self._install_snapshot, snapshot)

    def _install_snapshot(self, snapshot) -> bool:
        snapshot_last_update_id = snapshot[KEY_REST_LAST_UPDATE_ID]

        with self._state_lock:
            return self._install_snapshot_locked(snapshot, snapshot_last_update_id)

    def _install_snapshot_locked(self, snapshot, snapshot_last_update_id) -> bool:
        # Docs step 4: snapshot must cover the first buffered event.
        #
        # On Spot the rule is ``snapshot.lastUpdateId >= first_buffered.U``;
        # if that fails the snapshot is too old to bridge into the live diff
        # stream and we MUST refetch.  Futures uses the straddle rule
        # (``U <= snapshot.lastUpdateId <= u``) which is enforced separately
        # by ``FuturesOrderBook._update`` once the first post-snapshot event
        # arrives, but the spot pre-condition is still a useful sanity check
        # (a snapshot strictly older than the first buffered event cannot
        # straddle it either).  Apply it uniformly here.
        if self._unsolved_queue:
            first_U = self._unsolved_queue[0][KEY_FIRST_UPDATE_ID]
            if snapshot_last_update_id < first_U:
                # Drop the stale buffered events: on a refetch the WS stream
                # will deliver fresh ones, and replaying the old buffer
                # against the new snapshot is ambiguous (per the docs we
                # "go back to step 3" -- i.e. restart the buffering).
                self._unsolved_queue.clear()
                raise SnapshotTooOldException(
                    self._symbol,
                    snapshot_last_update_id,
                    first_U
                )

        self.asks.clear()
        self.bids.clear()

        self._merge(
            snapshot_last_update_id,
            snapshot[KEY_REST_ASKS],
            snapshot[KEY_REST_BIDS]
        )

        if len(self._unsolved_queue) == 0:
            return True

        counter = 0
        for payload in self._unsolved_queue:
            updated = self._update(payload)

            counter += 1

            if not updated:
                # If the current item is invalid,
                #   then remove the current item and all previous items
                del self._unsolved_queue[:counter]
                raise RuntimeError('fails to merge')

        self._unsolved_queue.clear()
        return True

    async def _fetch(self) -> None:
        """Should not be invoked directly by user, except for testing purpose
        """

        exception = None

        try:
            await self._fetch_snapshot()
        except Exception as e:
            exception = OrderBookFetchAbandonedException(
                self._symbol,
                e
            )

        with self._state_lock:
            self._fetching = False
        self._emit_updated(exception)

    def _mark_fetching_started(self) -> bool:
        with self._state_lock:
            if self._fetching:
                return False
            self._fetching = True
            return True

    def _start_fetching(self) -> None:
        if not self._mark_fetching_started():
            return

        loop = self._get_loop()
        task: Any
        if self._is_owning_loop_thread():
            task = loop.create_task(self._fetch())
        else:
            task = asyncio.run_coroutine_threadsafe(self._fetch(), loop)
        # Add exception handler to prevent "Future exception was never retrieved" warnings
        task.add_done_callback(self._handle_fetch_exception)

    def _handle_fetch_exception(
        self,
        task: Future | ConcurrentFuture
    ) -> None:
        """Handle exceptions from fetch task to prevent 'Future exception was never retrieved' warnings"""

        if task.cancelled():
            return

        # Retrieve the exception if the task failed
        exception = task.exception()
        if exception is not None and self._client is not None:
            # Log the error but don't re-raise as this is a background task
            self._client.logger.error(
                format_msg(
                    'Fetch task failed with exception: %s',
                    repr_exception(exception)
                )
            )

    async def fetch(self) -> None:
        """Manually fetches the new snapshot. Most usually, you should not call this method directly.

        However, this method is for testing purpose mainly.
        """
        if self._mark_fetching_started():
            await self._fetch()

    def _merge(
        self,
        last_update_id: int,
        asks: Iterable[Pair],
        bids: Iterable[Pair]
    ) -> None:
        self._last_update_id = last_update_id
        self.asks.merge(asks)
        self.bids.merge(bids)

    def update(self, payload) -> bool:
        """Applies the `depthUpdate` message to the orderbook. Most usually, you should not call this method directly, unless you want to manage the orderbook manually yourself. This method is called by `OrderBookHandlerBase` internally if the orderbook is created by a instance of `OrderBookHandlerBase`.

        Args:
            payload (dict): the message payload

        Returns:
            bool: `True` if the payload is ok to update into the orderbook, otherwise `False`
        """
        with self._state_lock:
            if self._fetching:
                # If fetching is not completed, we should not merge orderbook,
                # We put the payload into the queue and will **try** to merge the
                #   payload into orderbook
                self._buffer_policy.append(self._unsolved_queue, payload)
                return False

            updated = self._update(payload)

        if not updated:
            self._start_fetching()

        return updated

    @abstractmethod
    def _update(self, payload) -> bool:
        """Apply a *ready-to-process* diff payload to the local book.

        Concrete subclasses MUST implement this method, encoding their venue's
        sequence-id validation rule.  ``update()`` only calls this when the
        book is not fetching, so implementations may assume no snapshot is in
        flight.

        Args:
            payload: the diff event payload.

        Returns:
            bool: ``True`` when the diff is consistent with the local state
            (either already applied -- a no-op -- or successfully merged) so
            no resync is needed.  ``False`` to trigger a snapshot re-fetch.
        """
