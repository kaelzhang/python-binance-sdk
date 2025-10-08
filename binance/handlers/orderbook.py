import asyncio
from asyncio import Future
from typing import (
    Iterable,
    Optional
)

from aioretry import (
    retry,
    RetryPolicy
)

from binance.common.sequenced_list import (
    SequencedList,
    Pair
)
from binance.common.constants import (
    DEFAULT_DEPTH_LIMIT,
    DEFAULT_RETRY_POLICY,
    NO_RETRY_POLICY
)

from binance.common.utils import (
    normalize_symbol,
    create_future
)
from binance.common.exceptions import OrderBookFetchAbandonedException

KEY_FIRST_UPDATE_ID = 'U'
KEY_LAST_UPDATE_ID = 'u'

KEY_REST_LAST_UPDATE_ID = 'lastUpdateId'
KEY_REST_BIDS = 'bids'
KEY_REST_ASKS = 'asks'

KEY_BIDS = 'b'
KEY_ASKS = 'a'


class OrderBook:
    asks: SequencedList
    bids: SequencedList
    _retry_policy: RetryPolicy
    _limit: int
    _last_update_id: int
    __updated_future: Optional[Future]

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
        self._unsolved_queue = []
        self._onchange_callbacks = None
        self.__updated_future = None

        # Whether we are still fetching the depth snapshot
        self._fetching = False

        self.set_retry_policy(retry_policy)
        self.set_limit(limit)
        self.set_client(client)

    @property
    def _updated_future(self) -> Future:
        future = self.__updated_future
        if future is None:
            future = create_future()
            self.__updated_future = future

        return future

    @property
    def ready(self) -> bool:
        """bool: Whether the orderbook is updated. `False` indicates that the orderbook has not been initialized yet or is still fetching new snapshot.

        Most usually, you should not rely on this property or polling the value of this property. `await orderbook.updated()` is recommended for this scenario.
        """
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
        self._limit = limit

    def set_client(self, client) -> None:
        if not client:
            return

        self._client = client

        if not self.ready:
            self._start_fetching()

    def _emit_updated(self, exc: Optional[Exception] = None) -> None:
        if self._fetching:
            # If the orderbook is still fetching,
            # which means the orderbook is not completely updated,
            # we will not emit the event
            return

        if exc is None:
            self._updated_future.set_result(None)
        else:
            self._updated_future.set_exception(exc)

        self.__updated_future = create_future()

    @retry('_retry_policy')
    async def _fetch_snapshot(self):
        snapshot = await self._client.get_orderbook(
            symbol=self._symbol,
            limit=self._limit
        )

        self.asks.clear()
        self.bids.clear()

        self._merge(
            snapshot[KEY_REST_LAST_UPDATE_ID],
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

        self._fetching = False
        self._emit_updated(exception)

    def _start_fetching(self) -> None:
        if not self._fetching:
            self._fetching = True
            task = asyncio.create_task(self._fetch())
            # Add exception handler to prevent "Future exception was never retrieved" warnings
            task.add_done_callback(self._handle_fetch_exception)

    def _handle_fetch_exception(self, task):
        """Handle exceptions from fetch task to prevent 'Future exception was never retrieved' warnings"""
        try:
            # Retrieve the exception if the task failed
            exception = task.exception()
            if exception is not None:
                # Log the error but don't re-raise as this is a background task
                import logging
                logger = logging.getLogger(__name__)
                logger.error(
                    f'Fetch task failed with exception: {exception}'
                )
        except asyncio.CancelledError:
            # Task was cancelled, which is expected during cleanup
            pass
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f'Error handling fetch task exception: {e}'
            )

    async def fetch(self) -> None:
        """Manually fetches the new snapshot. Most usually, you should not call this method directly.

        However, this method is for testing purpose mainly.
        """
        if not self._fetching:
            self._fetching = True
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
        if self._fetching:
            # If fetching is not completed, we should not merge orderbook,
            # We put the payload into the queue and will **try** to merge the
            #   payload into orderbook
            self._unsolved_queue.append(payload)
            return False

        updated = self._update(payload)

        if not updated:
            self._start_fetching()

        return updated

    # Returns whether the payload is updated
    def _update(self, payload) -> bool:
        first = payload[KEY_FIRST_UPDATE_ID]
        last = payload[KEY_LAST_UPDATE_ID]
        current_last = self._last_update_id

        if last <= current_last:
            # abandon the payload,
            #   but however it is ok, it does not ruin the orderbook
            return True

        if first <= self._last_update_id + 1:
            # It is ok, just merge
            self._merge(last, payload[KEY_ASKS], payload[KEY_BIDS])
            self._emit_updated()
            return True

        return False
