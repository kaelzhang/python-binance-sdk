from typing import Optional

from aioretry import RetryPolicy
from stock_pandas import StockDataFrame

from binance.common.constants import (
    STREAM_TYPE_MAP,
    DEFAULT_DEPTH_LIMIT,
    DEFAULT_RETRY_POLICY
)

from binance.common.utils import (
    normalize_symbol,
    wrap_coroutine
)

from binance.common.types import DictPayload

from .base import Handler

from .orderbook import (
    OrderBook,
    KEY_FIRST_UPDATE_ID,
    KEY_LAST_UPDATE_ID,
    KEY_BIDS,
    KEY_ASKS
)

KEY_SYMBOL = 's'

ORDER_BOOK_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    KEY_SYMBOL: 'symbol',
    KEY_FIRST_UPDATE_ID: 'first_update_id',
    KEY_LAST_UPDATE_ID: 'last_update_id'
}

ORDER_BOOK_COLUMNS = ORDER_BOOK_COLUMNS_MAP.keys()


def create_depth_df(depth_list: list):
    """Convert a raw ``[price, quantity]`` depth list into a ``StockDataFrame``."""
    return StockDataFrame([
        {'price': x[0], 'quantity': x[1]} for x in depth_list
    ])


METHOD_NAME_RECEIVE = 'receive'


class OrderBookHandlerBase(Handler):
    """Base handler for the ``SubType.ORDER_BOOK`` (full depth diff) stream.

    Manages a collection of per-symbol ``OrderBook`` instances that are kept
    continuously in sync with the exchange by consuming ``depthUpdate`` stream
    events and periodically re-fetching REST snapshots via the Binance client.

    Typical usage — obtain an ``OrderBook`` for the symbol you subscribed to,
    then await updates::

        handler = MyOrderBookHandler()
        client.handler(handler)
        await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

        book = handler.orderbook('BTCUSDT')
        while True:
            await book.updated()
            process(book.bids, book.asks)

    Optionally override ``receive(payload)`` to be notified of every raw
    ``depthUpdate`` event as well; if no ``receive`` override is provided only
    the ``OrderBook`` objects are maintained and no additional dispatch occurs.

    Args:
        limit (int): Depth snapshot size (number of price levels) to request
            from the REST endpoint when (re-)initialising an ``OrderBook``.
            Defaults to ``DEFAULT_DEPTH_LIMIT`` (100).
        retry_policy (RetryPolicy): Retry strategy used when a REST snapshot
            fetch fails.  Defaults to ``DEFAULT_RETRY_POLICY`` (bounded
            exponential back-off with jitter).
    """

    COLUMNS_MAP = ORDER_BOOK_COLUMNS_MAP
    COLUMNS = ORDER_BOOK_COLUMNS

    def __init__(
        self,
        limit: int = DEFAULT_DEPTH_LIMIT,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    ) -> None:
        super().__init__()

        self._limit = limit
        self._retry_policy = retry_policy

        self._orderbooks = {}

        self._uninit_orderbooks = []

        # If the current class has no `receive` method,
        #   the raw payload will not be dispatched to self.receive
        self._has_receive = hasattr(self.__class__, METHOD_NAME_RECEIVE)

    def _receive(
        self,
        payload: DictPayload
    ):
        info = super()._receive(payload)

        bids = create_depth_df(payload[KEY_BIDS])
        asks = create_depth_df(payload[KEY_ASKS])

        return info, [bids, asks]

    def orderbook(
        self,
        symbol: str,
        limit: Optional[int] = None
    ) -> OrderBook:
        """Gets (or lazily creates) the ``OrderBook`` for a symbol.

        Don't forget to also subscribe to the symbol's depth stream::

            book = handler.orderbook('BTCUSDT', limit=1000)
            await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

        Args:
            symbol (str): The symbol name.
            limit (:obj:`int`, optional): REST depth-snapshot size for THIS
                symbol's book, overriding the handler-level default. Only
                applied when the book is first created — call this before
                subscribing to choose a per-symbol depth (Binance accepts 5,
                10, 20, 50, 100, 500, 1000, 5000). Defaults to the handler's
                ``limit``.

        Returns:
            OrderBook: The orderbook for ``symbol``.
        """
        symbol = normalize_symbol(symbol)

        if symbol in self._orderbooks:
            return self._orderbooks[symbol]

        orderbook = OrderBook(
            symbol,
            limit=self._limit if limit is None else limit,
            retry_policy=self._retry_policy
        )

        if self._client:
            orderbook.set_client(self._client)
        else:
            self._uninit_orderbooks.append(orderbook)

        self._orderbooks[symbol] = orderbook

        return orderbook

    def set_client(
        self,
        client
    ) -> None:
        """Sets the client for the orderbook. Most usually, you should not call this method directly. This method is invoked by `OrderBookHandlerBase` internally.

        Args:
            client (Client): the client instance of binance sdk
        """
        super().set_client(client)

        if len(self._uninit_orderbooks) == 0:
            return

        for orderbook in self._uninit_orderbooks:
            orderbook.set_client(client)

        self._uninit_orderbooks.clear()

    async def receiveDispatch(self, payload) -> None:
        """Receives a `depthUpdate` stream message. Most usually, you should not call this method directly. This method is invoked by `OrderBookHandlerBase` internally.

        Args:
            payload: the message payload of the stream
        """
        self.orderbook(payload[KEY_SYMBOL]).update(payload)

        if self._has_receive:
            await wrap_coroutine(self.receive(payload))
