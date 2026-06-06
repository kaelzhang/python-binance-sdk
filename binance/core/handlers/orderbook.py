"""Market-agnostic high-level order-book handler.

Wraps the per-market local-order-book lifecycle behind a single base class
that is identical for every venue. The concrete :class:`OrderBook`
implementation is injected by the client's
:attr:`~binance.core.market.MarketSpec.orderbook_impl`, so subscribing user
code (and all venue handler tests) sees one stable API:
``handler.orderbook(symbol)`` returns the right per-market book.

Because ``OrderBook`` is abstract and the choice of concrete subclass depends
on the bound client's market, books requested *before* the handler is bound
to a client cannot be constructed eagerly.  Instead, the handler returns a
small forwarding wrapper (``_PendingOrderBook``) that records the requested
``(symbol, limit)`` and -- once :meth:`OrderBookHandlerBase.set_client`
materialises the real :class:`OrderBook` via
:attr:`~binance.core.market.MarketSpec.orderbook_impl` -- transparently
forwards every attribute and method to the live instance.  This preserves
the historical behaviour that a reference captured *before*
``set_client`` keeps working *after* the handler is bound.
"""

from typing import Any, Dict, List, Optional

from aioretry import RetryPolicy
from volas import DataFrame

from binance.core.common.constants import (
    STREAM_TYPE_MAP,
    DEFAULT_DEPTH_LIMIT,
    DEFAULT_RETRY_POLICY
)

from binance.core.common.utils import (
    normalize_symbol,
    wrap_coroutine
)

from binance.core.common.types import DictPayload

from binance.core.handlers.base import Handler

from binance.core.orderbook import (
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
    """Convert a raw ``[price, quantity]`` depth list into a ``volas.DataFrame``."""
    return DataFrame({
        'price': [x[0] for x in depth_list],
        'quantity': [x[1] for x in depth_list],
    })


METHOD_NAME_RECEIVE = 'receive'


class _PendingOrderBook:
    """Forwarding wrapper for a book requested before a client is bound.

    Behaves as an opaque ``OrderBook`` reference: attribute access is
    forwarded to the real concrete ``OrderBook`` instance once
    :meth:`OrderBookHandlerBase.set_client` materialises it.  Until then,
    only ``_symbol`` and ``_limit`` are readable (used by tests that
    inspect the pre-client configuration).

    The wrapper deliberately does NOT subclass :class:`OrderBook`: the base
    is abstract and we don't know which concrete subclass to pick until the
    bound client's :class:`~binance.core.market.MarketSpec` becomes
    available.  Forwarding via ``__getattr__`` lets a single instance stand
    in for the eventual real book regardless of subclass.
    """

    __slots__ = ('_symbol', '_limit', '_real')

    def __init__(self, symbol: str, limit: int) -> None:
        # Note: a reference captured BEFORE ``set_client`` (this wrapper) and
        # one fetched AFTER (the real ``OrderBook``) are not ``is``-identical,
        # but both observe the same live state because the wrapper forwards
        # every attribute to ``_real`` once materialised. Two pre-``set_client``
        # calls for the same symbol still return the same wrapper, and two
        # post-``set_client`` calls still return the same real book.
        self._symbol = symbol
        self._limit = limit
        self._real: Optional[OrderBook] = None

    def _materialise(self, real: OrderBook) -> None:
        """Bind the real concrete ``OrderBook`` that this wrapper proxies for.

        Called from ``OrderBookHandlerBase.set_client`` once
        ``MarketSpec.orderbook_impl`` is known.
        """
        self._real = real

    def __getattr__(self, name: str) -> Any:
        # Reached only when the attribute is not in ``__slots__`` -- i.e.
        # for every ``OrderBook`` API (``ready``, ``updated``, ``asks``,
        # ``bids``, ``update``, ...).  We delegate to the real book once it
        # exists, and fail loudly otherwise to surface programming errors
        # (using the wrapper before ``set_client``).
        real = object.__getattribute__(self, '_real')
        if real is None:
            raise AttributeError(
                f'OrderBook attribute {name!r} accessed before the '
                'handler was bound to a client; call '
                '`client.handler(handler)` first.'
            )
        return getattr(real, name)


class OrderBookHandlerBase(Handler):
    """Base handler for the ``SubType.ORDER_BOOK`` (full depth diff) stream.

    Manages a collection of per-symbol ``OrderBook`` instances that are kept
    continuously in sync with the exchange by consuming ``depthUpdate`` stream
    events and periodically re-fetching depth snapshots via the Binance
    client.

    The concrete ``OrderBook`` subclass is chosen per-market: the handler
    looks up :attr:`~binance.core.market.MarketSpec.orderbook_impl` on the
    bound client's :attr:`~binance.core.client_base.BaseClient.MARKET` and
    uses that class to construct new books.  This lets one handler base
    serve every venue with no market-specific subclassing.

    Typical usage -- obtain an ``OrderBook`` for the symbol you subscribed
    to, then await updates::

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
            when (re-)initialising an ``OrderBook``.  Cap differs by market
            per developers.binance.com: Spot accepts 1–5000 (5000 hard cap);
            Futures (UM + CM) requires one of the discrete values
            ``{5, 10, 20, 50, 100, 500, 1000}`` (max 1000).  Defaults to
            ``DEFAULT_DEPTH_LIMIT`` (1000), which is valid on every market.
        retry_policy (RetryPolicy): Retry strategy used when a snapshot
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

        # Books that have a bound client are real ``OrderBook`` subclass
        # instances.  Books requested before ``set_client`` are
        # ``_PendingOrderBook`` wrappers; the handler swaps them for real
        # instances in-place inside ``set_client``.
        self._orderbooks: Dict[str, Any] = {}

        # Symbols requested via ``orderbook(symbol, limit)`` BEFORE the
        # handler is bound to a client.  The list holds the same
        # ``_PendingOrderBook`` objects stored under ``_orderbooks[symbol]``
        # so we can iterate them at ``set_client`` time without re-walking
        # the dict.
        self._uninit_orderbooks: List[_PendingOrderBook] = []

        # If the current class has no `receive` method,
        #   the raw payload will not be dispatched to self.receive
        self._has_receive = hasattr(self.__class__, METHOD_NAME_RECEIVE)

    def _receive(  # type: ignore[override]  # intentional narrowing: only dict payloads are valid for order book
        self,
        payload: DictPayload,
        index: Optional[List[int]] = None
    ) -> Any:
        info = super()._receive(payload, index)

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
                symbol's book, overriding the handler-level default.  Only
                applied when the book is first created -- call this before
                subscribing to choose a per-symbol depth.  Cap differs by
                market per developers.binance.com: Spot accepts 1–5000
                (5000 hard cap); Futures (UM + CM) requires one of the
                discrete values ``{5, 10, 20, 50, 100, 500, 1000}`` (max
                1000).  Defaults to the handler's ``limit``.

        Returns:
            OrderBook: The orderbook for ``symbol``.
        """
        symbol = normalize_symbol(symbol)

        if symbol in self._orderbooks:
            return self._orderbooks[symbol]

        resolved_limit = self._limit if limit is None else limit

        if self._client is None:
            # No client yet -> we don't know which concrete ``OrderBook``
            # subclass to construct.  Hand out a forwarding wrapper that
            # the handler swaps for a real book inside ``set_client``.
            pending = _PendingOrderBook(symbol, resolved_limit)
            self._orderbooks[symbol] = pending
            self._uninit_orderbooks.append(pending)
            return pending  # type: ignore[return-value]  # transparent wrapper

        orderbook = self._build_orderbook(symbol, resolved_limit)
        orderbook.set_client(self._client)
        self._orderbooks[symbol] = orderbook
        return orderbook

    def _build_orderbook(self, symbol: str, limit: int) -> OrderBook:
        """Construct the per-market ``OrderBook`` for ``symbol``.

        Looks up the concrete subclass via the bound client's
        ``MARKET.orderbook_impl``.  ``self._client`` MUST be set before
        calling this.
        """
        client = self._client
        assert client is not None, '_build_orderbook called before set_client'
        impl = client.MARKET.orderbook_impl
        return impl(
            symbol,
            limit=limit,
            retry_policy=self._retry_policy
        )

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

        pending = self._uninit_orderbooks
        self._uninit_orderbooks = []

        for placeholder in pending:
            orderbook = self._build_orderbook(
                placeholder._symbol, placeholder._limit
            )
            # Wire the real book first so the placeholder forwards
            # attribute access correctly; then attach the client (which
            # kicks off the initial snapshot fetch).
            placeholder._materialise(orderbook)
            self._orderbooks[placeholder._symbol] = orderbook
            orderbook.set_client(client)

    async def receiveDispatch(self, payload) -> None:
        """Receives a `depthUpdate` stream message. Most usually, you should not call this method directly. This method is invoked by `OrderBookHandlerBase` internally.

        Args:
            payload: the message payload of the stream
        """
        self.orderbook(payload[KEY_SYMBOL]).update(payload)

        if self._has_receive:
            await wrap_coroutine(self.receive(payload))
