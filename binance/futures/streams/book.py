"""Book ticker, partial depth, and diff depth stream handlers and processors.

Hosts ``BookTickerHandlerBase``, ``PartialOrderBookHandlerBase``,
``AllMarketBookTickerHandlerBase`` and their processors, plus the diff-depth
:class:`OrderBookProcessor` which routes events to the unified
:class:`~binance.core.handlers.orderbook.OrderBookHandlerBase`.  The
depth-parameter helpers and ``FUTURES_DEPTH_LEVELS`` /
``FUTURES_DEPTH_SPEEDS`` constants live in
:mod:`binance.futures.streams._common`.
"""

import re
from typing import ClassVar, List, Optional

from binance.core.common.constants import (
    SubType,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.common.utils import normalize_symbol
from binance.core.handlers.base import Handler
from binance.core.handlers.orderbook import OrderBookHandlerBase
from binance.core.orderbook import (
    KEY_BIDS,
    KEY_ASKS,
    KEY_LAST_UPDATE_ID,
)
from binance.core.processors.base import Processor

from binance.futures.streams._common import (
    _get_futures_depth_level,
    _get_futures_depth_speed,
)


# ---------------------------------------------------------------------------
# Futures BookTicker
# Confirmed fields per developers.binance.com (UM + CM, 2026-05):
#   e  event type ('bookTicker')
#   u  update id
#   E  event time
#   T  transaction time
#   s  symbol
#   b  best bid price
#   B  best bid quantity
#   a  best ask price
#   A  best ask quantity
# COIN-M additionally publishes 'ps' (pair); the CM-specific column map adds it
# (see binance.futures.cm.streams).  Routing across both per-symbol and
# all-market streams is by stream-name suffix so we accept both 'bookTicker'
# and shape-only payloads.
# Docs:
# - UM https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
# - CM https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
# ---------------------------------------------------------------------------

FUTURES_BOOK_TICKER_COLUMNS_MAP = {
    'e': 'type',
    'u': 'update_id',
    'E': 'event_time',
    'T': 'transaction_time',
    's': 'symbol',
    'b': 'best_bid_price',
    'B': 'best_bid_quantity',
    'a': 'best_ask_price',
    'A': 'best_ask_quantity',
}

FUTURES_BOOK_TICKER_COLUMNS = FUTURES_BOOK_TICKER_COLUMNS_MAP.keys()


class BookTickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.BOOK_TICKER`` (best bid/ask) stream.

    Shared across USDⓈ-M and COIN-M markets.  Per developers.binance.com the
    bookTicker payload carries ``e='bookTicker'`` as well as ``u`` (update_id),
    ``E`` (event_time), ``T`` (transaction_time), ``s`` (symbol), and the best
    bid/ask price + quantity pairs.  Processor dispatch still routes by
    stream-name suffix (``@bookTicker``) since the all-market variant
    (``!bookTicker``) shares the same event type.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
    """

    COLUMNS_MAP = FUTURES_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_BOOK_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# All-market: AllMarketBookTicker
# Wire stream: !bookTicker  (UM/CM; note: NO @arr suffix unlike Spot !bookTicker@arr)
# Delivers a bookTicker payload for any symbol whenever best bid/ask changes.
# ---------------------------------------------------------------------------

class AllMarketBookTickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.ALL_MARKET_BOOK_TICKER`` stream (``!bookTicker``).

    Shared by USDⓈ-M and COIN-M markets.  Delivers the best bid/ask for any symbol
    whenever it changes.  Note: the futures all-market book ticker stream name is
    ``!bookTicker`` (no ``@arr`` suffix), unlike Spot's deprecated ``!bookTicker@arr``.

    Per developers.binance.com the payload shape is identical to the per-symbol
    bookTicker (including ``e='bookTicker'``); processor dispatch matches by
    exact stream name.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    """

    COLUMNS_MAP = FUTURES_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_BOOK_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Futures Partial Order Book (depth snapshot)
# Confirmed fields per developers.binance.com (UM + CM, 2026-05):
#   { e, E, T, s, [ps], U, u, [pu], b, a }
# (distinct from the Spot REST-snapshot shape, which is
#  { lastUpdateId, bids, asks } - DO NOT confuse the two).
# b/a each: [ [price, quantity], ... ].  U/u/pu are first/final/previous-final
# update IDs (same diff-cursor semantics as the diff-depth stream).
# The handler surfaces ``u`` (final update id in event) as ``last_update_id``
# alongside the bids/asks dataframes so downstream consumers can reconcile
# the snapshot against the diff-depth stream's cursor.
# ---------------------------------------------------------------------------

FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP = {
    'price': 'price',
    'quantity': 'quantity',
}

FUTURES_PARTIAL_ORDER_BOOK_COLUMNS = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP.keys()


class PartialOrderBookHandlerBase(Handler):
    """Base handler for the futures ``SubType.PARTIAL_ORDER_BOOK`` (partial depth snapshot) stream.

    Shared across USDⓈ-M and COIN-M markets.  Per developers.binance.com the
    futures partial-depth payload exposes ``b`` and ``a`` arrays of
    ``[price, quantity]`` pairs plus ``U`` / ``u`` / ``pu`` update IDs
    (first / final / previous-final).  These are the SAME diff-cursor fields
    as the diff-depth stream — distinct from the Spot REST-snapshot shape
    (``lastUpdateId`` / ``bids`` / ``asks``); do NOT confuse the two.

    The internal ``_receive`` converts each side into a ``StockDataFrame``
    with ``price`` and ``quantity`` columns and surfaces ``u`` (the final
    update id in the event) as ``last_update_id`` so consumers can reconcile
    against the diff-depth cursor.  The handler returns a
    ``(last_update_id, bids_df, asks_df)`` tuple — same shape as the Spot
    PartialOrderBookHandlerBase.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
    """

    COLUMNS_MAP = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP
    COLUMNS = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        last_update_id = payload[KEY_LAST_UPDATE_ID]
        bids_data = [{'price': x[0], 'quantity': x[1]} for x in payload[KEY_BIDS]]
        asks_data = [{'price': x[0], 'quantity': x[1]} for x in payload[KEY_ASKS]]
        bids = super()._receive(bids_data, list(range(len(bids_data))) or [0])
        asks = super()._receive(asks_data, list(range(len(asks_data))) or [0])
        return last_update_id, bids, asks


# ---------------------------------------------------------------------------
# Futures Order Book (diff depth stream)
# Confirmed fields (UM + CM identical, 2026-05-26):
#   e  'depthUpdate'
#   E  event time
#   T  transaction time
#   s  symbol
#   U  first update id in event
#   u  final update id in event
#   pu previous final update id
#   b  bids to be updated [ [price, qty], ... ]
#   a  asks to be updated [ [price, qty], ... ]
#
# Diff events are routed to the unified
# :class:`~binance.core.handlers.orderbook.OrderBookHandlerBase` (via the
# processor below), which maintains a local order book using the per-market
# :class:`~binance.core.orderbook.OrderBook` subclass injected through
# ``MarketSpec.orderbook_impl`` (``FuturesOrderBook`` for both UM and CM).
# No raw diff-event handler is exposed: the user-facing API consumes the
# high-level local-orderbook view exclusively.
# ---------------------------------------------------------------------------


FUTURES_BOOK_TICKER_STREAM_SUFFIX = f'@{SubType.BOOK_TICKER}'


class BookTickerProcessor(Processor):
    """Processor for the futures book-ticker stream (``<symbol>@bookTicker``).

    Routing is by stream-name suffix because both the per-symbol and the
    all-market book-ticker streams share ``e='bookTicker'``; matching on the
    event type alone would also accept the all-market stream.
    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = BookTickerHandlerBase
    SUB_TYPE = SubType.BOOK_TICKER

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        # Only match per-symbol streams ending with '@bookTicker'.
        # All-market '!bookTicker' does NOT end with '@bookTicker', so no extra guard needed.
        if (
            stream_type is None
            or not stream_type.endswith(FUTURES_BOOK_TICKER_STREAM_SUFFIX)
        ):
            return False, None

        return True, msg.get(KEY_PAYLOAD)


FUTURES_PARTIAL_DEPTH_STREAM_PATTERN = re.compile(r'@depth\d+')


class PartialOrderBookProcessor(Processor):
    """Processor for the futures partial depth stream (``<symbol>@depth<N>[@speed]``).

    Routing is by stream-name pattern match (``@depth<N>`` + bids/asks payload check).
    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = PartialOrderBookHandlerBase
    SUB_TYPE = SubType.PARTIAL_ORDER_BOOK

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)
        payload = msg.get(KEY_PAYLOAD)

        if stream_type is None or not FUTURES_PARTIAL_DEPTH_STREAM_PATTERN.search(stream_type):
            return False, None

        if (
            payload is None
            or type(payload) is not dict
            or KEY_BIDS not in payload
            or KEY_ASKS not in payload
        ):
            return False, None

        return True, payload

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@depth<N>`` or ``<symbol>@depth<N>@<speed>ms``."""
        symbol = self._get_param_symbol(t, args)
        level = _get_futures_depth_level(t, args[1:])
        speed = _get_futures_depth_speed(t, args[2:])
        base = f'{normalize_symbol(symbol)}@depth{level}'
        if speed is not None:
            return f'{base}@{speed}ms'
        return base


class OrderBookProcessor(Processor):
    """Processor for the futures ``SubType.ORDER_BOOK`` (diff depth) stream.

    Routes diff events to the unified
    :class:`~binance.core.handlers.orderbook.OrderBookHandlerBase` which
    maintains a local order book using :class:`FuturesOrderBook` (the
    per-market sync algorithm injected via ``MarketSpec.orderbook_impl``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = OrderBookHandlerBase
    SUB_TYPE = SubType.ORDER_BOOK
    PAYLOAD_TYPE = 'depthUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@depth`` or ``<symbol>@depth@<speed>ms``."""
        symbol = self._get_param_symbol(t, args)
        speed = _get_futures_depth_speed(t, args[1:])
        base = f'{normalize_symbol(symbol)}@depth'
        if speed is not None:
            return f'{base}@{speed}ms'
        return base


FUTURES_ALL_MARKET_BOOK_TICKER_STREAM = '!bookTicker'


class AllMarketBookTickerProcessor(Processor):
    """Processor for the futures all-market book ticker stream (``!bookTicker``).

    Wire name is ``!bookTicker`` (no ``@arr`` suffix; distinct from Spot's deprecated
    ``!bookTicker@arr``).  Routing is by stream name since the per-symbol stream
    shares the same ``e='bookTicker'`` event type.
    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AllMarketBookTickerHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_BOOK_TICKER
    STREAM_TYPE_NAME: ClassVar[str] = FUTURES_ALL_MARKET_BOOK_TICKER_STREAM

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_BOOK_TICKER` expects no parameters'
            )
        return self.STREAM_TYPE_NAME
