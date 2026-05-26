"""Shared futures stream handlers and processors.

Contains the genuinely market-agnostic parts of the USDⓈ-M and COIN-M futures
stream handling: the base handler classes (column maps + flattening logic) and
their associated processors.

Key verified findings (2026-05-25):
- Both markets share the *same* ``markPriceUpdate`` event type.
- Both markets share the *same* ``forceOrder`` event type and nested ``o`` structure.
- USDⓈ-M markPrice payload includes ``ap`` (mark price moving average); COIN-M does NOT.
- COIN-M forceOrder nested ``o`` includes ``ps`` (pair); USDⓈ-M does NOT.

Key verified findings on shared stream schemas (2026-05-26):
- aggTrade: Futures adds field ``X`` (ignored/placeholder); does NOT have Spot-specific
  ``b``/``a`` (buyer/seller order id).  UM and CM share identical futures aggTrade schema.
- kline: Futures kline nested ``k`` object is identical in structure to Spot kline.
  However, futures klines include an extra ``ps`` (pair/symbol) field in UM;
  the shared base uses only the Spot-common fields (same as Spot ``KlineHandlerBase``).
- miniTicker / ticker: Futures 24hrMiniTicker and 24hrTicker payloads are identical
  in field structure to Spot equivalents.  Shared bases reuse Spot column maps.
- bookTicker: Futures bookTicker payloads have NO ``e`` event field (stream-name routing
  required).  Identical schema for UM and CM.
- depth (partial + diff): Futures depth stream payloads are structurally identical to
  Spot depth streams.  Update speed options differ (UM/CM: 100ms/500ms vs Spot 100ms/1000ms).
- continuousKline: Futures-specific stream; nested ``k`` object is identical to kline ``k``
  but the outer event has ``ps`` (pair) and ``ct`` (contract type) instead of ``s`` (symbol).
  Shared between UM and CM.
- contractInfo: Outer payload; same across UM and CM (contract spec change events).
- forceOrder all-market: ``!forceOrder@arr`` array; each element has the same nested ``o``
  structure as per-symbol forceOrder.  Shared between UM and CM.
- markPrice all-market: ``!markPrice@arr[@1s]`` array; each element is a markPriceUpdate dict.
  UM elements include ``ap``; CM elements do not -- handled via market-specific subclasses.

Therefore the shared base:
- ``MarkPriceHandlerBase``: exposes only the common fields; UM adds ``ap``, CM adds nothing extra.
- ``ForceOrderHandlerBase``: exposes only the common nested ``o`` fields; CM adds ``ps``.

The UM and CM modules provide *market-specific* column maps (via subclasses/overrides) while
inheriting the common ``_receive`` flattening logic from ``ForceOrderHandlerBase``.

Stream docs:
- UM Mark Price: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- CM Mark Price: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
- UM Force Order: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- CM Force Order: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- UM Agg Trade: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- CM Agg Trade: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- UM Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
- CM Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
- UM Continuous Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- CM Continuous Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- UM Mini Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
- UM Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
- UM Book Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- UM Partial Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- UM Diff Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- UM All Market Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
- UM All Book Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
- UM All Force Orders: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
- UM Contract Info: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
"""

import re
from typing import ClassVar, List, Optional

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    STREAM_OHLC_MAP,
    KLINE_TYPE_PREFIX,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor
from binance.core.common.utils import normalize_symbol


# ---------------------------------------------------------------------------
# Shared Mark Price base
# Common fields confirmed present in BOTH UM and CM (2026-05-25):
#   e  event type ('markPriceUpdate')
#   E  event time
#   s  symbol
#   p  mark price
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
#
# UM-only field: ap (mark price moving average)  — added by UMMarkPriceHandlerBase
# CM lacks 'ap' entirely.
# ---------------------------------------------------------------------------

# Common mark price fields shared by both markets.
MARK_PRICE_COLUMNS_MAP_BASE = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'p': 'mark_price',
    'i': 'index_price',
    'P': 'est_settle_price',
    'r': 'funding_rate',
    'T': 'next_funding_time',
}


class MarkPriceHandlerBase(Handler):
    """Base handler for the futures ``SubType.MARK_PRICE`` stream.

    Shared across USDⓈ-M (``UMFuturesClient``) and COIN-M (``CMFuturesClient``) markets.
    Covers the common payload fields: mark price, index price, estimated settlement price,
    funding rate, and next funding time.

    USDⓈ-M extends this with ``ap`` (mark price moving average) via the market-specific
    ``COLUMNS_MAP`` override in ``binance.futures.um.streams``.  COIN-M uses this base
    directly (COIN-M does not publish ``ap``).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusd_perp')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
    COLUMNS = MARK_PRICE_COLUMNS_MAP_BASE.keys()


# ---------------------------------------------------------------------------
# Shared Force Order (liquidation) base
# Common nested 'o' fields confirmed in BOTH UM and CM (2026-05-25):
#   e  event type ('forceOrder')
#   E  event time
#   o.s   symbol
#   o.S   side
#   o.o   order type
#   o.f   time in force
#   o.q   original quantity
#   o.p   price
#   o.ap  average price
#   o.X   order status
#   o.l   last filled quantity
#   o.z   accumulated filled quantity
#   o.T   order trade time
#
# CM-only nested field: o.ps (pair) — added in CMForceOrderHandlerBase
# UM lacks 'ps'.
# ---------------------------------------------------------------------------

# Common force order fields shared by both markets.
FORCE_ORDER_COLUMNS_MAP_BASE = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'S': 'side',
    'o': 'order_type',
    'f': 'time_in_force',
    'q': 'orig_quantity',
    'p': 'price',
    'ap': 'avg_price',
    'X': 'order_status',
    'l': 'last_filled_qty',
    'z': 'acc_filled_qty',
    'T': 'trade_time',
}


class ForceOrderHandlerBase(Handler):
    """Base handler for the futures ``SubType.FORCE_ORDER`` (liquidation order) stream.

    Shared across USDⓈ-M (``UMFuturesClient``) and COIN-M (``CMFuturesClient``) markets.
    The raw payload nests order details under an ``'o'`` key.  The internal ``_receive``
    flattens it — merging ``payload['o']`` fields into the top level with ``'e'`` and
    ``'E'`` from the outer payload — so the standard column-mapping applies uniformly.

    COIN-M extends this with ``ps`` (pair) in the nested order object via the market-specific
    ``COLUMNS_MAP`` override in ``binance.futures.cm.streams``.  USDⓈ-M uses the common
    column map directly (USDⓈ-M does not publish ``ps``).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, ForceOrderHandlerBase, SubType

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusd_perp')
    """

    COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP_BASE
    COLUMNS = FORCE_ORDER_COLUMNS_MAP_BASE.keys()

    def _receive(  # type: ignore[override]  # intentional narrowing: only dict payloads (with nested 'o') are valid for this handler
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        """Flatten the nested order object before standard column conversion.

        The raw liquidation payload has the form::

            {
                'e': 'forceOrder',
                'E': <event_time>,
                'o': {
                    's': <symbol>,
                    'S': <side>,
                    ...
                }
            }

        This method merges the 'o' dict into a flat dict (preserving 'e' and
        'E' from the outer payload) so the COLUMNS_MAP applies directly.
        """
        o = payload['o']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            **o,
        }
        return super()._receive(flat, index)


# ---------------------------------------------------------------------------
# Futures AggTrade
# Confirmed fields (UM + CM identical, 2026-05-26):
#   e  event type ('aggTrade')
#   E  event time
#   s  symbol
#   a  agg trade id
#   p  price
#   q  quantity
#   f  first trade id
#   l  last trade id
#   T  trade time
#   m  is maker
# (Futures aggTrade does NOT have Spot's 'b' / 'a' buyer/seller order ids)
# ---------------------------------------------------------------------------

FUTURES_AGG_TRADE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'a': 'agg_trade_id',
    'p': 'price',
    'q': 'quantity',
    'f': 'first_trade_id',
    'l': 'last_trade_id',
    'T': 'trade_time',
    'm': 'is_maker',
}

FUTURES_AGG_TRADE_COLUMNS = FUTURES_AGG_TRADE_COLUMNS_MAP.keys()


class AggTradeHandlerBase(Handler):
    """Base handler for the futures ``SubType.AGG_TRADE`` stream.

    Shared across USDⓈ-M and COIN-M markets.  Futures aggregate-trade payloads
    differ from Spot: they include ``agg_trade_id``, ``price``, ``quantity``,
    ``first_trade_id``, ``last_trade_id``, ``trade_time``, and ``is_maker``.
    Unlike the Spot variant, they do NOT include buyer/seller order IDs.

    Subclass this and override ``receive(payload)`` to handle events.  The base
    ``receive`` converts the raw dict into a ``StockDataFrame`` with human-readable
    column names.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
    """

    COLUMNS_MAP = FUTURES_AGG_TRADE_COLUMNS_MAP
    COLUMNS = FUTURES_AGG_TRADE_COLUMNS


# ---------------------------------------------------------------------------
# Futures Kline
# Confirmed fields (UM + CM identical, 2026-05-26):
# Same nested 'k' structure as Spot kline.  The shared Spot column map applies.
# Fields in nested 'k':
#   t  open time
#   T  close time
#   s  symbol
#   i  interval
#   f  first trade id
#   L  last trade id
#   o, h, l, c  OHLC
#   x  is closed
#   v  volume (base asset)
#   q  quote volume
#   V  taker volume
#   Q  taker quote volume
#   n  total trades
# Outer: E event time (lifted into k['E'] by _receive)
# ---------------------------------------------------------------------------

FUTURES_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    't': 'open_time',
    'T': 'close_time',
    's': 'symbol',
    'i': 'interval',
    'f': 'first_trade_id',
    'L': 'last_trade_id',
    **STREAM_OHLC_MAP,
    'x': 'is_closed',
    'v': 'volume',
    'q': 'quote_volume',
    'V': 'taker_volume',
    'Q': 'taker_quote_volume',
    'n': 'total_trades',
}

FUTURES_KLINE_COLUMNS = FUTURES_KLINE_COLUMNS_MAP.keys()

VALID_FUTURES_KLINE_INTERVALS = frozenset((
    '1s', '1m', '3m', '5m', '15m', '30m',
    '1h', '2h', '4h', '6h', '8h', '12h',
    '1d', '3d', '1w', '1M'
))


class KlineHandlerBase(Handler):
    """Base handler for the futures ``SubType.KLINE`` stream.

    Shared across USDⓈ-M and COIN-M markets.  The nested ``k`` payload structure
    is identical to the Spot kline; the internal ``_receive`` lifts ``E`` (event
    time) from the outer envelope into the flattened ``k`` dict before converting.

    Subclass this and override ``receive(payload)`` to handle events.  The base
    ``receive`` returns a ``StockDataFrame`` with human-readable column names
    (e.g. ``open``, ``close``, ``volume``, ``is_closed``).

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_KLINE_COLUMNS_MAP
    COLUMNS = FUTURES_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        k['E'] = payload['E']
        return super()._receive(k, index)


# ---------------------------------------------------------------------------
# Futures MiniTicker
# Confirmed fields (UM + CM identical to Spot miniTicker, 2026-05-26):
#   e  '24hrMiniTicker'
#   E  event time
#   s  symbol
#   o, h, l, c  OHLC
#   v  volume
#   q  quote volume
# ---------------------------------------------------------------------------

FUTURES_MINI_TICKER_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    **STREAM_OHLC_MAP,
    'v': 'volume',
    'q': 'quote_volume',
}

FUTURES_MINI_TICKER_COLUMNS = FUTURES_MINI_TICKER_COLUMNS_MAP.keys()


class MiniTickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.MINI_TICKER`` (24hrMiniTicker) stream.

    Shared across USDⓈ-M and COIN-M markets.  The payload is structurally identical
    to the Spot miniTicker.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_MINI_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_MINI_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Futures Ticker (24hrTicker)
# Confirmed fields (UM + CM identical to Spot ticker, 2026-05-26):
# Extends mini-ticker with: price change, percent, weighted avg price,
# last price (c), last quantity, best bid/ask, open/close time, trade ids.
# ---------------------------------------------------------------------------

FUTURES_TICKER_COLUMNS_MAP = {
    **FUTURES_MINI_TICKER_COLUMNS_MAP,
    'c': 'last_price',
    'p': 'price_change',
    'P': 'percent',
    'w': 'weighted_average_price',
    'x': 'first_trade_price',
    'Q': 'last_quantity',
    'b': 'best_bid_price',
    'B': 'best_bid_quantity',
    'a': 'best_ask_price',
    'A': 'best_ask_quantity',
    'O': 'stat_open_time',
    'C': 'stat_close_time',
    'F': 'first_trade_id',
    'L': 'last_trade_id',
    'n': 'total_trades',
}

FUTURES_TICKER_COLUMNS = FUTURES_TICKER_COLUMNS_MAP.keys()


class TickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.TICKER`` (24hrTicker) stream.

    Shared across USDⓈ-M and COIN-M markets.  The payload is structurally identical
    to the Spot ticker.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Futures BookTicker
# Confirmed fields (UM + CM identical, no 'e' event field, 2026-05-26):
#   u  update id
#   s  symbol  (and 'ps' pair on CM -- but CM does NOT add it to bookTicker;
#                per dstream docs the bookTicker payload is the same shape)
#   b  best bid price
#   B  best bid quantity
#   a  best ask price
#   A  best ask quantity
# Note: UM also has 'T' (transaction time) and 'E' (event time) since 2022.
# Verified against UM docs: {u, E, T, s, b, B, a, A}
# CM docs: {u, s, b, B, a, A} (no E/T in the dstream doc sample)
# We include E and T in the shared map; missing fields in payloads are simply absent.
# ---------------------------------------------------------------------------

FUTURES_BOOK_TICKER_COLUMNS_MAP = {
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

    Shared across USDⓈ-M and COIN-M markets.  The bookTicker payload has NO ``e``
    event field; routing is by stream-name suffix (``@bookTicker``).  Includes
    ``update_id``, ``symbol``, ``best_bid_price``, ``best_bid_quantity``,
    ``best_ask_price``, ``best_ask_quantity``, and optionally ``event_time`` /
    ``transaction_time`` (present in UM, may be absent in CM).

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_BOOK_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Futures Partial Order Book (depth snapshot)
# Confirmed fields (UM + CM identical, 2026-05-26):
# Same structure as Spot partial depth: { lastUpdateId, bids, asks }
# bids/asks each: [ [price, quantity], ... ]
# ---------------------------------------------------------------------------

FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP = {
    'price': 'price',
    'quantity': 'quantity',
}

FUTURES_PARTIAL_ORDER_BOOK_COLUMNS = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP.keys()

FUTURES_DEPTH_LEVELS = (5, 10, 20)
FUTURES_DEPTH_SPEEDS = (100, 500)


class PartialOrderBookHandlerBase(Handler):
    """Base handler for the futures ``SubType.PARTIAL_ORDER_BOOK`` (partial depth snapshot) stream.

    Shared across USDⓈ-M and COIN-M markets.  The raw payload has separate ``bids``
    and ``asks`` lists of ``[price, quantity]`` pairs.  The internal ``_receive``
    converts each side into a ``StockDataFrame`` with ``price`` and ``quantity`` columns;
    the two frames are forwarded to ``receive`` as a ``(bids_df, asks_df)`` tuple.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP
    COLUMNS = FUTURES_PARTIAL_ORDER_BOOK_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        bids_data = [{'price': x[0], 'quantity': x[1]} for x in payload['bids']]
        asks_data = [{'price': x[0], 'quantity': x[1]} for x in payload['asks']]
        bids = super()._receive(bids_data, list(range(len(bids_data))) or [0])
        asks = super()._receive(asks_data, list(range(len(asks_data))) or [0])
        return bids, asks


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
# ---------------------------------------------------------------------------

FUTURES_ORDER_BOOK_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    'T': 'transaction_time',
    's': 'symbol',
    'U': 'first_update_id',
    'u': 'final_update_id',
    'pu': 'prev_final_update_id',
}

FUTURES_ORDER_BOOK_COLUMNS = FUTURES_ORDER_BOOK_COLUMNS_MAP.keys()


class OrderBookHandlerBase(Handler):
    """Base handler for the futures ``SubType.ORDER_BOOK`` (diff depth) stream.

    Shared across USDⓈ-M and COIN-M markets.  Each payload carries the update IDs
    needed to maintain a local order book (``first_update_id``, ``final_update_id``,
    ``prev_final_update_id``) plus the ``bids`` and ``asks`` diff arrays.

    The base ``receive`` converts the metadata fields into a ``StockDataFrame``.
    The raw ``bids`` and ``asks`` diff arrays are accessible from the original payload.

    Subclass this and override ``receive(payload)`` to maintain your local book.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_ORDER_BOOK_COLUMNS_MAP
    COLUMNS = FUTURES_ORDER_BOOK_COLUMNS


# ---------------------------------------------------------------------------
# Futures ContinuousKline
# Wire stream: <pair>_<contractType>@continuousKline_<interval>
# Confirmed fields (UM + CM identical, 2026-05-26):
# Outer:
#   e  'continuous_kline'
#   E  event time
#   ps pair  (e.g. 'BTCUSDT' for UM, 'BTCUSD' for CM)
#   ct contract type (e.g. 'PERPETUAL', 'CURRENT_QUARTER')
# Nested 'k':
#   same kline fields as regular kline (t,T,i,f,L,o,h,l,c,v,n,q,V,Q,x)
#   but 's' is set to '' (empty string) in continuous kline
# ---------------------------------------------------------------------------

FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    'ps': 'pair',
    'ct': 'contract_type',
    't': 'open_time',
    'T': 'close_time',
    'i': 'interval',
    'f': 'first_trade_id',
    'L': 'last_trade_id',
    **STREAM_OHLC_MAP,
    'x': 'is_closed',
    'v': 'volume',
    'q': 'quote_volume',
    'V': 'taker_volume',
    'Q': 'taker_quote_volume',
    'n': 'total_trades',
}

FUTURES_CONTINUOUS_KLINE_COLUMNS = FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP.keys()

VALID_CONTRACT_TYPES = frozenset((
    'PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER',
    'CURRENT_QUARTER_DELIVERING', 'NEXT_QUARTER_DELIVERING',
))


class ContinuousKlineHandlerBase(Handler):
    """Base handler for the futures ``SubType.CONTINUOUS_KLINE`` stream.

    Shared across USDⓈ-M and COIN-M markets.  The stream name has the form
    ``<pair>_<contractType>@continuousKline_<interval>`` (e.g.
    ``btcusdt_perpetual@continuousKline_1m``).  The nested ``k`` dict is
    flattened; outer ``ps`` (pair) and ``ct`` (contract type) are merged in.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
    """

    COLUMNS_MAP = FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP
    COLUMNS = FUTURES_CONTINUOUS_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            'ps': payload['ps'],
            'ct': payload['ct'],
            **k,
        }
        return super()._receive(flat, index)


# ---------------------------------------------------------------------------
# Futures ContractInfo (shared UM + CM)
# Wire stream: !contractInfo
# Event type: 'contractInfo'
# Confirmed fields (UM 2026-05-26; CM shares same event):
#   e  'contractInfo'
#   E  event time
#   s  symbol
#   ps pair
#   ct contract type
#   dt delivery time (ms; 0 for perpetual)
#   ot onboard time
#   cs contract status
#   bks list of brackets (leverage/notional brackets)
# ---------------------------------------------------------------------------

CONTRACT_INFO_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'ps': 'pair',
    'ct': 'contract_type',
    'dt': 'delivery_time',
    'ot': 'onboard_time',
    'cs': 'contract_status',
}

CONTRACT_INFO_COLUMNS = CONTRACT_INFO_COLUMNS_MAP.keys()


class ContractInfoHandlerBase(Handler):
    """Base handler for the futures ``SubType.CONTRACT_INFO`` stream (``!contractInfo``).

    Shared across USDⓈ-M and COIN-M markets.  Receives contract specification
    change events such as listing, settlement, or bracket updates.  Each payload
    carries the symbol, pair, contract type, delivery time, onboard time,
    contract status, and a ``bks`` (brackets) list.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = CONTRACT_INFO_COLUMNS_MAP
    COLUMNS = CONTRACT_INFO_COLUMNS


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketMarkPrice
# Wire stream: !markPrice@arr[@1s]
# Each element is a markPriceUpdate dict (identical to per-symbol markPrice).
# UM elements include 'ap'; CM elements do not.
# The shared base uses MARK_PRICE_COLUMNS_MAP_BASE (no 'ap').
# UM extends via AllMarketMarkPriceHandlerBase in um/streams.py.
# ---------------------------------------------------------------------------

class AllMarketMarkPriceHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_MARK_PRICE`` stream (``!markPrice@arr[@1s]``).

    Shared by USDⓈ-M and COIN-M markets (base uses common fields only).
    Receives an array of ``markPriceUpdate`` events for every futures symbol.
    USDⓈ-M extends this base to include the ``ap`` (mark price moving average) field.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
    COLUMNS = MARK_PRICE_COLUMNS_MAP_BASE.keys()


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketLiquidation
# Wire stream: !forceOrder@arr
# Each element is a forceOrder payload with nested 'o' (same as per-symbol).
# CM elements in nested 'o' include 'ps' (pair); UM do not.
# The base handler does NOT flatten (array handler receives the list as-is).
# Flattening is done per-element by user code or the subclass.
# ---------------------------------------------------------------------------

class AllMarketLiquidationHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_LIQUIDATION`` stream (``!forceOrder@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of liquidation order
    events (``forceOrder``) covering all symbols on the market.  Each element has
    the same nested ``o`` structure as the per-symbol ``ForceOrderHandlerBase``.

    The base ``receive`` passes the raw array payload to a ``StockDataFrame`` with
    only the outer envelope column map (event type and event time per element).
    Subclasses may override to flatten the nested ``o`` objects.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = STREAM_TYPE_MAP
    COLUMNS = STREAM_TYPE_MAP.keys()


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketMiniTickers
# Wire stream: !miniTicker@arr
# Each element is a 24hrMiniTicker dict.
# ---------------------------------------------------------------------------

class AllMarketMiniTickersHandlerBase(Handler):
    """Base handler for the futures ``SubType.ALL_MARKET_MINI_TICKERS`` stream (``!miniTicker@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of ``24hrMiniTicker``
    events for every actively traded futures symbol.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_MINI_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_MINI_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketTickers
# Wire stream: !ticker@arr
# Each element is a 24hrTicker dict.
# ---------------------------------------------------------------------------

class AllMarketTickersHandlerBase(Handler):
    """Base handler for the futures ``SubType.ALL_MARKET_TICKERS`` stream (``!ticker@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of full ``24hrTicker``
    events for every actively traded futures symbol.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_TICKER_COLUMNS


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

    The payload has NO ``e`` event field; routing is by stream name.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_BOOK_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Shared processors
# These are concrete because both markets use the same SubType enum members,
# the same event type strings, and the same subscribe_param logic.
# ---------------------------------------------------------------------------

class MarkPriceProcessor(Processor):
    """Processor for the futures mark-price stream (``<symbol>@markPrice``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = MarkPriceHandlerBase
    SUB_TYPE = SubType.MARK_PRICE
    PAYLOAD_TYPE = 'markPriceUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPrice``.

        Accepts an optional second positional argument ``update_speed``:
        pass ``'1s'`` to get the 1-second stream (``<symbol>@markPrice@1s``).
        """
        symbol = self._get_param_symbol(t, args)
        base = f'{normalize_symbol(symbol)}@{SubType.MARK_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


class ForceOrderProcessor(Processor):
    """Processor for the futures liquidation order stream (``<symbol>@forceOrder``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = ForceOrderHandlerBase
    SUB_TYPE = SubType.FORCE_ORDER
    PAYLOAD_TYPE = 'forceOrder'


class AggTradeProcessor(Processor):
    """Processor for the futures aggregate trade stream (``<symbol>@aggTrade``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE
    PAYLOAD_TYPE = 'aggTrade'


class KlineProcessor(Processor):
    """Processor for the futures kline stream (``<symbol>@kline_<interval>``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = KlineHandlerBase
    SUB_TYPE = SubType.KLINE

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@kline_<interval>``."""
        symbol = self._get_param_symbol(t, args)

        if len(args) < 2:
            raise InvalidSubTypeParamException(
                t, 'interval', '`TimeFrame` expected but not specified')

        interval = args[1]
        interval_str = str(interval)

        if interval_str not in VALID_FUTURES_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t,
                'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_FUTURES_KLINE_INTERVALS))
            )

        return f'{normalize_symbol(symbol)}@{KLINE_TYPE_PREFIX}{interval}'


class MiniTickerProcessor(Processor):
    """Processor for the futures mini-ticker stream (``<symbol>@miniTicker``)."""

    HANDLER = MiniTickerHandlerBase
    SUB_TYPE = SubType.MINI_TICKER
    PAYLOAD_TYPE = '24hrMiniTicker'


class TickerProcessor(Processor):
    """Processor for the futures ticker stream (``<symbol>@ticker``)."""

    HANDLER = TickerHandlerBase
    SUB_TYPE = SubType.TICKER
    PAYLOAD_TYPE = '24hrTicker'


FUTURES_BOOK_TICKER_STREAM_SUFFIX = f'@{SubType.BOOK_TICKER}'


class BookTickerProcessor(Processor):
    """Processor for the futures book-ticker stream (``<symbol>@bookTicker``).

    Routing is by stream-name suffix (no ``e`` event field in payload).
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
            or 'bids' not in payload
            or 'asks' not in payload
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
    """Processor for the futures diff depth stream (``<symbol>@depth[@speed]``).

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


class ContinuousKlineProcessor(Processor):
    """Processor for the futures continuous-contract kline stream.

    Wire name: ``<pair>_<contractType>@continuousKline_<interval>``
    (e.g. ``btcusdt_perpetual@continuousKline_1m`` for UM).

    Shared by both USDⓈ-M and COIN-M markets.
    Subscription requires three positional parameters after the SubType:
    ``pair``, ``contract_type``, and ``interval``.
    """

    HANDLER = ContinuousKlineHandlerBase
    SUB_TYPE = SubType.CONTINUOUS_KLINE
    PAYLOAD_TYPE = 'continuous_kline'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<pair>_<contractType>@continuousKline_<interval>``.

        Args:
            args[0]: pair string (e.g. ``'BTCUSDT'`` for UM, ``'BTCUSD'`` for CM).
            args[1]: contract type string (e.g. ``'PERPETUAL'``, ``'CURRENT_QUARTER'``).
            args[2]: interval (``TimeFrame`` or str, e.g. ``TimeFrame.M1``).
        """
        if len(args) < 3:
            raise InvalidSubTypeParamException(
                t, 'pair/contract_type/interval',
                'CONTINUOUS_KLINE requires pair, contract_type, and interval parameters'
            )

        pair = args[0]
        contract_type = args[1]
        interval = args[2]

        if type(pair) is not str:
            raise InvalidSubTypeParamException(
                t, 'pair', 'string expected but got `%s`' % pair)

        if type(contract_type) is not str:
            raise InvalidSubTypeParamException(
                t, 'contract_type', 'string expected but got `%s`' % contract_type)

        ct_upper = contract_type.upper()
        if ct_upper not in VALID_CONTRACT_TYPES:
            raise InvalidSubTypeParamException(
                t, 'contract_type',
                'invalid contract type `%s`; must be one of %s'
                % (contract_type, sorted(VALID_CONTRACT_TYPES))
            )

        interval_str = str(interval)
        if interval_str not in VALID_FUTURES_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t, 'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_FUTURES_KLINE_INTERVALS))
            )

        return f'{normalize_symbol(pair)}_{ct_upper.lower()}@continuousKline_{interval}'


class ContractInfoProcessor(Processor):
    """Processor for the futures contract info stream (``!contractInfo``).

    Shared by both USDⓈ-M and COIN-M markets.  No symbol parameter required.
    """

    HANDLER = ContractInfoHandlerBase
    SUB_TYPE = SubType.CONTRACT_INFO
    PAYLOAD_TYPE = 'contractInfo'
    STREAM_TYPE_NAME = '!contractInfo'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.CONTRACT_INFO` expects no parameters'
            )
        return self.STREAM_TYPE_NAME


FUTURES_ALL_MARKET_BOOK_TICKER_STREAM = '!bookTicker'


class AllMarketBookTickerProcessor(Processor):
    """Processor for the futures all-market book ticker stream (``!bookTicker``).

    Wire name is ``!bookTicker`` (no ``@arr`` suffix; distinct from Spot's deprecated
    ``!bookTicker@arr``).  Routing is by stream name (no ``e`` event field).
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


class AllMarketMarkPriceProcessor(Processor):
    """Processor for the futures all-market mark price stream (``!markPrice@arr[@1s]``).

    Routing is by stream name prefix.  Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AllMarketMarkPriceHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MARK_PRICE
    STREAM_TYPE_PREFIX: ClassVar[str] = '!markPrice@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``!markPrice@arr`` or ``!markPrice@arr@1s``."""
        if len(args) > 1:
            raise InvalidSubTypeParamException(
                t, 'update_speed',
                '`SubType.ALL_MARKET_MARK_PRICE` accepts at most one optional '
                'parameter: update_speed (pass ``\'1s\'`` for 1-second updates)'
            )
        base = self.STREAM_TYPE_PREFIX
        if len(args) == 1 and args[0] == '1s':
            return f'{base}@1s'
        return base


class AllMarketLiquidationProcessor(Processor):
    """Processor for the futures all-market liquidation stream (``!forceOrder@arr``).

    Routing is by stream name.  Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AllMarketLiquidationHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_LIQUIDATION
    STREAM_TYPE_NAME: ClassVar[str] = '!forceOrder@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_LIQUIDATION` expects no parameters'
            )
        return self.STREAM_TYPE_NAME


class AllMarketMiniTickersProcessor(Processor):
    """Processor for the futures all-market mini-ticker stream (``!miniTicker@arr``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER: ClassVar[type] = AllMarketMiniTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MINI_TICKERS
    STREAM_TYPE_PREFIX: ClassVar[str] = '!miniTicker@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_MINI_TICKERS` expects no parameters'
            )
        return self.STREAM_TYPE_PREFIX


class AllMarketTickersProcessor(Processor):
    """Processor for the futures all-market ticker stream (``!ticker@arr``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER: ClassVar[type] = AllMarketTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_TICKERS
    STREAM_TYPE_PREFIX: ClassVar[str] = '!ticker@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_TICKERS` expects no parameters'
            )
        return self.STREAM_TYPE_PREFIX


# ---------------------------------------------------------------------------
# Futures depth parameter helpers
# ---------------------------------------------------------------------------

def _get_futures_depth_level(t, args, default=20):
    if len(args) == 0:
        return default

    level = args[0]

    if type(level) is not int:
        raise InvalidSubTypeParamException(
            t, 'level', '`int` expected but got `%s`' % level)

    if level not in FUTURES_DEPTH_LEVELS:
        raise InvalidSubTypeParamException(
            t, 'level',
            '`level` should be one of %s but got `%s`'
            % (FUTURES_DEPTH_LEVELS, level)
        )

    return level


def _get_futures_depth_speed(t, args):
    """Return the speed int (100 or 500) or None if not provided."""
    if len(args) == 0:
        return None

    speed = args[0]

    if type(speed) is not int:
        raise InvalidSubTypeParamException(
            t, 'speed', '`int` expected but got `%s`' % speed)

    if speed not in FUTURES_DEPTH_SPEEDS:
        raise InvalidSubTypeParamException(
            t, 'speed',
            '`speed` should be one of %s but got `%s`'
            % (FUTURES_DEPTH_SPEEDS, speed)
        )

    return speed


__all__ = [
    # Column maps
    'MARK_PRICE_COLUMNS_MAP_BASE',
    'FORCE_ORDER_COLUMNS_MAP_BASE',
    'FUTURES_AGG_TRADE_COLUMNS_MAP',
    'FUTURES_KLINE_COLUMNS_MAP',
    'FUTURES_MINI_TICKER_COLUMNS_MAP',
    'FUTURES_TICKER_COLUMNS_MAP',
    'FUTURES_BOOK_TICKER_COLUMNS_MAP',
    'FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP',
    'FUTURES_ORDER_BOOK_COLUMNS_MAP',
    'FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP',
    'CONTRACT_INFO_COLUMNS_MAP',
    # Handler bases
    'MarkPriceHandlerBase',
    'ForceOrderHandlerBase',
    'AggTradeHandlerBase',
    'KlineHandlerBase',
    'MiniTickerHandlerBase',
    'TickerHandlerBase',
    'BookTickerHandlerBase',
    'PartialOrderBookHandlerBase',
    'OrderBookHandlerBase',
    'ContinuousKlineHandlerBase',
    'ContractInfoHandlerBase',
    'AllMarketMarkPriceHandlerBase',
    'AllMarketLiquidationHandlerBase',
    'AllMarketMiniTickersHandlerBase',
    'AllMarketTickersHandlerBase',
    'AllMarketBookTickerHandlerBase',
    # Processors
    'MarkPriceProcessor',
    'ForceOrderProcessor',
    'AggTradeProcessor',
    'KlineProcessor',
    'MiniTickerProcessor',
    'TickerProcessor',
    'BookTickerProcessor',
    'PartialOrderBookProcessor',
    'OrderBookProcessor',
    'ContinuousKlineProcessor',
    'ContractInfoProcessor',
    'AllMarketMarkPriceProcessor',
    'AllMarketLiquidationProcessor',
    'AllMarketMiniTickersProcessor',
    'AllMarketTickersProcessor',
    'AllMarketBookTickerProcessor',
]
