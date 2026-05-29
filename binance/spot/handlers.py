from typing import List, Optional

from binance.core.common.constants import (
    STREAM_TYPE_MAP,
    STREAM_OHLC_MAP
)

from binance.core.common.types import (
    DictPayload,
    # ListPayload
)

from binance.core.handlers.base import Handler
from binance.core.handlers.framework import (  # noqa: F401  re-exported for backward compatibility
    StreamErrorHandlerBase,
    HandlerExceptionHandlerBase,
)


BASE_TRADE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'p': 'price',
    'q': 'quantity',
    'T': 'trade_time',
    'm': 'is_maker'
}

TRADE_COLUMNS_MAP = {
    **BASE_TRADE_COLUMNS_MAP,
    't': 'trade_id',
    'b': 'buyer_order_id',
    'a': 'seller_order_id'
}

TRADE_COLUMNS = TRADE_COLUMNS_MAP.keys()


class TradeHandlerBase(Handler):
    """Base handler for the ``SubType.TRADE`` (individual trade) stream.

    Receives one message per trade that executes on Binance.  Each payload
    describes a single matched trade, including the trade ID, buyer/seller
    order IDs, price, quantity, trade time, and whether the buyer is the
    market maker.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``trade_id``, ``price``, ``quantity``,
    ``is_maker``).
    """

    COLUMNS_MAP = TRADE_COLUMNS_MAP
    COLUMNS = TRADE_COLUMNS


AGG_TRADE_COLUMNS_MAP = {
    **BASE_TRADE_COLUMNS_MAP,
    'a': 'agg_trade_id',
    'f': 'first_trade_id',
    'l': 'last_trade_id',
}

AGG_TRADE_COLUMNS = AGG_TRADE_COLUMNS_MAP.keys()


class AggTradeHandlerBase(Handler):
    """Base handler for the ``SubType.AGG_TRADE`` (aggregate trade) stream.

    Receives one message per aggregate trade event on Binance.  An aggregate
    trade bundles all trades that fill at the same price in the same direction
    at the same moment into a single event, providing the aggregate trade ID,
    first and last constituent trade IDs, price, quantity, and whether the
    buyer was the market maker.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``agg_trade_id``, ``price``,
    ``quantity``, ``is_maker``).
    """

    COLUMNS_MAP = AGG_TRADE_COLUMNS_MAP
    COLUMNS = AGG_TRADE_COLUMNS


BLOCK_TRADE_COLUMNS_MAP = {
    **BASE_TRADE_COLUMNS_MAP,
    't': 'trade_id'
}

BLOCK_TRADE_COLUMNS = BLOCK_TRADE_COLUMNS_MAP.keys()


class BlockTradeHandlerBase(Handler):
    """Base handler for the ``SubType.BLOCK_TRADE`` (block trade) stream.

    Receives one message per block trade reported for the subscribed symbol.
    A block trade is a single large trade reported as a block; each payload
    carries the block trade ID, price, quantity, trade time, and whether the
    buyer is the market maker.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``trade_id``, ``price``, ``quantity``,
    ``is_maker``).
    """

    COLUMNS_MAP = BLOCK_TRADE_COLUMNS_MAP
    COLUMNS = BLOCK_TRADE_COLUMNS


REFERENCE_PRICE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    's': 'symbol',
    'r': 'reference_price',
    't': 'engine_time'
}

REFERENCE_PRICE_COLUMNS = REFERENCE_PRICE_COLUMNS_MAP.keys()


class ReferencePriceHandlerBase(Handler):
    """Base handler for the ``SubType.REFERENCE_PRICE`` (reference price) stream.

    Receives a reference-price event (~1000ms) for the subscribed symbol.
    Each payload carries the symbol, the reference price (a string, or
    ``null`` when there is no reference price), and the engine timestamp at
    which the reference price was valid.  Note: this stream has no separate
    event-time field.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``reference_price``, ``engine_time``).
    """

    COLUMNS_MAP = REFERENCE_PRICE_COLUMNS_MAP
    COLUMNS = REFERENCE_PRICE_COLUMNS


BOOK_TICKER_COLUMNS_MAP = {
    'u': 'update_id',
    's': 'symbol',
    'b': 'best_bid_price',
    'B': 'best_bid_quantity',
    'a': 'best_ask_price',
    'A': 'best_ask_quantity'
}

BOOK_TICKER_COLUMNS = BOOK_TICKER_COLUMNS_MAP.keys()


class BookTickerHandlerBase(Handler):
    """Base handler for the ``SubType.BOOK_TICKER`` (best bid/ask) stream.

    Receives one message every time the best bid or ask price/quantity changes
    for the subscribed symbol.  Each payload contains the update ID, symbol,
    best bid price and quantity, and best ask price and quantity.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``best_bid_price``, ``best_ask_price``).
    """

    COLUMNS_MAP = BOOK_TICKER_COLUMNS_MAP
    COLUMNS = BOOK_TICKER_COLUMNS


PARTIAL_ORDER_BOOK_COLUMNS_MAP = {
    'price': 'price',
    'quantity': 'quantity'
}

PARTIAL_ORDER_BOOK_COLUMNS = PARTIAL_ORDER_BOOK_COLUMNS_MAP.keys()


class PartialOrderBookHandlerBase(Handler):
    """Base handler for the ``SubType.PARTIAL_ORDER_BOOK`` (partial depth) stream.

    Receives a snapshot of the top-N levels of the order book for the
    subscribed symbol (5, 10, or 20 levels depending on the subscription
    parameter).  Each payload contains separate lists of bids and asks, each
    entry being a (price, quantity) pair.

    Subclass this and override ``receive(payload)`` to handle the event.
    The internal ``_receive`` splits the raw payload into two ``StockDataFrame``
    objects — one for bids and one for asks — each with ``price`` and
    ``quantity`` columns; these are forwarded to ``receive`` as a tuple
    ``(bids_df, asks_df)``.
    """

    COLUMNS_MAP = PARTIAL_ORDER_BOOK_COLUMNS_MAP
    COLUMNS = PARTIAL_ORDER_BOOK_COLUMNS

    def _receive(  # type: ignore[override]  # intentional narrowing: only dict payloads are valid for this handler
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        bids = super()._receive([
            {'price': x[0], 'quantity': x[1]}
            for x in payload['bids']
        ], None)
        asks = super()._receive([
            {'price': x[0], 'quantity': x[1]}
            for x in payload['asks']
        ], None)
        return bids, asks


KLINE_COLUMNS_MAP = {
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
    'n': 'total_trades'
}

KLINE_COLUMNS = KLINE_COLUMNS_MAP.keys()


class KlineHandlerBase(Handler):
    """Base handler for the ``SubType.KLINE`` (candlestick) stream.

    Receives one message per candlestick update for the subscribed symbol and
    interval.  Each payload includes OHLCV data (open, high, low, close,
    volume), quote volume, taker volumes, total trades, open/close times, and
    a flag indicating whether the candle is closed.

    Subclass this and override ``receive(payload)`` to handle the event.
    The internal ``_receive`` flattens the nested kline payload before
    converting it; the base ``receive`` then returns a ``StockDataFrame`` with
    human-readable column names (e.g. ``open``, ``high``, ``low``, ``close``,
    ``volume``, ``is_closed``).
    """

    COLUMNS_MAP = KLINE_COLUMNS_MAP
    COLUMNS = KLINE_COLUMNS

    def _receive(  # type: ignore[override]  # intentional narrowing: only dict payloads are valid for kline
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        """The payload of kline has unnecessary hierarchy,
        so just flatten it.
        """

        k = payload['k']
        k['E'] = payload['E']

        return super()._receive(k, index)


MINI_TICKER_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    **STREAM_OHLC_MAP,
    'v': 'volume',
    'q': 'quote_volume',
}

MINI_TICKER_COLUMNS = MINI_TICKER_COLUMNS_MAP.keys()


class MiniTickerHandlerBase(Handler):
    """Base handler for the ``SubType.MINI_TICKER`` (mini 24-hour ticker) stream.

    Receives a condensed 24-hour rolling-window statistics event for the
    subscribed symbol.  Each payload includes the event time, symbol, OHLC
    prices (open, high, low, close), total volume, and quote volume — a lighter
    alternative to the full ``TickerHandlerBase``.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``open``, ``high``, ``volume``).
    """

    COLUMNS_MAP = MINI_TICKER_COLUMNS_MAP
    COLUMNS = MINI_TICKER_COLUMNS


AVG_PRICE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'i': 'interval',
    'w': 'average_price',
    'T': 'last_trade_time'
}

AVG_PRICE_COLUMNS = AVG_PRICE_COLUMNS_MAP.keys()


class AvgPriceHandlerBase(Handler):
    """Base handler for the ``SubType.AVG_PRICE`` (average price) stream.

    Receives a rolling average-price event for the subscribed symbol.  Each
    payload contains the event time, symbol, averaging interval, weighted
    average price, and the time of the last trade that contributed to the
    average.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``average_price``, ``interval``,
    ``last_trade_time``).
    """

    COLUMNS_MAP = AVG_PRICE_COLUMNS_MAP
    COLUMNS = AVG_PRICE_COLUMNS


TICKER_COLUMNS_MAP = {
    **MINI_TICKER_COLUMNS_MAP,
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
    'n': 'total_trades'
}

TICKER_COLUMNS = TICKER_COLUMNS_MAP.keys()


class TickerHandlerBase(Handler):
    """Base handler for the ``SubType.TICKER`` (full 24-hour ticker) stream.

    Receives a full 24-hour rolling-window statistics event for the subscribed
    symbol.  The payload extends the mini-ticker with price change and percent
    change, weighted average price, first trade price, last quantity, best
    bid/ask prices and quantities, stat open/close times, and first/last trade
    IDs and total trade count.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``price``, ``percent``,
    ``weighted_average_price``, ``best_bid_price``).
    """

    COLUMNS_MAP = TICKER_COLUMNS_MAP
    COLUMNS = TICKER_COLUMNS


WINDOW_TICKER_COLUMNS_MAP = {
    **MINI_TICKER_COLUMNS_MAP,
    'p': 'price_change',
    'P': 'percent',
    'w': 'weighted_average_price',
    'O': 'stat_open_time',
    'C': 'stat_close_time',
    'F': 'first_trade_id',
    'L': 'last_trade_id',
    'n': 'total_trades'
}

WINDOW_TICKER_COLUMNS = WINDOW_TICKER_COLUMNS_MAP.keys()


class WindowTickerHandlerBase(Handler):
    """Base handler for the ``SubType.WINDOW_TICKER`` (rolling-window ticker) stream.

    Receives a rolling-window statistics event for the subscribed symbol over
    a configurable window (e.g. 1h, 4h, 1d).  The payload includes OHLC
    prices, volume, quote volume, price change and percent change, weighted
    average price, stat open/close times, and first/last trade IDs and total
    trade count.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``price_change``, ``percent``,
    ``weighted_average_price``).
    """

    COLUMNS_MAP = WINDOW_TICKER_COLUMNS_MAP
    COLUMNS = WINDOW_TICKER_COLUMNS


class AllMarketMiniTickersHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_MINI_TICKERS`` stream.

    Receives an array of condensed 24-hour rolling-window mini-ticker events
    for all actively traded symbols on the exchange.  Each element of the
    payload shares the same fields as ``MiniTickerHandlerBase``: event time,
    symbol, OHLC prices, total volume, and quote volume.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts each element into a row of a
    ``StockDataFrame`` with the same human-readable columns as
    ``MiniTickerHandlerBase``.
    """

    COLUMNS_MAP = MINI_TICKER_COLUMNS_MAP
    COLUMNS = MINI_TICKER_COLUMNS

    # def _receive(self, payload: ListPayload):
    #     return super()._receive(
    #         payload, None)


class AllMarketWindowTickersHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_WINDOW_TICKERS`` stream.

    Receives an array of rolling-window ticker events for all actively traded
    symbols on the exchange.  Each element of the payload shares the same
    fields as ``WindowTickerHandlerBase``: OHLC prices, volume, quote volume,
    price change, weighted average price, stat open/close times, and trade
    count.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts each element into a row of a
    ``StockDataFrame`` with the same human-readable columns as
    ``WindowTickerHandlerBase``.
    """

    COLUMNS_MAP = WINDOW_TICKER_COLUMNS_MAP
    COLUMNS = WINDOW_TICKER_COLUMNS

    # def _receive(self, payload: ListPayload):
    #     return super()._receive(
    #         payload, None)


# NOTE: !bookTicker@arr is DEPRECATED by Binance (announced 2021-11-05, removed 2022).
# Do NOT implement a handler or processor for this stream; the endpoint no longer exists
# on the Binance Spot data-stream servers.

# NOTE: !ticker@arr (the standalone all-market 24hr full ticker array) is NOT
# documented on the Spot WebSocket Streams page
# (https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).
# Only `!miniTicker@arr` and `!ticker_<window-size>@arr` are documented for the
# Spot all-market ticker family.  The SDK therefore does not ship a Spot
# binding for `!ticker@arr`; if you need full all-market ticker payloads on a
# documented stream, subscribe to `SubType.ALL_MARKET_WINDOW_TICKERS`
# (rolling-window) or aggregate per-symbol `SubType.TICKER` subscriptions.
# (Futures DOES document `!ticker@arr` -- see
# ``binance.futures.streams.AllMarketTickersHandlerBase``.)
