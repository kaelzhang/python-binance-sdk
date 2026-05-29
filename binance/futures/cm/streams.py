"""COIN-M Futures stream wiring: handlers, processors, and the PROCESSORS list.

COIN-M reuses the shared futures handler bases from :mod:`binance.futures.streams`.
This module:
1. Adds the COIN-M-specific ``ps`` (pair) field to the force-order column map.
2. Provides CM-only handler bases and processors for streams exclusive to COIN-M:
   ``<pair>@markPrice`` (Mark Price of All Symbols of a Pair),
   ``indexPrice``, ``indexPriceKline``, ``markPriceKline``.
3. Overrides ``subscribe_param`` on processors that take symbol/pair parameters
   to preserve underscores in COIN-M symbol names (e.g. ``BTCUSD_PERP``).
4. Registers the complete CM PROCESSORS list.

Confirmed CM vs UM payload differences (2026-05-26):
- Mark Price: CM does NOT have ``ap`` (mark price moving average); UM DOES.
  The shared :class:`~binance.futures.streams.MarkPriceHandlerBase` (without ``ap``)
  is used directly for COIN-M.
- Force Order nested 'o': CM has ``ps`` (pair); UM does NOT.
- All-market Mark Price: CM array elements do NOT include ``ap``; UM DOES.
- indexPrice, indexPriceKline, markPriceKline: CM-only (not present on UM fstream).
- contractInfo: present on both UM and CM (shared base used directly).
- Symbol normalization: COIN-M symbols contain underscores (e.g. ``BTCUSD_PERP``).
  The shared ``normalize_symbol`` helper strips underscores (designed for Spot/UM
  symbols like ``BTCUSDT``), which is incorrect for COIN-M.  All CM processors that
  accept a symbol or pair parameter override ``subscribe_param`` to use
  ``symbol.lower()`` instead, preserving the underscore.

CM-only streams:
- <pair>@markPrice (Mark Price of All Symbols of a Pair):
  https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-of-All-Symbols-of-a-Pair
- indexPrice: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Index-Price-Stream
- indexPriceKline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Index-Kline-Candlestick-Streams
- markPriceKline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Kline-Candlestick-Streams
"""

from typing import List, Optional

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

from binance.futures.streams import (  # noqa: F401  (re-exported for convenience)
    # Column maps
    MARK_PRICE_COLUMNS_MAP_BASE,
    FORCE_ORDER_COLUMNS_MAP_BASE,
    FUTURES_AGG_TRADE_COLUMNS_MAP,
    FUTURES_KLINE_COLUMNS_MAP,
    FUTURES_MINI_TICKER_COLUMNS_MAP,
    FUTURES_TICKER_COLUMNS_MAP,
    FUTURES_BOOK_TICKER_COLUMNS_MAP,
    FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP,
    FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP,
    CONTRACT_INFO_COLUMNS_MAP,
    FUTURES_DEPTH_LEVELS,
    FUTURES_DEPTH_SPEEDS,
    VALID_FUTURES_KLINE_INTERVALS,
    # Handler bases (shared; used directly by CM processors)
    MarkPriceHandlerBase,
    ForceOrderHandlerBase as _ForceOrderHandlerBase,
    ContinuousKlineHandlerBase,
    ContractInfoHandlerBase,
    AllMarketMarkPriceHandlerBase,
    AllMarketLiquidationHandlerBase,
    MiniTickerHandlerBase as _MiniTickerHandlerBase,
    TickerHandlerBase as _TickerHandlerBase,
    BookTickerHandlerBase as _BookTickerHandlerBase,
    AllMarketMiniTickersHandlerBase as _AllMarketMiniTickersHandlerBase,
    AllMarketTickersHandlerBase as _AllMarketTickersHandlerBase,
    AllMarketBookTickerHandlerBase as _AllMarketBookTickerHandlerBase,
    # Processor bases (shared; CM overrides subscribe_param for symbol handling)
    MarkPriceProcessor as _MarkPriceProcessor,
    ForceOrderProcessor as _ForceOrderProcessor,
    AggTradeProcessor as _AggTradeProcessor,
    KlineProcessor as _KlineProcessor,
    ContinuousKlineProcessor as _ContinuousKlineProcessor,
    MiniTickerProcessor as _MiniTickerProcessor,
    TickerProcessor as _TickerProcessor,
    BookTickerProcessor as _BookTickerProcessor,
    PartialOrderBookProcessor as _PartialOrderBookProcessor,
    OrderBookProcessor as _OrderBookProcessor,
    ContractInfoProcessor,
    AllMarketMarkPriceProcessor,
    AllMarketLiquidationProcessor,
    AllMarketMiniTickersProcessor as _AllMarketMiniTickersProcessor,
    AllMarketTickersProcessor as _AllMarketTickersProcessor,
    AllMarketBookTickerProcessor as _AllMarketBookTickerProcessor,
    _get_futures_depth_level,
    _get_futures_depth_speed,
)

from binance.futures.user_processor import FuturesUserProcessor  # noqa: F401  (re-exported)


# ---------------------------------------------------------------------------
# CM-specific column maps: COIN-M payloads include a ``ps`` (pair) field that
# USDⓈ-M does NOT publish, per developers.binance.com.  Applies to:
# - force-order (nested ``o``)
# - per-symbol miniTicker, ticker, bookTicker
# - and the corresponding all-market arrays (same per-element shape).
# ---------------------------------------------------------------------------

FORCE_ORDER_COLUMNS_MAP = {
    **FORCE_ORDER_COLUMNS_MAP_BASE,
    'ps': 'pair',  # CM-only: pair designation in the nested order object
}

FORCE_ORDER_COLUMNS = FORCE_ORDER_COLUMNS_MAP.keys()

CM_MINI_TICKER_COLUMNS_MAP = {
    **FUTURES_MINI_TICKER_COLUMNS_MAP,
    'ps': 'pair',
}
CM_MINI_TICKER_COLUMNS = CM_MINI_TICKER_COLUMNS_MAP.keys()

CM_TICKER_COLUMNS_MAP = {
    **FUTURES_TICKER_COLUMNS_MAP,
    'ps': 'pair',
}
CM_TICKER_COLUMNS = CM_TICKER_COLUMNS_MAP.keys()

CM_BOOK_TICKER_COLUMNS_MAP = {
    **FUTURES_BOOK_TICKER_COLUMNS_MAP,
    'ps': 'pair',
}
CM_BOOK_TICKER_COLUMNS = CM_BOOK_TICKER_COLUMNS_MAP.keys()


class ForceOrderHandlerBase(_ForceOrderHandlerBase):
    """Base handler for the COIN-M ``SubType.FORCE_ORDER`` (liquidation order) stream.

    Extends the shared :class:`~binance.futures.streams.ForceOrderHandlerBase` with
    the COIN-M-specific ``ps`` (pair) column, which is present in COIN-M nested
    order objects but absent from USDⓈ-M.

    The raw payload nests order details under an ``'o'`` key (inherited flattening
    from the shared base applies; ``ps`` is one of the nested fields).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names
    (e.g. ``symbol``, ``pair``, ``side``, ``price``, ``avg_price``, ``order_status``).

    Example::

        from binance import CMFuturesClient, SubType
        from binance.futures.cm.streams import ForceOrderHandlerBase

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['pair'], df['price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusd_perp')
    """

    COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP
    COLUMNS = FORCE_ORDER_COLUMNS


class MiniTickerHandlerBase(_MiniTickerHandlerBase):
    """Base handler for the COIN-M ``SubType.MINI_TICKER`` (24hrMiniTicker) stream.

    Extends the shared :class:`~binance.futures.streams.MiniTickerHandlerBase`
    with the COIN-M-specific ``ps`` (pair) column, which is published in
    COIN-M payloads but absent from USDⓈ-M.

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
    """

    COLUMNS_MAP = CM_MINI_TICKER_COLUMNS_MAP
    COLUMNS = CM_MINI_TICKER_COLUMNS


class TickerHandlerBase(_TickerHandlerBase):
    """Base handler for the COIN-M ``SubType.TICKER`` (24hrTicker) stream.

    Extends the shared :class:`~binance.futures.streams.TickerHandlerBase` with
    the COIN-M-specific ``ps`` (pair) column.

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
    """

    COLUMNS_MAP = CM_TICKER_COLUMNS_MAP
    COLUMNS = CM_TICKER_COLUMNS


class BookTickerHandlerBase(_BookTickerHandlerBase):
    """Base handler for the COIN-M ``SubType.BOOK_TICKER`` stream.

    Extends the shared :class:`~binance.futures.streams.BookTickerHandlerBase`
    with the COIN-M-specific ``ps`` (pair) column.  The payload still includes
    ``e='bookTicker'``; processor dispatch routes by stream-name suffix.

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
    """

    COLUMNS_MAP = CM_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = CM_BOOK_TICKER_COLUMNS


class AllMarketMiniTickersHandlerBase(_AllMarketMiniTickersHandlerBase):
    """Base handler for the COIN-M ``SubType.ALL_MARKET_MINI_TICKERS`` stream
    (``!miniTicker@arr``).

    Each element is a CM ``24hrMiniTicker`` event, so the column map mirrors
    :class:`MiniTickerHandlerBase` (i.e. includes ``ps``).

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream
    """

    COLUMNS_MAP = CM_MINI_TICKER_COLUMNS_MAP
    COLUMNS = CM_MINI_TICKER_COLUMNS


class AllMarketTickersHandlerBase(_AllMarketTickersHandlerBase):
    """Base handler for the COIN-M ``SubType.ALL_MARKET_TICKERS`` stream
    (``!ticker@arr``).

    Each element is a CM ``24hrTicker`` event; column map mirrors
    :class:`TickerHandlerBase` (i.e. includes ``ps``).

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    """

    COLUMNS_MAP = CM_TICKER_COLUMNS_MAP
    COLUMNS = CM_TICKER_COLUMNS


class AllMarketBookTickerHandlerBase(_AllMarketBookTickerHandlerBase):
    """Base handler for the COIN-M ``SubType.ALL_MARKET_BOOK_TICKER`` stream
    (``!bookTicker``).

    Payload shape mirrors the per-symbol CM book ticker (includes ``ps``).

    Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    """

    COLUMNS_MAP = CM_BOOK_TICKER_COLUMNS_MAP
    COLUMNS = CM_BOOK_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# CM processors
# All processors that accept a symbol parameter override subscribe_param
# to use symbol.lower() (preserving underscores in COIN-M symbol names).
# COIN-M stream names: btcusd_perp@markPrice, btcusd_perp@forceOrder
# The shared normalize_symbol() strips underscores; wrong for COIN-M.
# ---------------------------------------------------------------------------

class MarkPriceProcessor(_MarkPriceProcessor):
    """Processor for the COIN-M mark-price stream (``<symbol>@markPrice``).

    Overrides ``subscribe_param`` to use ``symbol.lower()`` (preserving underscores)
    instead of ``normalize_symbol`` (which would strip the underscore in
    ``BTCUSD_PERP`` -> ``btcusdperp``, incorrectly).
    """

    HANDLER = MarkPriceHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPrice``, preserving underscores in COIN-M symbols.

        Accepts an optional second positional argument ``update_speed``:
        pass ``'1s'`` to get the 1-second stream (``<symbol>@markPrice@1s``).
        """
        symbol = self._get_param_symbol(t, args)
        base = f'{symbol.lower()}@{SubType.MARK_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


class ForceOrderProcessor(_ForceOrderProcessor):
    """Processor for the COIN-M liquidation order stream (``<symbol>@forceOrder``).

    Uses the COIN-M :class:`ForceOrderHandlerBase` (which includes ``pair``).
    Overrides ``subscribe_param`` to use ``symbol.lower()`` (preserving underscores).
    """

    HANDLER = ForceOrderHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@forceOrder``, preserving underscores in COIN-M symbols."""
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@{SubType.FORCE_ORDER}'


class AggTradeProcessor(_AggTradeProcessor):
    """Processor for the COIN-M aggTrade stream (``<symbol>@aggTrade``).

    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@aggTrade'


class KlineProcessor(_KlineProcessor):
    """Processor for the COIN-M kline stream (``<symbol>@kline_<interval>``).

    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    def subscribe_param(self, _, t, *args) -> str:
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

        return f'{symbol.lower()}@{KLINE_TYPE_PREFIX}{interval}'


class ContinuousKlineProcessor(_ContinuousKlineProcessor):
    """Processor for the COIN-M continuous-contract kline stream.

    Wire name: ``<pair>_<contractType>@continuousKline_<interval>``
    Overrides ``subscribe_param`` to use ``pair.lower()`` (preserving underscores).
    """

    def subscribe_param(self, _, t, *args) -> str:
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
        if ct_upper not in self.VALID_CONTRACT_TYPES:
            raise InvalidSubTypeParamException(
                t, 'contract_type',
                'invalid contract type `%s`; must be one of %s'
                % (contract_type, sorted(self.VALID_CONTRACT_TYPES))
            )

        interval_str = str(interval)
        if interval_str not in VALID_FUTURES_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t, 'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_FUTURES_KLINE_INTERVALS))
            )

        return f'{pair.lower()}_{ct_upper.lower()}@continuousKline_{interval}'


class MiniTickerProcessor(_MiniTickerProcessor):
    """Processor for the COIN-M mini-ticker stream (``<symbol>@miniTicker``).

    Binds to the CM-specific :class:`MiniTickerHandlerBase` (includes ``pair``).
    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    HANDLER = MiniTickerHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@miniTicker'


class TickerProcessor(_TickerProcessor):
    """Processor for the COIN-M ticker stream (``<symbol>@ticker``).

    Binds to the CM-specific :class:`TickerHandlerBase` (includes ``pair``).
    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    HANDLER = TickerHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@ticker'


class BookTickerProcessor(_BookTickerProcessor):
    """Processor for the COIN-M book-ticker stream (``<symbol>@bookTicker``).

    Binds to the CM-specific :class:`BookTickerHandlerBase` (includes ``pair``).
    Routing remains by stream-name suffix since the per-symbol and all-market
    streams share ``e='bookTicker'``.
    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    HANDLER = BookTickerHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@{SubType.BOOK_TICKER}'


class AllMarketMiniTickersProcessor(_AllMarketMiniTickersProcessor):
    """Processor for the COIN-M all-market mini-ticker stream (``!miniTicker@arr``).

    Binds to :class:`AllMarketMiniTickersHandlerBase` so payload elements
    surface the CM ``pair`` column.
    """

    HANDLER = AllMarketMiniTickersHandlerBase


class AllMarketTickersProcessor(_AllMarketTickersProcessor):
    """Processor for the COIN-M all-market ticker stream (``!ticker@arr``).

    Binds to :class:`AllMarketTickersHandlerBase` so payload elements surface
    the CM ``pair`` column.
    """

    HANDLER = AllMarketTickersHandlerBase


class AllMarketBookTickerProcessor(_AllMarketBookTickerProcessor):
    """Processor for the COIN-M all-market book ticker stream (``!bookTicker``).

    Binds to :class:`AllMarketBookTickerHandlerBase` so each event surfaces
    the CM ``pair`` column.
    """

    HANDLER = AllMarketBookTickerHandlerBase


class PartialOrderBookProcessor(_PartialOrderBookProcessor):
    """Processor for the COIN-M partial depth stream (``<symbol>@depth<N>[@speed]``).

    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        level = _get_futures_depth_level(t, args[1:])
        speed = _get_futures_depth_speed(t, args[2:])
        base = f'{symbol.lower()}@depth{level}'
        if speed is not None:
            return f'{base}@{speed}ms'
        return base


class OrderBookProcessor(_OrderBookProcessor):
    """Processor for the COIN-M diff depth stream (``<symbol>@depth[@speed]``).

    Overrides ``subscribe_param`` to use ``symbol.lower()``.
    """

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        speed = _get_futures_depth_speed(t, args[1:])
        base = f'{symbol.lower()}@depth'
        if speed is not None:
            return f'{base}@{speed}ms'
        return base


# ---------------------------------------------------------------------------
# CM-only: PairMarkPrice (Mark Price of All Symbols of a Pair)
# Wire stream: <pair>@markPrice  or  <pair>@markPrice@1s
# Confirmed CM-only (2026-05): not documented on fstream (USDⓈ-M).
# Delivers an ARRAY of markPriceUpdate dicts covering every symbol of the
# given pair (e.g. BTCUSD_PERP, BTCUSD_201225, ...).  Distinct from
# <symbol>@markPrice (single dict) and !markPrice@arr (all markets).
# Default speed = 3000ms; @1s = 1000ms.
# Each element shape matches CM <symbol>@markPrice (no `ap` -- CM lacks it).
# Confirmed fields per CM docs:
#   e  'markPriceUpdate'
#   E  event time
#   s  symbol (specific symbol within the pair, e.g. BTCUSD_PERP)
#   p  mark price
#   P  estimated settle price (only useful in last hour before settlement)
#   i  index price
#   r  funding rate (perpetual) or empty string (delivery contracts)
#   T  next funding time (perpetual) or 0 (delivery contracts)
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-of-All-Symbols-of-a-Pair
# ---------------------------------------------------------------------------

CM_PAIR_MARK_PRICE_COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
CM_PAIR_MARK_PRICE_COLUMNS = CM_PAIR_MARK_PRICE_COLUMNS_MAP.keys()


class CMPairMarkPriceHandlerBase(Handler):
    """Base handler for the COIN-M ``SubType.PAIR_MARK_PRICE`` stream (``<pair>@markPrice[@1s]``).

    COIN-M only: delivers a ``markPriceUpdate`` array covering every symbol
    of the given pair (e.g. ``BTCUSD_PERP``, ``BTCUSD_201225``).  Distinct
    from the per-symbol ``<symbol>@markPrice`` stream (which delivers a
    single dict) and from ``!markPrice@arr`` (all markets, not pair-scoped).

    Each array element matches the per-symbol CM ``markPriceUpdate`` shape
    (no ``ap`` field — CM lacks it).

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-of-All-Symbols-of-a-Pair
    """

    COLUMNS_MAP = CM_PAIR_MARK_PRICE_COLUMNS_MAP
    COLUMNS = CM_PAIR_MARK_PRICE_COLUMNS


CM_PAIR_MARK_PRICE_STREAM_SUFFIX = '@markPrice'


class CMPairMarkPriceProcessor(Processor):
    """Processor for the COIN-M pair mark price stream (``<pair>@markPrice[@1s]``).

    COIN-M only.  Requires a ``pair`` string parameter (e.g. ``'BTCUSD'``).
    Accepts an optional second positional argument ``'1s'`` to select the
    1000ms variant (default is 3000ms per docs).

    Routing matches stream names ending in ``@markPrice`` or
    ``@markPrice@1s`` and requires an array payload, which disambiguates
    from:
    - ``<symbol>@markPrice`` (per-symbol; delivers a single dict, dispatched
      by :class:`~binance.futures.streams.MarkPriceProcessor`).
    - ``!markPrice@arr[@1s]`` (all-market; stream name starts with ``!``,
      dispatched by :class:`~binance.futures.streams.AllMarketMarkPriceProcessor`).
    """

    HANDLER = CMPairMarkPriceHandlerBase
    SUB_TYPE = SubType.PAIR_MARK_PRICE

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)
        payload = msg.get(KEY_PAYLOAD)

        if stream_type is None or stream_type.startswith('!'):
            return False, None

        # Match <pair>@markPrice or <pair>@markPrice@1s suffix.
        if not (
            stream_type.endswith(CM_PAIR_MARK_PRICE_STREAM_SUFFIX)
            or stream_type.endswith(CM_PAIR_MARK_PRICE_STREAM_SUFFIX + '@1s')
        ):
            return False, None

        # Per-symbol <symbol>@markPrice delivers a single dict; only the
        # pair variant delivers an array.
        if type(payload) is not list:
            return False, None

        return True, payload

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<pair>@markPrice`` or ``<pair>@markPrice@1s``.

        Uses ``pair.lower()`` (preserves underscores) for consistency with
        the rest of the CM module.
        """
        pair = self._get_param_symbol(t, args)

        if len(args) >= 2:
            speed = args[1]
            if speed != '1s':
                raise InvalidSubTypeParamException(
                    t, 'speed',
                    "`SubType.PAIR_MARK_PRICE` accepts only ``'1s'`` "
                    'for the optional update_speed parameter '
                    "(default is 3s); got `%s`" % (speed,)
                )
            return f'{pair.lower()}@markPrice@1s'

        return f'{pair.lower()}@markPrice'


# ---------------------------------------------------------------------------
# CM-only: IndexPrice
# Wire stream: <pair>@indexPrice[@1s]
# Confirmed CM-only (2026-05-26): not documented on fstream (USDⓈ-M).
# Event type: 'indexPriceUpdate'
# Confirmed fields from CM docs:
#   e  'indexPriceUpdate'
#   E  event time
#   i  pair (e.g. 'BTCUSD')
#   p  index price
# ---------------------------------------------------------------------------

INDEX_PRICE_COLUMNS_MAP = {
    'e': 'type',
    'E': 'event_time',
    'i': 'pair',
    'p': 'index_price',
}

INDEX_PRICE_COLUMNS = INDEX_PRICE_COLUMNS_MAP.keys()


class IndexPriceHandlerBase(Handler):
    """Base handler for the COIN-M ``SubType.INDEX_PRICE`` stream (``<pair>@indexPrice[@1s]``).

    COIN-M only: delivers spot index price updates for the underlying pair.
    Each payload carries the event time, pair (e.g. ``'BTCUSD'``), and index price.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Index-Price-Stream
    """

    COLUMNS_MAP = INDEX_PRICE_COLUMNS_MAP
    COLUMNS = INDEX_PRICE_COLUMNS


class IndexPriceProcessor(Processor):
    """Processor for the COIN-M index price stream (``<pair>@indexPrice[@1s]``).

    COIN-M only.  Requires a ``pair`` string parameter (e.g. ``'BTCUSD'``).
    """

    HANDLER = IndexPriceHandlerBase
    SUB_TYPE = SubType.INDEX_PRICE
    PAYLOAD_TYPE = 'indexPriceUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<pair>@indexPrice`` or ``<pair>@indexPrice@1s``."""
        pair = self._get_param_symbol(t, args)
        base = f'{pair.lower()}@{SubType.INDEX_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


# ---------------------------------------------------------------------------
# CM-only: IndexPriceKline
# Wire stream: <pair>@indexPriceKline_<interval>
# Confirmed CM-only (2026-05-26).
# Event type: 'indexPrice_kline'
# Confirmed fields from CM docs:
# Outer:
#   e  'indexPrice_kline'
#   E  event time
#   ps pair
# Nested 'k': same as kline 'k' object (open_time, close_time, interval, OHLCV, etc.)
# ---------------------------------------------------------------------------

INDEX_PRICE_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    'ps': 'pair',
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

INDEX_PRICE_KLINE_COLUMNS = INDEX_PRICE_KLINE_COLUMNS_MAP.keys()


class IndexPriceKlineHandlerBase(Handler):
    """Base handler for the COIN-M ``SubType.INDEX_PRICE_KLINE`` stream (``<pair>@indexPriceKline_<interval>``).

    COIN-M only: delivers candlestick bars built from the underlying spot index price.
    The nested ``k`` object is flattened; outer ``ps`` (pair) is merged in.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Index-Kline-Candlestick-Streams
    """

    COLUMNS_MAP = INDEX_PRICE_KLINE_COLUMNS_MAP
    COLUMNS = INDEX_PRICE_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            'ps': payload['ps'],
            **k,
        }
        return super()._receive(flat, index)


class IndexPriceKlineProcessor(Processor):
    """Processor for the COIN-M index price kline stream (``<pair>@indexPriceKline_<interval>``).

    COIN-M only.  Requires a ``pair`` string and an ``interval`` parameter.
    """

    HANDLER = IndexPriceKlineHandlerBase
    SUB_TYPE = SubType.INDEX_PRICE_KLINE
    PAYLOAD_TYPE = 'indexPrice_kline'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<pair>@indexPriceKline_<interval>``."""
        pair = self._get_param_symbol(t, args)

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

        return f'{pair.lower()}@indexPriceKline_{interval}'


# ---------------------------------------------------------------------------
# CM-only: MarkPriceKline
# Wire stream: <symbol>@markPriceKline_<interval>
# Confirmed CM-only (2026-05-26): not documented on fstream (USDⓈ-M).
# Event type: 'markPrice_kline'
# Confirmed fields from CM docs:
# Outer:
#   e  'markPrice_kline'
#   E  event time
#   ps pair
# Nested 'k': same kline structure
# ---------------------------------------------------------------------------

MARK_PRICE_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    'ps': 'pair',
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

MARK_PRICE_KLINE_COLUMNS = MARK_PRICE_KLINE_COLUMNS_MAP.keys()


class MarkPriceKlineHandlerBase(Handler):
    """Base handler for the COIN-M ``SubType.MARK_PRICE_KLINE`` stream (``<symbol>@markPriceKline_<interval>``).

    COIN-M only: delivers candlestick bars built from the futures mark price.
    The nested ``k`` object is flattened; outer ``ps`` (pair) is merged in.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Kline-Candlestick-Streams
    """

    COLUMNS_MAP = MARK_PRICE_KLINE_COLUMNS_MAP
    COLUMNS = MARK_PRICE_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            'ps': payload['ps'],
            **k,
        }
        return super()._receive(flat, index)


class MarkPriceKlineProcessor(Processor):
    """Processor for the COIN-M mark price kline stream (``<symbol>@markPriceKline_<interval>``).

    COIN-M only.  Requires a ``symbol`` string (e.g. ``'BTCUSD_PERP'``) and an
    ``interval`` parameter.  Uses ``symbol.lower()`` to preserve underscores.
    """

    HANDLER = MarkPriceKlineHandlerBase
    SUB_TYPE = SubType.MARK_PRICE_KLINE
    PAYLOAD_TYPE = 'markPrice_kline'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPriceKline_<interval>``."""
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

        return f'{symbol.lower()}@markPriceKline_{interval}'


PROCESSORS = [
    MarkPriceProcessor,
    AllMarketMarkPriceProcessor,
    AggTradeProcessor,
    KlineProcessor,
    ContinuousKlineProcessor,
    MiniTickerProcessor,
    AllMarketMiniTickersProcessor,
    TickerProcessor,
    AllMarketTickersProcessor,
    BookTickerProcessor,
    AllMarketBookTickerProcessor,
    ForceOrderProcessor,
    AllMarketLiquidationProcessor,
    PartialOrderBookProcessor,
    OrderBookProcessor,
    ContractInfoProcessor,
    CMPairMarkPriceProcessor,
    IndexPriceProcessor,
    IndexPriceKlineProcessor,
    MarkPriceKlineProcessor,
    FuturesUserProcessor,
]
