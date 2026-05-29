"""USDⓈ-M Futures stream wiring: handlers, processors, and the PROCESSORS list.

USDⓈ-M reuses the shared futures handler bases and processors from
:mod:`binance.futures.streams`.  This module:
1. Adds the USDⓈ-M-specific ``ap`` (mark price moving average) field to the
   mark-price column map.
2. Provides UM-only handler bases and processors for streams that do NOT exist
   on COIN-M: ``compositeIndex``, ``contractInfo``, ``assetIndex``,
   ``tradingSession``, ``rpiDepth`` (Diff Book Depth with RPI orders).
3. Registers the complete UM PROCESSORS list.

Confirmed UM vs CM payload differences (2026-05-26):
- Mark Price: UM has ``ap`` (mark price moving average); CM does NOT.
- Force Order nested 'o': UM does NOT have ``ps`` (pair); CM DOES.
- All-market Mark Price: UM array elements include ``ap``; CM does NOT.
- compositeIndex, assetIndex, tradingSession: UM-only (verified against dstream docs).
- contractInfo: present on both UM and CM (shared base in binance.futures.streams).

UM-only streams:
- compositeIndex: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Composite-Index-Symbol-Information-Streams
- contractInfo: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
- assetIndex: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Multi-Assets-Mode-Asset-Index
- tradingSession: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trading-Session-Stream
- rpiDepth (Diff Book Depth with RPI): https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams-RPI
"""

import re
from typing import ClassVar, List, Optional

from binance.core.common.constants import SubType, KEY_STREAM_TYPE, KEY_PAYLOAD
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor
from binance.core.common.utils import normalize_symbol

from binance.futures.streams import (  # noqa: F401  (re-exported for public API)
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
    VALID_CONTRACT_TYPES as _BASE_VALID_CONTRACT_TYPES,
    # Handler bases (shared)
    ForceOrderHandlerBase,
    ContinuousKlineHandlerBase,
    ContractInfoHandlerBase,
    AllMarketLiquidationHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketTickersHandlerBase,
    AllMarketBookTickerHandlerBase,
    # Processors (shared, used in PROCESSORS list below)
    ForceOrderProcessor,
    KlineProcessor,
    MiniTickerProcessor,
    TickerProcessor,
    BookTickerProcessor,
    PartialOrderBookProcessor,
    OrderBookProcessor,
    ContractInfoProcessor,
    AllMarketLiquidationProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketTickersProcessor,
    AllMarketBookTickerProcessor,
    # handler base aliases (re-exported under shared names)
    MarkPriceHandlerBase as _MarkPriceHandlerBase,
    AllMarketMarkPriceHandlerBase as _AllMarketMarkPriceHandlerBase,
    AggTradeHandlerBase as _AggTradeHandlerBase,
    MarkPriceProcessor as _MarkPriceProcessor,
    AllMarketMarkPriceProcessor as _AllMarketMarkPriceProcessor,
    AggTradeProcessor as _AggTradeProcessor,
    ContinuousKlineProcessor as _ContinuousKlineProcessor,
)

from binance.futures.user_processor import FuturesUserProcessor  # noqa: F401  (re-exported)


# ---------------------------------------------------------------------------
# UM-specific mark price: add the 'ap' (mark price moving average) field.
# Confirmed present in USDⓈ-M (2026-05-25); absent from COIN-M.
# Fields confirmed from docs (2026-05-25):
#   e  event type
#   E  event time
#   s  symbol
#   p  mark price
#   ap mark price moving average  <-- UM-only
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
# ---------------------------------------------------------------------------

MARK_PRICE_COLUMNS_MAP = {
    **MARK_PRICE_COLUMNS_MAP_BASE,
    'ap': 'mark_price_avg',  # UM-only field
}

MARK_PRICE_COLUMNS = MARK_PRICE_COLUMNS_MAP.keys()

FORCE_ORDER_COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP_BASE
FORCE_ORDER_COLUMNS = FORCE_ORDER_COLUMNS_MAP.keys()


class MarkPriceHandlerBase(_MarkPriceHandlerBase):
    """Base handler for the USDⓈ-M ``SubType.MARK_PRICE`` stream.

    Extends the shared :class:`~binance.futures.streams.MarkPriceHandlerBase` with
    the USDⓈ-M-specific ``ap`` (mark price moving average) column, which is present
    in USDⓈ-M payloads but absent from COIN-M.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``mark_price``, ``mark_price_avg``,
    ``funding_rate``, ``next_funding_time``).

    Example::

        from binance import UMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = UMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusdt')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP
    COLUMNS = MARK_PRICE_COLUMNS


class MarkPriceProcessor(_MarkPriceProcessor):
    """Processor for the USDⓈ-M mark-price stream (``<symbol>@markPrice``).

    Uses the USDⓈ-M :class:`MarkPriceHandlerBase` (which includes ``ap``).
    """

    HANDLER = MarkPriceHandlerBase


# ---------------------------------------------------------------------------
# UM-specific all-market mark price: array elements include 'ap'.
# ---------------------------------------------------------------------------

ALL_MARKET_MARK_PRICE_COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP
ALL_MARKET_MARK_PRICE_COLUMNS = MARK_PRICE_COLUMNS


class AllMarketMarkPriceHandlerBase(_AllMarketMarkPriceHandlerBase):
    """Base handler for the USDⓈ-M ``SubType.ALL_MARKET_MARK_PRICE`` stream (``!markPrice@arr[@1s]``).

    Extends the shared base with the USDⓈ-M-specific ``ap`` (mark price moving average)
    column.  COIN-M all-market mark-price elements do NOT include ``ap``.

    Subclass this and override ``receive(payload)`` to handle the event.
    """

    COLUMNS_MAP = ALL_MARKET_MARK_PRICE_COLUMNS_MAP
    COLUMNS = ALL_MARKET_MARK_PRICE_COLUMNS


class AllMarketMarkPriceProcessor(_AllMarketMarkPriceProcessor):
    """Processor for the USDⓈ-M all-market mark price stream (``!markPrice@arr[@1s]``).

    Uses the USDⓈ-M :class:`AllMarketMarkPriceHandlerBase` (which includes ``ap``).
    """

    HANDLER = AllMarketMarkPriceHandlerBase


# ---------------------------------------------------------------------------
# UM-specific agg trade: add the 'nq' (normal quantity excluding RPI trades).
# Per developers.binance.com USDⓈ-M aggregate-trade docs (2026-05), the UM
# payload carries ``nq`` — the aggregated quantity that excludes RPI (retail
# price improvement) trades.  COIN-M does NOT publish ``nq``; CM continues to
# use the shared ``AggTradeHandlerBase``.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
# ---------------------------------------------------------------------------

UM_AGG_TRADE_COLUMNS_MAP = {
    **FUTURES_AGG_TRADE_COLUMNS_MAP,
    'nq': 'normal_quantity',
}

UM_AGG_TRADE_COLUMNS = UM_AGG_TRADE_COLUMNS_MAP.keys()


class AggTradeHandlerBase(_AggTradeHandlerBase):
    """Base handler for the USDⓈ-M ``SubType.AGG_TRADE`` stream.

    Extends the shared :class:`~binance.futures.streams.AggTradeHandlerBase`
    with the USDⓈ-M-specific ``nq`` (normal quantity excluding RPI trades)
    column, which is published by USDⓈ-M aggregate-trade events but absent
    from COIN-M.
    """

    COLUMNS_MAP = UM_AGG_TRADE_COLUMNS_MAP
    COLUMNS = UM_AGG_TRADE_COLUMNS


class AggTradeProcessor(_AggTradeProcessor):
    """Processor for the USDⓈ-M aggregate trade stream (``<symbol>@aggTrade``).

    Binds to the USDⓈ-M :class:`AggTradeHandlerBase` (which includes
    ``normal_quantity``).
    """

    HANDLER = AggTradeHandlerBase


# ---------------------------------------------------------------------------
# UM Continuous Kline: widen contractType to include `tradifi_perpetual`.
#
# Per developers.binance.com 2025-12-11 changelog the TradFi-Perps product
# was added; the UM continuous-kline stream accepts ``tradifi_perpetual``
# in addition to the three base contract types (``PERPETUAL``,
# ``CURRENT_QUARTER``, ``NEXT_QUARTER``).  COIN-M does NOT publish
# ``tradifi_perpetual`` for this stream.
# Docs:
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
# ---------------------------------------------------------------------------

UM_VALID_CONTRACT_TYPES = _BASE_VALID_CONTRACT_TYPES | frozenset(
    ('TRADIFI_PERPETUAL',)
)


class ContinuousKlineProcessor(_ContinuousKlineProcessor):
    """Processor for the USDⓈ-M continuous-contract kline stream.

    Widens :attr:`VALID_CONTRACT_TYPES` to include ``TRADIFI_PERPETUAL`` per
    the 2025-12-11 derivatives changelog.  Otherwise identical to the shared
    futures :class:`_ContinuousKlineProcessor`.
    """

    VALID_CONTRACT_TYPES = UM_VALID_CONTRACT_TYPES


# ---------------------------------------------------------------------------
# UM-only: rpiDepth (Diff Book Depth Streams with RPI)
# Wire stream: <symbol>@rpiDepth@500ms
# USDⓈ-M only (not documented on COIN-M).  Payload schema mirrors the regular
# Diff Book Depth Stream — but the bids/asks arrays aggregate RPI (Retail
# Price Improvement) orders alongside the normal limit orders.  Per docs the
# stream supports only the 500ms speed (no other intervals).
#
# Confirmed fields per developers.binance.com (UM, 2026-05):
#   e   event type ('depthUpdate')
#   E   event time
#   T   transaction time
#   s   symbol
#   U   first update id in event
#   u   final update id in event
#   pu  final update id in last stream
#   b   bids to be updated [ [price, quantity], ... ]   (includes RPI)
#   a   asks to be updated [ [price, quantity], ... ]   (includes RPI)
# A price level whose quantity equals zero indicates either a filled / cancelled
# quotation or a hidden crossed RPI order at that level (RPI-specific
# semantics per docs).
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams-RPI
# ---------------------------------------------------------------------------

UM_RPI_DEPTH_COLUMNS_MAP = {
    'e': 'type',
    'E': 'event_time',
    'T': 'transaction_time',
    's': 'symbol',
    'U': 'first_update_id',
    'u': 'final_update_id',
    'pu': 'prev_final_update_id',
    'b': 'bids',
    'a': 'asks',
}

UM_RPI_DEPTH_COLUMNS = UM_RPI_DEPTH_COLUMNS_MAP.keys()

# Per docs the rpiDepth stream supports ONLY 500ms.
UM_RPI_DEPTH_SPEED = 500

# Match `<symbol>@rpiDepth[@<speed>ms]`.  The trailing `@500ms` is the only
# documented form, but the regex tolerates the absence of the suffix so that
# any future docs revision adding more speeds can be wired with a one-line
# change to the speed validator (not the dispatch).
UM_RPI_DEPTH_STREAM_PATTERN = re.compile(r'@rpiDepth(?:@\d+ms)?$')


class UMRpiDepthHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.RPI_DIFF_DEPTH`` stream (``<symbol>@rpiDepth@500ms``).

    USDⓈ-M only: not present on COIN-M.  Per developers.binance.com the payload
    is the same shape as the regular Diff Book Depth stream (``e='depthUpdate'``,
    ``E``, ``T``, ``s``, ``U``, ``u``, ``pu``, ``b``, ``a``) but the bids/asks
    arrays aggregate RPI (Retail Price Improvement) orders alongside the
    normal limit orders.

    The bids and asks arrays are passed through as single-cell lists so
    downstream consumers can iterate the price/quantity pairs without losing
    structure.  A price level whose quantity equals zero indicates either a
    filled/cancelled quotation or a hidden crossed RPI order at that level
    (RPI-specific semantics per docs).

    Subclass this and override ``receive(payload)`` to handle the event.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams-RPI
    """

    COLUMNS_MAP = UM_RPI_DEPTH_COLUMNS_MAP
    COLUMNS = UM_RPI_DEPTH_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        # Wrap the bids/asks arrays as single-cell values so pandas treats
        # each as one cell rather than expanding the price/qty rows.
        wrapped = dict(payload)
        if 'b' in wrapped:
            wrapped['b'] = [wrapped['b']]
        if 'a' in wrapped:
            wrapped['a'] = [wrapped['a']]
        return super()._receive(wrapped, index)


class UMRpiDepthProcessor(Processor):
    """Processor for the USDⓈ-M rpi diff-depth stream (``<symbol>@rpiDepth@500ms``).

    USDⓈ-M only.  Per docs only the 500ms speed is supported; the
    ``subscribe_param`` validator rejects any other value.

    Routing matches the stream-name suffix ``@rpiDepth[@<speed>ms]`` and
    excludes the regular ``@depth`` / ``@depth<N>`` streams (which carry the
    same ``e='depthUpdate'``).
    """

    HANDLER = UMRpiDepthHandlerBase
    SUB_TYPE = SubType.RPI_DIFF_DEPTH

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type is None or not UM_RPI_DEPTH_STREAM_PATTERN.search(stream_type):
            return False, None

        return True, msg.get(KEY_PAYLOAD)

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@rpiDepth@500ms``.

        Accepts an optional second positional argument ``update_speed``; per
        docs only ``500`` is documented and the validator rejects any other
        value.  Calling without a speed parameter defaults to ``500``.
        """
        symbol = self._get_param_symbol(t, args)

        if len(args) >= 2:
            speed = args[1]
            if type(speed) is not int:
                raise InvalidSubTypeParamException(
                    t, 'speed', '`int` expected but got `%s`' % speed)
            if speed != UM_RPI_DEPTH_SPEED:
                raise InvalidSubTypeParamException(
                    t, 'speed',
                    '`speed` must be %d (the only documented rpiDepth speed) '
                    'but got `%s`' % (UM_RPI_DEPTH_SPEED, speed)
                )

        return f'{normalize_symbol(symbol)}@rpiDepth@{UM_RPI_DEPTH_SPEED}ms'


# ---------------------------------------------------------------------------
# UM-only: CompositeIndex
# Wire stream: <symbol>@compositeIndex
# USDⓈ-M only (dstream / COIN-M docs do not list this stream).
# Event type: 'compositeIndex'
# Confirmed fields per developers.binance.com (UM, 2026-05):
#   e  'compositeIndex'
#   E  event time
#   s  symbol (composite index symbol, e.g. 'DEFIUSDT')
#   p  price
#   C  composition method (string label, e.g. 'baseAsset')
#   c  composition entries (list); each element has:
#         b  base asset symbol
#         q  quote asset
#         w  weight in quantity
#         W  weight in percentage
#         i  component index price
# The top-level COLUMNS_MAP surfaces the scalar fields plus the
# ``composition_method`` label; the ``composition`` list is exposed as a
# pass-through cell so downstream consumers can introspect.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Composite-Index-Symbol-Information-Streams
# ---------------------------------------------------------------------------

COMPOSITE_INDEX_COLUMNS_MAP = {
    'e': 'type',
    'E': 'event_time',
    's': 'symbol',
    'p': 'price',
    'C': 'composition_method',
    'c': 'composition',
}

COMPOSITE_INDEX_COLUMNS = COMPOSITE_INDEX_COLUMNS_MAP.keys()


class CompositeIndexHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.COMPOSITE_INDEX`` stream (``<symbol>@compositeIndex``).

    USDⓈ-M only: not present on COIN-M.  Delivers the current index composition
    and price for composite index symbols (e.g. ``DEFIUSDT``).  The payload's
    top-level ``C`` is the composition method label (e.g. ``baseAsset``); the
    ``c`` array carries each constituent's base asset symbol, weights and
    component index price.  The ``composition`` column is passed through as
    a Python list cell.

    Subclass this and override ``receive(payload)`` to handle the event.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Composite-Index-Symbol-Information-Streams
    """

    COLUMNS_MAP = COMPOSITE_INDEX_COLUMNS_MAP
    COLUMNS = COMPOSITE_INDEX_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        # ``c`` is the composition list; wrap it as a single-element outer
        # list so pandas treats it as one cell rather than expanding it to
        # one row per constituent.
        if 'c' in payload:
            payload = {**payload, 'c': [payload['c']]}
        return super()._receive(payload, index)


class CompositeIndexProcessor(Processor):
    """Processor for the USDⓈ-M composite index stream (``<symbol>@compositeIndex``).

    USDⓈ-M only.
    """

    HANDLER = CompositeIndexHandlerBase
    SUB_TYPE = SubType.COMPOSITE_INDEX
    PAYLOAD_TYPE = 'compositeIndex'


# ---------------------------------------------------------------------------
# UM-only: AssetIndex
# Wire streams:
#   !assetIndex@arr   — all-asset index array
#   <asset>@assetIndex — per-asset index
# Confirmed UM-only (2026-05-26): multi-assets mode is a UM feature only.
# Event type: 'assetIndexUpdate'
# Confirmed fields from UM docs:
#   e  'assetIndexUpdate'
#   E  event time
#   s  asset (e.g. 'ETH')
#   i  index price
#   b  bid buffer
#   a  ask buffer
#   B  bid rate
#   A  ask rate
#   q  auto-exchange bid buffer
#   g  auto-exchange ask buffer
#   Q  auto-exchange bid rate
#   G  auto-exchange ask rate
# ---------------------------------------------------------------------------

ASSET_INDEX_COLUMNS_MAP = {
    'e': 'type',
    'E': 'event_time',
    's': 'asset',
    'i': 'index_price',
    'b': 'bid_buffer',
    'a': 'ask_buffer',
    'B': 'bid_rate',
    'A': 'ask_rate',
    'q': 'auto_exchange_bid_buffer',
    'g': 'auto_exchange_ask_buffer',
    'Q': 'auto_exchange_bid_rate',
    'G': 'auto_exchange_ask_rate',
}

ASSET_INDEX_COLUMNS = ASSET_INDEX_COLUMNS_MAP.keys()


class AssetIndexHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.ASSET_INDEX`` stream.

    USDⓈ-M only: the multi-assets mode asset index stream.
    Supports two subscription forms:
    - ``<asset>@assetIndex`` (per-asset, requires ``asset`` parameter).
    - ``!assetIndex@arr`` (all-asset array, no parameter required).

    Delivers the asset index price plus bid/ask buffers and rates used in
    multi-assets margin mode calculations.

    Subclass this and override ``receive(payload)`` to handle the event.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Multi-Assets-Mode-Asset-Index
    """

    COLUMNS_MAP = ASSET_INDEX_COLUMNS_MAP
    COLUMNS = ASSET_INDEX_COLUMNS


class AssetIndexProcessor(Processor):
    """Processor for the USDⓈ-M per-asset index stream (``<asset>@assetIndex``).

    USDⓈ-M only.  Requires an ``asset`` string parameter (e.g. ``'ETH'``).
    """

    HANDLER = AssetIndexHandlerBase
    SUB_TYPE = SubType.ASSET_INDEX
    PAYLOAD_TYPE = 'assetIndexUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<asset>@assetIndex``."""
        asset = self._get_param_symbol(t, args)
        return f'{normalize_symbol(asset)}@assetIndex'


ALL_ASSET_INDEX_STREAM = '!assetIndex@arr'


class AllAssetIndexHandlerBase(Handler):
    """Base handler for the USDⓈ-M all-asset index array stream (``!assetIndex@arr``).

    Distinct from :class:`AssetIndexHandlerBase` so that the processor dispatch
    framework can route ``!assetIndex@arr`` (via :class:`AllAssetIndexProcessor`)
    independently from the per-asset ``<asset>@assetIndex`` stream (via
    :class:`AssetIndexProcessor`).  Both handlers use the same column mapping.

    Subclass this and override ``receive(payload)`` to handle the event.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Multi-Assets-Mode-Asset-Index
    """

    COLUMNS_MAP = ASSET_INDEX_COLUMNS_MAP
    COLUMNS = ASSET_INDEX_COLUMNS


class AllAssetIndexProcessor(Processor):
    """Processor for the USDⓈ-M all-asset index array stream (``!assetIndex@arr``).

    USDⓈ-M only.  No parameter required.  Uses :class:`AllAssetIndexHandlerBase`.
    """

    HANDLER = AllAssetIndexHandlerBase
    SUB_TYPE = SubType.ASSET_INDEX
    STREAM_TYPE_NAME: ClassVar[str] = ALL_ASSET_INDEX_STREAM

    def supports_subtype(self, t):
        # Both AssetIndexProcessor and AllAssetIndexProcessor share SUB_TYPE.ASSET_INDEX.
        # The distinction is that AllAssetIndexProcessor matches !assetIndex@arr by name.
        # supports_subtype is True for both; is_message_type differentiates at dispatch time.
        return t == self.SUB_TYPE

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'asset',
                'All-asset index array (``!assetIndex@arr``) expects no parameters'
            )
        return self.STREAM_TYPE_NAME


# ---------------------------------------------------------------------------
# UM-only: TradingSession
# Wire stream: tradingSession  (no symbol prefix, no @arr suffix)
# USDⓈ-M only: US equities / commodities session events.
# Event types: 'EquityUpdate' or 'CommodityUpdate'
# Confirmed fields per developers.binance.com (UM, 2026-05):
#   e  event type ('EquityUpdate' or 'CommodityUpdate')
#   E  event time
#   S  session type — one of PRE_MARKET, REGULAR, AFTER_MARKET, OVERNIGHT,
#                     NO_TRADING
#   t  session start time (ms timestamp)
#   T  session end time   (ms timestamp)
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trading-Session-Stream
# ---------------------------------------------------------------------------

TRADING_SESSION_COLUMNS_MAP = {
    'e': 'type',
    'E': 'event_time',
    'S': 'session_type',
    't': 'session_start_time',
    'T': 'session_end_time',
}

TRADING_SESSION_COLUMNS = TRADING_SESSION_COLUMNS_MAP.keys()

TRADING_SESSION_STREAM = 'tradingSession'
TRADING_SESSION_PAYLOAD_TYPES = ('EquityUpdate', 'CommodityUpdate')


class TradingSessionHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.TRADING_SESSION`` stream.

    USDⓈ-M only: delivers US equity and commodity market session events.
    Per developers.binance.com each payload carries the event type (one of
    ``'EquityUpdate'`` / ``'CommodityUpdate'``), the session type ``S``
    (``PRE_MARKET`` / ``REGULAR`` / ``AFTER_MARKET`` / ``OVERNIGHT`` /
    ``NO_TRADING``), the session start time ``t`` and session end time ``T``
    (both ms timestamps).

    Subclass this and override ``receive(payload)`` to handle the event.

    Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trading-Session-Stream
    """

    COLUMNS_MAP = TRADING_SESSION_COLUMNS_MAP
    COLUMNS = TRADING_SESSION_COLUMNS


class TradingSessionProcessor(Processor):
    """Processor for the USDⓈ-M trading session stream (``tradingSession``).

    USDⓈ-M only.  Matches both ``EquityUpdate`` and ``CommodityUpdate`` event types.
    """

    HANDLER = TradingSessionHandlerBase
    SUB_TYPE = SubType.TRADING_SESSION
    PAYLOAD_TYPES = TRADING_SESSION_PAYLOAD_TYPES
    STREAM_TYPE_NAME: ClassVar[str] = TRADING_SESSION_STREAM

    def is_message_type(self, msg):
        from binance.core.common.constants import KEY_PAYLOAD, KEY_PAYLOAD_TYPE
        payload = msg.get(KEY_PAYLOAD)

        if (
            payload is not None
            and type(payload) is dict
            and payload.get(KEY_PAYLOAD_TYPE) in self.PAYLOAD_TYPES
        ):
            return True, payload

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.TRADING_SESSION` expects no parameters'
            )
        return self.STREAM_TYPE_NAME


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
    CompositeIndexProcessor,
    ContractInfoProcessor,
    AssetIndexProcessor,
    AllAssetIndexProcessor,
    TradingSessionProcessor,
    UMRpiDepthProcessor,
    FuturesUserProcessor,
]
