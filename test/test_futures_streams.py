"""Tests for the shared futures market-data stream gap fills.

Covers all shared futures handler bases and processors in binance/futures/streams.py,
plus UM-specific and CM-specific streams in their respective modules.

Test pattern mirrors test_futures_um.py / test_futures_cm.py:
- subscribe_param produces the correct wire name
- client._receive routes payloads to handlers and column maps rename correctly
- All branches of is_message_type are exercised

Markets tested:
- Shared (via UMFuturesClient): aggTrade, kline, miniTicker, ticker, bookTicker,
  partialOrderBook, orderBook, continuousKline, contractInfo,
  !markPrice@arr, !forceOrder@arr, !miniTicker@arr, !ticker@arr, !bookTicker
- UM-specific: compositeIndex, assetIndex, !assetIndex@arr, tradingSession
- CM-specific (via CMFuturesClient): indexPrice, indexPriceKline, markPriceKline
"""

import pytest
from stock_pandas import TimeFrame

from binance import (
    UMFuturesClient,
    CMFuturesClient,
    SubType,
    FuturesAggTradeHandlerBase,
    FuturesKlineHandlerBase,
    FuturesMiniTickerHandlerBase,
    FuturesTickerHandlerBase,
    FuturesBookTickerHandlerBase,
    FuturesPartialOrderBookHandlerBase,
    FuturesContinuousKlineHandlerBase,
    FuturesContractInfoHandlerBase,
    AllMarketMarkPriceHandlerBase,
    FuturesAllMarketLiquidationHandlerBase,
    FuturesAllMarketMiniTickersHandlerBase,
    FuturesAllMarketTickersHandlerBase,
    FuturesAllMarketBookTickerHandlerBase,
    CompositeIndexHandlerBase,
    AssetIndexHandlerBase,
    AllAssetIndexHandlerBase,
    TradingSessionHandlerBase,
    IndexPriceHandlerBase,
    IndexPriceKlineHandlerBase,
    MarkPriceKlineHandlerBase,
)
from binance.core.common.utils import create_future
from binance.core.common.exceptions import InvalidSubTypeParamException


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def um_client():
    return UMFuturesClient().start()


@pytest.fixture
def cm_client():
    return CMFuturesClient().start()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def run_handler(client, HandlerBase, payload, stream):
    """Drive a payload through client._receive and return the handler DataFrame."""
    future = create_future()

    class Handler(HandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.handler(Handler())
    await client._receive({'data': payload, 'stream': stream})
    return await future


# ===========================================================================
# SHARED: AggTrade
# ===========================================================================

UM_AGG_TRADE_PAYLOAD = {
    'e': 'aggTrade',
    'E': 1591702613884,
    's': 'BTCUSDT',
    'a': 424951223,
    'p': '9703.80000000',
    'q': '0.01100000',
    'f': 606073342,
    'l': 606073342,
    'T': 1591702613869,
    'm': False,
}


def test_futures_agg_trade_subscribe_param():
    from binance.futures.streams import AggTradeProcessor
    proc = AggTradeProcessor(None)
    assert proc.subscribe_param(True, SubType.AGG_TRADE, 'BTCUSDT') == 'btcusdt@aggTrade'


@pytest.mark.asyncio
async def test_futures_agg_trade_handler_columns(um_client):
    # UMFuturesClient binds the UM-specific AggTradeHandlerBase (which adds
    # ``nq``).  The shared ``FuturesAggTradeHandlerBase`` is exported for use
    # on CMFuturesClient (CM does not publish ``nq``).
    from binance.futures.um.streams import AggTradeHandlerBase as UMAggTradeHandlerBase
    df = await run_handler(
        um_client, UMAggTradeHandlerBase, UM_AGG_TRADE_PAYLOAD, 'btcusdt@aggTrade'
    )
    row = df.iloc[0]
    assert row['type'] == 'aggTrade'
    assert row['event_time'] == 1591702613884
    assert row['symbol'] == 'BTCUSDT'
    assert row['agg_trade_id'] == 424951223
    assert row['price'] == '9703.80000000'
    assert row['quantity'] == '0.01100000'
    assert row['first_trade_id'] == 606073342
    assert row['last_trade_id'] == 606073342
    assert row['trade_time'] == 1591702613869
    assert row['is_maker'] == False  # noqa: E712  (numpy bool: 'is False' fails)


@pytest.mark.asyncio
async def test_cm_agg_trade_handler_columns_uses_shared_base(cm_client):
    # CMFuturesClient uses the shared FuturesAggTradeHandlerBase directly since
    # CM does not publish UM-only ``nq``.
    df = await run_handler(
        cm_client, FuturesAggTradeHandlerBase,
        {**UM_AGG_TRADE_PAYLOAD, 's': 'BTCUSD_PERP'}, 'btcusd_perp@aggTrade'
    )
    row = df.iloc[0]
    assert row['type'] == 'aggTrade'
    assert row['symbol'] == 'BTCUSD_PERP'


# ===========================================================================
# SHARED: Kline
# ===========================================================================

UM_KLINE_PAYLOAD = {
    'e': 'kline',
    'E': 1638747660000,
    's': 'BTCUSDT',
    'k': {
        't': 1638747660000,
        'T': 1638747719999,
        's': 'BTCUSDT',
        'i': '1m',
        'f': 100,
        'L': 200,
        'o': '53000.00',
        'h': '53500.00',
        'l': '52900.00',
        'c': '53200.00',
        'v': '10.5',
        'n': 100,
        'x': False,
        'q': '557100.0',
        'V': '5.2',
        'Q': '276000.0',
    }
}


def test_futures_kline_subscribe_param():
    from binance.futures.streams import KlineProcessor
    proc = KlineProcessor(None)
    assert proc.subscribe_param(True, SubType.KLINE, 'BTCUSDT', TimeFrame.m1) == 'btcusdt@kline_1m'


def test_futures_kline_subscribe_param_invalid_interval():
    from binance.futures.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSDT', TimeFrame.Y1)


def test_futures_kline_intervals_exclude_1s():
    """Per developers.binance.com, futures kline streams start at ``1m``.

    The ``1s`` interval is Spot-only and is not accepted on UM, CM, or any of
    the futures-derived kline streams (kline, continuousKline, indexPriceKline,
    markPriceKline).
    """
    from binance.futures.streams import VALID_FUTURES_KLINE_INTERVALS
    assert '1s' not in VALID_FUTURES_KLINE_INTERVALS


def test_futures_kline_subscribe_param_rejects_1s():
    from binance.futures.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSDT', TimeFrame.s1)


def test_futures_continuous_kline_subscribe_param_rejects_1s():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 'PERPETUAL', TimeFrame.s1
        )


def test_cm_kline_subscribe_param_rejects_1s():
    from binance.futures.cm.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSD_PERP', TimeFrame.s1)


def test_cm_index_price_kline_subscribe_param_rejects_1s():
    from binance.futures.cm.streams import IndexPriceKlineProcessor
    proc = IndexPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(True, SubType.INDEX_PRICE_KLINE, 'BTCUSD', TimeFrame.s1)


def test_cm_mark_price_kline_subscribe_param_rejects_1s():
    from binance.futures.cm.streams import MarkPriceKlineProcessor
    proc = MarkPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        proc.subscribe_param(True, SubType.MARK_PRICE_KLINE, 'BTCUSD_PERP', TimeFrame.s1)


def test_futures_kline_subscribe_param_no_interval():
    from binance.futures.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSDT')


@pytest.mark.asyncio
async def test_futures_kline_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesKlineHandlerBase, UM_KLINE_PAYLOAD, 'btcusdt@kline_1m'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSDT'
    assert row['interval'] == '1m'
    assert row['open'] == '53000.00'
    assert row['close'] == '53200.00'
    assert row['is_closed'] == False  # noqa: E712  (numpy bool)
    assert row['event_time'] == 1638747660000


# ===========================================================================
# SHARED: MiniTicker
# ===========================================================================

MINI_TICKER_PAYLOAD = {
    'e': '24hrMiniTicker',
    'E': 1638747660000,
    's': 'BTCUSDT',
    'o': '50000.0',
    'h': '55000.0',
    'l': '49000.0',
    'c': '53000.0',
    'v': '100.0',
    'q': '5200000.0',
}


def test_futures_mini_ticker_subscribe_param():
    from binance.futures.streams import MiniTickerProcessor
    proc = MiniTickerProcessor(None)
    assert proc.subscribe_param(True, SubType.MINI_TICKER, 'BTCUSDT') == 'btcusdt@miniTicker'


@pytest.mark.asyncio
async def test_futures_mini_ticker_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesMiniTickerHandlerBase, MINI_TICKER_PAYLOAD, 'btcusdt@miniTicker'
    )
    row = df.iloc[0]
    assert row['type'] == '24hrMiniTicker'
    assert row['event_time'] == 1638747660000
    assert row['symbol'] == 'BTCUSDT'
    assert row['open'] == '50000.0'
    assert row['volume'] == '100.0'


# ===========================================================================
# SHARED: Ticker (24hrTicker)
# ===========================================================================

# UM and CM ticker payloads per developers.binance.com (2026-05) include only:
# e, E, s, p, P, w, c, Q, o, h, l, v, q, O, C, F, L, n.
# Spot-only fields x (first_trade_price), b/B (best bid), a/A (best ask) are NOT
# present in futures ticker payloads.
TICKER_PAYLOAD = {
    'e': '24hrTicker',
    'E': 1638747660000,
    's': 'BTCUSDT',
    'p': '3000.0',
    'P': '6.00',
    'w': '52000.0',
    'c': '53000.0',
    'Q': '0.5',
    'o': '50000.0',
    'h': '55000.0',
    'l': '49000.0',
    'v': '100.0',
    'q': '5200000.0',
    'O': 0,
    'C': 86400000,
    'F': 100,
    'L': 200,
    'n': 100,
}


def test_futures_ticker_subscribe_param():
    from binance.futures.streams import TickerProcessor
    proc = TickerProcessor(None)
    assert proc.subscribe_param(True, SubType.TICKER, 'BTCUSDT') == 'btcusdt@ticker'


def test_futures_ticker_columns_map_excludes_spot_only_fields():
    """Per developers.binance.com, futures ticker payloads do not include the
    Spot-only fields ``x`` (first trade price), ``b``/``B`` (best bid price/qty),
    or ``a``/``A`` (best ask price/qty).  These keys must not appear in the
    shared FUTURES_TICKER_COLUMNS_MAP for either UM or CM.
    """
    from binance.futures.streams import FUTURES_TICKER_COLUMNS_MAP
    for stale_key in ('x', 'b', 'B', 'a', 'A'):
        assert stale_key not in FUTURES_TICKER_COLUMNS_MAP, (
            f'stale spot-only key {stale_key!r} must not appear in '
            f'FUTURES_TICKER_COLUMNS_MAP'
        )


@pytest.mark.asyncio
async def test_futures_ticker_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesTickerHandlerBase, TICKER_PAYLOAD, 'btcusdt@ticker'
    )
    row = df.iloc[0]
    assert row['type'] == '24hrTicker'
    assert row['symbol'] == 'BTCUSDT'
    assert row['price_change'] == '3000.0'
    assert row['percent'] == '6.00'
    assert row['last_price'] == '53000.0'
    assert row['weighted_average_price'] == '52000.0'
    assert row['last_quantity'] == '0.5'
    assert row['total_trades'] == 100


# ===========================================================================
# SHARED: BookTicker (per-symbol, no 'e' field)
# ===========================================================================

BOOK_TICKER_PAYLOAD = {
    'u': 400900217,
    'E': 1638747660000,
    'T': 1638747660001,
    's': 'BTCUSDT',
    'b': '50000.0',
    'B': '1.0',
    'a': '50001.0',
    'A': '2.0',
}


def test_futures_book_ticker_subscribe_param():
    from binance.futures.streams import BookTickerProcessor
    proc = BookTickerProcessor(None)
    assert proc.subscribe_param(True, SubType.BOOK_TICKER, 'BTCUSDT') == 'btcusdt@bookTicker'


def test_futures_book_ticker_is_message_type_matches():
    from binance.futures.streams import BookTickerProcessor
    proc = BookTickerProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': 'btcusdt@bookTicker',
        'data': BOOK_TICKER_PAYLOAD
    })
    assert is_match
    assert payload == BOOK_TICKER_PAYLOAD


def test_futures_book_ticker_is_message_type_excludes_all_market():
    from binance.futures.streams import BookTickerProcessor
    proc = BookTickerProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!bookTicker',
        'data': BOOK_TICKER_PAYLOAD
    })
    assert not is_match


def test_futures_book_ticker_is_message_type_no_stream():
    from binance.futures.streams import BookTickerProcessor
    proc = BookTickerProcessor(None)
    is_match, _ = proc.is_message_type({'data': BOOK_TICKER_PAYLOAD})
    assert not is_match


@pytest.mark.asyncio
async def test_futures_book_ticker_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesBookTickerHandlerBase, BOOK_TICKER_PAYLOAD, 'btcusdt@bookTicker'
    )
    row = df.iloc[0]
    assert row['update_id'] == 400900217
    assert row['symbol'] == 'BTCUSDT'
    assert row['best_bid_price'] == '50000.0'
    assert row['best_ask_price'] == '50001.0'


# ===========================================================================
# SHARED: PartialOrderBook
# ===========================================================================

# Futures partial-depth wire shape per developers.binance.com (UM + CM):
#   {e, E, T, s, [ps], U, u, [pu], b, a}
# distinct from Spot's snapshot shape ({lastUpdateId, bids, asks}); the futures
# stream emits `b` / `a` arrays of [price, qty] pairs and exposes both first
# (`U`) and final (`u`) update IDs alongside the previous-event final
# (`pu`) so consumers can walk the diff cursor.  The handler returns
# (last_update_id, bids_df, asks_df) using `u` as last_update_id, matching
# the Spot PartialOrderBookHandlerBase tuple shape.
# Docs:
# - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
# - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
PARTIAL_DEPTH_PAYLOAD = {
    'e': 'depthUpdate',
    'E': 1591270260907,
    'T': 1591270260891,
    's': 'BTCUSDT',
    'U': 7654320,
    'u': 7654321,
    'pu': 7654319,
    'b': [['50000.0', '1.0'], ['49999.0', '2.0']],
    'a': [['50001.0', '0.5'], ['50002.0', '1.5']],
}


def test_futures_partial_order_book_subscribe_param_default():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT') == 'btcusdt@depth20'


def test_futures_partial_order_book_subscribe_param_with_level():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5) == 'btcusdt@depth5'


def test_futures_partial_order_book_subscribe_param_with_speed():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 10, 500
    ) == 'btcusdt@depth10@500ms'


def test_futures_partial_order_book_subscribe_param_invalid_level():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='level'):
        proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 15)


def test_futures_partial_order_book_subscribe_param_level_not_int():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='level'):
        proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', '5')


def test_futures_partial_order_book_subscribe_param_invalid_speed():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5, 1000)


def test_futures_partial_order_book_subscribe_param_speed_250ms():
    """Per developers.binance.com, futures partial-depth update speed accepts
    ``100ms``, ``250ms`` (default) and ``500ms``.  ``250ms`` was missing from
    the validator and is now allowed.
    """
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 10, 250
    ) == 'btcusdt@depth10@250ms'


def test_futures_order_book_subscribe_param_with_250ms_speed():
    """Diff-depth (``<symbol>@depth``) also accepts 250ms per docs."""
    from binance.futures.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(
        True, SubType.ORDER_BOOK, 'BTCUSDT', 250
    ) == 'btcusdt@depth@250ms'


def test_futures_depth_speeds_includes_100_250_500():
    """Verify the canonical futures depth speed set per docs."""
    from binance.futures.streams import FUTURES_DEPTH_SPEEDS
    assert set(FUTURES_DEPTH_SPEEDS) == {100, 250, 500}


def test_cm_partial_order_book_subscribe_param_with_250ms_speed():
    from binance.futures.cm.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSD_PERP', 5, 250
    ) == 'btcusd_perp@depth5@250ms'


def test_cm_order_book_subscribe_param_with_250ms_speed():
    from binance.futures.cm.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(
        True, SubType.ORDER_BOOK, 'BTCUSD_PERP', 250
    ) == 'btcusd_perp@depth@250ms'


# ---------------------------------------------------------------------------
# CM partial-depth payload includes ``ps`` (pair), per docs:
# https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
# The CM-specific PartialOrderBookHandlerBase surfaces ``pair`` alongside
# ``lastUpdateId`` / bids / asks so consumers can route by pair.
# ---------------------------------------------------------------------------

# Futures CM partial-depth payload shape per docs:
#   {e, E, T, s, ps, U, u, pu, b, a} -- same as UM plus the CM-only ``ps``
CM_PARTIAL_DEPTH_PAYLOAD = {
    'e': 'depthUpdate',
    'E': 1591270260907,
    'T': 1591270260891,
    's': 'BTCUSD_PERP',
    'ps': 'BTCUSD',
    'U': 17276700,
    'u': 17276701,
    'pu': 17276699,
    'b': [['9523.0', '5'], ['9522.8', '8']],
    'a': [['9524.6', '2'], ['9524.7', '3']],
}


def test_cm_partial_order_book_handler_exposes_pair():
    """CM PartialOrderBookHandlerBase MUST return ``(pair, last_update_id, bids, asks)``
    per the CM docs that publish ``ps``.  Uses the docs-confirmed futures
    wire shape with ``b`` / ``a`` arrays and ``u`` as the final update id.
    """
    from binance.futures.cm.streams import PartialOrderBookHandlerBase
    handler = PartialOrderBookHandlerBase()
    result = handler._receive({
        'e': 'depthUpdate',
        'E': 1591270260907,
        'T': 1591270260891,
        's': 'BTCUSD_PERP',
        'ps': 'BTCUSD',
        'U': 17276700,
        'u': 17276701,
        'pu': 17276699,
        'b': [['9523.0', '5']],
        'a': [['9524.6', '2']],
    })
    pair, last_update_id, bids, asks = result
    assert pair == 'BTCUSD'
    assert last_update_id == 17276701
    assert bids.iloc[0]['price'] == '9523.0'
    assert asks.iloc[0]['price'] == '9524.6'


def test_cm_partial_order_book_processor_binds_cm_handler():
    """``CMPartialOrderBookProcessor`` MUST bind the CM-specific handler."""
    from binance.futures.cm.streams import (
        PartialOrderBookProcessor as CMPartialProc,
        PartialOrderBookHandlerBase as CMPartialHB,
    )
    assert CMPartialProc.HANDLER is CMPartialHB


# ---------------------------------------------------------------------------
# CM diff-depth payload includes ``ps`` (pair) per docs:
# https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
# The CM-specific OrderBookHandlerBase surfaces ``pair`` on the diff-event
# info DataFrame produced by ``_receive``.
# ---------------------------------------------------------------------------

def test_cm_diff_depth_handler_column_map_includes_pair():
    """The CM diff-depth handler's column map MUST include ``ps -> pair``."""
    from binance.futures.cm.streams import OrderBookHandlerBase as CMOrderBookHB
    assert CMOrderBookHB.COLUMNS_MAP.get('ps') == 'pair'


def test_cm_order_book_processor_binds_cm_handler():
    """``CMOrderBookProcessor`` MUST bind the CM-specific
    :class:`OrderBookHandlerBase` (which carries the ``ps -> pair`` column).
    """
    from binance.futures.cm.streams import (
        OrderBookProcessor as CMOrderProc,
        OrderBookHandlerBase as CMOrderBookHB,
    )
    assert CMOrderProc.HANDLER is CMOrderBookHB


def test_futures_partial_order_book_subscribe_param_speed_not_int():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5, '100')


def test_futures_partial_order_book_is_message_type_matches():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': 'btcusdt@depth5',
        'data': PARTIAL_DEPTH_PAYLOAD
    })
    assert is_match
    assert payload == PARTIAL_DEPTH_PAYLOAD


def test_futures_partial_order_book_is_message_type_with_speed():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth20@500ms',
        'data': PARTIAL_DEPTH_PAYLOAD
    })
    assert is_match


def test_futures_partial_order_book_is_message_type_rejects_diff_depth():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth',
        'data': PARTIAL_DEPTH_PAYLOAD
    })
    assert not is_match


def test_futures_partial_order_book_is_message_type_rejects_bad_payload():
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth5',
        'data': {'e': 'snapshot'}
    })
    assert not is_match


@pytest.mark.asyncio
async def test_futures_partial_order_book_handler(um_client):
    df = await run_handler(
        um_client, FuturesPartialOrderBookHandlerBase, PARTIAL_DEPTH_PAYLOAD, 'btcusdt@depth5'
    )
    # Per developers.binance.com (UM/CM Partial Book Depth Streams), the futures
    # partial-depth payload exposes ``b`` / ``a`` arrays and ``U`` / ``u`` / ``pu``
    # update IDs; the handler returns ``(last_update_id, bids_df, asks_df)``
    # using ``u`` (final update id in event) as ``last_update_id`` so downstream
    # consumers can reconcile against the diff-depth stream's cursor.
    last_update_id, bids, asks = df
    assert last_update_id == 7654321  # PARTIAL_DEPTH_PAYLOAD['u']
    assert bids.iloc[0]['price'] == '50000.0'
    assert asks.iloc[0]['price'] == '50001.0'


def test_futures_partial_order_book_handler_rejects_spot_shape():
    """Futures partial-depth handler MUST read ``b`` / ``a`` keys (not Spot
    ``bids`` / ``asks``).  A spot-shape payload missing ``b`` / ``a``
    raises ``KeyError`` rather than silently NaN-ing.  Pins the futures vs
    spot wire shape distinction per docs.
    """
    from binance.futures.streams import PartialOrderBookHandlerBase
    handler = PartialOrderBookHandlerBase()
    spot_shape = {
        'lastUpdateId': 1234,
        'bids': [['50000.0', '1.0']],
        'asks': [['50001.0', '0.5']],
    }
    with pytest.raises(KeyError):
        handler._receive(spot_shape)


def test_futures_partial_order_book_is_message_type_rejects_spot_shape():
    """Futures PartialOrderBookProcessor MUST gate by futures ``b`` / ``a``
    keys, not Spot ``bids`` / ``asks``.
    """
    from binance.futures.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth5',
        'data': {
            'lastUpdateId': 1234,
            'bids': [['50000.0', '1.0']],
            'asks': [['50001.0', '0.5']],
        },
    })
    assert not is_match


# ===========================================================================
# SHARED: OrderBook (diff depth)
#
# The diff-depth events are routed to the unified core
# :class:`~binance.OrderBookHandlerBase` (R9d wiring) which maintains a local
# :class:`~binance.futures.orderbook.FuturesOrderBook`. The local-book
# semantics are covered by ``test_futures_orderbook.py``; here we only
# exercise the ``OrderBookProcessor`` ``subscribe_param`` branches.
# ===========================================================================


def test_futures_order_book_subscribe_param_default():
    from binance.futures.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSDT') == 'btcusdt@depth'


def test_futures_order_book_subscribe_param_with_speed():
    from binance.futures.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSDT', 100) == 'btcusdt@depth@100ms'


def test_futures_order_book_subscribe_param_invalid_speed():
    from binance.futures.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        # 1000ms is Spot-only; futures depth supports 100 / 250 / 500.
        proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSDT', 1000)


# Note: the raw futures ``OrderBookHandlerBase`` has been replaced by the
# unified high-level :class:`~binance.OrderBookHandlerBase` (R9d). The diff
# events on this stream now drive a local :class:`FuturesOrderBook`; column
# conversion of raw diff events is no longer part of the public surface,
# so the former ``test_futures_order_book_handler_columns`` was removed.


# ===========================================================================
# SHARED: ContinuousKline
# ===========================================================================

CONTINUOUS_KLINE_PAYLOAD = {
    'e': 'continuous_kline',
    'E': 1638747660000,
    'ps': 'BTCUSDT',
    'ct': 'PERPETUAL',
    'k': {
        't': 1638747660000,
        'T': 1638747719999,
        's': '',
        'i': '1m',
        'f': 100,
        'L': 200,
        'o': '53000.00',
        'h': '53500.00',
        'l': '52900.00',
        'c': '53200.00',
        'v': '10.5',
        'n': 100,
        'x': True,
        'q': '557100.0',
        'V': '5.2',
        'Q': '276000.0',
    }
}


def test_futures_continuous_kline_subscribe_param():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 'PERPETUAL', TimeFrame.m1
    )
    assert result == 'btcusdt_perpetual@continuousKline_1m'


def test_futures_continuous_kline_subscribe_param_current_quarter():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 'CURRENT_QUARTER', TimeFrame.H1
    )
    assert result == 'btcusdt_current_quarter@continuousKline_1h'


def test_futures_continuous_kline_subscribe_param_no_args():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='pair/contract_type/interval'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE)


def test_futures_continuous_kline_subscribe_param_invalid_contract_type():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 'INVALID_TYPE', TimeFrame.m1
        )


def test_futures_continuous_kline_subscribe_param_invalid_interval():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 'PERPETUAL', TimeFrame.Y1
        )


def test_futures_continuous_kline_subscribe_param_bad_pair_type():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='pair'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 123, 'PERPETUAL', TimeFrame.m1)


def test_futures_continuous_kline_subscribe_param_bad_contract_type_type():
    from binance.futures.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', 123, TimeFrame.m1)


@pytest.mark.asyncio
async def test_futures_continuous_kline_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesContinuousKlineHandlerBase,
        CONTINUOUS_KLINE_PAYLOAD, 'btcusdt_perpetual@continuousKline_1m'
    )
    row = df.iloc[0]
    assert row['type'] == 'continuous_kline'
    assert row['pair'] == 'BTCUSDT'
    assert row['contract_type'] == 'PERPETUAL'
    assert row['open'] == '53000.00'
    assert row['is_closed'] == True  # noqa: E712  (numpy bool)


# ===========================================================================
# SHARED: ContractInfo
# ===========================================================================

CONTRACT_INFO_PAYLOAD = {
    'e': 'contractInfo',
    'E': 1638747660000,
    's': 'BTCUSDT_221230',
    'ps': 'BTCUSDT',
    'ct': 'CURRENT_QUARTER',
    'dt': 1672214400000,
    'ot': 1638316800000,
    'cs': 'TRADING',
    'bks': []
}


def test_futures_contract_info_subscribe_param():
    from binance.futures.streams import ContractInfoProcessor
    proc = ContractInfoProcessor(None)
    assert proc.subscribe_param(True, SubType.CONTRACT_INFO) == '!contractInfo'


def test_futures_contract_info_subscribe_param_rejects_args():
    from binance.futures.streams import ContractInfoProcessor
    proc = ContractInfoProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.CONTRACT_INFO, 'BTCUSDT')


def test_futures_contract_info_is_message_type_matches():
    from binance.futures.streams import ContractInfoProcessor
    proc = ContractInfoProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': '!contractInfo',
        'data': CONTRACT_INFO_PAYLOAD
    })
    assert is_match
    assert payload == CONTRACT_INFO_PAYLOAD


def test_futures_contract_info_is_message_type_no_match():
    from binance.futures.streams import ContractInfoProcessor
    proc = ContractInfoProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@something',
        'data': CONTRACT_INFO_PAYLOAD
    })
    assert not is_match


@pytest.mark.asyncio
async def test_futures_contract_info_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesContractInfoHandlerBase, CONTRACT_INFO_PAYLOAD, '!contractInfo'
    )
    row = df.iloc[0]
    assert row['type'] == 'contractInfo'
    assert row['symbol'] == 'BTCUSDT_221230'
    assert row['pair'] == 'BTCUSDT'
    assert row['contract_type'] == 'CURRENT_QUARTER'
    assert row['contract_status'] == 'TRADING'


# ===========================================================================
# SHARED: AllMarketMarkPrice
# ===========================================================================

MARK_PRICE_ITEM = {
    'e': 'markPriceUpdate',
    'E': 1638747660000,
    's': 'BTCUSDT',
    'p': '53000.0',
    'i': '52900.0',
    'P': '52950.0',
    'r': '0.0001',
    'T': 1638748800000,
}


def test_all_market_mark_price_subscribe_param_default():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_MARK_PRICE) == '!markPrice@arr'


def test_all_market_mark_price_subscribe_param_1s():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_MARK_PRICE, '1s') == '!markPrice@arr@1s'


def test_all_market_mark_price_subscribe_param_too_many_args():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='ALL_MARKET_MARK_PRICE'):
        proc.subscribe_param(True, SubType.ALL_MARKET_MARK_PRICE, '1s', 'extra')


def test_all_market_mark_price_is_message_type_matches():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!markPrice@arr',
        'data': [MARK_PRICE_ITEM]
    })
    assert is_match


def test_all_market_mark_price_is_message_type_matches_1s():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!markPrice@arr@1s',
        'data': [MARK_PRICE_ITEM]
    })
    assert is_match


def test_all_market_mark_price_is_message_type_no_match():
    from binance.futures.streams import AllMarketMarkPriceProcessor
    proc = AllMarketMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@markPrice',
        'data': MARK_PRICE_ITEM
    })
    assert not is_match


@pytest.mark.asyncio
async def test_all_market_mark_price_handler_columns(um_client):
    # Use the UM-specific AllMarketMarkPriceHandlerBase (includes 'ap'); the shared
    # FuturesAllMarketMarkPriceHandlerBase is the base class and is not accepted by the
    # UM AllMarketMarkPriceProcessor (HANDLER = UM-specific subclass).
    df = await run_handler(
        um_client, AllMarketMarkPriceHandlerBase, [MARK_PRICE_ITEM], '!markPrice@arr'
    )
    row = df.iloc[0]
    assert row['type'] == 'markPriceUpdate'
    assert row['symbol'] == 'BTCUSDT'
    assert row['mark_price'] == '53000.0'


# ===========================================================================
# SHARED: AllMarketLiquidation
# ===========================================================================

FORCE_ORDER_ITEM = {
    'e': 'forceOrder',
    'E': 1568014460893,
    'o': {
        's': 'BTCUSDT',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'IOC',
        'q': '0.014',
        'p': '9910',
        'ap': '9910',
        'X': 'FILLED',
        'l': '0.014',
        'z': '0.014',
        'T': 1568014460893,
    }
}


def test_all_market_liquidation_subscribe_param():
    from binance.futures.streams import AllMarketLiquidationProcessor
    proc = AllMarketLiquidationProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_LIQUIDATION) == '!forceOrder@arr'


def test_all_market_liquidation_subscribe_param_rejects_args():
    from binance.futures.streams import AllMarketLiquidationProcessor
    proc = AllMarketLiquidationProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.ALL_MARKET_LIQUIDATION, 'BTCUSDT')


def test_all_market_liquidation_is_message_type_matches():
    from binance.futures.streams import AllMarketLiquidationProcessor
    proc = AllMarketLiquidationProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!forceOrder@arr',
        'data': [FORCE_ORDER_ITEM]
    })
    assert is_match


def test_all_market_liquidation_is_message_type_no_match():
    from binance.futures.streams import AllMarketLiquidationProcessor
    proc = AllMarketLiquidationProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@forceOrder',
        'data': FORCE_ORDER_ITEM
    })
    assert not is_match


@pytest.mark.asyncio
async def test_all_market_liquidation_handler_columns(um_client):
    df = await run_handler(
        um_client, FuturesAllMarketLiquidationHandlerBase, [FORCE_ORDER_ITEM], '!forceOrder@arr'
    )
    row = df.iloc[0]
    assert row['type'] == 'forceOrder'


def test_force_order_handler_docs_largest_per_1000ms():
    """Per developers.binance.com (2026-04-10 derivatives changelog effective
    2026-04-14), the UM/CM Liquidation Order stream emits only the *largest*
    liquidation in each 1000ms window per symbol — NOT the latest one.  The
    handler's class docstring MUST surface this semantic so consumers don't
    expect tick-level coverage.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
    """
    import inspect
    from binance.futures.streams import ForceOrderHandlerBase
    doc = inspect.getdoc(ForceOrderHandlerBase) or ''
    assert 'largest' in doc
    assert '1000ms' in doc
    # Guard against drift: explicitly assert the wrong word is NOT present
    # in the docs-aggregation sentence.
    assert 'latest one' not in doc


def test_all_market_liquidation_handler_docs_largest_per_1000ms():
    """The all-market liquidation stream (``!forceOrder@arr``) emits the
    largest liquidation order per 1000ms per symbol (same aggregation as the
    per-symbol stream).  Pin the docstring semantic.
    """
    import inspect
    from binance.futures.streams import AllMarketLiquidationHandlerBase
    doc = inspect.getdoc(AllMarketLiquidationHandlerBase) or ''
    assert 'largest' in doc
    assert '1000ms' in doc
    assert 'latest one' not in doc


# ===========================================================================
# SHARED: AllMarketMiniTickers
# ===========================================================================

def test_futures_all_market_mini_tickers_subscribe_param():
    from binance.futures.streams import AllMarketMiniTickersProcessor
    proc = AllMarketMiniTickersProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_MINI_TICKERS) == '!miniTicker@arr'


def test_futures_all_market_mini_tickers_rejects_args():
    from binance.futures.streams import AllMarketMiniTickersProcessor
    proc = AllMarketMiniTickersProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.ALL_MARKET_MINI_TICKERS, 'BTCUSDT')


def test_futures_all_market_mini_tickers_is_message_type():
    from binance.futures.streams import AllMarketMiniTickersProcessor
    proc = AllMarketMiniTickersProcessor(None)
    is_match, _ = proc.is_message_type({'stream': '!miniTicker@arr', 'data': [MINI_TICKER_PAYLOAD]})
    assert is_match


def test_futures_all_market_mini_tickers_is_message_type_no_match():
    from binance.futures.streams import AllMarketMiniTickersProcessor
    proc = AllMarketMiniTickersProcessor(None)
    is_match, _ = proc.is_message_type({'stream': 'btcusdt@miniTicker', 'data': MINI_TICKER_PAYLOAD})
    assert not is_match


@pytest.mark.asyncio
async def test_futures_all_market_mini_tickers_handler(um_client):
    df = await run_handler(
        um_client, FuturesAllMarketMiniTickersHandlerBase, [MINI_TICKER_PAYLOAD], '!miniTicker@arr'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSDT'


# ===========================================================================
# SHARED: AllMarketTickers
# ===========================================================================

def test_futures_all_market_tickers_subscribe_param():
    from binance.futures.streams import AllMarketTickersProcessor
    proc = AllMarketTickersProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_TICKERS) == '!ticker@arr'


def test_futures_all_market_tickers_rejects_args():
    from binance.futures.streams import AllMarketTickersProcessor
    proc = AllMarketTickersProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.ALL_MARKET_TICKERS, 'BTCUSDT')


def test_futures_all_market_tickers_is_message_type():
    from binance.futures.streams import AllMarketTickersProcessor
    proc = AllMarketTickersProcessor(None)
    is_match, _ = proc.is_message_type({'stream': '!ticker@arr', 'data': [TICKER_PAYLOAD]})
    assert is_match


def test_futures_all_market_tickers_is_message_type_no_match():
    from binance.futures.streams import AllMarketTickersProcessor
    proc = AllMarketTickersProcessor(None)
    is_match, _ = proc.is_message_type({'stream': 'btcusdt@ticker', 'data': TICKER_PAYLOAD})
    assert not is_match


@pytest.mark.asyncio
async def test_futures_all_market_tickers_handler(um_client):
    df = await run_handler(
        um_client, FuturesAllMarketTickersHandlerBase, [TICKER_PAYLOAD], '!ticker@arr'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSDT'
    assert row['total_trades'] == 100


# ===========================================================================
# SHARED: AllMarketBookTicker
# ===========================================================================

def test_futures_all_market_book_ticker_subscribe_param():
    from binance.futures.streams import AllMarketBookTickerProcessor
    proc = AllMarketBookTickerProcessor(None)
    assert proc.subscribe_param(True, SubType.ALL_MARKET_BOOK_TICKER) == '!bookTicker'


def test_futures_all_market_book_ticker_rejects_args():
    from binance.futures.streams import AllMarketBookTickerProcessor
    proc = AllMarketBookTickerProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.ALL_MARKET_BOOK_TICKER, 'BTCUSDT')


def test_futures_all_market_book_ticker_is_message_type_matches():
    from binance.futures.streams import AllMarketBookTickerProcessor
    proc = AllMarketBookTickerProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!bookTicker',
        'data': BOOK_TICKER_PAYLOAD
    })
    assert is_match


def test_futures_all_market_book_ticker_is_message_type_no_match():
    from binance.futures.streams import AllMarketBookTickerProcessor
    proc = AllMarketBookTickerProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@bookTicker',
        'data': BOOK_TICKER_PAYLOAD
    })
    assert not is_match


@pytest.mark.asyncio
async def test_futures_all_market_book_ticker_handler(um_client):
    df = await run_handler(
        um_client, FuturesAllMarketBookTickerHandlerBase, BOOK_TICKER_PAYLOAD, '!bookTicker'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSDT'
    assert row['best_bid_price'] == '50000.0'


# ===========================================================================
# UM-specific: CompositeIndex
# ===========================================================================

COMPOSITE_INDEX_PAYLOAD = {
    'e': 'compositeIndex',
    'E': 1638747660000,
    's': 'DEFIUSDT',
    'p': '580.2',
    'C': [
        {'b': 'AAVE', 'w': '1.23', 'W': '0.21', 'c': None},
    ]
}


def test_um_composite_index_subscribe_param():
    from binance.futures.um.streams import CompositeIndexProcessor
    proc = CompositeIndexProcessor(None)
    assert proc.subscribe_param(True, SubType.COMPOSITE_INDEX, 'DEFIUSDT') == 'defiusdt@compositeIndex'


@pytest.mark.asyncio
async def test_um_composite_index_handler_columns(um_client):
    df = await run_handler(
        um_client, CompositeIndexHandlerBase, COMPOSITE_INDEX_PAYLOAD, 'defiusdt@compositeIndex'
    )
    row = df.iloc[0]
    assert row['type'] == 'compositeIndex'
    assert row['symbol'] == 'DEFIUSDT'
    assert row['price'] == '580.2'


# ===========================================================================
# UM-specific: AssetIndex (per-asset and all-asset)
# ===========================================================================

ASSET_INDEX_PAYLOAD = {
    'e': 'assetIndexUpdate',
    'E': 1638747660000,
    's': 'ETH',
    'i': '0.000500',
    'b': '0.000495',
    'a': '0.000505',
    'B': '0.99',
    'A': '1.01',
    'q': '0.000490',
    'g': '0.000510',
    'Q': '0.98',
    'G': '1.02',
}


def test_um_asset_index_subscribe_param():
    from binance.futures.um.streams import AssetIndexProcessor
    proc = AssetIndexProcessor(None)
    assert proc.subscribe_param(True, SubType.ASSET_INDEX, 'ETH') == 'eth@assetIndex'


@pytest.mark.asyncio
async def test_um_asset_index_handler_columns(um_client):
    df = await run_handler(
        um_client, AssetIndexHandlerBase, ASSET_INDEX_PAYLOAD, 'eth@assetIndex'
    )
    row = df.iloc[0]
    assert row['type'] == 'assetIndexUpdate'
    assert row['asset'] == 'ETH'
    assert row['index_price'] == '0.000500'
    assert row['bid_rate'] == '0.99'


def test_um_all_asset_index_subscribe_param():
    from binance.futures.um.streams import AllAssetIndexProcessor
    proc = AllAssetIndexProcessor(None)
    assert proc.subscribe_param(True, SubType.ASSET_INDEX) == '!assetIndex@arr'


def test_um_all_asset_index_subscribe_param_rejects_args():
    from binance.futures.um.streams import AllAssetIndexProcessor
    proc = AllAssetIndexProcessor(None)
    with pytest.raises(InvalidSubTypeParamException):
        proc.subscribe_param(True, SubType.ASSET_INDEX, 'ETH')


def test_um_all_asset_index_is_message_type_matches():
    from binance.futures.um.streams import AllAssetIndexProcessor
    proc = AllAssetIndexProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!assetIndex@arr',
        'data': [ASSET_INDEX_PAYLOAD]
    })
    assert is_match


def test_um_all_asset_index_is_message_type_no_match():
    from binance.futures.um.streams import AllAssetIndexProcessor
    proc = AllAssetIndexProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'eth@assetIndex',
        'data': ASSET_INDEX_PAYLOAD
    })
    assert not is_match


@pytest.mark.asyncio
async def test_um_all_asset_index_handler_columns(um_client):
    # AllAssetIndexHandlerBase is the distinct class for the !assetIndex@arr stream.
    # Using AssetIndexHandlerBase would bind to AssetIndexProcessor (per-symbol),
    # which does not match the !assetIndex@arr stream name.
    df = await run_handler(
        um_client, AllAssetIndexHandlerBase, [ASSET_INDEX_PAYLOAD], '!assetIndex@arr'
    )
    row = df.iloc[0]
    assert row['asset'] == 'ETH'


# ===========================================================================
# UM-specific: TradingSession
# ===========================================================================

# Per developers.binance.com Trading-Session-Stream docs, ``T`` is the session
# end time (ms timestamp), ``t`` is the session start time, and ``S`` is the
# session type (one of PRE_MARKET, REGULAR, AFTER_MARKET, OVERNIGHT,
# NO_TRADING).  The original SDK mislabeled ``T`` as a "session state" string.
EQUITY_UPDATE_PAYLOAD = {
    'e': 'EquityUpdate',
    'E': 1638747660000,
    'S': 'REGULAR',
    't': 1638748000000,
    'T': 1638780000000,
}

COMMODITY_UPDATE_PAYLOAD = {
    'e': 'CommodityUpdate',
    'E': 1638747661000,
    'S': 'AFTER_MARKET',
    't': 1638780000001,
    'T': 1638790000000,
}


def test_um_trading_session_subscribe_param():
    from binance.futures.um.streams import TradingSessionProcessor
    proc = TradingSessionProcessor(None)
    assert proc.subscribe_param(True, SubType.TRADING_SESSION) == 'tradingSession'


def test_um_trading_session_subscribe_param_rejects_args():
    from binance.futures.um.streams import TradingSessionProcessor
    proc = TradingSessionProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        proc.subscribe_param(True, SubType.TRADING_SESSION, 'extra')


def test_um_trading_session_is_message_type_equity_update():
    from binance.futures.um.streams import TradingSessionProcessor
    proc = TradingSessionProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': 'tradingSession',
        'data': EQUITY_UPDATE_PAYLOAD
    })
    assert is_match
    assert payload == EQUITY_UPDATE_PAYLOAD


def test_um_trading_session_is_message_type_commodity_update():
    from binance.futures.um.streams import TradingSessionProcessor
    proc = TradingSessionProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'tradingSession',
        'data': COMMODITY_UPDATE_PAYLOAD
    })
    assert is_match


def test_um_trading_session_is_message_type_no_match():
    from binance.futures.um.streams import TradingSessionProcessor
    proc = TradingSessionProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'tradingSession',
        'data': {'e': 'unknownEvent', 'E': 123}
    })
    assert not is_match


@pytest.mark.asyncio
async def test_um_trading_session_handler_equity(um_client):
    df = await run_handler(
        um_client, TradingSessionHandlerBase, EQUITY_UPDATE_PAYLOAD, 'tradingSession'
    )
    row = df.iloc[0]
    assert row['type'] == 'EquityUpdate'
    assert row['session_type'] == 'REGULAR'
    assert row['session_start_time'] == 1638748000000
    assert row['session_end_time'] == 1638780000000


@pytest.mark.asyncio
async def test_um_trading_session_handler_commodity(um_client):
    df = await run_handler(
        um_client, TradingSessionHandlerBase, COMMODITY_UPDATE_PAYLOAD, 'tradingSession'
    )
    row = df.iloc[0]
    assert row['type'] == 'CommodityUpdate'
    assert row['session_type'] == 'AFTER_MARKET'
    assert row['session_start_time'] == 1638780000001
    assert row['session_end_time'] == 1638790000000


# ===========================================================================
# UM-specific: AllMarketMarkPrice includes 'ap' field
# ===========================================================================

MARK_PRICE_ITEM_WITH_AP = {
    **MARK_PRICE_ITEM,
    'ap': '52980.0',
}


@pytest.mark.asyncio
async def test_um_all_market_mark_price_includes_ap(um_client):
    from binance.futures.um.streams import AllMarketMarkPriceHandlerBase as UMAllMarketMarkPriceHandlerBase
    df = await run_handler(
        um_client, UMAllMarketMarkPriceHandlerBase, [MARK_PRICE_ITEM_WITH_AP], '!markPrice@arr'
    )
    row = df.iloc[0]
    assert row['mark_price_avg'] == '52980.0'


# ===========================================================================
# CM-specific: IndexPrice
# ===========================================================================

INDEX_PRICE_PAYLOAD = {
    'e': 'indexPriceUpdate',
    'E': 1638747660000,
    'i': 'BTCUSD',
    'p': '52900.0',
}


def test_cm_index_price_subscribe_param_default():
    from binance.futures.cm.streams import IndexPriceProcessor
    proc = IndexPriceProcessor(None)
    assert proc.subscribe_param(True, SubType.INDEX_PRICE, 'BTCUSD') == 'btcusd@indexPrice'


def test_cm_index_price_subscribe_param_1s():
    from binance.futures.cm.streams import IndexPriceProcessor
    proc = IndexPriceProcessor(None)
    assert proc.subscribe_param(True, SubType.INDEX_PRICE, 'BTCUSD', '1s') == 'btcusd@indexPrice@1s'


@pytest.mark.asyncio
async def test_cm_index_price_handler_columns(cm_client):
    df = await run_handler(
        cm_client, IndexPriceHandlerBase, INDEX_PRICE_PAYLOAD, 'btcusd@indexPrice'
    )
    row = df.iloc[0]
    assert row['type'] == 'indexPriceUpdate'
    assert row['pair'] == 'BTCUSD'
    assert row['index_price'] == '52900.0'


# ===========================================================================
# CM-specific: IndexPriceKline
# ===========================================================================

INDEX_PRICE_KLINE_PAYLOAD = {
    'e': 'indexPrice_kline',
    'E': 1638747660000,
    'ps': 'BTCUSD',
    'k': {
        't': 1638747660000,
        'T': 1638747719999,
        's': '',
        'i': '1m',
        'f': 0,
        'L': 0,
        'o': '52800.0',
        'h': '52900.0',
        'l': '52700.0',
        'c': '52900.0',
        'v': '0',
        'n': 0,
        'x': False,
        'q': '0',
        'V': '0',
        'Q': '0',
    }
}


def test_cm_index_price_kline_subscribe_param():
    from binance.futures.cm.streams import IndexPriceKlineProcessor
    proc = IndexPriceKlineProcessor(None)
    assert proc.subscribe_param(
        True, SubType.INDEX_PRICE_KLINE, 'BTCUSD', TimeFrame.m1
    ) == 'btcusd@indexPriceKline_1m'


def test_cm_index_price_kline_subscribe_param_no_interval():
    from binance.futures.cm.streams import IndexPriceKlineProcessor
    proc = IndexPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.INDEX_PRICE_KLINE, 'BTCUSD')


def test_cm_index_price_kline_subscribe_param_invalid_interval():
    from binance.futures.cm.streams import IndexPriceKlineProcessor
    proc = IndexPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.INDEX_PRICE_KLINE, 'BTCUSD', TimeFrame.Y1)


@pytest.mark.asyncio
async def test_cm_index_price_kline_handler_columns(cm_client):
    df = await run_handler(
        cm_client, IndexPriceKlineHandlerBase,
        INDEX_PRICE_KLINE_PAYLOAD, 'btcusd@indexPriceKline_1m'
    )
    row = df.iloc[0]
    assert row['type'] == 'indexPrice_kline'
    assert row['pair'] == 'BTCUSD'
    assert row['open'] == '52800.0'
    assert row['close'] == '52900.0'


# ===========================================================================
# CM-specific: MarkPriceKline
# ===========================================================================

MARK_PRICE_KLINE_PAYLOAD = {
    'e': 'markPrice_kline',
    'E': 1638747660000,
    'ps': 'BTCUSD',
    'k': {
        't': 1638747660000,
        'T': 1638747719999,
        's': 'BTCUSD_PERP',
        'i': '1m',
        'f': 0,
        'L': 0,
        'o': '52850.0',
        'h': '52950.0',
        'l': '52750.0',
        'c': '52950.0',
        'v': '0',
        'n': 0,
        'x': True,
        'q': '0',
        'V': '0',
        'Q': '0',
    }
}


def test_cm_mark_price_kline_subscribe_param():
    from binance.futures.cm.streams import MarkPriceKlineProcessor
    proc = MarkPriceKlineProcessor(None)
    assert proc.subscribe_param(
        True, SubType.MARK_PRICE_KLINE, 'BTCUSD_PERP', TimeFrame.m1
    ) == 'btcusd_perp@markPriceKline_1m'


def test_cm_mark_price_kline_subscribe_param_no_interval():
    from binance.futures.cm.streams import MarkPriceKlineProcessor
    proc = MarkPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.MARK_PRICE_KLINE, 'BTCUSD_PERP')


def test_cm_mark_price_kline_subscribe_param_invalid_interval():
    from binance.futures.cm.streams import MarkPriceKlineProcessor
    proc = MarkPriceKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.MARK_PRICE_KLINE, 'BTCUSD_PERP', TimeFrame.Y1)


@pytest.mark.asyncio
async def test_cm_mark_price_kline_handler_columns(cm_client):
    df = await run_handler(
        cm_client, MarkPriceKlineHandlerBase,
        MARK_PRICE_KLINE_PAYLOAD, 'btcusd_perp@markPriceKline_1m'
    )
    row = df.iloc[0]
    assert row['type'] == 'markPrice_kline'
    assert row['pair'] == 'BTCUSD'
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['open'] == '52850.0'
    assert row['is_closed'] == True  # noqa: E712  (numpy bool)


# ===========================================================================
# CM: per-symbol streams preserve underscores
# ===========================================================================

def test_cm_agg_trade_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import AggTradeProcessor
    proc = AggTradeProcessor(None)
    assert proc.subscribe_param(True, SubType.AGG_TRADE, 'BTCUSD_PERP') == 'btcusd_perp@aggTrade'


def test_cm_kline_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import KlineProcessor
    proc = KlineProcessor(None)
    result = proc.subscribe_param(True, SubType.KLINE, 'BTCUSD_PERP', TimeFrame.m1)
    assert result == 'btcusd_perp@kline_1m'


def test_cm_kline_subscribe_param_invalid_interval():
    from binance.futures.cm.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSD_PERP', TimeFrame.Y1)


def test_cm_kline_subscribe_param_no_interval():
    from binance.futures.cm.streams import KlineProcessor
    proc = KlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.KLINE, 'BTCUSD_PERP')


def test_cm_mini_ticker_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import MiniTickerProcessor
    proc = MiniTickerProcessor(None)
    assert proc.subscribe_param(True, SubType.MINI_TICKER, 'BTCUSD_PERP') == 'btcusd_perp@miniTicker'


def test_cm_ticker_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import TickerProcessor
    proc = TickerProcessor(None)
    assert proc.subscribe_param(True, SubType.TICKER, 'BTCUSD_PERP') == 'btcusd_perp@ticker'


def test_cm_book_ticker_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import BookTickerProcessor
    proc = BookTickerProcessor(None)
    assert proc.subscribe_param(True, SubType.BOOK_TICKER, 'BTCUSD_PERP') == 'btcusd_perp@bookTicker'


def test_cm_partial_order_book_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    result = proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSD_PERP')
    assert result == 'btcusd_perp@depth20'


def test_cm_partial_order_book_subscribe_param_with_speed():
    from binance.futures.cm.streams import PartialOrderBookProcessor
    proc = PartialOrderBookProcessor(None)
    result = proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSD_PERP', 5, 100)
    assert result == 'btcusd_perp@depth5@100ms'


def test_cm_order_book_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSD_PERP') == 'btcusd_perp@depth'


def test_cm_order_book_subscribe_param_with_speed():
    from binance.futures.cm.streams import OrderBookProcessor
    proc = OrderBookProcessor(None)
    assert proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSD_PERP', 500) == 'btcusd_perp@depth@500ms'


def test_cm_continuous_kline_subscribe_param_preserves_underscore():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSD', 'PERPETUAL', TimeFrame.m1
    )
    assert result == 'btcusd_perpetual@continuousKline_1m'


def test_cm_continuous_kline_subscribe_param_invalid_contract_type():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 'BTCUSD', 'FAKE', TimeFrame.m1)


def test_cm_continuous_kline_subscribe_param_invalid_interval():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 'BTCUSD', 'PERPETUAL', TimeFrame.Y1)


def test_cm_continuous_kline_subscribe_param_bad_pair_type():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='pair'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 123, 'PERPETUAL', TimeFrame.m1)


def test_cm_continuous_kline_subscribe_param_bad_contract_type_type():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE, 'BTCUSD', 123, TimeFrame.m1)


def test_cm_continuous_kline_subscribe_param_no_args():
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='pair/contract_type/interval'):
        proc.subscribe_param(True, SubType.CONTINUOUS_KLINE)


# ===========================================================================
# CM: ForceOrder includes 'pair' (ps) column
# ===========================================================================

CM_FORCE_ORDER_PAYLOAD = {
    'e': 'forceOrder',
    'E': 1638747660000,
    'o': {
        's': 'BTCUSD_PERP',
        'ps': 'BTCUSD',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'IOC',
        'q': '100',
        'p': '52000',
        'ap': '52000',
        'X': 'FILLED',
        'l': '100',
        'z': '100',
        'T': 1638747660000,
    }
}


@pytest.mark.asyncio
async def test_cm_force_order_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import ForceOrderHandlerBase as CMForceOrderHandlerBase
    df = await run_handler(
        cm_client, CMForceOrderHandlerBase, CM_FORCE_ORDER_PAYLOAD, 'btcusd_perp@forceOrder'
    )
    row = df.iloc[0]
    assert row['pair'] == 'BTCUSD'
    assert row['symbol'] == 'BTCUSD_PERP'


# ===========================================================================
# UM-only: AggTrade includes ``nq`` (non-RPI normal quantity)
# Per developers.binance.com UM aggregate trade docs, the USDⓈ-M payload now
# includes ``nq`` — the quantity excluding RPI (retail price improvement)
# trades.  COIN-M does NOT publish this field.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
# ===========================================================================

UM_AGG_TRADE_PAYLOAD_WITH_NQ = {
    **UM_AGG_TRADE_PAYLOAD,
    'nq': '0.01000000',
}


@pytest.mark.asyncio
async def test_um_agg_trade_handler_includes_nq(um_client):
    from binance.futures.um.streams import AggTradeHandlerBase as UMAggTradeHandlerBase
    df = await run_handler(
        um_client, UMAggTradeHandlerBase,
        UM_AGG_TRADE_PAYLOAD_WITH_NQ, 'btcusdt@aggTrade'
    )
    row = df.iloc[0]
    assert row['type'] == 'aggTrade'
    assert row['agg_trade_id'] == 424951223
    assert row['normal_quantity'] == '0.01000000'


def test_um_agg_trade_columns_map_includes_nq():
    from binance.futures.um.streams import UM_AGG_TRADE_COLUMNS_MAP
    assert UM_AGG_TRADE_COLUMNS_MAP.get('nq') == 'normal_quantity'


def test_cm_agg_trade_columns_map_excludes_nq():
    """COIN-M agg-trade payloads do not include ``nq`` per docs.

    The shared FUTURES_AGG_TRADE_COLUMNS_MAP must therefore not advertise it,
    so CM consumers do not see a NaN column.
    """
    from binance.futures.streams import FUTURES_AGG_TRADE_COLUMNS_MAP
    assert 'nq' not in FUTURES_AGG_TRADE_COLUMNS_MAP


# ===========================================================================
# BookTicker: payload includes ``e`` event type ("bookTicker") on both UM + CM
# Per developers.binance.com, the per-symbol and all-market bookTicker payloads
# both carry the ``e`` event type field.  Previously this field was excluded
# from the shared column map (causing it to drop on dataframe conversion).
# Docs:
#   UM https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
#   CM https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
# ===========================================================================

BOOK_TICKER_PAYLOAD_WITH_E = {
    'e': 'bookTicker',
    **BOOK_TICKER_PAYLOAD,
}


def test_futures_book_ticker_columns_map_includes_event_type():
    from binance.futures.streams import FUTURES_BOOK_TICKER_COLUMNS_MAP
    assert FUTURES_BOOK_TICKER_COLUMNS_MAP.get('e') == 'type'


@pytest.mark.asyncio
async def test_futures_book_ticker_handler_surfaces_event_type(um_client):
    df = await run_handler(
        um_client, FuturesBookTickerHandlerBase,
        BOOK_TICKER_PAYLOAD_WITH_E, 'btcusdt@bookTicker'
    )
    row = df.iloc[0]
    assert row['type'] == 'bookTicker'
    assert row['symbol'] == 'BTCUSDT'


@pytest.mark.asyncio
async def test_futures_all_market_book_ticker_surfaces_event_type(um_client):
    df = await run_handler(
        um_client, FuturesAllMarketBookTickerHandlerBase,
        BOOK_TICKER_PAYLOAD_WITH_E, '!bookTicker'
    )
    row = df.iloc[0]
    assert row['type'] == 'bookTicker'


# ===========================================================================
# CM-only: ``ps`` (pair) is present on per-symbol miniTicker, ticker,
# bookTicker, and the corresponding all-market arrays.  Per developers.binance.com
# COIN-M docs, each event includes the ``ps`` pair string alongside the
# instrument symbol ``s``.
# Docs (per-symbol):
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
# Docs (all-market):
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
#   https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
# ===========================================================================

CM_MINI_TICKER_PAYLOAD = {
    'e': '24hrMiniTicker',
    'E': 1638747660000,
    's': 'BTCUSD_PERP',
    'ps': 'BTCUSD',
    'o': '50000.0',
    'h': '55000.0',
    'l': '49000.0',
    'c': '53000.0',
    'v': '100.0',
    'q': '5200000.0',
}

CM_TICKER_PAYLOAD = {
    'e': '24hrTicker',
    'E': 1638747660000,
    's': 'BTCUSD_PERP',
    'ps': 'BTCUSD',
    'p': '3000.0',
    'P': '6.00',
    'w': '52000.0',
    'c': '53000.0',
    'Q': '0.5',
    'o': '50000.0',
    'h': '55000.0',
    'l': '49000.0',
    'v': '100.0',
    'q': '5200000.0',
    'O': 0,
    'C': 86400000,
    'F': 100,
    'L': 200,
    'n': 100,
}

CM_BOOK_TICKER_PAYLOAD = {
    'e': 'bookTicker',
    'u': 400900217,
    'E': 1638747660000,
    'T': 1638747660001,
    's': 'BTCUSD_PERP',
    'ps': 'BTCUSD',
    'b': '50000.0',
    'B': '1.0',
    'a': '50001.0',
    'A': '2.0',
}


def test_cm_mini_ticker_columns_map_includes_ps():
    from binance.futures.cm.streams import CM_MINI_TICKER_COLUMNS_MAP
    assert CM_MINI_TICKER_COLUMNS_MAP.get('ps') == 'pair'


def test_cm_ticker_columns_map_includes_ps():
    from binance.futures.cm.streams import CM_TICKER_COLUMNS_MAP
    assert CM_TICKER_COLUMNS_MAP.get('ps') == 'pair'


def test_cm_book_ticker_columns_map_includes_ps():
    from binance.futures.cm.streams import CM_BOOK_TICKER_COLUMNS_MAP
    assert CM_BOOK_TICKER_COLUMNS_MAP.get('ps') == 'pair'


@pytest.mark.asyncio
async def test_cm_mini_ticker_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import MiniTickerHandlerBase as CMMiniTickerHandlerBase
    df = await run_handler(
        cm_client, CMMiniTickerHandlerBase,
        CM_MINI_TICKER_PAYLOAD, 'btcusd_perp@miniTicker'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


@pytest.mark.asyncio
async def test_cm_ticker_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import TickerHandlerBase as CMTickerHandlerBase
    df = await run_handler(
        cm_client, CMTickerHandlerBase,
        CM_TICKER_PAYLOAD, 'btcusd_perp@ticker'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


@pytest.mark.asyncio
async def test_cm_book_ticker_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import BookTickerHandlerBase as CMBookTickerHandlerBase
    df = await run_handler(
        cm_client, CMBookTickerHandlerBase,
        CM_BOOK_TICKER_PAYLOAD, 'btcusd_perp@bookTicker'
    )
    row = df.iloc[0]
    assert row['type'] == 'bookTicker'
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


@pytest.mark.asyncio
async def test_cm_all_market_mini_tickers_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import (
        AllMarketMiniTickersHandlerBase as CMAllMarketMiniTickersHandlerBase,
    )
    df = await run_handler(
        cm_client, CMAllMarketMiniTickersHandlerBase,
        [CM_MINI_TICKER_PAYLOAD], '!miniTicker@arr'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


@pytest.mark.asyncio
async def test_cm_all_market_tickers_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import (
        AllMarketTickersHandlerBase as CMAllMarketTickersHandlerBase,
    )
    df = await run_handler(
        cm_client, CMAllMarketTickersHandlerBase,
        [CM_TICKER_PAYLOAD], '!ticker@arr'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


@pytest.mark.asyncio
async def test_cm_all_market_book_ticker_handler_includes_pair(cm_client):
    from binance.futures.cm.streams import (
        AllMarketBookTickerHandlerBase as CMAllMarketBookTickerHandlerBase,
    )
    df = await run_handler(
        cm_client, CMAllMarketBookTickerHandlerBase,
        CM_BOOK_TICKER_PAYLOAD, '!bookTicker'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'


# ===========================================================================
# UM-only: CompositeIndex top-level ``C`` (composition method) + nested ``c``
# composition array.  Each composition entry has ``b`` (base asset symbol),
# ``q`` (quote asset), ``w`` (weight in quantity), ``W`` (weight in percentage),
# and ``i`` (component index price).  The top-level COLUMNS_MAP gains ``C``
# and the composition list ``c`` as pass-through fields.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Composite-Index-Symbol-Information-Streams
# ===========================================================================

COMPOSITE_INDEX_PAYLOAD_FULL = {
    'e': 'compositeIndex',
    'E': 1638747660000,
    's': 'DEFIUSDT',
    'p': '580.2',
    'C': 'baseAsset',
    'c': [
        {'b': 'AAVE', 'q': 'USDT', 'w': '1.23',
         'W': '0.21', 'i': '92.0'},
        {'b': 'SUSHI', 'q': 'USDT', 'w': '0.45',
         'W': '0.08', 'i': '5.7'},
    ],
}


def test_um_composite_index_columns_map_includes_C_and_c():
    from binance.futures.um.streams import COMPOSITE_INDEX_COLUMNS_MAP
    assert COMPOSITE_INDEX_COLUMNS_MAP.get('C') == 'composition_method'
    assert COMPOSITE_INDEX_COLUMNS_MAP.get('c') == 'composition'


@pytest.mark.asyncio
async def test_um_composite_index_handler_surfaces_C_and_c(um_client):
    df = await run_handler(
        um_client, CompositeIndexHandlerBase,
        COMPOSITE_INDEX_PAYLOAD_FULL, 'defiusdt@compositeIndex'
    )
    row = df.iloc[0]
    assert row['symbol'] == 'DEFIUSDT'
    assert row['price'] == '580.2'
    assert row['composition_method'] == 'baseAsset'
    composition = row['composition']
    assert isinstance(composition, list)
    assert composition[0]['b'] == 'AAVE'
    assert composition[0]['i'] == '92.0'


# ===========================================================================
# UM-only: TradingSession column map sanity (assert the corrected ``T``, ``S``,
# ``t`` labels per developers.binance.com docs).
# Handler row checks live with the EQUITY_UPDATE_PAYLOAD / COMMODITY_UPDATE_PAYLOAD
# tests above (which were updated in this commit to reflect the corrected
# semantics — ``T`` = session end time, ``S`` = session type, ``t`` = start time).
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trading-Session-Stream
# ===========================================================================


def test_um_trading_session_columns_map_has_correct_T_S_t():
    from binance.futures.um.streams import TRADING_SESSION_COLUMNS_MAP
    assert TRADING_SESSION_COLUMNS_MAP.get('T') == 'session_end_time'
    assert TRADING_SESSION_COLUMNS_MAP.get('S') == 'session_type'
    assert TRADING_SESSION_COLUMNS_MAP.get('t') == 'session_start_time'


# ===========================================================================
# UM + CM: ContractInfo includes ``bks`` (brackets) per developers.binance.com.
# The brackets array contains nested elements ({bs, bnf, bnc, mmr, cf, mi, ma})
# describing the notional brackets.  The handler surfaces the list as the
# ``brackets`` column so downstream callers can introspect.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
# ===========================================================================

CONTRACT_INFO_PAYLOAD_WITH_BKS = {
    **CONTRACT_INFO_PAYLOAD,
    'bks': [
        {'bs': 1, 'bnf': 0, 'bnc': 50000, 'mmr': '0.004',
         'cf': 0, 'mi': 1, 'ma': 20},
        {'bs': 2, 'bnf': 50000, 'bnc': 250000, 'mmr': '0.005',
         'cf': 50, 'mi': 1, 'ma': 15},
    ],
}


def test_futures_contract_info_columns_map_includes_bks():
    from binance.futures.streams import CONTRACT_INFO_COLUMNS_MAP
    assert CONTRACT_INFO_COLUMNS_MAP.get('bks') == 'brackets'


@pytest.mark.asyncio
async def test_futures_contract_info_handler_surfaces_bks(um_client):
    df = await run_handler(
        um_client, FuturesContractInfoHandlerBase,
        CONTRACT_INFO_PAYLOAD_WITH_BKS, '!contractInfo'
    )
    row = df.iloc[0]
    assert row['type'] == 'contractInfo'
    assert row['symbol'] == 'BTCUSDT_221230'
    brackets = row['brackets']
    assert isinstance(brackets, list)
    assert brackets[0]['bs'] == 1
    assert brackets[0]['bnc'] == 50000
    assert brackets[1]['mmr'] == '0.005'


# ===========================================================================
# UM-only: rpiDepth (Diff Book Depth Streams with RPI)
# Wire stream: <symbol>@rpiDepth@500ms
# Per developers.binance.com (UM, 2026-05) the payload schema mirrors the
# regular Diff Book Depth stream — `e` ('depthUpdate'), `E`, `T`, `s`, `U`,
# `u`, `pu`, `b`, `a` — but the bids/asks arrays aggregate RPI (Retail Price
# Improvement) orders.  Only the 500ms speed is supported.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams-RPI
# ===========================================================================

RPI_DEPTH_PAYLOAD = {
    'e': 'depthUpdate',
    'E': 1591270260907,
    'T': 1591270260893,
    's': 'BTCUSDT',
    'U': 35092002,
    'u': 35092050,
    'pu': 35091926,
    'b': [['9650.0', '0.0']],
    'a': [['9651.0', '1.234']],
}


def test_um_rpi_diff_depth_subtype_value():
    # The new SubType wire name should be the SDK-internal key 'rpiDiffDepth'.
    assert str(SubType.RPI_DIFF_DEPTH) == 'rpiDiffDepth'


def test_um_rpi_diff_depth_subscribe_param_default():
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    assert proc.subscribe_param(
        True, SubType.RPI_DIFF_DEPTH, 'BTCUSDT'
    ) == 'btcusdt@rpiDepth@500ms'


def test_um_rpi_diff_depth_subscribe_param_with_explicit_500ms():
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    assert proc.subscribe_param(
        True, SubType.RPI_DIFF_DEPTH, 'BTCUSDT', 500
    ) == 'btcusdt@rpiDepth@500ms'


def test_um_rpi_diff_depth_subscribe_param_rejects_other_speed():
    """Per docs, rpiDepth supports ONLY the 500ms speed."""
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.RPI_DIFF_DEPTH, 'BTCUSDT', 100)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.RPI_DIFF_DEPTH, 'BTCUSDT', 250)


def test_um_rpi_diff_depth_subscribe_param_rejects_string_speed():
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.RPI_DIFF_DEPTH, 'BTCUSDT', '500')


def test_um_rpi_diff_depth_is_message_type_matches():
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': 'btcusdt@rpiDepth@500ms',
        'data': RPI_DEPTH_PAYLOAD,
    })
    assert is_match
    assert payload == RPI_DEPTH_PAYLOAD


def test_um_rpi_diff_depth_is_message_type_rejects_plain_depth():
    """`btcusdt@depth` (regular diff depth) must NOT match the rpi processor."""
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth',
        'data': RPI_DEPTH_PAYLOAD,
    })
    assert not is_match


def test_um_rpi_diff_depth_is_message_type_rejects_partial_depth():
    """`btcusdt@depth5` (partial depth snapshot) must NOT match."""
    from binance.futures.um.streams import UMRpiDepthProcessor
    proc = UMRpiDepthProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusdt@depth5@100ms',
        'data': RPI_DEPTH_PAYLOAD,
    })
    assert not is_match


def test_um_rpi_diff_depth_columns_map():
    from binance.futures.um.streams import UM_RPI_DEPTH_COLUMNS_MAP
    # Mirror the regular diff-depth field set.
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('e') == 'type'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('E') == 'event_time'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('T') == 'transaction_time'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('s') == 'symbol'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('U') == 'first_update_id'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('u') == 'final_update_id'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('pu') == 'prev_final_update_id'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('b') == 'bids'
    assert UM_RPI_DEPTH_COLUMNS_MAP.get('a') == 'asks'


@pytest.mark.asyncio
async def test_um_rpi_diff_depth_handler_columns(um_client):
    from binance import UMRpiDepthHandlerBase
    df = await run_handler(
        um_client, UMRpiDepthHandlerBase,
        RPI_DEPTH_PAYLOAD, 'btcusdt@rpiDepth@500ms'
    )
    row = df.iloc[0]
    assert row['type'] == 'depthUpdate'
    assert row['symbol'] == 'BTCUSDT'
    assert row['first_update_id'] == 35092002
    assert row['final_update_id'] == 35092050
    assert row['prev_final_update_id'] == 35091926
    # bids/asks pass through as lists (single-cell semantics)
    assert isinstance(row['bids'], list)
    assert isinstance(row['asks'], list)
    assert row['bids'][0] == ['9650.0', '0.0']
    assert row['asks'][0] == ['9651.0', '1.234']


def test_um_rpi_diff_depth_in_um_processors():
    """The new processor MUST be wired into the UM PROCESSORS list."""
    from binance.futures.um.streams import PROCESSORS, UMRpiDepthProcessor
    assert UMRpiDepthProcessor in PROCESSORS


# ===========================================================================
# CM-only: <pair>@markPrice (Mark Price of All Symbols of a Pair)
# Wire stream: <pair>@markPrice or <pair>@markPrice@1s
# COIN-M only: delivers a markPriceUpdate ARRAY containing every symbol of a
# given pair (e.g. BTCUSD_PERP, BTCUSD_201225, ...).  Distinct from
# `<symbol>@markPrice` (single symbol) and `!markPrice@arr` (all markets).
# Default speed = 3000ms; @1s = 1000ms (per docs).
# Each element is a markPriceUpdate dict — same field set as per-symbol CM
# markPrice (no `ap`, since CM lacks `ap`).
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-of-All-Symbols-of-a-Pair
# ===========================================================================

CM_PAIR_MARK_PRICE_ITEM = {
    'e': 'markPriceUpdate',
    'E': 1638747660000,
    's': 'BTCUSD_200925',
    'p': '11185.87786614',
    'P': '11185.87786614',
    'i': '11185.87786614',
    'r': '0.00038167',
    'T': 1638748800000,
}


def test_cm_pair_mark_price_subtype_value():
    assert str(SubType.PAIR_MARK_PRICE) == 'pairMarkPrice'


def test_cm_pair_mark_price_subscribe_param_default():
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PAIR_MARK_PRICE, 'BTCUSD'
    ) == 'btcusd@markPrice'


def test_cm_pair_mark_price_subscribe_param_1s():
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PAIR_MARK_PRICE, 'BTCUSD', '1s'
    ) == 'btcusd@markPrice@1s'


def test_cm_pair_mark_price_subscribe_param_rejects_other_speed():
    """Per docs only the (default) 3s stream and the @1s variant exist."""
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.PAIR_MARK_PRICE, 'BTCUSD', '3s')
    with pytest.raises(InvalidSubTypeParamException, match='speed'):
        proc.subscribe_param(True, SubType.PAIR_MARK_PRICE, 'BTCUSD', 1000)


def test_cm_pair_mark_price_subscribe_param_no_pair():
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='symbol'):
        proc.subscribe_param(True, SubType.PAIR_MARK_PRICE)


def test_cm_pair_mark_price_is_message_type_matches_default():
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    is_match, payload = proc.is_message_type({
        'stream': 'btcusd@markPrice',
        'data': [CM_PAIR_MARK_PRICE_ITEM],
    })
    assert is_match
    assert payload == [CM_PAIR_MARK_PRICE_ITEM]


def test_cm_pair_mark_price_is_message_type_matches_1s():
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusd@markPrice@1s',
        'data': [CM_PAIR_MARK_PRICE_ITEM],
    })
    assert is_match


def test_cm_pair_mark_price_is_message_type_rejects_per_symbol():
    """Per-symbol `<symbol>@markPrice` delivers a dict, not an array.
    The pair processor MUST reject scalar-dict payloads so it does not
    hijack the per-symbol markPrice stream.
    """
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusd_perp@markPrice',
        'data': CM_PAIR_MARK_PRICE_ITEM,   # single dict, not list
    })
    assert not is_match


def test_cm_pair_mark_price_is_message_type_rejects_all_market():
    """`!markPrice@arr` is the all-markets stream — distinct from `<pair>@markPrice`."""
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': '!markPrice@arr',
        'data': [CM_PAIR_MARK_PRICE_ITEM],
    })
    assert not is_match


def test_cm_pair_mark_price_is_message_type_rejects_unrelated_stream():
    """Streams that do not end in `@markPrice[@1s]` must not match."""
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    is_match, _ = proc.is_message_type({
        'stream': 'btcusd@indexPrice',
        'data': [CM_PAIR_MARK_PRICE_ITEM],
    })
    assert not is_match
    # Missing stream key entirely.
    is_match, _ = proc.is_message_type({'data': [CM_PAIR_MARK_PRICE_ITEM]})
    assert not is_match


def test_cm_pair_mark_price_columns_map():
    from binance.futures.cm.streams import CM_PAIR_MARK_PRICE_COLUMNS_MAP
    # Mirrors the CM per-symbol markPrice fields (no `ap`).
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('e') == 'type'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('E') == 'event_time'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('s') == 'symbol'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('p') == 'mark_price'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('i') == 'index_price'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('P') == 'est_settle_price'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('r') == 'funding_rate'
    assert CM_PAIR_MARK_PRICE_COLUMNS_MAP.get('T') == 'next_funding_time'
    # CM has no `ap`.
    assert 'ap' not in CM_PAIR_MARK_PRICE_COLUMNS_MAP


@pytest.mark.asyncio
async def test_cm_pair_mark_price_handler_columns(cm_client):
    from binance import CMPairMarkPriceHandlerBase
    df = await run_handler(
        cm_client, CMPairMarkPriceHandlerBase,
        [CM_PAIR_MARK_PRICE_ITEM], 'btcusd@markPrice'
    )
    row = df.iloc[0]
    assert row['type'] == 'markPriceUpdate'
    assert row['symbol'] == 'BTCUSD_200925'
    assert row['mark_price'] == '11185.87786614'
    assert row['index_price'] == '11185.87786614'
    assert row['funding_rate'] == '0.00038167'


def test_cm_pair_mark_price_preserves_underscore():
    """CM pair stream names are lowercase but must still preserve pair
    underscores when the pair contains one (some indexes use `BTCUSD_NEXT`)."""
    from binance.futures.cm.streams import CMPairMarkPriceProcessor
    proc = CMPairMarkPriceProcessor(None)
    assert proc.subscribe_param(
        True, SubType.PAIR_MARK_PRICE, 'BTCUSD_PERP'
    ) == 'btcusd_perp@markPrice'


def test_cm_pair_mark_price_in_cm_processors():
    """The new processor MUST be wired into the CM PROCESSORS list."""
    from binance.futures.cm.streams import PROCESSORS, CMPairMarkPriceProcessor
    assert CMPairMarkPriceProcessor in PROCESSORS


# ===========================================================================
# Layering sanity
# ===========================================================================

def test_no_spot_import_in_futures():
    import binance.futures.streams as fs
    import binance.futures.um.streams as ums
    import binance.futures.cm.streams as cms
    import inspect

    for mod in (fs, ums, cms):
        src = inspect.getsource(mod)
        assert 'from binance.spot' not in src, f'{mod.__name__} imports from binance.spot'
        assert 'import binance.spot' not in src, f'{mod.__name__} imports from binance.spot'


def test_no_cm_import_in_um():
    import binance.futures.um.streams as ums
    import inspect
    src = inspect.getsource(ums)
    assert 'from binance.futures.cm' not in src
    assert 'import binance.futures.cm' not in src


def test_no_um_import_in_cm():
    import binance.futures.cm.streams as cms
    import inspect
    src = inspect.getsource(cms)
    assert 'from binance.futures.um' not in src
    assert 'import binance.futures.um' not in src


# ===========================================================================
# SubType enum values
# ===========================================================================

def test_subtype_values():
    assert str(SubType.AGG_TRADE) == 'aggTrade'
    assert str(SubType.KLINE) == 'kline'
    assert str(SubType.MINI_TICKER) == 'miniTicker'
    assert str(SubType.TICKER) == 'ticker'
    assert str(SubType.BOOK_TICKER) == 'bookTicker'
    assert str(SubType.PARTIAL_ORDER_BOOK) == 'partialDepth'
    assert str(SubType.ORDER_BOOK) == 'depth'
    assert str(SubType.CONTINUOUS_KLINE) == 'continuousKline'
    assert str(SubType.CONTRACT_INFO) == 'contractInfo'
    assert str(SubType.ALL_MARKET_MARK_PRICE) == 'allMarketMarkPrice'
    assert str(SubType.ALL_MARKET_LIQUIDATION) == 'allMarketLiquidation'
    assert str(SubType.ALL_MARKET_TICKERS) == 'allMarketTickers'
    assert str(SubType.ALL_MARKET_BOOK_TICKER) == 'allMarketBookTicker'
    assert str(SubType.COMPOSITE_INDEX) == 'compositeIndex'
    assert str(SubType.ASSET_INDEX) == 'assetIndex'
    assert str(SubType.TRADING_SESSION) == 'tradingSession'
    assert str(SubType.INDEX_PRICE) == 'indexPrice'
    assert str(SubType.INDEX_PRICE_KLINE) == 'indexPriceKline'
    assert str(SubType.MARK_PRICE_KLINE) == 'markPriceKline'
    assert str(SubType.RPI_DIFF_DEPTH) == 'rpiDiffDepth'
    assert str(SubType.PAIR_MARK_PRICE) == 'pairMarkPrice'


def test_subtype_all_market_book_ticker_docstring_is_futures_only():
    """Per developers.binance.com, ``!bookTicker`` (the all-market book
    ticker stream) is documented ONLY for USDⓈ-M and COIN-M; the Spot
    WebSocket Streams page does NOT document an all-market book ticker
    stream (``!bookTicker@arr`` was deprecated on Spot in 2021).

    The ``SubType.ALL_MARKET_BOOK_TICKER`` standalone string docstring
    (the literal that follows the enum assignment in source) MUST mark
    this as Futures-only (matching the ``ALL_MARKET_TICKERS`` member's
    wording) and MUST NOT advertise the deprecated Spot variant
    ``!bookTicker@arr`` as a positive Spot surface.

    The class-level docstring's enumeration MUST also be updated so the
    one-line summary near ``ALL_MARKET_BOOK_TICKER:`` no longer claims
    a Spot binding.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
    """
    import inspect
    import re
    from binance.core.common.constants import SubType
    src = inspect.getsource(SubType)

    # 1) The standalone literal docstring tied to the
    #    ``ALL_MARKET_BOOK_TICKER`` assignment in the enum body MUST be
    #    Futures-only.  We slice out the docstring that follows the
    #    assignment and pin its wording.
    m = re.search(
        r"ALL_MARKET_BOOK_TICKER\s*=\s*'allMarketBookTicker'\s*\n\s*\"\"\"(.*?)\"\"\"",
        src, re.DOTALL,
    )
    assert m, 'standalone member docstring for ALL_MARKET_BOOK_TICKER missing'
    standalone = m.group(1)
    assert 'Futures only' in standalone
    assert '!bookTicker' in standalone
    # Standalone MUST NOT advertise the deprecated Spot stream.
    assert '!bookTicker@arr' not in standalone

    # 2) The class-docstring enumeration line for ALL_MARKET_BOOK_TICKER
    #    MUST NOT claim a Spot binding either.
    m2 = re.search(
        r"ALL_MARKET_BOOK_TICKER:\s*([^\n]+(?:\n\s{8,}[^\n]+)*)",
        src,
    )
    assert m2, 'class-docstring enumeration line for ALL_MARKET_BOOK_TICKER missing'
    enum_line = m2.group(1)
    # Should NOT show the deprecated Spot stream as a positive variant.
    assert '!bookTicker@arr' not in enum_line
