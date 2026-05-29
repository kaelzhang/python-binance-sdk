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
    df = await run_handler(
        um_client, FuturesAggTradeHandlerBase, UM_AGG_TRADE_PAYLOAD, 'btcusdt@aggTrade'
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

PARTIAL_DEPTH_PAYLOAD = {
    'lastUpdateId': 1234,
    'bids': [['50000.0', '1.0'], ['49999.0', '2.0']],
    'asks': [['50001.0', '0.5'], ['50002.0', '1.5']],
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
        proc.subscribe_param(True, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5, 250)


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
    bids, asks = df
    assert bids.iloc[0]['price'] == '50000.0'
    assert asks.iloc[0]['price'] == '50001.0'


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
        proc.subscribe_param(True, SubType.ORDER_BOOK, 'BTCUSDT', 250)


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

EQUITY_UPDATE_PAYLOAD = {
    'e': 'EquityUpdate',
    'E': 1638747660000,
    'T': 'OPEN',
}

COMMODITY_UPDATE_PAYLOAD = {
    'e': 'CommodityUpdate',
    'E': 1638747661000,
    'T': 'CLOSE',
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
    assert row['session_state'] == 'OPEN'


@pytest.mark.asyncio
async def test_um_trading_session_handler_commodity(um_client):
    df = await run_handler(
        um_client, TradingSessionHandlerBase, COMMODITY_UPDATE_PAYLOAD, 'tradingSession'
    )
    row = df.iloc[0]
    assert row['type'] == 'CommodityUpdate'
    assert row['session_state'] == 'CLOSE'


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
