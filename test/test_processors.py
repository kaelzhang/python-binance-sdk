import pytest

from binance.processors import (
    KlineUTC8Processor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
    WindowTickerProcessor,
    BlockTradeProcessor,
)
from binance.common.constants import SubType
from binance.common.exceptions import InvalidSubTypeParamException
from stock_pandas import TimeFrame


def test_block_trade_processor():
    processor = BlockTradeProcessor(None)
    assert processor.subscribe_param(
        None, SubType.BLOCK_TRADE, 'BTCUSDT'
    ) == 'btcusdt@blockTrade'


def test_mini_ticker_processor():
    processor = AllMarketMiniTickersProcessor(None)

    assert processor.subscribe_param(
        None, SubType.ALL_MARKET_MINI_TICKERS
    ) == '!miniTicker@arr'

    with pytest.raises(InvalidSubTypeParamException, match='expects no'):
        processor.subscribe_param(
            None, SubType.ALL_MARKET_MINI_TICKERS, 2000
        )


def test_all_market_window_ticker_processor():
    processor = AllMarketWindowTickersProcessor(None)

    assert processor.subscribe_param(
        None, SubType.ALL_MARKET_WINDOW_TICKERS
    ) == '!ticker_1h@arr'

    assert processor.subscribe_param(
        None, SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4
    ) == '!ticker_4h@arr'


def test_window_ticker_processor():
    processor = WindowTickerProcessor(None)

    assert processor.subscribe_param(
        None, SubType.WINDOW_TICKER, 'BTCUSDT'
    ) == 'btcusdt@ticker_1h'

    assert processor.subscribe_param(
        None, SubType.WINDOW_TICKER, 'BTCUSDT', TimeFrame.D1
    ) == 'btcusdt@ticker_1d'

    with pytest.raises(InvalidSubTypeParamException, match='window'):
        processor.subscribe_param(
            None, SubType.WINDOW_TICKER, 'BTCUSDT', TimeFrame.H2
        )


def test_kline_utc8_processor():
    processor = KlineUTC8Processor(None)

    assert processor.subscribe_param(
        None, SubType.KLINE_UTC8, 'BTCUSDT', TimeFrame.D1
    ) == 'btcusdt@kline_1d@+08:00'


def test_kline_invalid_interval():
    """An invalid kline interval (not in Binance allowlist) raises InvalidSubTypeParamException."""
    from binance.processors.processors import KlineProcessor
    processor = KlineProcessor(None)

    # TimeFrame.Y1 ('1y') is a valid TimeFrame but NOT a valid Binance kline interval.
    with pytest.raises(InvalidSubTypeParamException, match='invalid kline interval'):
        processor.subscribe_param(None, SubType.KLINE, 'BTCUSDT', TimeFrame.Y1)


def test_order_book_processor():
    processor = OrderBookProcessor(None)

    assert processor.subscribe_param(
        None, SubType.ORDER_BOOK, 'BTCUSDT'
    ) == 'btcusdt@depth'

    assert processor.subscribe_param(
        None, SubType.ORDER_BOOK, 'BTCUSDT', 100
    ) == 'btcusdt@depth@100ms'

    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        processor.subscribe_param(
            None, SubType.ORDER_BOOK, 'BTCUSDT', 50
        )


def test_partial_order_book_processor():
    processor = PartialOrderBookProcessor(None)

    assert processor.subscribe_param(
        None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT'
    ) == 'btcusdt@depth20'

    assert processor.subscribe_param(
        None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5
    ) == 'btcusdt@depth5'

    assert processor.subscribe_param(
        None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5, 100
    ) == 'btcusdt@depth5@100ms'

    with pytest.raises(InvalidSubTypeParamException, match='level'):
        processor.subscribe_param(
            None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 6
        )

    with pytest.raises(InvalidSubTypeParamException, match='interval'):
        processor.subscribe_param(
            None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 5, 50
        )


def test_partial_order_book_processor_message_routing():
    processor = PartialOrderBookProcessor(None)

    is_partial, payload = processor.is_message_type({
        'stream': 'btcusdt@depth5',
        'data': {
            'bids': [],
            'asks': []
        }
    })
    assert is_partial
    assert payload == {'bids': [], 'asks': []}

    is_partial, _ = processor.is_message_type({
        'stream': 'btcusdt@depth20@100ms',
        'data': {
            'bids': [],
            'asks': []
        }
    })
    assert is_partial

    is_partial, _ = processor.is_message_type({
        'stream': 'btcusdt@depth',
        'data': {
            'e': 'depthUpdate',
            'b': [],
            'a': []
        }
    })
    assert not is_partial


def test_partial_order_book_rejects_diff_depth_stream():
    """F-10: a diff-depth `@depth` stream must NOT route to the partial
    processor even if the payload happens to carry bids/asks keys; only
    `@depth<level>` streams are partial."""
    processor = PartialOrderBookProcessor(None)

    is_partial, _ = processor.is_message_type({
        'stream': 'btcusdt@depth',
        'data': {'bids': [], 'asks': []}
    })
    assert not is_partial

    is_partial, _ = processor.is_message_type({
        'stream': 'btcusdt@depth@100ms',
        'data': {'bids': [], 'asks': []}
    })
    assert not is_partial

    # A @depth<n> stream that passes the regex but lacks a valid payload
    # (exercises the second gate: bids/asks check returns False, None).
    is_partial, _ = processor.is_message_type({
        'stream': 'btcusdt@depth5',
        'data': {'e': 'snapshot'}
    })
    assert not is_partial
