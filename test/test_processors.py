import pytest

from binance.processors import (
    KlineUTC8Processor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
    WindowTickerProcessor
)
from binance.common.constants import SubType
from binance.common.exceptions import InvalidSubTypeParamException
from stock_pandas import TimeFrame


def test_mini_ticker_processor():
    processor = AllMarketMiniTickersProcessor(None)

    assert processor.subscribe_param(None, None) == '!miniTicker@arr@1000ms'
    assert processor.subscribe_param(
        None, None, 2000
    ) == '!miniTicker@arr@2000ms'


def test_all_market_window_ticker_processor():
    processor = AllMarketWindowTickersProcessor(None)

    assert processor.subscribe_param(
        None, SubType.ALL_MARKET_WINDOW_TICKERS
    ) == '!ticker_1h@arr'

    assert processor.subscribe_param(
        None, SubType.ALL_MARKET_WINDOW_TICKERS, '4h'
    ) == '!ticker_4h@arr'


def test_window_ticker_processor():
    processor = WindowTickerProcessor(None)

    assert processor.subscribe_param(
        None, SubType.WINDOW_TICKER, 'BTCUSDT'
    ) == 'btcusdt@ticker_1h'

    assert processor.subscribe_param(
        None, SubType.WINDOW_TICKER, 'BTCUSDT', '1d'
    ) == 'btcusdt@ticker_1d'

    with pytest.raises(InvalidSubTypeParamException, match='window'):
        processor.subscribe_param(
            None, SubType.WINDOW_TICKER, 'BTCUSDT', '7d'
        )


def test_kline_utc8_processor():
    processor = KlineUTC8Processor(None)

    assert processor.subscribe_param(
        None, SubType.KLINE_UTC8, 'BTCUSDT', TimeFrame.D1
    ) == 'btcusdt@kline_1d@+08:00'


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

    with pytest.raises(InvalidSubTypeParamException, match='level'):
        processor.subscribe_param(
            None, SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 6
        )
