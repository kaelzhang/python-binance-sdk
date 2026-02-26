"""
Ref: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
"""

from stock_pandas import TimeFrame

from binance.handlers.handlers import (
    HandlerExceptionHandlerBase,
    KlineHandlerBase,
    TradeHandlerBase,
    AggTradeHandlerBase,
    BookTickerHandlerBase,
    PartialOrderBookHandlerBase,
    AvgPriceHandlerBase,
    WindowTickerHandlerBase,
    MiniTickerHandlerBase,
    TickerHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketWindowTickersHandlerBase
)

from binance.handlers.orderbook_handler import OrderBookHandlerBase

from binance.common.constants import (
    SubType,
    KLINE_TYPE_PREFIX,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
    KEY_PAYLOAD_TYPE
)
from binance.common.exceptions import InvalidSubTypeParamException
from binance.common.utils import normalize_symbol

from .base import Processor


WINDOW_TIME_FRAMES = (
    TimeFrame.H1,
    TimeFrame.H4,
    TimeFrame.D1
)
WINDOW_PAYLOAD_TYPES = tuple(
    f'{time_frame}Ticker' for time_frame in WINDOW_TIME_FRAMES
)
ORDER_BOOK_INTERVALS = (1000, 100)
PARTIAL_ORDER_BOOK_LEVELS = (5, 10, 20)


def _get_window(t, args, default=TimeFrame.H1):
    if len(args) == 0:
        return default

    window = args[0]

    if not isinstance(window, TimeFrame):
        raise InvalidSubTypeParamException(
            t,
            'window',
            '`TimeFrame` expected but got `%s`' % window
        )

    if window not in WINDOW_TIME_FRAMES:
        raise InvalidSubTypeParamException(
            t,
            'window',
            '`window` should be one of %s but got `%s`'
            % (tuple(map(str, WINDOW_TIME_FRAMES)), window)
        )

    return window


def _get_order_book_interval(t, args, default=1000):
    if len(args) == 0:
        return default

    interval = args[0]

    if type(interval) is not int:
        raise InvalidSubTypeParamException(
            t,
            'interval',
            '`int` expected but got `%s`' % interval
        )

    if interval not in ORDER_BOOK_INTERVALS:
        raise InvalidSubTypeParamException(
            t,
            'interval',
            '`interval` should be one of %s but got `%s`'
            % (ORDER_BOOK_INTERVALS, interval)
        )

    return interval


def _get_partial_depth_level(t, args, default=20):
    if len(args) == 0:
        return default

    level = args[0]

    if type(level) is not int:
        raise InvalidSubTypeParamException(
            t,
            'level',
            '`int` expected but got `%s`' % level
        )

    if level not in PARTIAL_ORDER_BOOK_LEVELS:
        raise InvalidSubTypeParamException(
            t,
            'level',
            '`level` should be one of %s but got `%s`'
            % (PARTIAL_ORDER_BOOK_LEVELS, level)
        )

    return level


class ExceptionProcessor(Processor):
    HANDLER = HandlerExceptionHandlerBase


class KlineProcessor(Processor):
    HANDLER = KlineHandlerBase
    SUB_TYPE = SubType.KLINE

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)

        length = len(args)

        if length == 2:
            interval = args[1]
        else:
            raise InvalidSubTypeParamException(
                t, 'interval', '`TimeFrame` expected but not specified')

        if not isinstance(interval, TimeFrame):
            raise InvalidSubTypeParamException(
                t, 'interval', '`TimeFrame` expected but got `%s`' % symbol)

        return f'{normalize_symbol(symbol)}@{KLINE_TYPE_PREFIX}{interval}'


class KlineUTC8Processor(KlineProcessor):
    SUB_TYPE = SubType.KLINE_UTC8

    def subscribe_param(self, _, t, *args) -> str:
        stream = super().subscribe_param(_, t, *args)
        return f'{stream}@+08:00'


class TradeProcessor(Processor):
    HANDLER = TradeHandlerBase
    SUB_TYPE = SubType.TRADE


class AggTradeProcessor(Processor):
    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE


class BookTickerProcessor(Processor):
    HANDLER = BookTickerHandlerBase
    SUB_TYPE = SubType.BOOK_TICKER

    STREAM_SUFFIX = f'@{SubType.BOOK_TICKER}'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is None
            or not stream_type.endswith(self.STREAM_SUFFIX)
        ):
            return False, None

        return True, msg.get(KEY_PAYLOAD)


class AvgPriceProcessor(Processor):
    HANDLER = AvgPriceHandlerBase
    SUB_TYPE = SubType.AVG_PRICE
    PAYLOAD_TYPE = 'avgPrice'


class OrderBookProcessor(Processor):
    HANDLER = OrderBookHandlerBase
    SUB_TYPE = SubType.ORDER_BOOK
    PAYLOAD_TYPE = 'depthUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        interval = _get_order_book_interval(t, args[1:])

        stream = f'{normalize_symbol(symbol)}@{t}'

        if interval == 100:
            return f'{stream}@100ms'

        return stream


class PartialOrderBookProcessor(Processor):
    HANDLER = PartialOrderBookHandlerBase
    SUB_TYPE = SubType.PARTIAL_ORDER_BOOK

    STREAM_PREFIX = '@depth'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type is None or self.STREAM_PREFIX not in stream_type:
            return False, None

        level = stream_type.split(self.STREAM_PREFIX, 1)[1]

        if level not in ('5', '10', '20'):
            return False, None

        return True, msg.get(KEY_PAYLOAD)

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        level = _get_partial_depth_level(t, args[1:])

        return f'{normalize_symbol(symbol)}@depth{level}'


class MiniTickerProcessor(Processor):
    HANDLER = MiniTickerHandlerBase
    SUB_TYPE = SubType.MINI_TICKER
    PAYLOAD_TYPE = '24hrMiniTicker'


class TickerProcessor(Processor):
    HANDLER = TickerHandlerBase
    SUB_TYPE = SubType.TICKER
    PAYLOAD_TYPE = '24hrTicker'


class WindowTickerProcessor(Processor):
    HANDLER = WindowTickerHandlerBase
    SUB_TYPE = SubType.WINDOW_TICKER
    PAYLOAD_TYPES = WINDOW_PAYLOAD_TYPES

    def is_message_type(self, msg):
        payload = msg.get(KEY_PAYLOAD)

        if (
            payload is not None
            and type(payload) is dict
            and payload.get(KEY_PAYLOAD_TYPE) in self.PAYLOAD_TYPES
        ):
            return True, payload

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        window = _get_window(t, args[1:])

        return f'{normalize_symbol(symbol)}@ticker_{window}'


class AllMarketMiniTickersProcessor(Processor):
    HANDLER = AllMarketMiniTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MINI_TICKERS
    STREAM_TYPE_PREFIX = '!miniTicker@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is None
            or not stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return False, None

        return True, msg.get(KEY_PAYLOAD)

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) == 0:
            interval = 1000
        else:
            interval = args[0]

        return f'{self.STREAM_TYPE_PREFIX}@{interval}ms'


class AllMarketWindowTickersProcessor(AllMarketMiniTickersProcessor):
    HANDLER = AllMarketWindowTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_WINDOW_TICKERS
    STREAM_TYPE_PREFIX = '!ticker_'

    def subscribe_param(self, _, t, *args) -> str:
        window = _get_window(t, args)
        return f'{self.STREAM_TYPE_PREFIX}{window}@arr'
