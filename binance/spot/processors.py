"""
Ref: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
"""

import re
from typing import ClassVar
from stock_pandas import TimeFrame

from binance.spot.handlers import (
    KlineHandlerBase,
    TradeHandlerBase,
    AggTradeHandlerBase,
    BlockTradeHandlerBase,
    ReferencePriceHandlerBase,
    BookTickerHandlerBase,
    PartialOrderBookHandlerBase,
    AvgPriceHandlerBase,
    WindowTickerHandlerBase,
    MiniTickerHandlerBase,
    TickerHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketWindowTickersHandlerBase
)

from binance.core.processors.framework import (  # noqa: F401  re-exported for backward compatibility
    StreamErrorProcessor,
    ExceptionProcessor,
)

from binance.spot.orderbook_handler import OrderBookHandlerBase

from binance.core.common.constants import (
    SubType,
    KLINE_TYPE_PREFIX,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
    KEY_PAYLOAD_TYPE
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.utils import normalize_symbol

from binance.core.processors.base import Processor


VALID_KLINE_INTERVALS = frozenset((
    '1s', '1m', '3m', '5m', '15m', '30m',
    '1h', '2h', '4h', '6h', '8h', '12h',
    '1d', '3d', '1w', '1M'
))

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


def _get_order_book_interval(t, args, default=1000) -> int:
    if len(args) == 0:
        interval = default
    else:
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


def _order_book_interval_suffix(interval: int) -> str:
    if interval == 100:
        return '@100ms'

    return ''


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


class KlineProcessor(Processor):
    """Processor for the kline stream."""
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

        interval_str = str(interval)
        if interval_str not in VALID_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t,
                'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_KLINE_INTERVALS))
            )

        return f'{normalize_symbol(symbol)}@{KLINE_TYPE_PREFIX}{interval}'


class KlineUTC8Processor(KlineProcessor):
    """Processor for the UTC+8 kline stream (appends ``@+08:00`` suffix)."""
    SUB_TYPE = SubType.KLINE_UTC8

    def subscribe_param(self, _, t, *args) -> str:
        stream = super().subscribe_param(_, t, *args)
        return f'{stream}@+08:00'


class TradeProcessor(Processor):
    """Processor for the trade stream."""
    HANDLER = TradeHandlerBase
    SUB_TYPE = SubType.TRADE


class AggTradeProcessor(Processor):
    """Processor for the aggTrade stream."""
    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE


class BlockTradeProcessor(Processor):
    """Processor for the blockTrade stream."""
    HANDLER = BlockTradeHandlerBase
    SUB_TYPE = SubType.BLOCK_TRADE


class ReferencePriceProcessor(Processor):
    """Processor for the referencePrice stream."""
    HANDLER = ReferencePriceHandlerBase
    SUB_TYPE = SubType.REFERENCE_PRICE


class BookTickerProcessor(Processor):
    """Processor for the bookTicker stream (matched via stream name, not payload 'e')."""
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
    """Processor for the avgPrice stream."""
    HANDLER = AvgPriceHandlerBase
    SUB_TYPE = SubType.AVG_PRICE
    PAYLOAD_TYPE = 'avgPrice'


class OrderBookProcessor(Processor):
    """Processor for the full-depth order-book diff stream."""
    HANDLER = OrderBookHandlerBase
    SUB_TYPE = SubType.ORDER_BOOK
    PAYLOAD_TYPE = 'depthUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        symbol = self._get_param_symbol(t, args)
        interval = _get_order_book_interval(t, args[1:])
        return (
            f'{normalize_symbol(symbol)}@{t}'
            f'{_order_book_interval_suffix(interval)}'
        )


class PartialOrderBookProcessor(Processor):
    """Processor for the partial-depth order-book snapshot stream."""
    HANDLER = PartialOrderBookHandlerBase
    SUB_TYPE = SubType.PARTIAL_ORDER_BOOK

    STREAM_PATTERN = re.compile(r'@depth\d+')

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)
        payload = msg.get(KEY_PAYLOAD)

        if stream_type is None or not self.STREAM_PATTERN.search(stream_type):
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
        symbol = self._get_param_symbol(t, args)
        level = _get_partial_depth_level(t, args[1:])
        interval = _get_order_book_interval(t, args[2:])

        return (
            f'{normalize_symbol(symbol)}@depth{level}'
            f'{_order_book_interval_suffix(interval)}'
        )


class MiniTickerProcessor(Processor):
    """Processor for the mini-ticker stream."""
    HANDLER = MiniTickerHandlerBase
    SUB_TYPE = SubType.MINI_TICKER
    PAYLOAD_TYPE = '24hrMiniTicker'


class TickerProcessor(Processor):
    """Processor for the ticker stream."""
    HANDLER = TickerHandlerBase
    SUB_TYPE = SubType.TICKER
    PAYLOAD_TYPE = '24hrTicker'


class WindowTickerProcessor(Processor):
    """Processor for the rolling-window ticker stream (1h/4h/1d)."""
    HANDLER = WindowTickerHandlerBase
    SUB_TYPE = SubType.WINDOW_TICKER
    PAYLOAD_TYPES = WINDOW_PAYLOAD_TYPES

    def is_message_type(self, msg):
        """Match by ``'e'`` membership in ``PAYLOAD_TYPES`` (multiple window variants)."""
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
    """Processor for the all-market mini-ticker array stream (``!miniTicker@arr``)."""
    HANDLER: ClassVar[type] = AllMarketMiniTickersHandlerBase
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
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t,
                'interval',
                '`SubType.ALL_MARKET_MINI_TICKERS` expects no parameters'
            )

        return self.STREAM_TYPE_PREFIX


class AllMarketWindowTickersProcessor(AllMarketMiniTickersProcessor):
    """Processor for the all-market rolling-window ticker array stream (``!ticker_<window>@arr``)."""
    HANDLER = AllMarketWindowTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_WINDOW_TICKERS
    STREAM_TYPE_PREFIX = '!ticker_'

    def subscribe_param(self, _, t, *args) -> str:
        window = _get_window(t, args)
        return f'{self.STREAM_TYPE_PREFIX}{window}@arr'
