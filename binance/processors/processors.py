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


class ExceptionProcessor(Processor):
    """Processor that routes exceptions thrown during message dispatch to registered exception handlers.

    Does not correspond to any ``SubType`` stream; it is held separately by
    ``HandlerContext`` and receives exceptions forwarded by ``receive()``.
    """
    HANDLER = HandlerExceptionHandlerBase


class KlineProcessor(Processor):
    """Processor for ``SubType.KLINE`` — individual kline/candlestick streams (UTC).

    Handles the ``<symbol>@kline_<interval>`` Binance WebSocket stream.
    """
    HANDLER = KlineHandlerBase
    SUB_TYPE = SubType.KLINE

    def subscribe_param(self, _, t, *args) -> str:
        """Build the kline stream name: ``<symbol>@kline_<interval>``.

        Specialised to require a ``TimeFrame`` interval as the second argument
        in addition to the symbol.

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.KLINE``.
            *args: Must contain ``(symbol: str, interval: TimeFrame)``.

        Returns:
            str: e.g. ``'btcusdt@kline_1d'``.

        Raises:
            InvalidSubTypeParamException: If symbol is missing, or interval
                is missing or is not a ``TimeFrame`` instance.
        """
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
    """Processor for ``SubType.KLINE_UTC8`` — kline streams anchored to UTC+8 daily boundaries.

    Extends ``KlineProcessor``; the stream name gains a ``@+08:00`` suffix
    (e.g. ``'btcusdt@kline_1d@+08:00'``).
    """
    SUB_TYPE = SubType.KLINE_UTC8

    def subscribe_param(self, _, t, *args) -> str:
        """Build the UTC+8 kline stream name by appending ``@+08:00`` to the base kline name.

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.KLINE_UTC8``.
            *args: Same as ``KlineProcessor.subscribe_param`` — ``(symbol, TimeFrame)``.

        Returns:
            str: e.g. ``'btcusdt@kline_1d@+08:00'``.
        """
        stream = super().subscribe_param(_, t, *args)
        return f'{stream}@+08:00'


class TradeProcessor(Processor):
    """Processor for ``SubType.TRADE`` — raw individual trade streams (``<symbol>@trade``)."""
    HANDLER = TradeHandlerBase
    SUB_TYPE = SubType.TRADE


class AggTradeProcessor(Processor):
    """Processor for ``SubType.AGG_TRADE`` — aggregate trade streams (``<symbol>@aggTrade``)."""
    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE


class BookTickerProcessor(Processor):
    """Processor for ``SubType.BOOK_TICKER`` — best bid/ask streams (``<symbol>@bookTicker``).

    Uses stream-name matching (``msg['stream']`` suffix) rather than the
    payload ``'e'`` field, because book-ticker messages do not carry an event
    type field.
    """
    HANDLER = BookTickerHandlerBase
    SUB_TYPE = SubType.BOOK_TICKER

    STREAM_SUFFIX = f'@{SubType.BOOK_TICKER}'

    def is_message_type(self, msg):
        """Match on ``msg['stream']`` ending with ``@bookTicker`` instead of payload ``'e'``.

        Book-ticker messages have no ``'e'`` event-type field; recognition is
        done via the stream name in the combined-stream envelope.

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, msg['data'])`` when matched,
            ``(False, None)`` otherwise.
        """
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is None
            or not stream_type.endswith(self.STREAM_SUFFIX)
        ):
            return False, None

        return True, msg.get(KEY_PAYLOAD)


class AvgPriceProcessor(Processor):
    """Processor for ``SubType.AVG_PRICE`` — average price streams (``<symbol>@avgPrice``).

    Matches on payload ``'e' == 'avgPrice'``.
    """
    HANDLER = AvgPriceHandlerBase
    SUB_TYPE = SubType.AVG_PRICE
    PAYLOAD_TYPE = 'avgPrice'


class OrderBookProcessor(Processor):
    """Processor for ``SubType.ORDER_BOOK`` — full-depth order-book diff streams (``<symbol>@depth``).

    Matches on payload ``'e' == 'depthUpdate'``. Supports an optional update
    interval (1000 ms default or 100 ms) appended as ``@100ms``.
    """
    HANDLER = OrderBookHandlerBase
    SUB_TYPE = SubType.ORDER_BOOK
    PAYLOAD_TYPE = 'depthUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Build the order-book diff stream name with an optional update-speed suffix.

        Produces ``<symbol>@depth`` or ``<symbol>@depth@100ms`` depending on
        the interval argument (1000 ms = no suffix; 100 ms = ``@100ms``).

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.ORDER_BOOK``.
            *args: ``(symbol: str[, interval: int])`` — interval defaults to
                1000 ms if omitted. Must be one of ``(1000, 100)``.

        Returns:
            str: e.g. ``'btcusdt@depth'`` or ``'btcusdt@depth@100ms'``.

        Raises:
            InvalidSubTypeParamException: If the interval is not one of the
                accepted values.
        """
        symbol = self._get_param_symbol(t, args)
        interval = _get_order_book_interval(t, args[1:])
        return (
            f'{normalize_symbol(symbol)}@{t}'
            f'{_order_book_interval_suffix(interval)}'
        )


class PartialOrderBookProcessor(Processor):
    """Processor for ``SubType.PARTIAL_ORDER_BOOK`` — partial depth snapshot streams.

    Handles ``<symbol>@depth<level>`` and ``<symbol>@depth<level>@100ms``
    streams. Uses stream-name prefix matching combined with payload shape
    inspection (requires ``'bids'`` and ``'asks'`` keys) rather than an
    ``'e'`` event-type field.
    """
    HANDLER = PartialOrderBookHandlerBase
    SUB_TYPE = SubType.PARTIAL_ORDER_BOOK

    STREAM_PREFIX = '@depth'

    def is_message_type(self, msg):
        """Match partial-depth messages by stream-name prefix and payload shape.

        Recognises messages whose ``msg['stream']`` contains ``'@depth'`` and
        whose ``msg['data']`` is a dict with both ``'bids'`` and ``'asks'``
        keys (distinguishing partial-depth snapshots from full-depth diffs).

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, payload)`` when matched,
            ``(False, None)`` otherwise.
        """
        stream_type = msg.get(KEY_STREAM_TYPE)
        payload = msg.get(KEY_PAYLOAD)

        if stream_type is None or self.STREAM_PREFIX not in stream_type:
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
        """Build the partial-depth stream name: ``<symbol>@depth<level>[@100ms]``.

        Accepts an optional depth level (5, 10, or 20; default 20) and an
        optional update interval (1000 or 100 ms; default 1000).

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.PARTIAL_ORDER_BOOK``.
            *args: ``(symbol: str[, level: int[, interval: int]])``.

        Returns:
            str: e.g. ``'btcusdt@depth20'`` or ``'btcusdt@depth5@100ms'``.

        Raises:
            InvalidSubTypeParamException: If symbol is missing, or level/
                interval are invalid.
        """
        symbol = self._get_param_symbol(t, args)
        level = _get_partial_depth_level(t, args[1:])
        interval = _get_order_book_interval(t, args[2:])

        return (
            f'{normalize_symbol(symbol)}@depth{level}'
            f'{_order_book_interval_suffix(interval)}'
        )


class MiniTickerProcessor(Processor):
    """Processor for ``SubType.MINI_TICKER`` — 24-hour rolling-window mini-ticker streams (``<symbol>@miniTicker``).

    Matches on payload ``'e' == '24hrMiniTicker'``.
    """
    HANDLER = MiniTickerHandlerBase
    SUB_TYPE = SubType.MINI_TICKER
    PAYLOAD_TYPE = '24hrMiniTicker'


class TickerProcessor(Processor):
    """Processor for ``SubType.TICKER`` — 24-hour rolling-window full ticker streams (``<symbol>@ticker``).

    Matches on payload ``'e' == '24hrTicker'``.
    """
    HANDLER = TickerHandlerBase
    SUB_TYPE = SubType.TICKER
    PAYLOAD_TYPE = '24hrTicker'


class WindowTickerProcessor(Processor):
    """Processor for ``SubType.WINDOW_TICKER`` — rolling-window ticker streams for 1h, 4h, or 1d windows.

    Stream names follow the pattern ``<symbol>@ticker_<window>`` where
    ``window`` is a ``stock_pandas.TimeFrame`` (H1, H4, D1). Matches on
    payload ``'e'`` being one of the ``WINDOW_PAYLOAD_TYPES`` strings
    (e.g. ``'1hTicker'``).
    """
    HANDLER = WindowTickerHandlerBase
    SUB_TYPE = SubType.WINDOW_TICKER
    PAYLOAD_TYPES = WINDOW_PAYLOAD_TYPES

    def is_message_type(self, msg):
        """Match window-ticker messages by checking payload ``'e'`` against all accepted window types.

        Unlike the base class, this checks membership in ``PAYLOAD_TYPES``
        (a tuple of strings) rather than equality with a single ``PAYLOAD_TYPE``.

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, payload)`` when matched,
            ``(False, None)`` otherwise.
        """
        payload = msg.get(KEY_PAYLOAD)

        if (
            payload is not None
            and type(payload) is dict
            and payload.get(KEY_PAYLOAD_TYPE) in self.PAYLOAD_TYPES
        ):
            return True, payload

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        """Build the window-ticker stream name: ``<symbol>@ticker_<window>``.

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.WINDOW_TICKER``.
            *args: ``(symbol: str[, window: TimeFrame])`` — window defaults to
                ``TimeFrame.H1`` if omitted. Must be one of H1, H4, D1.

        Returns:
            str: e.g. ``'btcusdt@ticker_1h'``.

        Raises:
            InvalidSubTypeParamException: If window is not a valid
                ``TimeFrame`` or is not in ``WINDOW_TIME_FRAMES``.
        """
        symbol = self._get_param_symbol(t, args)
        window = _get_window(t, args[1:])

        return f'{normalize_symbol(symbol)}@ticker_{window}'


class AllMarketMiniTickersProcessor(Processor):
    """Processor for ``SubType.ALL_MARKET_MINI_TICKERS`` — all-market 24h mini-ticker array stream.

    Handles the ``!miniTicker@arr`` stream which broadcasts mini-ticker
    snapshots for every trading pair as a JSON array. Recognised via
    ``msg['stream']`` starting with ``'!miniTicker@arr'``. Requires no
    symbol or other parameters.
    """
    HANDLER = AllMarketMiniTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MINI_TICKERS
    STREAM_TYPE_PREFIX = '!miniTicker@arr'

    def is_message_type(self, msg):
        """Match by ``msg['stream']`` starting with ``'!miniTicker@arr'``.

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, msg['data'])`` when matched,
            ``(False, None)`` otherwise.
        """
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is None
            or not stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return False, None

        return True, msg.get(KEY_PAYLOAD)

    def subscribe_param(self, _, t, *args) -> str:
        """Return the fixed stream name ``'!miniTicker@arr'``; rejects any extra arguments.

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.ALL_MARKET_MINI_TICKERS``.
            *args: Must be empty — this stream accepts no parameters.

        Returns:
            str: ``'!miniTicker@arr'``.

        Raises:
            InvalidSubTypeParamException: If any extra arguments are passed.
        """
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t,
                'interval',
                '`SubType.ALL_MARKET_MINI_TICKERS` expects no parameters'
            )

        return self.STREAM_TYPE_PREFIX


class AllMarketWindowTickersProcessor(AllMarketMiniTickersProcessor):
    """Processor for ``SubType.ALL_MARKET_WINDOW_TICKERS`` — all-market rolling-window ticker array streams.

    Handles ``!ticker_<window>@arr`` streams (e.g. ``!ticker_1h@arr``).
    Extends ``AllMarketMiniTickersProcessor``; only the stream prefix and the
    ``subscribe_param`` logic differ (a ``TimeFrame`` window is required).
    """
    HANDLER = AllMarketWindowTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_WINDOW_TICKERS
    STREAM_TYPE_PREFIX = '!ticker_'

    def subscribe_param(self, _, t, *args) -> str:
        """Build the all-market window-ticker stream name: ``!ticker_<window>@arr``.

        Args:
            _: Unused ``subscribe`` flag.
            t: ``SubType.ALL_MARKET_WINDOW_TICKERS``.
            *args: ``(window: TimeFrame,)`` — required; must be one of H1, H4, D1.

        Returns:
            str: e.g. ``'!ticker_1h@arr'``.

        Raises:
            InvalidSubTypeParamException: If window is missing, not a
                ``TimeFrame``, or not in ``WINDOW_TIME_FRAMES``.
        """
        window = _get_window(t, args)
        return f'{self.STREAM_TYPE_PREFIX}{window}@arr'
