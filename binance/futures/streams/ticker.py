"""Mini-ticker and ticker stream handlers and processors (per-symbol and all-market).

Hosts ``MiniTickerHandlerBase``/``MiniTickerProcessor``, ``TickerHandlerBase``/
``TickerProcessor``, ``AllMarketMiniTickersHandlerBase``/processor, and
``AllMarketTickersHandlerBase``/processor.  See
:mod:`binance.futures.streams._common` for the per-stream verified findings.
"""

from typing import ClassVar

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    STREAM_OHLC_MAP,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Futures MiniTicker
# Confirmed fields (UM + CM identical to Spot miniTicker, 2026-05-26):
#   e  '24hrMiniTicker'
#   E  event time
#   s  symbol
#   o, h, l, c  OHLC
#   v  volume
#   q  quote volume
# ---------------------------------------------------------------------------

FUTURES_MINI_TICKER_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    **STREAM_OHLC_MAP,
    'v': 'volume',
    'q': 'quote_volume',
}

FUTURES_MINI_TICKER_COLUMNS = FUTURES_MINI_TICKER_COLUMNS_MAP.keys()


class MiniTickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.MINI_TICKER`` (24hrMiniTicker) stream.

    Shared across USDⓈ-M and COIN-M markets.  The payload is structurally identical
    to the Spot miniTicker.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_MINI_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_MINI_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# Futures Ticker (24hrTicker)
# Confirmed fields (UM + CM identical to Spot ticker, 2026-05-26):
# Extends mini-ticker with: price change, percent, weighted avg price,
# last price (c), last quantity, best bid/ask, open/close time, trade ids.
# ---------------------------------------------------------------------------

FUTURES_TICKER_COLUMNS_MAP = {
    **FUTURES_MINI_TICKER_COLUMNS_MAP,
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
    'n': 'total_trades',
}

FUTURES_TICKER_COLUMNS = FUTURES_TICKER_COLUMNS_MAP.keys()


class TickerHandlerBase(Handler):
    """Base handler for the futures ``SubType.TICKER`` (24hrTicker) stream.

    Shared across USDⓈ-M and COIN-M markets.  The payload is structurally identical
    to the Spot ticker.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketMiniTickers
# Wire stream: !miniTicker@arr
# Each element is a 24hrMiniTicker dict.
# ---------------------------------------------------------------------------

class AllMarketMiniTickersHandlerBase(Handler):
    """Base handler for the futures ``SubType.ALL_MARKET_MINI_TICKERS`` stream (``!miniTicker@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of ``24hrMiniTicker``
    events for every actively traded futures symbol.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_MINI_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_MINI_TICKER_COLUMNS


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketTickers
# Wire stream: !ticker@arr
# Each element is a 24hrTicker dict.
# ---------------------------------------------------------------------------

class AllMarketTickersHandlerBase(Handler):
    """Base handler for the futures ``SubType.ALL_MARKET_TICKERS`` stream (``!ticker@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of full ``24hrTicker``
    events for every actively traded futures symbol.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_TICKER_COLUMNS_MAP
    COLUMNS = FUTURES_TICKER_COLUMNS


class MiniTickerProcessor(Processor):
    """Processor for the futures mini-ticker stream (``<symbol>@miniTicker``)."""

    HANDLER = MiniTickerHandlerBase
    SUB_TYPE = SubType.MINI_TICKER
    PAYLOAD_TYPE = '24hrMiniTicker'


class TickerProcessor(Processor):
    """Processor for the futures ticker stream (``<symbol>@ticker``)."""

    HANDLER = TickerHandlerBase
    SUB_TYPE = SubType.TICKER
    PAYLOAD_TYPE = '24hrTicker'


class AllMarketMiniTickersProcessor(Processor):
    """Processor for the futures all-market mini-ticker stream (``!miniTicker@arr``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER: ClassVar[type] = AllMarketMiniTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MINI_TICKERS
    STREAM_TYPE_PREFIX: ClassVar[str] = '!miniTicker@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_MINI_TICKERS` expects no parameters'
            )
        return self.STREAM_TYPE_PREFIX


class AllMarketTickersProcessor(Processor):
    """Processor for the futures all-market ticker stream (``!ticker@arr``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER: ClassVar[type] = AllMarketTickersHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_TICKERS
    STREAM_TYPE_PREFIX: ClassVar[str] = '!ticker@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_TICKERS` expects no parameters'
            )
        return self.STREAM_TYPE_PREFIX
