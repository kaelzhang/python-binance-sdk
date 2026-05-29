"""The USDⓈ-M Futures :class:`~binance.core.market.MarketSpec` instance.

Confirmed 2026-04-23 decommission of the legacy ``wss://fstream.binance.com/ws``
and ``wss://fstream.binance.com/stream`` URLs splits USDⓈ-M subscriptions into
three categories:

- ``wss://fstream.binance.com/public/stream`` — high-frequency: depth, RPI
  depth, bookTicker (per-symbol and all-market).
- ``wss://fstream.binance.com/market/stream`` — every other market-data stream:
  aggregate trade, mark price, klines, tickers, liquidations, composite index,
  contract info, asset index, trading session.
- ``wss://fstream.binance.com/private/ws/<listenKey>`` — dedicated user-data
  stream for the listenKey flow.

Docs:
- Important WebSocket Change Notice:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- 2026-04-02 derivatives changelog:
  https://developers.binance.com/docs/derivatives/change-log
"""

import re

from binance.core.market import MarketSpec

from binance.futures.orderbook import FuturesOrderBook
from binance.futures.um.constants import (
    UM_REST_HOST,
    UM_STREAM_HOST,
    UM_WS_API_HOST,
)
from binance.futures.um.endpoints import REST_ENDPOINTS, WS_API_ENDPOINTS
from binance.futures.um.rate_limit import DEFAULT_RULES, WS_MESSAGE_RULE
from binance.futures.um.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)
from binance.futures.um.streams import PROCESSORS


# Per the Important WebSocket Change Notice, /public/stream receives only the
# high-frequency streams: regular depth, RPI depth, per-symbol bookTicker, and
# the all-market !bookTicker.  Everything else routes to /market/stream.
#
# Suffixes (after the symbol-prefix or for the no-prefix variants):
#   @depth, @depth<N>, @depth[@<speed>ms] -> public
#   @rpiDepth, @rpiDepth@<speed>ms        -> public
#   @bookTicker, !bookTicker               -> public
# Everything else                         -> market
_PUBLIC_STREAM_PATTERN = re.compile(
    r'(?:'
    r'@depth(?:\d+)?(?:@\d+ms)?'        # @depth, @depth<N>, with/without speed
    r'|@rpiDepth(?:@\d+ms)?'            # @rpiDepth, @rpiDepth@500ms
    r'|@bookTicker'                     # per-symbol bookTicker
    r')$'
)

UM_PUBLIC_STREAM_PATH = '/public/stream'
UM_MARKET_STREAM_PATH = '/market/stream'


def um_data_stream_router(stream_name: str) -> str:
    """Map a UM stream name to its connection path per the 2026-04-23 docs.

    Args:
        stream_name: A wire stream name (e.g. ``'btcusdt@depth'``).

    Returns:
        ``'/public/stream'`` for depth, RPI depth, and bookTicker streams;
        ``'/market/stream'`` for everything else.
    """
    # The all-market book ticker is the only "public" stream without a leading
    # ``@`` — match it explicitly.
    if stream_name == '!bookTicker':
        return UM_PUBLIC_STREAM_PATH
    if _PUBLIC_STREAM_PATTERN.search(stream_name):
        return UM_PUBLIC_STREAM_PATH
    return UM_MARKET_STREAM_PATH


UM_MARKET = MarketSpec(
    rest_host=UM_REST_HOST,
    ws_api_host=UM_WS_API_HOST,
    stream_host=UM_STREAM_HOST,
    rules=tuple(DEFAULT_RULES),
    ws_message_rule=WS_MESSAGE_RULE,
    processors=PROCESSORS,
    exception_processor=ExceptionProcessor,
    stream_error_processor=StreamErrorProcessor,
    endpoints=WS_API_ENDPOINTS + REST_ENDPOINTS,
    orderbook_impl=FuturesOrderBook,
    data_stream_paths=(UM_PUBLIC_STREAM_PATH, UM_MARKET_STREAM_PATH),
    data_stream_router=um_data_stream_router,
    user_stream_path_template='/private/ws/{listen_key}',
)
