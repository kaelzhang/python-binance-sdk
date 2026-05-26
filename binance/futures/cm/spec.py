"""The COIN-M Futures :class:`~binance.core.market.MarketSpec` instance."""

from binance.core.market import MarketSpec

from binance.futures.orderbook import FuturesOrderBook
from binance.futures.cm.constants import (
    CM_REST_HOST,
    CM_STREAM_HOST,
    CM_WS_API_HOST,
)
from binance.futures.cm.endpoints import REST_ENDPOINTS, WS_API_ENDPOINTS
from binance.futures.cm.rate_limit import DEFAULT_RULES
from binance.futures.cm.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)
from binance.futures.cm.streams import PROCESSORS


CM_MARKET = MarketSpec(
    rest_host=CM_REST_HOST,
    ws_api_host=CM_WS_API_HOST,
    stream_host=CM_STREAM_HOST,
    rules=tuple(DEFAULT_RULES),
    processors=PROCESSORS,
    exception_processor=ExceptionProcessor,
    stream_error_processor=StreamErrorProcessor,
    endpoints=WS_API_ENDPOINTS + REST_ENDPOINTS,
    orderbook_impl=FuturesOrderBook,
)
