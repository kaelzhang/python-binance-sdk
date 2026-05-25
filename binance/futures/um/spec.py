"""The USDⓈ-M Futures :class:`~binance.core.market.MarketSpec` instance."""

from binance.core.market import MarketSpec

from binance.futures.um.constants import (
    UM_REST_HOST,
    UM_STREAM_HOST,
    UM_WS_API_HOST,
)
from binance.futures.um.endpoints import REST_ENDPOINTS, WS_API_ENDPOINTS
from binance.futures.um.rate_limit import DEFAULT_RULES
from binance.futures.um.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)
from binance.futures.um.streams import PROCESSORS


UM_MARKET = MarketSpec(
    rest_host=UM_REST_HOST,
    ws_api_host=UM_WS_API_HOST,
    stream_host=UM_STREAM_HOST,
    rules=tuple(DEFAULT_RULES),
    processors=PROCESSORS,
    exception_processor=ExceptionProcessor,
    stream_error_processor=StreamErrorProcessor,
    endpoints=WS_API_ENDPOINTS + REST_ENDPOINTS,
)
