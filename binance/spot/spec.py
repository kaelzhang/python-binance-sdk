"""The Spot :class:`~binance.core.market.MarketSpec` instance."""

from binance.core.market import MarketSpec

from binance.spot.constants import (
    REST_API_HOST,
    STREAM_HOST,
    WS_API_HOST,
)
from binance.spot.endpoints import WS_APIS
from binance.spot.orderbook import SpotOrderBook
from binance.spot.rate_limit import DEFAULT_RULES
from binance.spot.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)
from binance.spot.streams import PROCESSORS


SPOT_MARKET = MarketSpec(
    rest_host=REST_API_HOST,
    ws_api_host=WS_API_HOST,
    stream_host=STREAM_HOST,
    rules=tuple(DEFAULT_RULES),
    processors=PROCESSORS,
    exception_processor=ExceptionProcessor,
    stream_error_processor=StreamErrorProcessor,
    endpoints=WS_APIS,
    orderbook_impl=SpotOrderBook,
)
