import random
from enum import Enum as _Enum

from aioretry import (
    RetryPolicyStrategy,
    RetryInfo
)

KLINE_TYPE_PREFIX = 'kline_'


class Enum(_Enum):
    def __str__(self) -> str:
        return str(self.value)


class SubType(Enum):
    KLINE = 'kline'
    KLINE_UTC8 = 'klineUTC8'

    TRADE = 'trade'
    AGG_TRADE = 'aggTrade'
    BOOK_TICKER = 'bookTicker'
    AVG_PRICE = 'avgPrice'
    WINDOW_TICKER = 'windowTicker'
    MINI_TICKER = 'miniTicker'
    TICKER = 'ticker'
    ORDER_BOOK = 'depth'
    PARTIAL_ORDER_BOOK = 'partialDepth'

    ALL_MARKET_MINI_TICKERS = 'allMarketMiniTickers'
    ALL_MARKET_WINDOW_TICKERS = 'allMarketWindowTickers'

    USER = 'user'


MSG_PREFIX = '[BinanceSDK] '

# RetryPolicy
# ==================================================

RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0


def DEFAULT_RETRY_POLICY(info: RetryInfo) -> RetryPolicyStrategy:
    # Bounded exponential backoff with full jitter and a floor, never abandoning.
    # Combined with the per-IP connection limiter this cannot breach 300/5min.
    ceiling = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** min(info.fails - 1, 5)))
    delay = ceiling / 2 + random.uniform(0, ceiling / 2)
    return False, delay


def NO_RETRY_POLICY(_) -> RetryPolicyStrategy:
    return True, 0


# Streams
# ==================================================


STREAM_HOST = 'wss://stream.binance.com'
WS_API_HOST = 'wss://ws-api.binance.com/ws-api/v3'

DEFAULT_STREAM_TIMEOUT = 5

# Close code used by binance.Stream
# https://tools.ietf.org/html/rfc6455#section-7.4.2
DEFAULT_STREAM_CLOSE_CODE = 4999

DEFAULT_DEPTH_LIMIT = 100

STREAM_TYPE_MAP = {
    'e': 'type'
}

STREAM_OHLC_MAP = {
    'o': 'open',
    'h': 'high',
    'l': 'low',
    'c': 'close'
}

KEY_PAYLOAD = 'data'
KEY_PAYLOAD_TYPE = 'e'
KEY_STREAM_TYPE = 'stream'

ATOM = {}

# APIs
# ==================================================


class SecurityType(Enum):
    # {TYPE} = (NEED_API_KEY, NEED_SIGNATURE)
    NONE = (False, False)
    TRADE = (True, True)
    USER_DATA = (True, True)
    USER_STREAM = (True, False)
    MARKET_DATA = (True, False)


class RequestMethod(Enum):
    GET = 'get'
    POST = 'post'
    PUT = 'put'
    DELETE = 'delete'


class OrderSide(Enum):
    BUY = 'BUY'
    SELL = 'SELL'


class OrderType(Enum):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP_LOSS = 'STOP_LOSS'
    STOP_LOSS_LIMIT = 'STOP_LOSS_LIMIT'
    TAKE_PROFIT = 'TAKE_PROFIT'
    TAKE_PROFIT_LIMIT = 'TAKE_PROFIT_LIMIT'
    LIMIT_MAKER = 'LIMIT_MAKER'


class OrderRespType(Enum):
    ACK = 'ACK'
    RESULT = 'RESULT'
    FULL = 'FULL'


class TimeInForce(Enum):
    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'


HEADER_API_KEY = 'X-MBX-APIKEY'

REST_API_VERSION = 'v3'
REST_API_HOST = 'https://api.binance.com'

STREAM_KEY_ID = 'id'
STREAM_KEY_RESULT = 'result'
STREAM_KEY_ERROR = 'error'
ERROR_KEY_CODE = 'code'
ERROR_KEY_MESSAGE = 'msg'

# Rate limits — verified 2026-05-23 against Binance Spot API docs
# ==================================================

# REST (rest-api.md LIMITS, faqs/rate_limits.md)
HEADER_USED_WEIGHT_PREFIX = 'x-mbx-used-weight-'   # e.g. x-mbx-used-weight-1m
HEADER_ORDER_COUNT_PREFIX = 'x-mbx-order-count-'   # e.g. x-mbx-order-count-1m
HEADER_RETRY_AFTER = 'Retry-After'

HTTP_TOO_MANY_REQUESTS = 429
HTTP_IP_BANNED = 418

DEFAULT_REQUEST_WEIGHT_LIMIT = 6000      # weight / interval / IP (since 2023-08-25)
DEFAULT_REQUEST_WEIGHT_INTERVAL = 60.0   # seconds
DEFAULT_WEIGHT_SAFETY_RATIO = 0.9        # only use 90% of the budget client-side

DEFAULT_RAW_REQUESTS_LIMIT = 300000
DEFAULT_RAW_REQUESTS_INTERVAL = 300.0    # 5 minutes
DEFAULT_ORDERS_10S_LIMIT = 100
DEFAULT_ORDERS_10S_INTERVAL = 10.0
DEFAULT_ORDERS_1D_LIMIT = 200000
DEFAULT_ORDERS_1D_INTERVAL = 86400.0

# WebSocket streams (web-socket-streams.md)
WS_MAX_CONNECTIONS = 300
WS_CONNECTION_WINDOW = 300.0             # seconds (5 minutes)
WS_CONNECTION_SAFETY = 290               # stay below the 300 hard cap
WS_MAX_MESSAGES_PER_SEC = 5
WS_MESSAGE_WINDOW = 1.0
WS_MAX_STREAMS_PER_CONNECTION = 1024

# WS-API / stream rate-limit signalling (web-socket-api.md)
ERROR_CODE_TOO_MANY_REQUESTS = -1003
EVENT_SERVER_SHUTDOWN = 'serverShutdown'
