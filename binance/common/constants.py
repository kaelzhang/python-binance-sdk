from enum import Enum as _Enum

from aioretry import (
    RetryPolicyStrategy,
    RetryInfo
)

from stock_pandas import TimeFrame


KLINE_TYPE_PREFIX = 'kline_'


class Enum(_Enum):
    def __str__(self) -> str:
        return str(self.value)


class SubType(Enum):
    KLINE = 'kline'

    TRADE = 'trade'
    AGG_TRADE = 'aggTrade'
    MINI_TICKER = 'miniTicker'
    TICKER = 'ticker'
    ORDER_BOOK = 'depth'

    ALL_MARKET_MINI_TICKERS = 'allMarketMiniTickers'
    ALL_MARKET_TICKERS = 'allMarketTickers'

    USER = 'user'


KlineInterval = TimeFrame


MSG_PREFIX = '[BinanceSDK] '

# RetryPolicy
# ==================================================

ATOM_RETRY_DELAY = 0.1
MAX_RETRIES_BEFORE_RESET = 10

# If the network connection fails,
#   we increase the delay by 100ms per failure
#   and reset the retry counter after 10 failures


def DEFAULT_RETRY_POLICY(info: RetryInfo) -> RetryPolicyStrategy:
    return (
        False,
        (info.fails - 1) % MAX_RETRIES_BEFORE_RESET * ATOM_RETRY_DELAY
    )


def NO_RETRY_POLICY(_) -> RetryPolicyStrategy:
    return True, 0


# Streams
# ==================================================


STREAM_HOST = 'wss://stream.binance.com'

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
