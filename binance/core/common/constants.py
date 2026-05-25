import random
from enum import Enum as _Enum

from aioretry import (
    RetryPolicyStrategy,
    RetryInfo
)

KLINE_TYPE_PREFIX = 'kline_'


class StringEnum(str, _Enum):
    """A ``str``-subclass enum whose ``str(member)`` is the raw wire value
    (e.g. ``str(OrderSide.BUY) == 'BUY'``), used for serialization.

    Members can be passed directly to string contexts (query parameters, JSON
    bodies, log messages) without explicit ``.value`` access.  Compare members
    to members (``side == OrderSide.BUY``), not to raw strings.
    """

    def __str__(self) -> str:
        return str(self.value)


class SubType(StringEnum):
    """WebSocket stream subscription types supported by the Binance SDK.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(SubType.TRADE) == 'trade'``.  Compare members to members
    (``subtype == SubType.TRADE``).  Members are passed as the first argument to
    `client.subscribe(subtype, ...)`.  Required and optional per-type
    parameters are documented in the README's SubType section and summarised
    below.

    Member families:

    Candlestick / kline streams (require `symbol` and `interval`):
        KLINE: Candlestick updates in UTC.
        KLINE_UTC8: Candlestick updates anchored to UTC+8 (Asia/Shanghai).

    Per-symbol streams (require `symbol`):
        TRADE: Individual trade events as they occur.
        AGG_TRADE: Aggregated trade events (multiple trades at the same price
            and direction collapsed into one event).
        BLOCK_TRADE: Block-trade events (large trades reported as a block).
        REFERENCE_PRICE: Reference-price events (1000ms; price may be null when there is no reference price).
        BOOK_TICKER: Best bid/ask price and quantity for a symbol.
        AVG_PRICE: Current average price over a rolling window.
        MINI_TICKER: Compact 24-hour rolling-window statistics.
        TICKER: Full 24-hour rolling-window statistics.
        ORDER_BOOK: Managed local order book depth stream (requires `symbol`;
            accepts an optional `updateInterval` of `100` or `1000` ms,
            defaulting to `1000`).
        PARTIAL_ORDER_BOOK: Partial depth snapshot stream (requires `symbol`
            and `level` in {5, 10, 20}; optional `updateInterval` of
            `100` or `1000` ms).

    Per-symbol window ticker (require `symbol` and optional `window`):
        WINDOW_TICKER: Rolling-window statistics for a configurable window
            (`TimeFrame.H1`, `TimeFrame.H4`, or `TimeFrame.D1`;
            default `TimeFrame.H1`).

    All-market streams (no `symbol` argument):
        ALL_MARKET_MINI_TICKERS: Mini-ticker events for every symbol.
        ALL_MARKET_WINDOW_TICKERS: Window-ticker events for every symbol
            (optional `window` argument as above).

    User data stream:
        USER: Account and order update events for the authenticated user.
            Requires a valid API key to have been configured on the client.
    """

    KLINE = 'kline'
    KLINE_UTC8 = 'klineUTC8'

    TRADE = 'trade'
    AGG_TRADE = 'aggTrade'
    BLOCK_TRADE = 'blockTrade'
    REFERENCE_PRICE = 'referencePrice'
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
    """The default `Client(stream_retry_policy=...)`: bounded full-jitter
    exponential backoff capped at 30s, never abandoning. Stays safely under
    Binance's 300-connections-per-5-min limit when paired with the connection
    limiter.
    """
    # Bounded exponential backoff with full jitter and a floor, never abandoning.
    # Combined with the per-IP connection limiter this cannot breach 300/5min.
    ceiling = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** min(info.fails - 1, 5)))
    delay = ceiling / 2 + random.uniform(0, ceiling / 2)
    return False, delay


def NO_RETRY_POLICY(_) -> RetryPolicyStrategy:
    """A `stream_retry_policy` that abandons immediately on the first failure
    (never reconnects) — use it to handle disconnections yourself."""
    return True, 0


# Streams
# ==================================================


# Seconds of stream silence before the SDK proactively pings to probe a
# possibly-dead connection. Kept above Binance's 20s server-ping cadence so it
# only fires as a dead-connection detector, not a redundant keepalive (the
# websockets library already auto-replies pong to server pings).
DEFAULT_STREAM_TIMEOUT = 30

# Close code used by binance.Stream
# https://tools.ietf.org/html/rfc6455#section-7.4.2
DEFAULT_STREAM_CLOSE_CODE = 4999

DEFAULT_DEPTH_LIMIT = 1000

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

ATOM: dict = {}

# APIs
# ==================================================


class SecurityType(_Enum):
    """REST endpoint authentication requirements.

    Each member is a `(need_api_key, need_signed)` tuple that the request
    builder inspects to decide which credentials to attach.  Because the values
    are tuples (not strings), this class uses the plain stdlib ``_Enum`` base
    rather than ``StringEnum``; the string representation is the tuple's repr,
    not typically used in wire messages directly.

    Members:
        NONE: No credentials required — public market data endpoints.
            Value: (False, False).
        TRADE: Requires an API key and an HMAC-SHA256 signature.  Used for
            order placement and management endpoints.
            Value: (True, True).
        USER_DATA: Requires an API key and a signature.  Used for account
            information and trade history endpoints.
            Value: (True, True).
        USER_STREAM: Requires only an API key (no signature).  Used for
            managing user-data stream listen keys.
            Value: (True, False).
        MARKET_DATA: Requires only an API key (no signature).  Used for
            some historical market-data endpoints.
            Value: (True, False).
    """

    # {TYPE} = (NEED_API_KEY, NEED_SIGNATURE)
    NONE = (False, False)
    TRADE = (True, True)
    USER_DATA = (True, True)
    USER_STREAM = (True, False)
    MARKET_DATA = (True, False)


class RequestMethod(StringEnum):
    """HTTP verbs used when defining REST API endpoints.

    Each member's wire value is the lowercase method name as expected by
    ``aiohttp.ClientSession``; ``str(RequestMethod.GET) == 'get'``.  Compare
    members to members (``method == RequestMethod.GET``).

    Members:
        GET: HTTP GET — used for read-only data retrieval.
        POST: HTTP POST — used for creating resources (e.g. placing orders).
        PUT: HTTP PUT — used for updating resources (e.g. renewing listen keys).
        DELETE: HTTP DELETE — used for removing resources (e.g. cancelling orders).
    """

    GET = 'get'
    POST = 'post'
    PUT = 'put'
    DELETE = 'delete'


class OrderSide(StringEnum):
    """Direction of a Binance order.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(OrderSide.BUY) == 'BUY'``.  Compare members to members
    (``side == OrderSide.BUY``).

    Members:
        BUY: Purchase the base asset (open a long position).
        SELL: Sell the base asset (close a long or open a short position).
    """

    BUY = 'BUY'
    SELL = 'SELL'


class OrderType(StringEnum):
    """Binance order execution type.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(OrderType.LIMIT) == 'LIMIT'``.  Compare members to members
    (``order_type == OrderType.LIMIT``).  Different order types require different
    combinations of parameters (price, quantity, stopPrice, etc.) as
    documented in the Binance REST API reference.

    Members:
        LIMIT: Limit order — executes at `price` or better; requires `price`
            and `timeInForce`.
        MARKET: Market order — executes immediately at the best available
            price; requires `quantity` or `quoteOrderQty`.
        STOP_LOSS: Stop-market order — becomes a market order once
            `stopPrice` is triggered.
        STOP_LOSS_LIMIT: Stop-limit order — becomes a limit order once
            `stopPrice` is triggered; requires `price` and `timeInForce`.
        TAKE_PROFIT: Take-profit market order — triggered by `stopPrice`.
        TAKE_PROFIT_LIMIT: Take-profit limit order — triggered by
            `stopPrice`; requires `price` and `timeInForce`.
        LIMIT_MAKER: Post-only limit order — rejected (not queued) if it
            would execute immediately as a taker; used for maker-only strategies.
    """

    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP_LOSS = 'STOP_LOSS'
    STOP_LOSS_LIMIT = 'STOP_LOSS_LIMIT'
    TAKE_PROFIT = 'TAKE_PROFIT'
    TAKE_PROFIT_LIMIT = 'TAKE_PROFIT_LIMIT'
    LIMIT_MAKER = 'LIMIT_MAKER'


class OrderRespType(StringEnum):
    """Controls how much detail the Binance REST API returns after placing an order.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(OrderRespType.ACK) == 'ACK'``.  Compare members to members
    (``resp_type == OrderRespType.ACK``).  Passed as the ``newOrderRespType`` parameter
    to order-creation endpoints.

    Members:
        ACK: Minimal response — returns only `orderId`, `clientOrderId`, and
            status confirmation.  Fastest acknowledgement with no fill details.
        RESULT: Returns order status and cumulative fill quantities, but not
            individual fill records.
        FULL: Returns the complete order response including all individual
            fill records (`fills` list).  Default for MARKET and LIMIT orders.
    """

    ACK = 'ACK'
    RESULT = 'RESULT'
    FULL = 'FULL'


class TimeInForce(StringEnum):
    """Specifies how long a Binance limit order remains active before it is cancelled.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(TimeInForce.GTC) == 'GTC'``.  Compare members to members
    (``tif == TimeInForce.GTC``).  Required for ``LIMIT``, ``STOP_LOSS_LIMIT``, and
    ``TAKE_PROFIT_LIMIT`` order types.

    Members:
        GTC: Good Till Cancelled — the order stays open until it is fully
            filled or explicitly cancelled by the user.
        IOC: Immediate Or Cancel — the order executes immediately for
            whatever quantity is available, and the remainder is cancelled.
        FOK: Fill Or Kill — the order must be filled in its entirety
            immediately or it is entirely cancelled (no partial fills).
    """

    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'


HEADER_API_KEY = 'X-MBX-APIKEY'

STREAM_KEY_ID = 'id'
STREAM_KEY_RESULT = 'result'
STREAM_KEY_ERROR = 'error'
STREAM_KEY_RATE_LIMITS = 'rateLimits'
ERROR_KEY_CODE = 'code'
ERROR_KEY_MESSAGE = 'msg'

# WS-API session.logon (web-socket-api.md "Authenticate after connection")
WS_API_METHOD_SESSION_LOGON = 'session.logon'
ERROR_CODE_UNAUTHORIZED = -2015   # API-key revoked / session invalid/expired

# WS-API timeUnit (F-13, web-socket-api.md "Timing security").
# Opt in to microsecond-precision timestamps for an entire WS-API connection by
# appending `?timeUnit=MICROSECOND` to the connection URL. Default (None /
# omitted) keeps Binance's millisecond default.
WS_API_TIME_UNIT_QUERY = 'timeUnit'
WS_API_TIME_UNIT_MICROSECOND = 'MICROSECOND'
WS_API_TIME_UNIT_MILLISECOND = 'MILLISECOND'

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
ERROR_CODE_INVALID_TIMESTAMP = -1021
EVENT_SERVER_SHUTDOWN = 'serverShutdown'
EVENT_STREAM_TERMINATED = 'eventStreamTerminated'
