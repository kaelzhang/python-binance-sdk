from __future__ import annotations

# coding=utf-8

import json
import re
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from binance.rate_limit.types import RateLimitScope, RateLimitType

from aiohttp import ClientResponse

from .utils import format_msg
from .constants import SubType

_SENSITIVE_PARAMS = re.compile(
    r'(?<=[?&])(?:signature|apiKey)=([^&]*)',
    re.IGNORECASE
)


def _redact_url(url) -> str:
    """Return `url` as a string with `signature` and `apiKey` query values replaced by ``***``.

    Handles both ``yarl.URL`` objects and plain strings.  When there is no
    query string the URL is returned unchanged.
    """
    url_str = str(url)
    return _SENSITIVE_PARAMS.sub(
        lambda m: m.group(0)[: m.start(1) - m.start()] + '***',
        url_str
    )


class UserStreamNotSubscribedException(Exception):
    """Raised when a user-stream operation is attempted before subscribing.

    Binance user-data streams require an active subscription established via
    `client.subscribe(SubType.USER, ...)` before any user-stream operations
    (such as listening to account or order updates) can succeed.  This
    exception is raised if such an operation is attempted without a prior
    successful subscription.
    """

    def __str__(self) -> str:
        return format_msg('user stream is not subscribed')


class StreamDisconnectedException(Exception):
    """Raised when a WebSocket stream is disconnected and cannot be used.

    Occurs if a stream was never successfully connected, or if the stream
    exhausted all reconnection attempts permitted by the configured
    `stream_retry_policy` and was consequently abandoned.  The caller should
    invoke `stream.connect()` to initiate (or re-initiate) the connection.

    Attributes:
        uri: The WebSocket URI of the disconnected stream.
    """

    def __init__(
        self,
        uri: str
    ) -> None:
        self.uri = uri

    def __str__(self) -> str:
        return format_msg(
            'stream "%s" is never connected or is abandoned after too many retries according to the `retry_policy`, run `stream.connect()`', self.uri)


class StreamSubscribeException(Exception):
    """Base class for errors returned by the Binance WebSocket API during subscription.

    Raised when the Binance server responds to a subscription request with an
    error frame containing a numeric error code and a human-readable message.
    `StreamRateLimitException` (e.g. Binance error code -1003) is a notable
    subclass that additionally carries a retry hint.

    Attributes:
        code: The Binance WebSocket error code (e.g. -1003 for rate-limit errors).
        message: The human-readable error description returned by the server.
    """

    def __init__(
        self,
        code: int,
        message: str
    ):
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return format_msg(
            'fails to subscribe, code: %s, reason: %s',
            self.code,
            self.message
        )


class APIKeyNotDefinedException(Exception):
    """Raised when an API key is required for an endpoint but has not been configured.

    Endpoints with `SecurityType.USER_STREAM`, `SecurityType.MARKET_DATA`,
    `SecurityType.USER_DATA`, or `SecurityType.TRADE` all require an API key.
    The client raises this exception before sending the request if no key has
    been provided during client construction.

    Attributes:
        url: The REST API URL that required the missing API key.
    """

    def __init__(
        self,
        url: str
    ) -> None:
        self.url = url

    def __str__(self) -> str:
        return format_msg(
            'api_key is required for requesting "%s"', self.url)


class APISecretNotDefinedException(Exception):
    """Raised when an API secret is required for a signed endpoint but has not been configured.

    Endpoints with `SecurityType.TRADE` or `SecurityType.USER_DATA` require
    both an API key and a signature generated from the API secret.  This
    exception is raised before sending the request if the secret is absent.

    Attributes:
        url: The REST API URL that required the missing API secret.
    """

    def __init__(
        self,
        url: str
    ) -> None:
        self.url = url

    def __str__(self) -> str:
        return format_msg(
            'api_secret is required for requesting "%s"', self.url)


class StatusException(Exception):
    """Base class for HTTP error responses from the Binance REST API.

    Raised when the server returns a non-2xx HTTP status.  The constructor
    parses the response body to extract the Binance application error code and
    message.  For 5xx server errors the body is not parsed and a generic
    message is used instead.  `RateLimitException` (HTTP 429) and
    `IPBannedException` (HTTP 418) are notable subclasses that additionally
    carry a `retry_after` value.

    Attributes:
        code: The Binance application error code extracted from the JSON body
            (e.g. -1121 for an invalid symbol).  Set to 0 when the response
            is a 5xx server error or when the body cannot be parsed as JSON.
        message: Human-readable error description from the JSON body, or a
            generic fallback string for 5xx responses and unparsable bodies.
        status: The HTTP status code (integer, e.g. 400, 429, 418).
        response: The raw `aiohttp.ClientResponse` object.
        request: The originating `aiohttp.ClientRequest`, or None if the
            response does not carry a back-reference to the request.
    """

    def __init__(
        self,
        response: ClientResponse,
        text: str
    ) -> None:
        self.code = 0
        status = response.status

        if not str(status).startswith('5'):
            try:
                json_res = json.loads(text)
            except ValueError:
                self.message = f'Invalid JSON error message from Binance: {text}'  # noqa:E501
            else:
                self.code = json_res.get('code', '<no-code>')
                self.message = json_res.get('msg', '<no-message>')
        else:
            self.message = 'Binance server error'

        self.status = status
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self) -> str:
        return format_msg(
            'response error for "%s", status %s, code %s: %s',
            _redact_url(self.response.url),
            self.status,
            self.code,
            self.message
        )


class RateLimitException(StatusException):
    """HTTP 429 - request weight or order-count rate limit exceeded.

    `retry_after` is the number of seconds the caller MUST wait before
    retrying, taken from the `Retry-After` response header.
    """

    def __init__(
        self,
        response: ClientResponse,
        text: str,
        retry_after: Optional[int] = None
    ) -> None:
        super().__init__(response, text)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'rate limit exceeded (HTTP 429) for "%s", retry after %s second(s)',
            _redact_url(self.response.url),
            self.retry_after
        )


class RateLimitReachedException(Exception):
    """Raised proactively (client-side, before sending) when a request would
    exceed a RAISE-mode rate-limit rule -- fail fast instead of firing a
    request that Binance will reject with 429.
    """

    def __init__(
        self,
        scope: RateLimitScope,
        limit_type: RateLimitType,
        interval: str,
        retry_after: int
    ) -> None:
        self.scope = scope
        self.limit_type = limit_type
        self.interval = interval
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'rate limit reached for %s %s (%s); retry after ~%s second(s)',
            self.scope,
            self.limit_type,
            self.interval,
            self.retry_after
        )


class IPBannedException(StatusException):
    """HTTP 418 - IP auto-banned for sending requests after a 429.

    `retry_after` is the number of seconds until the ban is lifted.
    """

    def __init__(
        self,
        response: ClientResponse,
        text: str,
        retry_after: Optional[int] = None
    ) -> None:
        super().__init__(response, text)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'IP banned (HTTP 418) for "%s", banned for %s more second(s)',
            _redact_url(self.response.url),
            self.retry_after
        )


class TooManyStreamsException(Exception):
    """Raised when a single connection would exceed Binance's 1024-stream limit."""

    def __init__(self, requested: int, limit: int) -> None:
        self.requested = requested
        self.limit = limit

    def __str__(self) -> str:
        return format_msg(
            'requested %s streams on one connection exceeds the Binance limit of %s',
            self.requested,
            self.limit
        )


class StreamRateLimitException(StreamSubscribeException):
    """WebSocket-API rate-limit error (e.g. code -1003) with a retry hint."""

    def __init__(
        self,
        code: int,
        message: str,
        retry_after: Optional[int] = None
    ) -> None:
        super().__init__(code, message)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'stream rate limit (code %s): %s, retry after %s second(s)',
            self.code,
            self.message,
            self.retry_after
        )


class InvalidResponseException(Exception):
    """Raised when a Binance REST API response body cannot be decoded as JSON.

    The client expects all successful responses to contain valid JSON.  This
    exception is raised when that assumption fails (e.g. the server returns
    plain text or an empty body for a 2xx response).

    Attributes:
        response: The raw `aiohttp.ClientResponse` object.
        response_text: The raw response body text that could not be parsed.
    """

    def __init__(
        self,
        response: ClientResponse,
        text: str
    ) -> None:
        self.response = response
        self.response_text = text

    def __str__(self) -> str:
        return format_msg(
            'invalid response for "%s": %s',
            self.response.url,
            self.response_text
        )


class InvalidSubParamsException(Exception):
    """Raised when the parameters supplied to a subscribe call are invalid.

    Used as a general container for subscription parameter validation errors
    that do not fall under a more specific exception type such as
    `InvalidSubTypeParamException`.

    Attributes:
        message: A human-readable description of what was wrong with the
            supplied parameters.
    """

    def __init__(
        self,
        message: str
    ) -> None:
        self.message = message

    def __str__(self) -> str:
        return format_msg('invalid subscribe params: %s', self.message)


class UnsupportedSubTypeException(Exception):
    """Raised when an unrecognised subscription type is passed to the client.

    The client validates the `subtype` argument against the known members of
    `SubType`.  If the value does not match any recognised type, this
    exception is raised before any network request is made.

    Attributes:
        subtype: The value that was not recognised as a valid `SubType`.
    """

    def __init__(
        self,
        subtype: Any
    ) -> None:
        self.subtype = subtype

    def __str__(self) -> str:
        return format_msg('subtype "%s" is not supported', self.subtype)


class InvalidSubTypeParamException(Exception):
    """Raised when a specific parameter for a recognised subscription type is invalid.

    More granular than `InvalidSubParamsException`: identifies exactly which
    `SubType` was being subscribed to, which parameter failed validation, and
    why.  For example, `PARTIAL_ORDER_BOOK` requires `level` to be one of
    5, 10, or 20; passing any other value raises this exception.

    Attributes:
        subtype: The `SubType` that was being subscribed to when the error
            was detected.
        param_name: The name of the parameter that failed validation.
        reason: A human-readable explanation of the validation failure.
    """

    def __init__(
        self,
        subtype: SubType,
        param_name: str,
        reason: str
    ) -> None:
        self.subtype = subtype
        self.param_name = param_name
        self.reason = reason

    def __str__(self) -> str:
        return format_msg(
            'invalid param `%s` for subtype "%s", %s',
            self.param_name,
            self.subtype,
            self.reason
        )


class InvalidHandlerException(Exception):
    """Raised when a handler passed to the client does not satisfy the required interface.

    Handlers must be instances of the appropriate `HandlerBase` subclass (e.g.
    `TickerHandlerBase`, `TradeHandlerBase`).  Passing an object that is not a
    recognised handler type (e.g. a plain function or an unrelated class
    instance) raises this exception.

    Attributes:
        handler: The object that was rejected as an invalid handler.
    """

    def __init__(
        self,
        handler: Any
    ) -> None:
        self.handler = handler

    def __str__(self) -> str:
        return format_msg('invalid handler `%s`', self.handler)


class ReuseHandlerException(Exception):
    """Raised when the same handler instance is registered with more than one client.

    Handler instances maintain internal state (e.g. an `OrderBook` per
    symbol) that is specific to the client they are attached to.  Sharing a
    single handler instance across multiple clients leads to data corruption,
    so the SDK detects this and raises this exception when a handler that is
    already owned by one client is passed to another.

    Attributes:
        handler: The handler instance that was found to already belong to
            another client.
    """

    def __init__(
        self,
        handler: Any
    ) -> None:
        self.handler = handler

    def __str__(self) -> str:
        return format_msg(
            'handler `%s` should not be used in more than one clients',
            self.handler
        )


class OrderBookFetchAbandonedException(Exception):
    """Raised when the managed order-book snapshot fetch is permanently abandoned.

    `OrderBookHandlerBase` bootstraps the local order book by fetching a REST
    snapshot before replaying buffered WebSocket depth updates.  If every
    snapshot attempt fails and the configured `stream_retry_policy` signals
    that retrying should be abandoned (by returning `abandon=True`), this
    exception is raised instead of making further attempts.

    Attributes:
        symbol: The trading pair symbol (e.g. 'BTCUSDT') whose order-book
            snapshot could not be fetched.
        exception: The last exception that caused a snapshot fetch to fail,
            providing the underlying error details.
    """

    def __init__(
        self,
        symbol: str,
        exception: Exception
    ) -> None:
        self.symbol = symbol
        self.exception = exception

    def __str__(self) -> str:
        return format_msg(
            'orderbook for `%s` failed to fetch snapshot and fetching is abandoned by retry policy, reason: %s',
            self.symbol,
            self.exception
        )
