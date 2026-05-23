# coding=utf-8

import json
from typing import Any, Optional

from aiohttp import ClientResponse

from .utils import format_msg
from .constants import SubType


class UserStreamNotSubscribedException(Exception):
    def __str__(self) -> str:
        return format_msg('user stream is not subscribed')


class StreamDisconnectedException(Exception):
    def __init__(
        self,
        uri: str
    ) -> None:
        self.uri = uri

    def __str__(self) -> str:
        return format_msg(
            'stream "%s" is never connected or is abandoned after too many retries according to the `retry_policy`, run `stream.connect()`', self.uri)


class StreamSubscribeException(Exception):
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
    def __init__(
        self,
        url: str
    ) -> None:
        self.url = url

    def __str__(self) -> str:
        return format_msg(
            'api_key is required for requesting "%s"', self.url)


class APISecretNotDefinedException(Exception):
    def __init__(
        self,
        url: str
    ) -> None:
        self.url = url

    def __str__(self) -> str:
        return format_msg(
            'api_secret is required for requesting "%s"', self.url)


class StatusException(Exception):
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

    def __str__(self) -> str:  # pragma: no cover
        return format_msg(
            'response error for "%s", status %s, code %s: %s',
            self.response.url,
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
            self.response.url,
            self.retry_after
        )


class RateLimitReachedException(Exception):
    """Raised proactively (client-side, before sending) when a request would
    exceed a RAISE-mode rate-limit rule -- fail fast instead of firing a
    request that Binance will reject with 429.
    """

    def __init__(
        self,
        scope: str,
        limit_type: str,
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
            self.response.url,
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
    def __init__(
        self,
        message: str
    ) -> None:
        self.message = message

    def __str__(self) -> str:
        return format_msg('invalid subscribe params: %s', self.message)


class UnsupportedSubTypeException(Exception):
    def __init__(
        self,
        subtype: Any
    ) -> None:
        self.subtype = subtype

    def __str__(self) -> str:
        return format_msg('subtype "%s" is not supported', self.subtype)


class InvalidSubTypeParamException(Exception):
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
    def __init__(
        self,
        handler: Any
    ) -> None:
        self.handler = handler

    def __str__(self) -> str:
        return format_msg('invalid handler `%s`', self.handler)


class ReuseHandlerException(Exception):
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
