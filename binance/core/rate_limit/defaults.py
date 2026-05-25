"""Market-agnostic rate-limit helpers and per-connection rules.

Holds the response-header helper (:func:`parse_retry_after`) and the two
per-connection :class:`~binance.core.rate_limit.types.RateLimitRule` instances
(:data:`WS_MESSAGE_RULE` and :data:`WS_STREAMS_RULE`) that are instantiated
per WebSocket connection and are identical across all markets.

Market-specific default rule sets (e.g. Spot's :data:`DEFAULT_RULES`) live in
their respective market packages (e.g. :mod:`binance.spot.rate_limit`).
"""

from typing import Optional

from binance.core.common.constants import (
    HEADER_RETRY_AFTER,
    WS_MAX_MESSAGES_PER_SEC, WS_MESSAGE_WINDOW,
    WS_MAX_STREAMS_PER_CONNECTION,
)
from binance.core.rate_limit.types import (
    RateLimitRule, RateLimitScope, RateLimitType, RateLimitKind, EnforceMode
)


def parse_retry_after(response) -> Optional[int]:
    """Read the integer `Retry-After` (seconds) from a response, or None.

    Only the integer-seconds form is parsed; Binance does not send the
    RFC 7231 HTTP-date form of `Retry-After`.
    """
    value = response.headers.get(HEADER_RETRY_AFTER)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


WS_MESSAGE_RULE = RateLimitRule(
    RateLimitScope.CONNECTION, RateLimitType.WS_MESSAGES,
    WS_MESSAGE_WINDOW, WS_MAX_MESSAGES_PER_SEC,
    RateLimitKind.COUNT, EnforceMode.SLEEP)

WS_STREAMS_RULE = RateLimitRule(
    RateLimitScope.CONNECTION, RateLimitType.WS_STREAMS,
    0.0, WS_MAX_STREAMS_PER_CONNECTION, RateLimitKind.CAP, EnforceMode.RAISE)
