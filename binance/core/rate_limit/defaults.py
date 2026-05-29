"""Market-agnostic rate-limit helpers and per-connection rules.

Holds the response-header helper (:func:`parse_retry_after`), the per-connection
streams :class:`~binance.core.rate_limit.types.RateLimitRule` (which is identical
across markets), and a :func:`build_ws_message_rule` factory used by each
market's ``rate_limit.py`` to produce a per-market ws-messages rule whose
incoming-message cap matches that market's documented limit (Spot 5/s vs
futures 10/s per ``developers.binance.com``).

Market-specific default rule sets (e.g. Spot's :data:`DEFAULT_RULES`) live in
their respective market packages (e.g. :mod:`binance.spot.rate_limit`).
"""

from typing import Optional

from binance.core.common.constants import (
    HEADER_RETRY_AFTER,
    WS_MESSAGE_WINDOW,
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


def build_ws_message_rule(max_messages_per_sec: int) -> RateLimitRule:
    """Build the per-connection ws-messages rule for one market.

    The Spot WS-streams docs cap incoming messages at 5/s; the USDⓈ-M and
    COIN-M futures WS-streams docs cap at 10/s. The cap is the only thing that
    varies per market, so each market's ``rate_limit.py`` calls this factory
    with the value pulled from its own constants module.
    """
    return RateLimitRule(
        RateLimitScope.CONNECTION, RateLimitType.WS_MESSAGES,
        WS_MESSAGE_WINDOW, int(max_messages_per_sec),
        RateLimitKind.COUNT, EnforceMode.SLEEP)


WS_STREAMS_RULE = RateLimitRule(
    RateLimitScope.CONNECTION, RateLimitType.WS_STREAMS,
    0.0, WS_MAX_STREAMS_PER_CONNECTION, RateLimitKind.CAP, EnforceMode.RAISE)
