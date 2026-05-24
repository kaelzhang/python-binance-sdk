"""Type vocabulary for the rate-limit core.

These enums and the :class:`RateLimitRule` dataclass describe *what* each
Binance rate-limit pool is and *how* the core should treat it. They are pure
data; the live counters and runtime behaviour live in
:mod:`binance.rate_limit.bucket` and :mod:`binance.rate_limit.core`.
"""

from dataclasses import dataclass
from enum import Enum


class RateLimitScope(str, Enum):
    """Who a limit's budget is shared across.

    The scope decides which requests compete for the same allowance:

    - ``IP`` -- shared by every request from your IP address (REQUEST_WEIGHT,
      RAW_REQUESTS, and WebSocket connection attempts). Tripping it affects
      every API key used from that host.
    - ``ACCOUNT`` -- shared by every request signed with the same API account
      (the ORDERS pools). Independent of IP.
    - ``CONNECTION`` -- local to a single WebSocket connection (its outgoing
      message rate and its concurrent stream count).

    This is a ``str`` enum, so a member compares equal to its value
    (``RateLimitScope.IP == 'ip'`` is ``True``); the same string appears as
    :attr:`RateLimitWindow.scope` in a snapshot.
    """

    IP = 'ip'
    ACCOUNT = 'account'
    CONNECTION = 'connection'


class RateLimitType(str, Enum):
    """The specific Binance limit pool a rule or bucket represents.

    - ``REQUEST_WEIGHT`` -- the weighted REST budget (per IP, e.g. 6000/min);
      each endpoint costs a documented weight.
    - ``RAW_REQUESTS`` -- the raw REST request *count* (per IP, e.g.
      300000/5min): one unit per call regardless of its weight.
    - ``ORDERS`` -- the account order-placement count (e.g. 100/10s and
      200000/day); two independent intervals exist for this type.
    - ``WS_CONNECTIONS`` -- WebSocket connection *attempts* (per IP, 300/5min).
    - ``WS_MESSAGES`` -- outgoing WebSocket messages on one connection (5/s),
      counting subscribe/unsubscribe and ping/pong.
    - ``WS_STREAMS`` -- concurrent streams on one connection (a cap of 1024).

    These string values appear verbatim as :attr:`RateLimitWindow.type` in a
    snapshot, so windows can be filtered with either a literal
    (``w.type == 'request_weight'``) or the enum
    (``w.type == RateLimitType.REQUEST_WEIGHT``).
    """

    REQUEST_WEIGHT = 'request_weight'
    RAW_REQUESTS = 'raw_requests'
    ORDERS = 'orders'
    WS_CONNECTIONS = 'ws_connections'
    WS_MESSAGES = 'ws_messages'
    WS_STREAMS = 'ws_streams'


class RateLimitKind(str, Enum):
    """How a bucket accounts usage over time.

    - ``WEIGHT`` -- a monotonic sliding window summing per-event *cost*
      (REQUEST_WEIGHT).
    - ``COUNT`` -- a monotonic sliding window counting events, one unit each
      (RAW_REQUESTS, ORDERS, WS_MESSAGES, WS_CONNECTIONS).
    - ``CAP`` -- an instantaneous current-count ceiling, not a time window; it
      rises on ``reserve`` and falls on ``release`` (WS_STREAMS).
    """

    WEIGHT = 'weight'   # cost-weighted sliding window
    COUNT = 'count'     # 1-per-event sliding window
    CAP = 'cap'         # instantaneous current-count ceiling


class EnforceMode(str, Enum):
    """What a bucket does when a request would exceed the effective limit.

    Usage is *always* accounted regardless of mode; the mode only decides the
    action taken on overflow:

    - ``SLEEP`` -- block (``await``) until the window frees enough headroom.
    - ``RAISE`` -- fail fast, raising
      :class:`~binance.common.exceptions.RateLimitReachedException` immediately
      instead of waiting (used for ORDERS, where a delayed order can be worse
      than a rejected one).
    - ``TRACK`` -- never block or raise; only record usage. This is what every
      pool effectively does when the client is constructed with the rate-limit
      guard disabled, so monitoring still works.
    """

    SLEEP = 'sleep'     # block until headroom
    RAISE = 'raise'     # raise immediately when a request would exceed
    TRACK = 'track'     # never block/raise; only account


def interval_label(seconds: float) -> str:
    """Internal: render a window length in seconds as a compact label
    (``60 -> '1m'``, ``86400 -> '1d'``); ``''`` when ``seconds <= 0``."""
    if seconds <= 0:
        return ''
    if seconds % 86400 == 0:
        return f'{int(seconds // 86400)}d'
    if seconds % 3600 == 0:
        return f'{int(seconds // 3600)}h'
    if seconds % 60 == 0:
        return f'{int(seconds // 60)}m'
    return f'{int(seconds)}s'


@dataclass(frozen=True)
class RateLimitRule:
    """Immutable description of one rate-limit pool.

    A rule is the static *configuration* of a pool; the live counters live in a
    :class:`~binance.rate_limit.bucket.RateLimitBucket` built from it. The
    default rules for every Binance pool are in
    :mod:`binance.rate_limit.defaults`.

    Attributes:
        scope: Who shares this budget (see :class:`RateLimitScope`).
        type: Which Binance pool this is (see :class:`RateLimitType`).
        interval_seconds: Length of the sliding window in seconds. For ``CAP``
            kinds, which have no window, this is ``0.0``.
        limit: The documented maximum for the window, *before* ``safety_ratio``
            is applied.
        kind: How usage is accounted (see :class:`RateLimitKind`).
        enforce: The action taken on overflow (see :class:`EnforceMode`).
        safety_ratio: Fraction of ``limit`` actually used client-side, in the
            range ``(0, 1]``. ``0.9`` throttles at 90% of the documented cap to
            leave headroom; ``1.0`` (the default) uses the full cap.
    """

    scope: RateLimitScope
    type: RateLimitType
    interval_seconds: float
    limit: int
    kind: RateLimitKind
    enforce: EnforceMode
    safety_ratio: float = 1.0

    @property
    def interval(self) -> str:
        """The window length as a compact label (e.g. ``'1m'``, ``'10s'``).

        Returns ``''`` for ``CAP`` rules, which have no time window. The value
        matches the interval keys in Binance's rate-limit headers, so it is also
        the key used to reconcile this rule against an authoritative reading.
        """
        return interval_label(self.interval_seconds)
