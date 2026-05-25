from dataclasses import dataclass
from typing import (
    Union,
    Callable,
    Awaitable,
    Optional
)

from binance.common.constants import StringEnum


APIResponse = Union[dict, list]

Timeout = Union[int, float]

Payload = Union[dict, list]

DictPayload = dict

ListPayload = dict

EventCallback = Callable[..., Optional[Awaitable[None]]]

WrappedEventCallback = Callable[..., Awaitable[None]]


class StreamName(StringEnum):
    """Which physical WebSocket connection a :class:`StreamError` refers to.

    - ``DATA`` -- the market-data stream (``wss://stream.binance.com``).
    - ``USER`` -- the WS-API connection (``wss://ws-api...``) that also carries
      the user-data stream subscription.

    ``str(member)`` returns the raw wire value (e.g. ``str(StreamName.DATA) == 'data'``).
    Compare with the enum member (e.g. ``error.stream == StreamName.DATA``).
    """

    DATA = 'data'
    USER = 'user'


class StreamErrorPhase(StringEnum):
    """Which post-reconnect recovery phase failed for a :class:`StreamError`.

    - ``RESUBSCRIBE`` -- replaying subscriptions after a reconnect failed.
    - ``LOGON`` -- the WS-API ``session.logon`` failed after a reconnect.

    ``str(member)`` returns the raw wire value (e.g. ``str(StreamErrorPhase.LOGON) == 'logon'``).
    Compare with the enum member (e.g. ``error.phase == StreamErrorPhase.LOGON``).
    """

    RESUBSCRIBE = 'resubscribe'
    LOGON = 'logon'


@dataclass(frozen=True)
class StreamError:
    """Structured error object delivered to ``StreamErrorHandlerBase.receive``.

    Attributes:
        stream: :attr:`StreamName.DATA` for the market-data WebSocket or
            :attr:`StreamName.USER` for the WS-API / user-data stream.
            Compare with the member, e.g. ``err.stream == StreamName.DATA``.
        phase: :attr:`StreamErrorPhase.RESUBSCRIBE` when a post-reconnect
            subscription replay failed, or :attr:`StreamErrorPhase.LOGON` when
            the WS-API ``session.logon`` failed.  Compare with the member, e.g.
            ``err.phase == StreamErrorPhase.RESUBSCRIBE``.
        exception: the underlying exception that was raised.
        recovering: ``True`` when the SDK has already scheduled a
            :meth:`~binance.subscribe.stream.Stream.recycle` on the affected
            stream to initiate a fresh reconnect cycle.
    """
    stream: StreamName
    phase: StreamErrorPhase
    exception: Exception
    recovering: bool
