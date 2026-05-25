from dataclasses import dataclass
from enum import Enum
from typing import (
    Union,
    Callable,
    Awaitable,
    Optional
)


APIResponse = Union[dict, list]

Timeout = Union[int, float]

Payload = Union[dict, list]

DictPayload = dict

ListPayload = dict

EventCallback = Callable[..., Optional[Awaitable[None]]]

WrappedEventCallback = Callable[..., Awaitable[None]]


class StreamName(str, Enum):
    """Which physical WebSocket connection a :class:`StreamError` refers to.

    - ``DATA`` -- the market-data stream (``wss://stream.binance.com``).
    - ``USER`` -- the WS-API connection (``wss://ws-api...``) that also carries
      the user-data stream subscription.

    A ``str`` enum, so ``StreamName.DATA == 'data'`` is ``True``.
    """

    DATA = 'data'
    USER = 'user'


class StreamErrorPhase(str, Enum):
    """Which post-reconnect recovery phase failed for a :class:`StreamError`.

    - ``RESUBSCRIBE`` -- replaying subscriptions after a reconnect failed.
    - ``LOGON`` -- the WS-API ``session.logon`` failed after a reconnect.

    A ``str`` enum, so ``StreamErrorPhase.LOGON == 'logon'`` is ``True``.
    """

    RESUBSCRIBE = 'resubscribe'
    LOGON = 'logon'


@dataclass(frozen=True)
class StreamError:
    """Structured error object delivered to ``StreamErrorHandlerBase.receive``.

    Attributes:
        stream: ``'data'`` for the market-data WebSocket or ``'user'`` for the
            WS-API / user-data stream.
        phase: ``'resubscribe'`` when a post-reconnect subscription replay
            failed, or ``'logon'`` when the WS-API ``session.logon`` failed.
        exception: the underlying exception that was raised.
        recovering: ``True`` when the SDK has already scheduled a
            :meth:`~binance.subscribe.stream.Stream.recycle` on the affected
            stream to initiate a fresh reconnect cycle.
    """
    stream: StreamName
    phase: StreamErrorPhase
    exception: Exception
    recovering: bool
