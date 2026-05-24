from dataclasses import dataclass
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
    stream: str
    phase: str
    exception: Exception
    recovering: bool
