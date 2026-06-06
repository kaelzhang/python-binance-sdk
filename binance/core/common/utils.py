import json
import inspect
import warnings
import asyncio
from typing import (
    Any,
    Optional
)

from .constants import MSG_PREFIX
from .types import (
    EventCallback,
    WrappedEventCallback
)


def make_list(subject: Any) -> list:
    return subject if isinstance(subject, list) else [subject]


def format_msg(string, *args) -> str:
    return MSG_PREFIX + string % args


def json_stringify(obj) -> str:
    """Compact JSON serialization with no whitespace (for WebSocket payloads)."""
    return json.dumps(obj, separators=(',', ':'))


def normalize_symbol(symbol: str, upper: bool = False) -> str:
    """Strip underscores and normalise case (lower by default, upper when ``upper=True``)."""
    symbol = symbol.replace('_', '')
    return symbol.upper() if upper else symbol.lower()


async def wrap_coroutine(ret):
    if inspect.iscoroutine(ret):
        return await ret
    else:
        return ret


def repr_exception(e: Exception) -> str:
    """Better stringify an exception
    """

    s = str(e)
    class_name = type(e).__name__

    return class_name if not s else f'{class_name}: {s}'


def wrap_event_callback(
    fn: Optional[EventCallback],
    event_name: str,
    required: bool
) -> Optional[WrappedEventCallback]:
    """Wrap a user callback into an async callable that catches exceptions as RuntimeWarnings."""
    if fn is None:
        if required:
            raise ValueError(
                format_msg('event callback `%s` is required', event_name)
            )

        return

    async def callback(*args):
        try:
            if inspect.iscoroutinefunction(fn):
                ret = fn(*args)
            else:
                ret = await asyncio.to_thread(fn, *args)
            await wrap_coroutine(ret)
        except Exception as e:
            # This is a bug which is blamed to the user and
            # should be fixed.
            # So use warnings.
            warnings.warn(
                format_msg("""`%s` raises:
    %s
And you should fix this""", event_name, repr_exception(e)),
                RuntimeWarning
            )

    return callback


def create_future() -> asyncio.Future:
    """
    Do not use `asyncio.Future()` which
    could not bind the Future with the current running event loop
    """
    return asyncio.get_running_loop().create_future()
