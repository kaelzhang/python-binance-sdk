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
    """Ensure ``subject`` is a list, wrapping it if necessary.

    Args:
        subject: Any value.  If it is already a ``list`` it is returned
            unchanged; otherwise it is wrapped in a single-element list.

    Returns:
        The original list, or ``[subject]`` when ``subject`` is not a list.
    """
    return subject if isinstance(subject, list) else [subject]


def format_msg(string, *args) -> str:
    """Format a human-readable SDK message with a standard prefix.

    Prepends ``MSG_PREFIX`` (defined in ``constants``) to the result of
    ``string % args``, producing a consistently prefixed log or error string.

    Args:
        string: A %-style format string.
        *args: Positional arguments substituted into ``string``.

    Returns:
        The fully formatted message string with the SDK prefix.
    """
    return MSG_PREFIX + string % args


def json_stringify(obj) -> str:
    """Serialize ``obj`` to a compact JSON string with no extra whitespace.

    Uses ``separators=(',', ':')`` so the output has no spaces after commas
    or colons, which minimises payload size when sending data over WebSocket.

    Args:
        obj: Any JSON-serialisable Python object.

    Returns:
        A compact JSON string representation of ``obj``.
    """
    return json.dumps(obj, separators=(',', ':'))


def normalize_symbol(symbol: str, upper: bool = False) -> str:
    """Normalize a trading symbol to a canonical form used by Binance streams.

    Removes underscore separators (e.g. ``'BTC_USDT'`` → ``'BTCUSDT'``) and
    converts to the requested case.  Binance WebSocket stream names use
    lower-case symbols; REST responses use upper-case.

    Args:
        symbol: Raw symbol string, optionally containing underscores.
        upper: When ``True`` the result is upper-cased; when ``False``
            (default) it is lower-cased.

    Returns:
        The normalized symbol string without underscores, in the requested case.
    """
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
    """Wrap a user-supplied event callback so that exceptions are caught and warned.

    Converts ``fn`` (which may be a plain function or a coroutine function)
    into an async callable that silently survives exceptions by issuing a
    ``RuntimeWarning`` via ``warnings.warn``.  This prevents user callback
    bugs from crashing the SDK's internal async machinery.

    Args:
        fn: The callback to wrap.  May be ``None`` if the event is optional.
            If it is a coroutine function it will be awaited; otherwise it is
            called synchronously and the return value is discarded.
        event_name: Human-readable name of the event, used in the warning
            message and the ``ValueError`` raised when ``required`` is
            ``True`` and ``fn`` is ``None``.
        required: When ``True`` and ``fn`` is ``None``, raises ``ValueError``
            immediately instead of returning ``None``.

    Returns:
        An async wrapper around ``fn``, or ``None`` if ``fn`` is ``None`` and
        ``required`` is ``False``.

    Raises:
        ValueError: If ``fn`` is ``None`` and ``required`` is ``True``.
    """
    if fn is None:
        if required:
            raise ValueError(
                format_msg('event callback `%s` is required', event_name)
            )

        return

    async def callback(*args):
        try:
            await wrap_coroutine(fn(*args))
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
