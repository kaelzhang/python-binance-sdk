import traceback
from datetime import datetime
import sys
from typing import TextIO

from binance.core.common.types import StreamError
from binance.core.handlers.base import Handler


class StreamErrorHandlerBase(Handler):
    """Base handler for stream-control errors (resubscribe / logon failures).

    When a post-reconnect subscription replay or WS-API ``session.logon``
    fails, the SDK logs the error at ERROR level, schedules a
    :meth:`~binance.core.transport.stream.Stream.recycle` on the affected stream
    to trigger a fresh reconnect, and then calls ``receive`` on every
    registered instance of this class.

    Subclass this and override ``receive`` (sync or async) to implement
    custom alerting or recovery logic.

    Example::

        from binance import StreamErrorHandlerBase

        class MyStreamErrors(StreamErrorHandlerBase):
            async def receive(self, error):
                # error.stream      -> 'data' | 'user'
                # error.phase       -> 'resubscribe' | 'logon'
                # error.exception   -> the underlying exception
                # error.recovering  -> bool
                await alert_ops_team(error)

        client.handler(MyStreamErrors())
    """

    def receive(self, error: StreamError):
        """Called when a stream-control error occurs.

        Args:
            error (StreamError): structured error describing what failed.
        """


class HandlerExceptionHandlerBase(Handler):
    """Base handler for exceptions raised by other stream handlers.

    When a handler's ``receive`` method raises an exception, the SDK routes
    that exception to every registered instance of this class instead of
    propagating it.  Subclass this and override ``receive`` to implement
    custom error-reporting or recovery logic.

    The default implementation (provided here) prints the current timestamp
    together with the full traceback to *stderr* and returns the exception
    object unchanged.
    """

    def receive(
        _,
        e: Exception,
        file: TextIO = sys.stderr
    ):
        """
        Print current datetime and error call stacks

        Args:
            e (Exception): the error
            file (:obj:`TextIO`, optional): output target of the printer, defaults to `sys.stderr`

        Returns:
            Exception: the error itself
        """

        print(f'[{datetime.now()}] ', end='', file=file)
        traceback.print_exc(file=file)

        return e
