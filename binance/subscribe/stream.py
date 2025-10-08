import json
import logging
import asyncio
import time
from asyncio import Future
from typing import (
    Optional,
    Dict,
    Any
)

from websockets import (
    connect,
    ClientConnection
)
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
    ConnectionClosedError
)

from aioretry import (
    RetryPolicy,
    RetryInfo,
    retry
)

from binance.common.utils import (
    json_stringify,
    format_msg,
    repr_exception,
    wrap_event_callback,
    create_future
)

from binance.common.exceptions import (
    StreamDisconnectedException,
    StreamSubscribeException
)

from binance.common.constants import (
    DEFAULT_RETRY_POLICY,
    DEFAULT_STREAM_TIMEOUT,
    DEFAULT_STREAM_CLOSE_CODE,
    STREAM_KEY_ID,
    STREAM_KEY_RESULT,
    STREAM_KEY_ERROR,
    ERROR_KEY_CODE,
    ERROR_KEY_MESSAGE
)

from binance.common.types import (
    EventCallback,
    Timeout
)


logger = logging.getLogger(__name__)

ON_MESSAGE = 'on_message'
ON_CONNECTED = 'on_connected'
ON_RECONNECTED = 'on_reconnected'


class RateLimiter:
    """Rate limiter to enforce 5 messages per second limit for Binance WebSocket streams"""

    def __init__(self, max_messages: int = 5, time_window: float = 1.0):
        self.max_messages = max_messages
        self.time_window = time_window
        self.messages = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire permission to send a message, waiting if necessary to respect rate limits"""
        async with self._lock:
            now = time.time()

            # Remove messages older than the time window
            self.messages = [
                msg_time
                for msg_time in self.messages
                if now - msg_time < self.time_window
            ]

            # If we're at the limit, wait until the oldest message expires
            if len(self.messages) >= self.max_messages:
                oldest_message = min(self.messages)
                wait_time = self.time_window - (now - oldest_message)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

            # Record this message
            self.messages.append(now)


class Stream:
    """Class to handle Binance streams

    Args:
        uri (str): stream uri
        on_message (Callback): either sync or async callable to receive stream message
        on_connected (:obj:`Callable`, optional): invoked when the socket is connected
        retry_policy (RetryPolicy): see document
        timeout (float): timeout in seconds to receive the next websocket message
    """

    _socket: Optional[ClientConnection]
    _message_futures: Dict[int, Future]
    _retry_policy: RetryPolicy
    _rate_limiter: RateLimiter

    def __init__(
        self,
        uri: str,
        on_message: EventCallback,
        on_connected: Optional[EventCallback] = None,
        on_reconnected: Optional[EventCallback] = None,
        # We redundant the default value here,
        #   because `binance.Stream` is also a public class
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        timeout: Timeout = DEFAULT_STREAM_TIMEOUT
    ) -> None:
        # Will be used by `self._emit`
        self._on_message = wrap_event_callback(on_message, ON_MESSAGE, True)

        # Will be used by `self._emit`
        self._on_connected = wrap_event_callback(
            on_connected,
            ON_CONNECTED,
            False
        )

        self._on_reconnected = wrap_event_callback(
            on_reconnected,
            ON_RECONNECTED,
            False
        )

        self._retry_policy = retry_policy
        self._timeout = timeout

        self._socket = None
        self._conn_task = None
        self._connected_task = None

        # message_id
        self._message_id = 0
        self._message_futures = {}

        self._open_future = None
        self._closing = False

        self._uri = uri

        # Initialize rate limiter for 5 messages per second
        self._rate_limiter = RateLimiter(max_messages=5, time_window=1.0)

    def _set_socket(self, socket) -> None:
        if self._open_future:
            self._open_future.set_result(socket)
            self._open_future = None

        self._socket = socket

    def connect(self):
        self._before_connect()

        self._conn_task = asyncio.create_task(self._connect())
        # Add exception handler to prevent "Future exception was never retrieved" warnings
        self._conn_task.add_done_callback(self._handle_task_exception)
        return self

    async def _emit(
        self,
        event_name: str,
        *args
    ) -> None:
        event_callback = getattr(self, f'_{event_name}', None)

        if event_callback is None:
            return

        return await event_callback(*args)

    async def _handle_message(self, msg) -> None:
        # > The id used in the JSON payloads is an unsigned INT used as
        # > an identifier to uniquely identify the messages going back and forth
        if (
            STREAM_KEY_ID not in msg
        ) or (
            msg[STREAM_KEY_ID] not in self._message_futures
        ):
            await self._emit(ON_MESSAGE, msg)
            return

        message_id = msg[STREAM_KEY_ID]
        future = self._message_futures[message_id]

        if STREAM_KEY_RESULT in msg:
            future.set_result(msg[STREAM_KEY_RESULT])

        elif STREAM_KEY_ERROR in msg:
            error = msg[STREAM_KEY_ERROR]

            future.set_exception(
                StreamSubscribeException(
                    error[ERROR_KEY_CODE],
                    error[ERROR_KEY_MESSAGE]
                )
            )

        del self._message_futures[message_id]

    def _before_connect(self) -> None:
        self._open_future = create_future()

    async def _receive(self) -> None:
        try:
            msg = await asyncio.wait_for(
                self._socket.recv(), timeout=self._timeout)
        except asyncio.TimeoutError:
            try:
                # Apply rate limiting before sending ping
                await self._rate_limiter.acquire()

                # Send ping and wait for pong with a shorter timeout
                pong_waiter = await self._socket.ping()
                await asyncio.wait_for(pong_waiter, timeout=10.0)
                logger.debug("WebSocket ping successful")
            except asyncio.TimeoutError:
                logger.warning("WebSocket ping timeout - connection may be stale")
                # Let the connection retry mechanism handle this
                raise ConnectionClosedError(None, None, "ping timeout")
            except Exception as e:
                logger.error(
                    format_msg(
                        'WebSocket ping failed: %s',
                        repr_exception(e)
                    )
                )
                raise e
            return

        # Other exceptions for socket.recv():
        # - ConnectionClosed
        # - ConnectionClosedOK
        # - ConnectionClosedError
        # which should be handled by self._connect()

        else:
            try:
                parsed = json.loads(msg)
            except ValueError as e:
                logger.error(
                    format_msg(
                        'stream message "%s" is an invalid JSON: reason: %s',
                        msg,
                        e
                    )
                )

                return
            else:
                await self._handle_message(parsed)

    @retry(
        retry_policy='_retry_policy',
        before_retry='_reconnect'
    )
    async def _connect(self) -> None:
        async with connect(self._uri) as socket:
            self._set_socket(socket)

            self._connected_task = asyncio.create_task(
                self._emit(ON_CONNECTED)
            )
            # Add exception handler to prevent "Future exception was never retrieved" warnings
            self._connected_task.add_done_callback(self._handle_task_exception)

            try:
                # Do not receive messages if the stream is closing
                while not self._closing:
                    await self._receive()

            except (
                ConnectionClosed,
                # Binance stream never close unless errored
                ConnectionClosedOK,
                ConnectionClosedError,
                # task cancel
                asyncio.CancelledError
            ) as e:
                if self._closing:
                    # The socket is closed by `await self.close()`
                    return

                # Raise, so aioretry will reconnecting
                raise e

    async def _reconnect(self, info: RetryInfo) -> None:
        logger.error(
            format_msg(
                'socket error %s, reconnecting %s...',
                repr_exception(info.exception),
                info.fails
            )
        )

        if self._connected_task is not None:
            self._connected_task.cancel()

            try:
                await self._connected_task
            except asyncio.CancelledError:
                # Expected when cancelling
                pass
            except Exception as e:
                logger.error(
                    format_msg(
                        'Error cleaning up connected task: %s',
                        repr_exception(e)
                    )
                )

            self._connected_task = None

        self._before_connect()

    async def close(
        self,
        code: int = DEFAULT_STREAM_CLOSE_CODE
    ) -> None:
        """Close the current socket connection

        Args:
            code (:obj:`int`, optional): socket close code, defaults to 4999
        """

        if not self._conn_task:
            raise StreamDisconnectedException(self._uri)

        # A lot of incomming messages might prevent
        #   the socket from gracefully shutting down,
        #    which leads `websockets` to fail connection
        #    and result in a 1006 close code (ConnectionClosedError).
        # In that situation, we can not properly figure out whether the socket
        #   is closed by socket.close() or network connection error.
        # So just set up a flag to do the trick
        self._closing = True

        tasks = [self._conn_task]

        if self._socket:
            tasks.append(
                # make socket.close run in background
                self._socket.close(code)
            )

        self._conn_task.cancel()

        # Also cancel the connected task if it exists
        if self._connected_task:
            self._connected_task.cancel()

        # Make sure:
        # - conn_task is cancelled
        # - socket is closed
        # - connected_task is cancelled
        if self._connected_task:
            tasks.append(self._connected_task)

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                logger.error(
                    format_msg('close tasks error: %s', e)
                )

        self._socket = None
        self._closing = False

    # Ref: https://academy.binance.com/en/articles/what-are-binance-websocket-limits

    # Connection Limits
    # There is a limit of 300 connection attempts per five-minute period per IP address for both Websocket tools.

    # For WebSocket streams, users are limited to five incoming messages per second, including Ping frames, Pong frames, and JSON-controlled messages such as subscribe/unsubscribe commands. Connections exceeding this limit are disconnected, and repeated violations may result in an IP ban.

    # A single connection can handle a maximum of 1,024 streams, making it suitable for large-scale data monitoring setups in high-frequency trading or analytics platforms.

    async def send(
        self,
        msg: dict
    ) -> Any:
        """Send a request to Binance stream
        and handle the asynchronous socket response

        Request::

            {
                "method": "SUBSCRIBE",
                "params": [
                    "btcusdt@aggTrade",
                    "btcusdt@depth"
                ],
                "id": 1
            }

        Response::

            {
                "result": null,
                "id": 1
            }

        Then the result of `self.send()` is `None` (null)
        """

        # Apply rate limiting before sending
        await self._rate_limiter.acquire()

        socket = self._socket

        if not socket:
            if self._open_future:
                socket = await self._open_future
            else:
                raise StreamDisconnectedException(self._uri)

        future = create_future()

        message_id = self._message_id
        self._message_id += 1

        msg[STREAM_KEY_ID] = message_id
        self._message_futures[message_id] = future

        await socket.send(json_stringify(msg))
        return await future

    def _handle_task_exception(self, task):
        """Handle exceptions from background tasks to prevent 'Future exception was never retrieved' warnings"""
        try:
            # Retrieve the exception if the task failed
            exception = task.exception()
            if exception is not None:
                logger.error(
                    format_msg(
                        'Background task failed with exception: %s',
                        repr_exception(exception)
                    )
                )
        except asyncio.CancelledError:
            # Task was cancelled, which is expected during cleanup
            pass
        except Exception as e:
            logger.error(
                format_msg(
                    'Error handling task exception: %s',
                    repr_exception(e)
                )
            )
