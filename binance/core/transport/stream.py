import json
import asyncio
import inspect
from asyncio import Future, Task
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)
from logging import Logger

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

from binance.core.common.utils import (
    json_stringify,
    format_msg,
    repr_exception,
    wrap_event_callback,
    wrap_coroutine,
    create_future
)

from binance.core.common.exceptions import (
    StreamDisconnectedException,
    StreamResponseTimeoutException,
    StreamSubscribeException,
    StreamRateLimitException
)

from binance.core.common.constants import (
    DEFAULT_RETRY_POLICY,
    DEFAULT_STREAM_TIMEOUT,
    DEFAULT_STREAM_CLOSE_CODE,
    STREAM_KEY_ID,
    STREAM_KEY_RESULT,
    STREAM_KEY_ERROR,
    ERROR_KEY_CODE,
    ERROR_KEY_MESSAGE,
    ERROR_CODE_TOO_MANY_REQUESTS,
    HTTP_IP_BANNED,
    HTTP_TOO_MANY_REQUESTS
)

from binance.core.rate_limit import ConnectionLease, RateLimiter

from binance.core.common.types import (
    EventCallback,
    Timeout
)

from .request_registry import RequestRegistry
from .event_dispatcher import (
    DEFAULT_EVENT_QUEUE_LIMIT,
    DEFAULT_EVENT_WORKER_LIMIT,
    StreamEventDispatcher,
)


ON_MESSAGE = 'on_message'
ON_CONNECTED = 'on_connected'
ON_RECONNECTED = 'on_reconnected'


class Stream:
    """Class to handle Binance streams

    Args:
        uri (str): stream uri
        on_message (Callback): either sync or async callable to receive stream message
        on_connected (:obj:`Callable`, optional): invoked when the socket is connected
        retry_policy (RetryPolicy): see document
        timeout (float): timeout in seconds to receive the next websocket message
        rate_limiter (RateLimiter, optional): shared rate-limit core enforcing both the per-IP connection-attempt budget (300/5min) and the per-connection outgoing-message budget (5/s). Pass ONE shared instance when running multiple Streams against the same IP so the limits are enforced across them; defaults to a fresh private core.
        connection_id (str, optional): identifies this stream's per-connection message pool within the shared `rate_limiter`. Defaults to 'default'.
    """

    _socket: Optional[ClientConnection]
    _open_future: Optional[Future[Any]]
    _conn_task: Optional[Task[None]]
    _connected_task: Optional[Task[None]]
    _message_tasks: Set[Task[None]]
    _response_tasks: Set[Task[None]]
    _response_lock: Optional[asyncio.Lock]
    _event_dispatcher: StreamEventDispatcher
    _event_overflow_task: Optional[Task[None]]
    _event_dispatch_paused: bool
    _request_registry: RequestRegistry
    _retry_policy: RetryPolicy
    _rate_limiter: RateLimiter
    _connection_id: str
    _connection_lease: ConnectionLease

    def __init__(
        self,
        uri: str,
        on_message: EventCallback,
        logger: Logger,
        on_connected: Optional[EventCallback] = None,
        on_reconnected: Optional[EventCallback] = None,
        on_response: Optional[EventCallback] = None,
        # We redundant the default value here,
        #   because `binance.Stream` is also a public class
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        rate_limiter: Optional[RateLimiter] = None,
        connection_id: str = 'default',
        connection_lease: Optional[ConnectionLease] = None
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

        # Optional hook handed the FULL id-correlated response message (incl.
        # the WS-API `rateLimits` array) before the awaiting future resolves.
        # The market-data stream passes none -> behaviour is unchanged.
        self._on_response = on_response

        self._retry_policy = retry_policy
        self._timeout = timeout
        self._logger = logger

        self._socket = None
        self._conn_task = None
        self._connected_task = None
        self._message_tasks = set()
        self._response_tasks = set()
        self._response_lock = None
        self._event_dispatcher = self._new_event_dispatcher()
        self._event_overflow_task = None
        self._event_dispatch_paused = False

        # message_id
        self._request_registry = RequestRegistry()

        self._open_future = None
        self._closing = False
        self._connection_error = False

        self._uri = uri

        # Unified rate-limit core: enforces the per-IP connection-attempt
        #   budget (300/5min) and the per-connection outgoing-message budget
        #   (5/s). A fresh private core is used when none is shared in.
        self._rate_limiter = rate_limiter if rate_limiter is not None \
            else RateLimiter()
        self._connection_id = connection_id
        self._connection_lease = (
            connection_lease if connection_lease is not None
            else self._rate_limiter.open_connection(
                kind='raw_stream',
                route=uri,
                label=connection_id,
            )
        )
        self._rate_limiter.register_connection(self._connection_lease)

    def _connection_ref(self):
        return getattr(self, '_connection_lease', self._connection_id)

    def _requests(self) -> RequestRegistry:
        registry = getattr(self, '_request_registry', None)
        if registry is None:
            registry = RequestRegistry()
            self._request_registry = registry
        return registry

    def _new_event_dispatcher(self) -> StreamEventDispatcher:
        return StreamEventDispatcher(
            self._emit_message,
            self._logger,
            max_queue_size=getattr(
                self,
                '_event_queue_limit',
                DEFAULT_EVENT_QUEUE_LIMIT,
            ),
            max_workers=getattr(
                self,
                '_event_worker_limit',
                DEFAULT_EVENT_WORKER_LIMIT,
            ),
        )

    def _events(self) -> StreamEventDispatcher:
        dispatcher = getattr(self, '_event_dispatcher', None)
        queue_limit = getattr(
            self,
            '_event_queue_limit',
            DEFAULT_EVENT_QUEUE_LIMIT,
        )
        worker_limit = getattr(
            self,
            '_event_worker_limit',
            DEFAULT_EVENT_WORKER_LIMIT,
        )
        if (
            dispatcher is None
            or dispatcher._queue.maxsize != queue_limit
            or dispatcher._max_workers != max(worker_limit, 1)
        ):
            dispatcher = self._new_event_dispatcher()
            self._event_dispatcher = dispatcher
            self._message_tasks = dispatcher.tasks
        return dispatcher

    async def _emit_message(self, msg) -> None:
        await self._emit(ON_MESSAGE, msg)

    def _schedule_event_overflow_recycle(self) -> bool:
        task = getattr(self, '_event_overflow_task', None)
        if task is not None and not task.done():
            return False

        stale_dispatcher = self._events()
        self._event_dispatch_paused = True
        self._event_dispatcher = self._new_event_dispatcher()
        self._message_tasks = self._event_dispatcher.tasks

        async def recycle_after_overflow() -> None:
            try:
                await stale_dispatcher.close()
                await self.recycle()
            finally:
                self._event_overflow_task = None

        task = asyncio.get_running_loop().create_task(recycle_after_overflow())
        self._event_overflow_task = task
        task.add_done_callback(self._handle_task_exception)
        return True

    @property
    def _message_id(self) -> int:
        return self._requests().next_id

    @_message_id.setter
    def _message_id(self, value: int) -> None:
        self._requests().next_id = value

    @property
    def _message_futures(self) -> Dict[int, Future]:
        return self._requests().pending

    @_message_futures.setter
    def _message_futures(self, value: Dict[int, Future]) -> None:
        self._requests().replace_pending(value)

    def _set_socket(self, socket) -> None:
        self._event_dispatch_paused = False
        if self._open_future:
            if not self._open_future.done():
                self._open_future.set_result(socket)
            self._open_future = None

        self._socket = socket

    def connect(self):
        """Kick off the background connect/reconnect task and return self.

        Creates an asyncio Task that runs the internal ``_connect`` coroutine,
        which opens the WebSocket to ``_uri``, emits ``on_connected``, and
        drives the receive loop. If the connection drops, ``aioretry`` invokes
        ``_reconnect`` between attempts according to ``_retry_policy``.

        Must be called from within a running event loop (i.e. inside an
        ``async`` context or after ``asyncio.run`` / ``loop.run_until_complete``
        has started). The connection is established asynchronously; await
        ``send()`` or listen for ``on_connected`` to detect readiness.

        Returns:
            Stream: self, to allow chaining (e.g. ``Stream(...).connect()``).
        """
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

    def _reject_pending(self, exception: Exception) -> None:
        """Reject every in-flight request future because the connection was lost.

        Without this a caller awaiting :meth:`send` hangs forever when the socket
        drops between sending a request and receiving its id-correlated response
        (only :meth:`_handle_message` ever resolves these futures).
        """
        self._requests().reject_all(exception)

    def _reject_open_future(self, exception: Exception) -> None:
        open_future = getattr(self, '_open_future', None)
        self._open_future = None
        if open_future is not None and not open_future.done():
            open_future.add_done_callback(self._consume_future_exception)
            open_future.set_exception(exception)

    def _consume_future_exception(self, future: Future) -> None:
        try:
            future.exception()
        except asyncio.CancelledError:
            pass

    def _schedule_message_callback(self, msg) -> None:
        if getattr(self, '_event_dispatch_paused', False):
            return
        dispatcher = self._events()
        self._message_tasks = dispatcher.tasks
        if dispatcher.submit(msg):
            return
        if self._schedule_event_overflow_recycle():
            self._logger.error(format_msg(
                'stream event queue overflow for %s; recycling connection',
                self._uri,
            ))

    def _handle_message_task_exception(self, task: Task) -> None:
        message_tasks = getattr(self, '_message_tasks', None)
        if message_tasks is not None:
            message_tasks.discard(task)
        self._handle_task_exception(task)

    def _handle_response_task_exception(self, task: Task) -> None:
        response_tasks = getattr(self, '_response_tasks', None)
        if response_tasks is not None:
            response_tasks.discard(task)
        self._handle_task_exception(task)

    def _get_response_lock(self) -> asyncio.Lock:
        lock = getattr(self, '_response_lock', None)
        if lock is None:
            lock = asyncio.Lock()
            self._response_lock = lock
        return lock

    def _get_response_tasks(self) -> Set[Task[None]]:
        tasks = getattr(self, '_response_tasks', None)
        if tasks is None:
            tasks = set()
            self._response_tasks = tasks
        return tasks

    async def _run_response_hook(self, msg) -> None:
        on_response = getattr(self, '_on_response', None)
        if on_response is None:
            return
        try:
            if inspect.iscoroutinefunction(on_response):
                ret = on_response(msg)
            else:
                ret = await asyncio.to_thread(on_response, msg)
            await wrap_coroutine(ret)
        except Exception as e:
            self._logger.error(
                format_msg('on_response hook error: %s', repr_exception(e)))

    def _resolve_response_message(
        self,
        requests: RequestRegistry,
        message_id: int,
        msg,
    ) -> None:
        if STREAM_KEY_RESULT in msg:
            requests.resolve_result(message_id, msg[STREAM_KEY_RESULT])

        elif STREAM_KEY_ERROR in msg:
            error = msg[STREAM_KEY_ERROR]
            code = error[ERROR_KEY_CODE]
            message = error[ERROR_KEY_MESSAGE]
            status = msg.get('status')

            if (
                code == ERROR_CODE_TOO_MANY_REQUESTS
                or status in (HTTP_IP_BANNED, HTTP_TOO_MANY_REQUESTS)
            ):
                data = error.get('data') or {}
                requests.resolve_error(
                    message_id,
                    StreamRateLimitException(
                        code,
                        message,
                        data.get('retryAfter'),
                    )
                )
            else:
                requests.resolve_error(
                    message_id,
                    StreamSubscribeException(code, message),
                )
        else:
            requests.remove(message_id)

    async def _deliver_response_message(self, message_id: int, msg) -> None:
        async with self._get_response_lock():
            requests = self._requests()
            if requests.get(message_id) is None:
                return
            await self._run_response_hook(msg)
            self._resolve_response_message(requests, message_id, msg)

    def _schedule_response_message(self, message_id: int, msg) -> None:
        task = asyncio.get_running_loop().create_task(
            self._deliver_response_message(message_id, msg)
        )
        self._get_response_tasks().add(task)
        task.add_done_callback(self._handle_response_task_exception)

    async def _handle_message(self, msg) -> None:
        # > The id used in the JSON payloads is an unsigned INT used as
        # > an identifier to uniquely identify the messages going back and forth
        if STREAM_KEY_ID not in msg:
            self._schedule_message_callback(msg)
            return

        message_id = msg[STREAM_KEY_ID]
        requests = self._requests()
        future = requests.get(message_id)
        if future is None:
            self._logger.warning(format_msg(
                'ignoring late or unknown stream response id %s',
                message_id
            ))
            return

        # Hand the full message to the reconcile hook (WS-API `rateLimits`)
        # before resolving. When a hook exists, deliver the response from a
        # lifecycle-owned task so a slow hook cannot stop the socket reader loop.
        if getattr(self, '_on_response', None) is not None:
            self._schedule_response_message(message_id, msg)
        else:
            self._resolve_response_message(requests, message_id, msg)

    def _before_connect(self) -> None:
        open_future = getattr(self, '_open_future', None)
        if open_future is None or open_future.done():
            self._open_future = create_future()

    async def _receive(self) -> None:
        socket = self._socket
        assert socket is not None  # _receive is only called from _connect after _set_socket
        try:
            msg = await asyncio.wait_for(
                socket.recv(), timeout=self._timeout)
        except asyncio.TimeoutError:
            try:
                # Apply rate limiting before sending ping
                await self._rate_limiter.acquire_message(self._connection_ref())

                # Send ping and wait for pong with a shorter timeout
                pong_waiter = await socket.ping()
                await asyncio.wait_for(pong_waiter, timeout=10.0)
                self._logger.debug("WebSocket ping successful")
            except asyncio.TimeoutError:
                self._logger.warning("WebSocket ping timeout - connection may be stale")
                # Let the connection retry mechanism handle this. Pass no
                # close frames: websockets' ConnectionClosedError asserts that
                # the 3rd arg (rcvd_then_sent) is None unless BOTH rcvd and
                # sent are set, so a reason string here would raise instead.
                raise ConnectionClosedError(None, None)
            except Exception as e:
                self._logger.error(
                    format_msg(
                        'WebSocket ping failed: %s',
                        repr_exception(e)
                    )
                )

                # Other exceptions for socket.recv():
                # - ConnectionClosed
                # - ConnectionClosedOK
                # - ConnectionClosedError
                # which should be handled by self._connect()
                raise e
            return
        else:
            if self._connection_error:
                self._connection_error = False
                self._logger.info(
                    format_msg('Websocket connection recovered')
                )

            try:
                parsed = json.loads(msg)
            except ValueError as e:
                self._logger.error(
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
        try:
            await self._rate_limiter.acquire_connection()
        except asyncio.CancelledError:
            if self._closing:
                # Cancelled by `await self.close()` while the connection
                # limiter was throttling a (re)connect attempt
                return

            # Re-raise a genuine cancellation (not triggered by close()).
            raise  # pragma: no cover

        # ping_interval=None disables the websockets library's own client-side
        # keepalive pings (redundant: the SDK runs its own recv-timeout ping in
        # _receive, and the library still auto-replies pong to Binance's server
        # pings at the protocol layer regardless of this setting).
        async with connect(self._uri, ping_interval=None) as socket:
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
        self._connection_error = True

        self._logger.error(
            format_msg(
                'socket error %s, reconnecting %s...',
                repr_exception(info.exception),
                info.fails
            )
        )

        self._reject_pending(StreamDisconnectedException(self._uri))

        if self._connected_task is not None:
            self._connected_task.cancel()

            try:
                await self._connected_task
            except asyncio.CancelledError:
                # Expected when cancelling
                pass
            except Exception as e:
                self._logger.error(
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
        disconnected = StreamDisconnectedException(self._uri)
        self._reject_pending(disconnected)
        self._reject_open_future(disconnected)

        current_task = asyncio.current_task()
        tasks: List[Any] = []

        if self._socket:
            tasks.append(
                # make socket.close run in background
                self._socket.close(code)
            )

        if self._conn_task is not current_task:
            self._conn_task.cancel()
            tasks.append(self._conn_task)

        # Also cancel the connected task if it exists
        if self._connected_task and self._connected_task is not current_task:
            self._connected_task.cancel()

        for task in list(getattr(self, '_message_tasks', set())):
            if task is current_task:
                continue
            if not task.done():
                task.cancel()
            tasks.append(task)

        for task in list(getattr(self, '_response_tasks', set())):
            if task is current_task:
                continue
            if not task.done():
                task.cancel()
            tasks.append(task)

        event_dispatcher = getattr(self, '_event_dispatcher', None)
        if event_dispatcher is not None:
            tasks.append(event_dispatcher.close())

        event_overflow_task = getattr(self, '_event_overflow_task', None)
        if event_overflow_task is not None and event_overflow_task is not current_task:
            if not event_overflow_task.done():
                event_overflow_task.cancel()
            tasks.append(event_overflow_task)

        # Make sure:
        # - conn_task is cancelled
        # - socket is closed
        # - connected_task is cancelled
        if self._connected_task and self._connected_task is not current_task:
            tasks.append(self._connected_task)

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._logger.error(
                    format_msg('close tasks error: %s', e)
                )

        self._socket = None
        self._closing = False
        connection_lease = getattr(self, '_connection_lease', None)
        if connection_lease is not None:
            self._rate_limiter.unregister_connection(connection_lease)

    async def recycle(self) -> None:
        """Proactively drop the current socket so aioretry reconnects.

        Used on `serverShutdown` to reconnect before the 24h forced cut.
        Unlike `close()`, this does NOT set `_closing`, so the reconnect
        machinery (and the connection limiter) take over.
        """
        socket = self._socket
        if socket is not None:
            await socket.close(DEFAULT_STREAM_CLOSE_CODE)

    # Ref: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#websocket-limits

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
        await self._rate_limiter.acquire_message(self._connection_ref())

        socket = self._socket

        if not socket:
            if self._open_future:
                socket = await self._open_future
            else:
                raise StreamDisconnectedException(self._uri)

        requests = self._requests()
        message_id, future = requests.create()

        msg[STREAM_KEY_ID] = message_id

        try:
            await socket.send(json_stringify(msg))
            timeout = getattr(self, '_timeout', DEFAULT_STREAM_TIMEOUT)
            return await requests.wait_for(message_id, timeout=timeout)
        except asyncio.TimeoutError as e:
            timeout = getattr(self, '_timeout', DEFAULT_STREAM_TIMEOUT)
            raise StreamResponseTimeoutException(
                self._uri,
                message_id,
                timeout
            ) from e
        except asyncio.CancelledError:
            requests.remove(message_id)
            if not future.done():
                future.cancel()
            raise
        except Exception:
            requests.remove(message_id)
            raise

    def _handle_task_exception(self, task):
        """Handle exceptions from background tasks to prevent 'Future exception was never retrieved' warnings"""

        if task.cancelled():
            return

        # Retrieve the exception if the task failed
        exception = task.exception()
        if exception is not None:
            self._logger.error(
                format_msg(
                    'Background task failed with exception: %s',
                    repr_exception(exception)
                )
            )
