from logging import Logger
from typing import Any, Awaitable, Callable, Dict, Optional

from aioretry import RetryPolicy

from binance.core.common.types import Timeout
from binance.core.rate_limit import ConnectionLease, RateLimiter

from .stream import Stream


def data_connection_id(path: str) -> str:
    """Return the display label for a data stream connection path."""
    if path == '/stream':
        return 'data'
    return f'data:{path}'


class DataStreamRegistry:
    """Owns data WebSocket streams and their connection leases."""

    def __init__(
        self,
        *,
        stream_host: str,
        retry_policy: RetryPolicy,
        timeout: Timeout,
        logger: Logger,
        rate_limiter: RateLimiter,
        on_message: Callable[[Stream, Any], Awaitable[None]],
        on_connected: Callable[[str], Callable[[], Awaitable[None]]],
        stream_factory: Callable[..., Stream] = Stream,
    ) -> None:
        self._stream_host = stream_host
        self._retry_policy = retry_policy
        self._timeout = timeout
        self._logger = logger
        self._rate_limiter = rate_limiter
        self._on_message = on_message
        self._on_connected = on_connected
        self._stream_factory = stream_factory
        self.streams: Dict[str, Stream] = {}
        self.leases: Dict[str, ConnectionLease] = {}

    def set_stream_factory(self, stream_factory: Callable[..., Stream]) -> None:
        self._stream_factory = stream_factory

    def get_lease(self, path: str) -> ConnectionLease:
        lease = self.leases.get(path)
        if lease is None:
            lease = self._rate_limiter.open_connection(
                kind='data',
                route=path,
                label=data_connection_id(path),
            )
            self.leases[path] = lease
        return lease

    def drop_unopened_lease(self, path: str) -> None:
        if path in self.streams:
            return
        lease = self.leases.pop(path, None)
        if lease is not None:
            self._rate_limiter.unregister_connection(lease)

    def get_stream(self, path: str) -> Stream:
        stream = self.streams.get(path)
        if stream is not None:
            return stream

        stream_holder: Dict[str, Stream] = {}
        connection_lease = self.get_lease(path)
        replay_on_connected = self._on_connected(path)
        initial_connection = True

        async def on_message_bound(msg):
            await self._on_message(stream_holder['stream'], msg)

        async def on_reconnected_bound():
            nonlocal initial_connection
            if initial_connection:
                initial_connection = False
                return
            await replay_on_connected()

        stream = self._stream_factory(
            self._stream_host + path,
            on_message=on_message_bound,
            on_connected=on_reconnected_bound,
            retry_policy=self._retry_policy,
            timeout=self._timeout,
            logger=self._logger,
            rate_limiter=self._rate_limiter,
            connection_id=data_connection_id(path),
            connection_lease=connection_lease,
        ).connect()
        stream_holder['stream'] = stream
        self.streams[path] = stream
        return stream

    async def close_all(self, code: int) -> None:
        first_error: Optional[Exception] = None
        for path, stream in list(self.streams.items()):
            try:
                await stream.close(code)
            except Exception as e:
                if first_error is None:
                    first_error = e
            finally:
                self.streams.pop(path, None)
                lease = self.leases.pop(path, None)
                if lease is not None:
                    self._rate_limiter.unregister_connection(lease)
        if first_error is not None:
            raise first_error
