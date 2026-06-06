import asyncio
from logging import Logger
from typing import Any, Awaitable, Callable, Set

from binance.core.common.utils import format_msg, repr_exception


DEFAULT_EVENT_QUEUE_LIMIT = 4096
DEFAULT_EVENT_WORKER_LIMIT = 32


class StreamEventDispatcher:
    """Bounded worker queue for non-response websocket events."""

    def __init__(
        self,
        callback: Callable[[Any], Awaitable[None]],
        logger: Logger,
        *,
        max_queue_size: int = DEFAULT_EVENT_QUEUE_LIMIT,
        max_workers: int = DEFAULT_EVENT_WORKER_LIMIT,
    ) -> None:
        self._callback = callback
        self._logger = logger
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue_size)
        self._max_workers = max(max_workers, 1)
        self._workers: Set[asyncio.Task[None]] = set()
        self._active = 0

    @property
    def backlog_size(self) -> int:
        return self._queue.qsize() + self._active

    @property
    def tasks(self) -> Set[asyncio.Task[None]]:
        return self._workers

    def submit(self, msg: Any) -> bool:
        self._start()
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            return False
        return True

    def _start(self) -> None:
        while len(self._workers) < self._max_workers:
            task = asyncio.get_running_loop().create_task(self._worker())
            self._workers.add(task)
            task.add_done_callback(self._workers.discard)

    async def _worker(self) -> None:
        while True:
            msg = await self._queue.get()
            self._active += 1
            try:
                await self._callback(msg)
            except Exception as e:
                self._logger.error(format_msg(
                    'stream event handler failed: %s',
                    repr_exception(e),
                ))
            finally:
                self._active -= 1
                self._queue.task_done()

    async def close(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

        workers = list(self._workers)
        self._workers.clear()
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
