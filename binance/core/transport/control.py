import asyncio
from functools import partial
from logging import Logger
from typing import Any, Callable, Coroutine, Dict, Hashable

from binance.core.common.utils import format_msg, repr_exception


class ControlTaskSupervisor:
    """Owns long-running control-plane tasks without blocking message receipt."""

    def __init__(self, logger: Logger) -> None:
        self._logger = logger
        self._tasks: Dict[Hashable, asyncio.Task[object]] = {}

    def run_once(
        self,
        key: Hashable,
        coro_factory: Callable[[], Coroutine[Any, Any, object]]
    ) -> bool:
        task = self._tasks.get(key)
        if task is not None and not task.done():
            return False

        task = asyncio.get_running_loop().create_task(coro_factory())
        self._tasks[key] = task
        task.add_done_callback(partial(self._done, key))
        return True

    def _done(self, key: Hashable, task: asyncio.Task[object]) -> None:
        if self._tasks.get(key) is task:
            del self._tasks[key]

        if task.cancelled():
            return

        exception = task.exception()
        if exception is not None:
            if isinstance(exception, Exception):
                exception_repr = repr_exception(exception)
            else:
                exception_repr = type(exception).__name__
            self._logger.error(format_msg(
                'control task %r failed: %s',
                key,
                exception_repr
            ))

    async def close(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()

        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
