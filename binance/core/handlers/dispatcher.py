import asyncio
import inspect
from typing import Set

from binance.core.handlers.base import Handler


DEFAULT_MAX_CONCURRENT_HANDLERS = 32


class HandlerDispatcher:
    """Executes stream handlers without letting sync callbacks block the loop."""

    def __init__(
        self,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT_HANDLERS,
    ) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def dispatch(self, payload, handlers: Set[Handler]) -> None:
        if not handlers:
            return

        await asyncio.gather(*(
            self._dispatch_one(payload, handler)
            for handler in tuple(handlers)
        ))

    async def _dispatch_one(self, payload, handler: Handler) -> None:
        async with self._semaphore:
            ret = await asyncio.to_thread(handler.receiveDispatch, payload)
            if inspect.isawaitable(ret):
                await ret
