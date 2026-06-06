import asyncio
from asyncio import Future
from typing import Any, Dict, Optional

from binance.core.common.utils import create_future


class RequestRegistry:
    """Owns id-correlated websocket request futures for a single stream."""

    def __init__(
        self,
        *,
        start_id: int = 0,
        pending: Optional[Dict[int, Future]] = None,
    ) -> None:
        self._next_id = start_id
        self._pending: Dict[int, Future] = dict(pending or {})

    @property
    def next_id(self) -> int:
        return self._next_id

    @next_id.setter
    def next_id(self, value: int) -> None:
        self._next_id = value

    @property
    def pending(self) -> Dict[int, Future]:
        return self._pending

    def replace_pending(self, pending: Dict[int, Future]) -> None:
        self._pending = pending

    def create(self) -> tuple[int, Future]:
        message_id = self._next_id
        self._next_id += 1
        future = create_future()
        self._pending[message_id] = future
        return message_id, future

    def get(self, message_id: int) -> Optional[Future]:
        return self._pending.get(message_id)

    def remove(self, message_id: int) -> Optional[Future]:
        return self._pending.pop(message_id, None)

    async def wait_for(self, message_id: int, timeout: float) -> Any:
        future = self._pending[message_id]
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except BaseException:
            self.remove(message_id)
            raise

    def resolve_result(self, message_id: int, result: Any) -> bool:
        future = self.remove(message_id)
        if future is None:
            return False
        if not future.done():
            future.set_result(result)
        return True

    def resolve_error(self, message_id: int, exception: Exception) -> bool:
        future = self.remove(message_id)
        if future is None:
            return False
        if not future.done():
            future.set_exception(exception)
        return True

    def reject_all(self, exception: Exception) -> None:
        if not self._pending:
            return
        pending = self._pending
        self._pending = {}
        for future in pending.values():
            if not future.done():
                future.set_exception(exception)
